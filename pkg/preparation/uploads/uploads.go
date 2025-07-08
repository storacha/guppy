package uploads

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var log = logging.Logger("uploads")

type API struct {
	Repo                Repo
	RunNewScan          RunNewScanFn
	UploadDAGScanWorker func(ctx context.Context, work <-chan struct{}, uploadID id.UploadID, nodeCB func(node dagmodel.Node, data []byte) error) error
}

// RunNewScanFn is a function that initiates a new scan for a given upload ID, returning the root file system entry ID.
type RunNewScanFn func(ctx context.Context, uploadID id.UploadID, fsEntryCb func(id id.FSEntryID, isDirectory bool) error) (id.FSEntryID, error)

// CreateUploads creates uploads for a given configuration and its associated sources.
func (a API) CreateUploads(ctx context.Context, configurationID id.ConfigurationID) ([]*model.Upload, error) {
	log.Debugf("Creating uploads for configuration %s", configurationID)
	sources, err := a.Repo.ListConfigurationSources(ctx, configurationID)
	if err != nil {
		return nil, err
	}

	log.Debugf("Found %d sources for configuration %s", len(sources), configurationID)

	uploads, err := a.Repo.CreateUploads(ctx, configurationID, sources)
	if err != nil {
		return nil, err
	}
	log.Debugf("Created %d uploads for configuration %s", len(uploads), configurationID)
	return uploads, nil
}

// GetSourceIDForUploadID retrieves the source ID associated with a given upload ID.
func (a API) GetSourceIDForUploadID(ctx context.Context, uploadID id.UploadID) (id.SourceID, error) {
	return a.Repo.GetSourceIDForUploadID(ctx, uploadID)
}

// GetUploadByID retrieves an upload by its unique ID.
func (a API) GetUploadByID(ctx context.Context, uploadID id.UploadID) (*model.Upload, error) {
	return a.Repo.GetUploadByID(ctx, uploadID)
}

// ExecuteUpload executes the upload process for a given upload, handling its state transitions and processing steps.
func (a API) ExecuteUpload(ctx context.Context, upload *model.Upload, opts ...ExecutionOption) error {
	e := setupExecutor(ctx, upload, a, opts...)
	log.Debugf("Executing upload %s in state %s", upload.ID(), upload.State())
	if e.upload.State() == model.UploadStateScanning || e.upload.State() == model.UploadStateGeneratingDAG || e.upload.State() == model.UploadStateSharding {
		e.start()
	}
	defer e.shutdown()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// continue processing
		}
		switch upload.State() {
		case model.UploadStatePending:
			// TK: Persist state?
			err := upload.Start()
			if err != nil {
				return fmt.Errorf("starting upload: %w", err)
			}
			e.start()
		case model.UploadStateScanning:
			fsEntryID, err := a.RunNewScan(ctx, upload.ID(), func(id id.FSEntryID, isDirectory bool) error {
				err := a.Repo.CreateDAGScan(ctx, id, isDirectory, upload.ID())
				if err != nil {
					return fmt.Errorf("creating DAG scan: %w", err)
				}
				// indicate new work is available for the DAG scan worker
				select {
				case e.dagWork <- struct{}{}:
				default:
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("running new scan: %w", err)
			}
			// check if scan completed successfully
			if fsEntryID != id.Nil {
				close(e.dagWork) // close the work channel to signal completion
				if err := upload.ScanComplete(fsEntryID); err != nil {
					return fmt.Errorf("completing scan: %w", err)
				}
			} else {
				if err := e.restart(); err != nil {
					return fmt.Errorf("restarting upload: %w", err)
				}
			}
		case model.UploadStateGeneratingDAG:
			// wait for the DAG scan worker to finish
			select {
			case err := <-e.dagResult:
				if err != nil {
					return fmt.Errorf("DAG scan worker error: %w", err)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			rootCid, err := a.Repo.CIDForFSEntry(ctx, upload.RootFSEntryID())
			if err != nil {
				return fmt.Errorf("retrieving CID for root fs entry: %w", err)
			}
			if rootCid != cid.Undef {
				if err := upload.DAGGenerationComplete(rootCid); err != nil {
					return fmt.Errorf("completing DAG generation: %w", err)
				}
			} else {
				if err := e.restart(); err != nil {
					return fmt.Errorf("restarting upload: %w", err)
				}
			}
		case model.UploadStateSharding:
			// wait for the shards worker to finish
			select {
			case err := <-e.shardResult:
				if err != nil {
					return fmt.Errorf("shards worker error: %w", err)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			// just advance as this is currently a placeholder
			if err := upload.ShardingComplete(); err != nil {
				return fmt.Errorf("completing sharding: %w", err)
			}
		case model.UploadStateUploading:
			// wait for the upload worker to finish
			select {
			case err := <-e.uploadResult:
				if err != nil {
					return fmt.Errorf("upload worker error: %w", err)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			// just advance as this is currently a placeholder
			if err := upload.Complete(); err != nil {
				return fmt.Errorf("completing upload: %w", err)
			}
		case model.UploadStateFailed:
			return fmt.Errorf("upload failed: %w", upload.Error())
		case model.UploadStateCanceled:
			return context.Canceled
		case model.UploadStateCompleted:
			// upload is complete, no further action needed
			return nil
		default:
			return fmt.Errorf("unknown upload state: %s", upload.State())
		}
		// persist the state change
		if err := a.Repo.UpdateUpload(ctx, upload); err != nil {
			return fmt.Errorf("updating upload: %w", err)
		}
	}
}

const defaultMaxRestarts = 10

type executor struct {
	remainingRestarts uint64
	originalCtx       context.Context
	ctx               context.Context
	cancel            context.CancelFunc
	dagWork           chan struct{}
	wg                sync.WaitGroup
	dagResult         chan error
	shardResult       chan error
	uploadResult      chan error
	upload            *model.Upload
	u                 API
}

func setupExecutor(originalCtx context.Context, upload *model.Upload, u API, opts ...ExecutionOption) *executor {
	ctx, cancel := context.WithCancel(originalCtx)
	dagWork := make(chan struct{}, 1)
	dagResult := make(chan error, 1)
	shardResult := make(chan error, 1)
	uploadResult := make(chan error, 1)
	executor := &executor{
		remainingRestarts: defaultMaxRestarts,
		originalCtx:       originalCtx,
		ctx:               ctx,
		cancel:            cancel,
		dagWork:           dagWork,
		dagResult:         dagResult,
		shardResult:       shardResult,
		uploadResult:      uploadResult,
		upload:            upload,
		u:                 u,
	}
	for _, opt := range opts {
		opt(executor)
	}
	return executor
}

func (e *executor) start() {
	log.Debugf("Starting upload execution for upload %s in state %s", e.upload.ID(), e.upload.State())
	// start the workers for all states not yet handled
	if e.upload.State() == model.UploadStateScanning {
		e.runDAGScanWorker()
	}
	if e.upload.State() == model.UploadStateScanning || e.upload.State() == model.UploadStateGeneratingDAG {
		e.runShardsWorker()
	}
	if e.upload.State() == model.UploadStateScanning || e.upload.State() == model.UploadStateGeneratingDAG || e.upload.State() == model.UploadStateSharding {
		e.uploadWorker()
	}
}

func (e *executor) runDAGScanWorker() {
	log.Debugf("Starting DAG scan worker for upload %s", e.upload.ID())
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.dagResult <- e.u.UploadDAGScanWorker(e.ctx, e.dagWork, e.upload.ID(), func(node dagmodel.Node, data []byte) error {
			log.Debugf("DAG scan worker found node %s", node.CID())
			return nil
		})
	}()
}

func (e *executor) runShardsWorker() {
	log.Debugf("Starting shards worker for upload %s", e.upload.ID())
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		// put the worker for shards processing here, when it exists
		e.shardResult <- nil // Placeholder for shard processing result
	}()
}

func (e *executor) uploadWorker() {
	log.Debugf("Starting upload worker for upload %s", e.upload.ID())
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		// put the worker for upload processing here, when it exists
		e.uploadResult <- nil // Placeholder for upload processing result
	}()
}

func (e *executor) shutdown() {
	e.cancel()
	e.wg.Wait()
}

func (e *executor) reset() {
	e.ctx, e.cancel = context.WithCancel(e.originalCtx)
	e.dagWork = make(chan struct{}, 1)
}

func (e *executor) restart() error {
	e.shutdown()
	e.reset()
	if e.remainingRestarts == 0 {
		if err := e.upload.Fail("maximum number of restarts reached"); err != nil {
			return fmt.Errorf("failing upload: %w", err)
		}
		return nil
	}
	e.remainingRestarts--
	if err := e.upload.Restart(); err != nil {
		return fmt.Errorf("restarting upload: %w", err)
	}
	return nil
}

// ExecutionOption is a function that modifies the executor's configuration.
type ExecutionOption func(*executor)

// WithMaxRestarts sets the maximum number of restarts for an upload execution.
func WithMaxRestarts(max uint64) ExecutionOption {
	return func(e *executor) {
		e.remainingRestarts = max
	}
}
