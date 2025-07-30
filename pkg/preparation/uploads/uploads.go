package uploads

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var log = logging.Logger("preparation/uploads")

type RunNewScanFn func(ctx context.Context, uploadID id.UploadID, fsEntryCb func(id id.FSEntryID, isDirectory bool) error) (id.FSEntryID, error)
type RunDagScansForUploadFn func(ctx context.Context, uploadID id.UploadID, nodeCB func(node dagmodel.Node, data []byte) error) error
type RestartDagScansForUploadFn func(ctx context.Context, uploadID id.UploadID) error
type AddNodeToUploadShardsFn func(ctx context.Context, uploadID id.UploadID, nodeCID cid.Cid) error

type API struct {
	Repo                     Repo
	RunNewScan               RunNewScanFn
	RunDagScansForUpload     RunDagScansForUploadFn
	RestartDagScansForUpload RestartDagScansForUploadFn
	AddNodeToUploadShards    AddNodeToUploadShardsFn
}

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
func (a API) ExecuteUpload(ctx context.Context, upload *model.Upload) error {
	e := setupExecutor(ctx, upload, a)
	log.Debugf("Executing upload %s in state %s", upload.ID(), upload.State())
	if e.upload.State() == model.UploadStateStarted || e.upload.State() == model.UploadStateScanned || e.upload.State() == model.UploadStateDagged {
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
		log.Debugf("Processing upload %s in state %s", upload.ID(), upload.State())
		switch upload.State() {
		case model.UploadStatePending:
			log.Debugf("Starting upload %s in state %s", upload.ID(), upload.State())
			err := upload.Start()
			if err != nil {
				return fmt.Errorf("starting upload: %w", err)
			}
			e.start()
		case model.UploadStateStarted:
			log.Debugf("Running new scan for upload %s in state %s", upload.ID(), upload.State())
			fsEntryID, err := a.RunNewScan(ctx, upload.ID(), func(id id.FSEntryID, isDirectory bool) error {
				_, err := a.Repo.CreateDAGScan(ctx, id, isDirectory, upload.ID())
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
				log.Debugf("Scan completed successfully, root fs entry ID: %s", fsEntryID)
				log.Debugf("Closing dag work channel %v", e.dagWork)
				close(e.dagWork) // close the work channel to signal completion
				if err := upload.ScanComplete(fsEntryID); err != nil {
					return fmt.Errorf("completing scan: %w", err)
				}
			} else {
				if err := e.upload.Fail("scan failed"); err != nil {
					return fmt.Errorf("failing upload: %w", err)
				}
			}
		case model.UploadStateScanned:
			log.Debugf("Waiting for DAG scan worker to finish for upload %s in state %s", upload.ID(), upload.State())
			// wait for the DAG scan worker to finish
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-e.dagResult:
				log.Debugf("DAG scan worker finished for upload %s with error: %v", upload.ID(), err)
				if err != nil {
					return fmt.Errorf("DAG scan worker error: %w", err)
				}
			}
			log.Debugf("Looking up CID for RootFSEntryID %s for upload %s", upload.RootFSEntryID(), upload.ID())
			rootCid, err := a.Repo.CIDForFSEntry(ctx, upload.RootFSEntryID())
			if err != nil {
				var incompleteErr IncompleteDagScanError
				if errors.As(err, &incompleteErr) {
					log.Debugf("DAG scan for root fs entry %s is not completed, failing upload %s: %s", incompleteErr.DagScan.FsEntryID(), upload.ID(), incompleteErr.DagScan.Error())
					if err := e.upload.Fail("dag scan failed"); err != nil {
						return fmt.Errorf("failing upload: %w", err)
					}
				}

				return fmt.Errorf("retrieving CID for root fs entry: %w", err)
			}

			close(e.shardWork) // close the work channel to signal completion
			if err := upload.DAGGenerationComplete(rootCid); err != nil {
				return fmt.Errorf("completing DAG generation: %w", err)
			}

		case model.UploadStateDagged:
			log.Debugf("Waiting for shards worker to finish for upload %s in state %s", upload.ID(), upload.State())
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
		case model.UploadStateSharded:
			log.Debugf("Waiting for upload worker to finish for upload %s in state %s", upload.ID(), upload.State())
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

type executor struct {
	originalCtx  context.Context
	ctx          context.Context
	cancel       context.CancelFunc
	dagWork      chan struct{}
	shardWork    chan struct{}
	wg           sync.WaitGroup
	dagResult    chan error
	shardResult  chan error
	uploadResult chan error
	upload       *model.Upload
	u            API
}

func setupExecutor(originalCtx context.Context, upload *model.Upload, u API) *executor {
	ctx, cancel := context.WithCancel(originalCtx)
	dagWork := make(chan struct{}, 1)
	shardWork := make(chan struct{}, 1)
	dagResult := make(chan error, 1)
	shardResult := make(chan error, 1)
	uploadResult := make(chan error, 1)
	executor := &executor{
		originalCtx:  originalCtx,
		ctx:          ctx,
		cancel:       cancel,
		dagWork:      dagWork,
		shardWork:    shardWork,
		dagResult:    dagResult,
		shardResult:  shardResult,
		uploadResult: uploadResult,
		upload:       upload,
		u:            u,
	}
	return executor
}

func (e *executor) start() {
	log.Debugf("Starting upload execution for upload %s in state %s", e.upload.ID(), e.upload.State())
	// start the workers for all states not yet handled
	if e.upload.State() == model.UploadStateStarted {
		e.runDAGScanWorker()
	}
	if e.upload.State() == model.UploadStateStarted || e.upload.State() == model.UploadStateScanned {
		e.runShardsWorker()
	}
	if e.upload.State() == model.UploadStateStarted || e.upload.State() == model.UploadStateScanned || e.upload.State() == model.UploadStateDagged {
		e.runUploadWorker()
	}
}

func (e *executor) runDAGScanWorker() {
	log.Debugf("Starting DAG scan worker for upload %s", e.upload.ID())
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		err := e.u.RestartDagScansForUpload(e.ctx, e.upload.ID())
		if err != nil {
			e.dagResult <- fmt.Errorf("restarting scans for upload %s: %w", e.upload.ID(), err)
			return
		}

		e.dagResult <- Worker(e.ctx, e.dagWork, func() error {
			err := e.u.RunDagScansForUpload(e.ctx, e.upload.ID(), func(node dagmodel.Node, data []byte) error {
				log.Debugf("Processing node %s for upload %s", node.CID(), e.upload.ID())
				if err := e.u.AddNodeToUploadShards(e.ctx, e.upload.ID(), node.CID()); err != nil {
					return fmt.Errorf("adding node to upload shard: %w", err)
				}
				// TK: Only signal if there's a new *closed* shard, ideally.
				log.Debugf("Adding node %s to upload shards for upload %s", node.CID(), e.upload.ID())
				e.shardWork <- struct{}{} // signal that there is work to be done for shards
				return nil
			})

			if err != nil {
				return fmt.Errorf("running dag scans for upload %s: %w", e.upload.ID(), err)
			}

			return nil
		}, nil)
	}()
}

func (e *executor) runShardsWorker() {
	log.Debugf("Starting shards worker for upload %s", e.upload.ID())
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		log.Debugf("Running upload shard worker for upload %s", e.upload.ID())
		e.shardResult <- Worker(e.ctx, e.shardWork, func() error {
			log.Debugf("Would process shard here for %s", e.upload.ID())
			return nil
		}, nil)
	}()
}

func (e *executor) runUploadWorker() {
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
