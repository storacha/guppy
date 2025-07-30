package uploads

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
	"golang.org/x/sync/errgroup"
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

	// This one is just marking it as started, so it can be synchronous.
	if e.upload.NeedsStart() {
		if err := e.upload.Start(); err != nil {
			return fmt.Errorf("starting scan: %w", err)
		}
		if err := e.u.Repo.UpdateUpload(e.ctx, e.upload); err != nil {
			return fmt.Errorf("updating upload: %w", err)
		}
	}

	// start the workers for all states not yet handled
	if e.upload.NeedsScan() {
		e.eg.Go(e.runScanWorker)
	}
	if e.upload.NeedsDagScan() {
		e.eg.Go(e.runDAGScanWorker)
	}

	log.Debugf("Waiting for workers to finish for upload %s", upload.ID())
	err := e.eg.Wait()

	if errors.Is(err, context.Canceled) {
		log.Debugf("Upload %s was canceled", upload.ID())
		if err := upload.Cancel(); err != nil {
			return fmt.Errorf("cancelling upload: %w", err)
		}
		if err := a.Repo.UpdateUpload(context.WithoutCancel(ctx), upload); err != nil {
			return fmt.Errorf("updating upload after failure: %w", err)
		}
	} else if err != nil {
		log.Errorf("Error executing upload %s: %v", upload.ID(), err)
		if failErr := upload.Fail(err.Error()); failErr != nil {
			return fmt.Errorf("failing upload: %w", failErr)
		}
		if err := a.Repo.UpdateUpload(context.WithoutCancel(ctx), upload); err != nil {
			return fmt.Errorf("updating upload after failure: %w", err)
		}

	}

	return err
}

type executor struct {
	originalCtx context.Context
	ctx         context.Context
	cancel      context.CancelFunc
	dagWork     chan struct{}
	shardWork   chan struct{}
	eg          *errgroup.Group
	upload      *model.Upload
	u           API
}

func setupExecutor(originalCtx context.Context, upload *model.Upload, u API) *executor {
	ctx, cancel := context.WithCancel(originalCtx)
	eg, ctx := errgroup.WithContext(ctx)
	dagWork := make(chan struct{}, 1)
	shardWork := make(chan struct{}, 1)
	executor := &executor{
		originalCtx: originalCtx,
		ctx:         ctx,
		cancel:      cancel,
		dagWork:     dagWork,
		shardWork:   shardWork,
		eg:          eg,
		upload:      upload,
		u:           u,
	}
	return executor
}

// signalWorkAvailable signals on a channel that work is available. The channel
// should be buffered (generally with a size of 1). If the channel is full, it
// will not block, as no further signal is needed: two messages saying that work
// is available are the same as one.
func signalWorkAvailable(work chan struct{}) {
	select {
	case work <- struct{}{}:
	default:
		// channel is full, no need to signal
	}
}

func (e *executor) runScanWorker() error {
	log.Debugf("Running new scan for upload %s in state %s", e.upload.ID(), e.upload.State())

	// Unlike later stages, this one doesn't need to watch a work channel with
	// [Worker], because it never has to wait for work.

	fsEntryID, err := e.u.RunNewScan(e.ctx, e.upload.ID(), func(id id.FSEntryID, isDirectory bool) error {
		_, err := e.u.Repo.CreateDAGScan(e.ctx, id, isDirectory, e.upload.ID())
		if err != nil {
			return fmt.Errorf("creating DAG scan: %w", err)
		}
		signalWorkAvailable(e.dagWork)
		return nil
	})

	if err != nil {
		return fmt.Errorf("running new scan: %w", err)
	}

	// check if scan completed successfully
	if fsEntryID == id.Nil {
		return errors.New("scan did not complete successfully")
	}

	log.Debugf("Scan completed successfully, root fs entry ID: %s", fsEntryID)
	close(e.dagWork) // close the work channel to signal completion

	if err := e.upload.ScanComplete(fsEntryID); err != nil {
		return fmt.Errorf("completing scan: %w", err)
	}
	if err := e.u.Repo.UpdateUpload(e.ctx, e.upload); err != nil {
		return fmt.Errorf("updating upload: %w", err)
	}

	return nil
}

// runShardsWorker runs the worker that scans files and directories into blocks,
// and buckets them into shards.
func (e *executor) runDAGScanWorker() error {
	err := e.u.RestartDagScansForUpload(e.ctx, e.upload.ID())
	if err != nil {
		return fmt.Errorf("restarting scans for upload %s: %w", e.upload.ID(), err)
	}

	return Worker(
		e.ctx,
		e.dagWork,

		// doWork
		func() error {
			err := e.u.RunDagScansForUpload(e.ctx, e.upload.ID(), func(node dagmodel.Node, data []byte) error {
				log.Debugf("Processing node %s for upload %s", node.CID(), e.upload.ID())
				if err := e.u.AddNodeToUploadShards(e.ctx, e.upload.ID(), node.CID()); err != nil {
					return fmt.Errorf("adding node to upload shard: %w", err)
				}
				// TK: Only signal if there's a new *closed* shard, ideally.
				log.Debugf("Adding node %s to upload shards for upload %s", node.CID(), e.upload.ID())
				signalWorkAvailable(e.shardWork)
				return nil
			})

			if err != nil {
				return fmt.Errorf("running dag scans for upload %s: %w", e.upload.ID(), err)
			}

			return nil
		},

		// finalize
		func() error {
			rootCid, err := e.u.Repo.CIDForFSEntry(e.ctx, e.upload.RootFSEntryID())
			if err != nil {
				var incompleteErr IncompleteDagScanError
				if errors.As(err, &incompleteErr) {
					log.Debugf("DAG scan for root fs entry %s is not completed, failing upload %s: %s", incompleteErr.DagScan.FsEntryID(), e.upload.ID(), incompleteErr.DagScan.Error())
					if err := e.upload.Fail("dag scan failed"); err != nil {
						return fmt.Errorf("failing upload: %w", err)
					}
				}

				return fmt.Errorf("retrieving CID for root fs entry: %w", err)
			}

			close(e.shardWork) // close the work channel to signal completion

			if err := e.upload.DAGGenerationComplete(rootCid); err != nil {
				return fmt.Errorf("completing DAG generation: %w", err)
			}
			if err := e.u.Repo.UpdateUpload(e.ctx, e.upload); err != nil {
				return fmt.Errorf("updating upload: %w", err)
			}

			return nil
		},
	)
}
