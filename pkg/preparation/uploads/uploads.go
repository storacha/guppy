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
type AddNodeToUploadShardsFn func(ctx context.Context, uploadID id.UploadID, nodeCID cid.Cid) (bool, error)
type CloseUploadShardsFn func(ctx context.Context, uploadID id.UploadID) error

type API struct {
	Repo                     Repo
	RunNewScan               RunNewScanFn
	RunDagScansForUpload     RunDagScansForUploadFn
	RestartDagScansForUpload RestartDagScansForUploadFn
	// AddNodeToUploadShards adds a node to the upload's shards, creating a new
	// shard if necessary. It returns true if a new shard was created, false if the node
	// was added to an existing shard.
	AddNodeToUploadShards AddNodeToUploadShardsFn
	CloseUploadShards     CloseUploadShardsFn
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
	return executor{
		upload: upload,
		api:    a,
	}.execute(ctx)
}

type executor struct {
	upload *model.Upload
	api    API
}

// signalWorkAvailable signals on a channel that work is available. The channel
// should be buffered (generally with a size of 1). If the channel is full, it
// will not block, as no further signal is needed: two messages saying that work
// is available are the same as one.
func signalWorkAvailable(work chan<- struct{}) {
	select {
	case work <- struct{}{}:
	default:
		// channel is full, no need to signal
	}
}

func (e executor) execute(ctx context.Context) error {
	log.Debugf("Executing upload %s in state %s", e.upload.ID(), e.upload.State())

	eg, ctx := errgroup.WithContext(ctx)
	dagWork := make(chan struct{}, 1)
	shardWork := make(chan struct{}, 1)

	// This one is just marking it as started, so it can be synchronous.
	if e.upload.NeedsStart() {
		if err := e.upload.Start(); err != nil {
			return fmt.Errorf("starting scan: %w", err)
		}
		if err := e.api.Repo.UpdateUpload(ctx, e.upload); err != nil {
			return fmt.Errorf("updating upload: %w", err)
		}
	}

	// start the workers for all states not yet handled
	if e.upload.NeedsScan() {
		eg.Go(func() error {
			return e.runScanWorker(ctx, dagWork)
		})
	}
	if e.upload.NeedsDagScan() {
		eg.Go(func() error {
			return e.runDAGScanWorker(ctx, dagWork, shardWork)
		})
	}

	log.Debugf("Waiting for workers to finish for upload %s", e.upload.ID())
	err := eg.Wait()

	if errors.Is(err, context.Canceled) {
		log.Debugf("Upload %s was canceled", e.upload.ID())
		if err := e.upload.Cancel(); err != nil {
			return fmt.Errorf("cancelling upload: %w", err)
		}
		if err := e.api.Repo.UpdateUpload(context.WithoutCancel(ctx), e.upload); err != nil {
			return fmt.Errorf("updating upload after failure: %w", err)
		}
	} else if err != nil {
		log.Errorf("Error executing upload %s: %v", e.upload.ID(), err)
		if failErr := e.upload.Fail(err.Error()); failErr != nil {
			return fmt.Errorf("failing upload: %w", failErr)
		}
		if err := e.api.Repo.UpdateUpload(context.WithoutCancel(ctx), e.upload); err != nil {
			return fmt.Errorf("updating upload after failure: %w", err)
		}

	}

	return err
}

func (e *executor) runScanWorker(ctx context.Context, dagWork chan<- struct{}) error {
	log.Debugf("Running new scan for upload %s in state %s", e.upload.ID(), e.upload.State())

	// Unlike later stages, this one doesn't need to watch a work channel with
	// [Worker], because it never has to wait for work.

	fsEntryID, err := e.api.RunNewScan(ctx, e.upload.ID(), func(id id.FSEntryID, isDirectory bool) error {
		_, err := e.api.Repo.CreateDAGScan(ctx, id, isDirectory, e.upload.ID())
		if err != nil {
			return fmt.Errorf("creating DAG scan: %w", err)
		}
		signalWorkAvailable(dagWork)
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
	close(dagWork) // close the work channel to signal completion

	if err := e.upload.ScanComplete(fsEntryID); err != nil {
		return fmt.Errorf("completing scan: %w", err)
	}
	if err := e.api.Repo.UpdateUpload(ctx, e.upload); err != nil {
		return fmt.Errorf("updating upload: %w", err)
	}

	return nil
}

// runShardsWorker runs the worker that scans files and directories into blocks,
// and buckets them into shards.
func (e *executor) runDAGScanWorker(ctx context.Context, dagWork <-chan struct{}, shardWork chan<- struct{}) error {
	err := e.api.RestartDagScansForUpload(ctx, e.upload.ID())
	if err != nil {
		return fmt.Errorf("restarting scans for upload %s: %w", e.upload.ID(), err)
	}

	return Worker(
		ctx,
		dagWork,

		// doWork
		func() error {
			err := e.api.RunDagScansForUpload(ctx, e.upload.ID(), func(node dagmodel.Node, data []byte) error {
				log.Debugf("Adding node %s to upload shards for upload %s", node.CID(), e.upload.ID())
				shardCreated, err := e.api.AddNodeToUploadShards(ctx, e.upload.ID(), node.CID())
				if err != nil {
					return fmt.Errorf("adding node to upload shard: %w", err)
				}

				if shardCreated {
					signalWorkAvailable(shardWork)
				}

				return nil
			})

			if err != nil {
				return fmt.Errorf("running dag scans for upload %s: %w", e.upload.ID(), err)
			}

			return nil
		},

		// finalize
		func() error {
			rootCid, err := e.api.Repo.CIDForFSEntry(ctx, e.upload.RootFSEntryID())
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

			// We're out of nodes, so we can close any open shards for this upload.
			if err := e.api.CloseUploadShards(ctx, e.upload.ID()); err != nil {
				return fmt.Errorf("closing upload shards for upload %s: %w", e.upload.ID(), err)
			}

			close(shardWork) // close the work channel to signal completion

			if err := e.upload.DAGGenerationComplete(rootCid); err != nil {
				return fmt.Errorf("completing DAG generation: %w", err)
			}
			if err := e.api.Repo.UpdateUpload(ctx, e.upload); err != nil {
				return fmt.Errorf("updating upload: %w", err)
			}

			return nil
		},
	)
}
