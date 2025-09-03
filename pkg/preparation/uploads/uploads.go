package uploads

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
	"golang.org/x/sync/errgroup"
)

var log = logging.Logger("preparation/uploads")

type ExecuteScanFunc func(ctx context.Context, uploadID id.UploadID, nodeCB func(node scanmodel.FSEntry) error) error
type ExecuteDagScansForUploadFunc func(ctx context.Context, uploadID id.UploadID, nodeCB func(node dagmodel.Node, data []byte) error) error
type AddNodeToUploadShardsFunc func(ctx context.Context, uploadID id.UploadID, nodeCID cid.Cid) (bool, error)
type CloseUploadShardsFunc func(ctx context.Context, uploadID id.UploadID) (bool, error)
type SpaceBlobAddShardsForUploadFunc func(ctx context.Context, uploadID id.UploadID) error
type AddIndexesForUploadFunc func(ctx context.Context, uploadID id.UploadID) error
type AddStorachaUploadForUploadFunc func(ctx context.Context, uploadID id.UploadID) error

type API struct {
	Repo                        Repo
	ExecuteScan                 ExecuteScanFunc
	ExecuteDagScansForUpload    ExecuteDagScansForUploadFunc
	SpaceBlobAddShardsForUpload SpaceBlobAddShardsForUploadFunc
	AddIndexesForUpload         AddIndexesForUploadFunc
	AddStorachaUploadForUpload  AddStorachaUploadForUploadFunc

	// AddNodeToUploadShards adds a node to the upload's shards, creating a new
	// shard if necessary. It returns true if an existing open shard was closed,
	// false otherwise.
	AddNodeToUploadShards AddNodeToUploadShardsFunc

	// CloseUploadShards closes any remaining open shard for the upload. It
	// returns true if an existing open shard was in fact closed, false if there
	// was no open shard to close.
	CloseUploadShards CloseUploadShardsFunc
}

// CreateUploads creates uploads for a given space and its associated sources.
func (a API) CreateUploads(ctx context.Context, spaceDID did.DID) ([]*model.Upload, error) {
	log.Debugf("Creating uploads for space %s", spaceDID)
	sources, err := a.Repo.ListSpaceSources(ctx, spaceDID)
	if err != nil {
		return nil, err
	}

	log.Debugf("Found %d sources for space %s", len(sources), spaceDID)

	uploads, err := a.Repo.CreateUploads(ctx, spaceDID, sources)
	if err != nil {
		return nil, err
	}
	log.Debugf("Created %d uploads for space %s", len(uploads), spaceDID)
	return uploads, nil
}

// GetUploadByID retrieves an upload by its unique ID.
func (a API) GetUploadByID(ctx context.Context, uploadID id.UploadID) (*model.Upload, error) {
	return a.Repo.GetUploadByID(ctx, uploadID)
}

// ExecuteUpload executes the upload process for a given upload, handling its state transitions and processing steps.
func (a API) ExecuteUpload(ctx context.Context, upload *model.Upload) (cid.Cid, error) {
	return executor{
		upload: upload,
		api:    a,
	}.execute(ctx)
}

type executor struct {
	upload *model.Upload
	api    API
}

// signal signals on a channel that work is available. The channel
// should be buffered (generally with a size of 1). If the channel is full, it
// will not block, as no further signal is needed: two messages saying that work
// is available are the same as one.
func signal(work chan<- struct{}) {
	select {
	case work <- struct{}{}:
	default:
		// channel is full, no need to signal
	}
}

func (e executor) execute(ctx context.Context) (cid.Cid, error) {
	log.Debugf("Executing upload %s in state %s", e.upload.ID(), e.upload.State())

	eg, ctx := errgroup.WithContext(ctx)

	var (
		scansAvailable        = make(chan struct{}, 1)
		dagScansAvailable     = make(chan struct{}, 1)
		closedShardsAvailable = make(chan struct{}, 1)
	)

	// Start the workers
	eg.Go(func() error { return e.runScanWorker(ctx, scansAvailable, dagScansAvailable) })
	eg.Go(func() error { return e.runDAGScanWorker(ctx, dagScansAvailable, closedShardsAvailable) })
	eg.Go(func() error { return e.runStorachaWorker(ctx, closedShardsAvailable) })

	// Kick them all off. There may be work available in the DB from a previous
	// attempt.
	signal(scansAvailable)
	signal(dagScansAvailable)
	signal(closedShardsAvailable)
	close(scansAvailable)

	log.Debugf("Waiting for workers to finish for upload %s", e.upload.ID())
	workersErr := eg.Wait()

	// Reload the upload to get the latest state from the DB.
	upload, err := e.api.Repo.GetUploadByID(context.WithoutCancel(ctx), e.upload.ID())
	if err != nil {
		return cid.Undef, fmt.Errorf("reloading upload: %w", err)
	}

	return upload.RootCID(), workersErr
}

func (e *executor) runScanWorker(ctx context.Context, scansAvailable <-chan struct{}, dagScansAvailable chan<- struct{}) error {
	return Worker(
		ctx,
		scansAvailable,

		// doWork
		func() error {
			err := e.api.ExecuteScan(ctx, e.upload.ID(), func(entry scanmodel.FSEntry) error {
				_, isDirectory := entry.(*scanmodel.Directory)
				_, err := e.api.Repo.CreateDAGScan(ctx, entry.ID(), isDirectory, e.upload.ID(), e.upload.SpaceDID())
				if err != nil {
					return fmt.Errorf("creating DAG scan: %w", err)
				}
				signal(dagScansAvailable)
				return nil
			})

			if err != nil {
				return fmt.Errorf("running scans: %w", err)
			}

			return nil
		},

		// finalize
		func() error {
			close(dagScansAvailable)
			return nil
		},
	)
}

// runDAGScanWorker runs the worker that scans files and directories into blocks,
// and buckets them into shards.
func (e *executor) runDAGScanWorker(ctx context.Context, dagScansAvailable <-chan struct{}, closedShardsAvailable chan<- struct{}) error {
	return Worker(
		ctx,
		dagScansAvailable,

		// doWork
		func() error {
			err := e.api.ExecuteDagScansForUpload(ctx, e.upload.ID(), func(node dagmodel.Node, data []byte) error {
				log.Debugf("Adding node %s to upload shards for upload %s", node.CID(), e.upload.ID())
				shardClosed, err := e.api.AddNodeToUploadShards(ctx, e.upload.ID(), node.CID())
				if err != nil {
					return fmt.Errorf("adding node to upload shard: %w", err)
				}

				if shardClosed {
					signal(closedShardsAvailable)
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
			// We're out of nodes, so we can close any open shards for this upload.
			shardClosed, err := e.api.CloseUploadShards(ctx, e.upload.ID())
			if err != nil {
				return fmt.Errorf("closing upload shards for upload %s: %w", e.upload.ID(), err)
			}

			if shardClosed {
				signal(closedShardsAvailable)
			}

			// Reload the upload to get the latest state from the DB.
			upload, err := e.api.Repo.GetUploadByID(ctx, e.upload.ID())
			if err != nil {
				return fmt.Errorf("reloading upload: %w", err)
			}
			rootCid, err := e.api.Repo.CIDForFSEntry(ctx, upload.RootFSEntryID())
			if err != nil {
				return fmt.Errorf("retrieving CID for root fs entry: %w", err)
			}
			if err := upload.SetRootCID(rootCid); err != nil {
				return fmt.Errorf("completing DAG generation: %w", err)
			}
			if err := e.api.Repo.UpdateUpload(ctx, upload); err != nil {
				return fmt.Errorf("updating upload: %w", err)
			}

			close(closedShardsAvailable)

			return nil
		},
	)
}

// runStorachaWorker runs the worker that adds shards and indexes to Storacha.
func (e *executor) runStorachaWorker(ctx context.Context, blobWork <-chan struct{}) error {
	return Worker(
		ctx,
		blobWork,

		// doWork
		func() error {
			err := e.api.SpaceBlobAddShardsForUpload(ctx, e.upload.ID())
			if err != nil {
				return fmt.Errorf("`space/blob/add`ing shards for upload %s: %w", e.upload.ID(), err)
			}

			return nil
		},

		// finalize
		func() error {
			err := e.api.AddIndexesForUpload(ctx, e.upload.ID())
			if err != nil {
				return fmt.Errorf("`space/blob/add`ing index for upload %s: %w", e.upload.ID(), err)
			}

			err = e.api.AddStorachaUploadForUpload(ctx, e.upload.ID())
			if err != nil {
				return fmt.Errorf("`upload/add`ing upload %s: %w", e.upload.ID(), err)
			}

			return nil
		},
	)
}
