package uploads

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/bettererrgroup"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	log    = logging.Logger("preparation/uploads")
	tracer = otel.Tracer("preparation/uploads")
)

type ExecuteScanFunc func(ctx context.Context, uploadID id.UploadID, nodeCB func(node scanmodel.FSEntry) error) error
type ExecuteDagScansForUploadFunc func(ctx context.Context, uploadID id.UploadID, nodeCB func(node dagmodel.Node, data []byte) error) error
type AddNodeToUploadShardsFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, nodeCID cid.Cid, data []byte) (bool, error)
type CloseUploadShardsFunc func(ctx context.Context, uploadID id.UploadID) (bool, error)
type AddShardsForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error
type AddIndexesForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error
type AddStorachaUploadForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error
type RemoveBadFSEntryFunc func(ctx context.Context, spaceDID did.DID, fsEntryID id.FSEntryID) error
type RemoveBadNodesFunc func(ctx context.Context, spaceDID did.DID, nodeCIDs []cid.Cid) error
type RemoveShardFunc func(ctx context.Context, shardID id.ShardID) error

type API struct {
	Repo                       Repo
	ExecuteScan                ExecuteScanFunc
	ExecuteDagScansForUpload   ExecuteDagScansForUploadFunc
	AddShardsForUpload         AddShardsForUploadFunc
	AddIndexesForUpload        AddIndexesForUploadFunc
	AddStorachaUploadForUpload AddStorachaUploadForUploadFunc
	RemoveBadFSEntry           RemoveBadFSEntryFunc
	RemoveBadNodes             RemoveBadNodesFunc
	RemoveShard                RemoveShardFunc

	// AddNodeToUploadShards adds a node to the upload's shards, creating a new
	// shard if necessary. It returns true if an existing open shard was closed,
	// false otherwise.
	AddNodeToUploadShards AddNodeToUploadShardsFunc

	// CloseUploadShards closes any remaining open shard for the upload. It
	// returns true if an existing open shard was in fact closed, false if there
	// was no open shard to close.
	CloseUploadShards CloseUploadShardsFunc
}

// FindOrCreateUploads creates uploads for a given space and its associated sources.
func (a API) FindOrCreateUploads(ctx context.Context, spaceDID did.DID) ([]*model.Upload, error) {
	log.Debugf("Creating uploads for space %s", spaceDID)
	sources, err := a.Repo.ListSpaceSources(ctx, spaceDID)
	if err != nil {
		return nil, err
	}

	log.Debugf("Found %d sources for space %s", len(sources), spaceDID)

	uploads, err := a.Repo.FindOrCreateUploads(ctx, spaceDID, sources)
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

// ExecuteUpload executes the upload process for a given upload, handling its state transitions and processing steps.
func (a API) ExecuteUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) (cid.Cid, error) {
	ctx, span := tracer.Start(ctx, "execute-upload", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer span.End()

	var (
		scansAvailable        = make(chan struct{}, 1)
		dagScansAvailable     = make(chan struct{}, 1)
		closedShardsAvailable = make(chan struct{}, 1)
	)

	// Start the workers
	eg, wCtx := bettererrgroup.WithContext(ctx)
	eg.Go(func() error {
		err := runScanWorker(wCtx, a, uploadID, spaceDID, scansAvailable, dagScansAvailable)
		if err != nil {
			return fmt.Errorf("scan worker: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := runDAGScanWorker(wCtx, a, uploadID, spaceDID, dagScansAvailable, closedShardsAvailable)
		if err != nil {
			return fmt.Errorf("DAG scan worker: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := runStorachaWorker(wCtx, a, uploadID, spaceDID, closedShardsAvailable)
		if err != nil {
			return fmt.Errorf("storacha worker: %w", err)
		}
		return nil
	})

	// Kick them all off. There may be work available in the DB from a previous
	// attempt.
	signal(scansAvailable)
	signal(dagScansAvailable)
	signal(closedShardsAvailable)
	close(scansAvailable)

	log.Debugf("Waiting for workers to finish for upload %s", uploadID)
	workersErr := eg.Wait()

	// Reload the upload to get the latest state from the DB.
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return cid.Undef, fmt.Errorf("reloading upload: %w", err)
	}

	return upload.RootCID(), workersErr
}

func runScanWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, scansAvailable <-chan struct{}, dagScansAvailable chan<- struct{}) error {
	ctx, span := tracer.Start(ctx, "scan-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("Scan worker for upload %s exiting", uploadID)
	defer span.End()

	return Worker(
		ctx,
		scansAvailable,

		// doWork
		func() error {
			err := api.ExecuteScan(ctx, uploadID, func(entry scanmodel.FSEntry) error {
				_, isDirectory := entry.(*scanmodel.Directory)
				_, err := api.Repo.CreateDAGScan(ctx, entry.ID(), isDirectory, uploadID, spaceDID)
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
func runDAGScanWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, dagScansAvailable <-chan struct{}, closedShardsAvailable chan<- struct{}) error {
	ctx, span := tracer.Start(ctx, "dag-scan-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("DAG scan worker for upload %s exiting", uploadID)
	defer span.End()

	return Worker(
		ctx,
		dagScansAvailable,

		// doWork
		func() error {
			err := api.ExecuteDagScansForUpload(ctx, uploadID, func(node dagmodel.Node, data []byte) error {
				log.Debugf("Adding node %s to upload shards for upload %s", node.CID(), uploadID)
				shardClosed, err := api.AddNodeToUploadShards(ctx, uploadID, spaceDID, node.CID(), data)
				if err != nil {
					return fmt.Errorf("adding node to upload shard: %w", err)
				}

				if shardClosed {
					signal(closedShardsAvailable)
				}

				return nil
			})

			var badFSEntriesErr types.BadFSEntriesError
			if errors.As(err, &badFSEntriesErr) {
				upload, err := api.Repo.GetUploadByID(ctx, uploadID)
				if err != nil {
					return fmt.Errorf("getting upload %s after finding bad FS entry: %w", uploadID, err)
				}

				for _, badFSEntryErr := range badFSEntriesErr.Errs() {
					log.Debug("Removing bad FS entry from upload ", uploadID, ": ", badFSEntryErr.FsEntryID())
					err = api.RemoveBadFSEntry(ctx, upload.SpaceDID(), badFSEntryErr.FsEntryID())
					if err != nil {
						return fmt.Errorf("removing bad FS entry for upload %s: %w", uploadID, err)
					}
				}
				log.Debug("Invalidating upload ", uploadID, " after removing bad FS entries")

				err = upload.Invalidate()
				if err != nil {
					return fmt.Errorf("invalidating upload %s after removing bad FS entry: %w", uploadID, err)
				}

				err = api.Repo.UpdateUpload(ctx, upload)
				if err != nil {
					return fmt.Errorf("updating upload %s after removing bad FS entry: %w", uploadID, err)
				}
			}

			if err != nil {
				return fmt.Errorf("running dag scans for upload %s: %w", uploadID, err)
			}

			return nil
		},

		// finalize
		func() error {
			// We're out of nodes, so we can close any open shards for this upload.
			shardClosed, err := api.CloseUploadShards(ctx, uploadID)
			if err != nil {
				return fmt.Errorf("closing upload shards for upload %s: %w", uploadID, err)
			}

			if shardClosed {
				signal(closedShardsAvailable)
			}

			// Reload the upload to get the latest state from the DB.
			upload, err := api.Repo.GetUploadByID(ctx, uploadID)
			if err != nil {
				return fmt.Errorf("reloading upload: %w", err)
			}
			rootCID, err := api.Repo.CIDForFSEntry(ctx, upload.RootFSEntryID())
			if err != nil {
				return fmt.Errorf("retrieving CID for root fs entry: %w", err)
			}
			if err := upload.SetRootCID(rootCID); err != nil {
				return fmt.Errorf("completing DAG generation: %w", err)
			}
			if err := api.Repo.UpdateUpload(ctx, upload); err != nil {
				return fmt.Errorf("updating upload: %w", err)
			}

			close(closedShardsAvailable)

			return nil
		},
	)
}

// runStorachaWorker runs the worker that adds shards and indexes to Storacha.
func runStorachaWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, blobWork <-chan struct{}) error {
	ctx, span := tracer.Start(ctx, "storacha-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("Storacha worker for upload %s exiting", uploadID)
	defer span.End()

	return Worker(
		ctx,
		blobWork,

		// doWork
		func() error {
			err := api.AddShardsForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				log.Debug("Error adding shards for upload ", uploadID, ": ", err)
				var badNodesErr types.BadNodesError
				if errors.As(err, &badNodesErr) {
					upload, err := api.Repo.GetUploadByID(ctx, uploadID)
					if err != nil {
						return fmt.Errorf("getting upload %s after finding bad nodes: %w", uploadID, err)
					}

					var cids []cid.Cid
					for _, e := range badNodesErr.Errs() {
						cids = append(cids, e.CID())
					}
					log.Debug("Removing bad nodes from upload ", uploadID, ": ", cids)
					err = api.RemoveBadNodes(ctx, upload.SpaceDID(), cids)
					if err != nil {
						return fmt.Errorf("removing bad nodes for upload %s: %w", uploadID, err)
					}

					log.Debug("Removing bad shard for upload ", uploadID, ": ", badNodesErr.ShardID())
					err = api.RemoveShard(ctx, badNodesErr.ShardID())
					if err != nil {
						return fmt.Errorf("removing bad shard %s for upload %s: %w", badNodesErr.ShardID(), uploadID, err)
					}

					log.Debug("Adding good CIDs back to upload in a different shard", uploadID, ": ", badNodesErr.GoodCIDs())
					for _, goodCID := range badNodesErr.GoodCIDs().Keys() {
						_, err := api.AddNodeToUploadShards(ctx, uploadID, spaceDID, goodCID, nil)
						if err != nil {
							return fmt.Errorf("adding good CID %s back to upload %s: %w", goodCID, uploadID, err)
						}
					}
					err = upload.Invalidate()
					if err != nil {
						return fmt.Errorf("invalidating upload %s after removing bad nodes: %w", uploadID, err)
					}

					err = api.Repo.UpdateUpload(ctx, upload)
					if err != nil {
						return fmt.Errorf("updating upload %s after removing bad nodes: %w", uploadID, err)
					}
				}
				return fmt.Errorf("`space/blob/add`ing shards for upload %s: %w", uploadID, err)
			}

			return nil
		},

		// finalize
		func() error {
			err := api.AddIndexesForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				return fmt.Errorf("`space/blob/add`ing index for upload %s: %w", uploadID, err)
			}

			err = api.AddStorachaUploadForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				return fmt.Errorf("`upload/add`ing upload %s: %w", uploadID, err)
			}

			return nil
		},
	)
}
