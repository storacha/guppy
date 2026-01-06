package uploads

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/bettererrgroup"
	blobsmodel "github.com/storacha/guppy/pkg/preparation/blobs/model"
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
type AddNodeToUploadShardsFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, nodeCID cid.Cid, data []byte, shardCB func(shard *blobsmodel.Shard) error) error
type AddShardToUploadIndexesFunc func(ctx context.Context, uploadID id.UploadID, shardID id.ShardID, indexCB func(index *blobsmodel.Index) error) error
type CloseUploadShardsFunc func(ctx context.Context, uploadID id.UploadID, shardCB func(shard *blobsmodel.Shard) error) error
type CloseUploadIndexesFunc func(ctx context.Context, uploadID id.UploadID, indexCB func(index *blobsmodel.Index) error) error
type AddShardsForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, shardCB func(shard *blobsmodel.Shard) error) error
type AddNodesToUploadShardsFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, shardCB func(shard *blobsmodel.Shard) error) error
type AddIndexesForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, indexCB func(index *blobsmodel.Index) error) error
type PostProcessUploadedShardsFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error
type PostProcessUploadedIndexesFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error
type AddStorachaUploadForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error
type RemoveBadFSEntryFunc func(ctx context.Context, spaceDID did.DID, fsEntryID id.FSEntryID) error
type RemoveBadNodesFunc func(ctx context.Context, spaceDID did.DID, nodeCIDs []cid.Cid) error
type RemoveShardFunc func(ctx context.Context, shardID id.ShardID) error

type API struct {
	Repo                       Repo
	ExecuteScan                ExecuteScanFunc
	ExecuteDagScansForUpload   ExecuteDagScansForUploadFunc
	AddShardsForUpload         AddShardsForUploadFunc
	PostProcessUploadedShards  PostProcessUploadedShardsFunc
	PostProcessUploadedIndexes PostProcessUploadedIndexesFunc
	AddIndexesForUpload        AddIndexesForUploadFunc
	AddStorachaUploadForUpload AddStorachaUploadForUploadFunc
	RemoveBadFSEntry           RemoveBadFSEntryFunc
	RemoveBadNodes             RemoveBadNodesFunc
	RemoveShard                RemoveShardFunc

	// AddNodesToUploadShards assigns all unsharded nodes for an upload to shards.
	AddNodesToUploadShards AddNodesToUploadShardsFunc

	// AddShardToUploadIndexes adds the contents of a shard to the upload's indexes, creating new
	// indexes if necessary. If an index is closed as a result, the provided
	// `indexCB` callback is called with the closed index. `indexCB` may be nil.
	AddShardToUploadIndexes AddShardToUploadIndexesFunc

	// CloseUploadShards closes any remaining open shard for the upload.
	CloseUploadShards CloseUploadShardsFunc

	// CloseUploadIndexes closes any remaining open index for the upload.
	CloseUploadIndexes CloseUploadIndexesFunc
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
		scansAvailable           = make(chan struct{}, 1)
		dagScansAvailable        = make(chan struct{}, 1)
		nodeUploadsAvailable     = make(chan struct{}, 1)
		closedShardsAvailable    = make(chan struct{}, 1)
		closedIndexesAvailable   = make(chan struct{}, 1)
		uploadedShardsAvailable  = make(chan struct{}, 1)
		uploadedIndexesAvailable = make(chan struct{}, 1)
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
		err := runDAGScanWorker(wCtx, a, uploadID, spaceDID, dagScansAvailable, nodeUploadsAvailable)
		if err != nil {
			return fmt.Errorf("DAG scan worker: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := runShardingWorker(wCtx, a, uploadID, spaceDID, nodeUploadsAvailable, closedShardsAvailable, closedIndexesAvailable)
		if err != nil {
			return fmt.Errorf("sharding worker: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := runShardWorker(wCtx, a, uploadID, spaceDID, closedShardsAvailable, uploadedShardsAvailable)
		if err != nil {
			return fmt.Errorf("shard worker: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := runIndexWorker(wCtx, a, uploadID, spaceDID, closedIndexesAvailable, uploadedIndexesAvailable)
		if err != nil {
			return fmt.Errorf("index worker: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := runPostProcessShardWorker(wCtx, a, uploadID, spaceDID, uploadedShardsAvailable)
		if err != nil {
			return fmt.Errorf("post-process worker: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := runPostProcessIndexWorker(wCtx, a, uploadID, spaceDID, uploadedIndexesAvailable)
		if err != nil {
			return fmt.Errorf("post-process worker: %w", err)
		}
		return nil
	})
	// Kick them all off. There may be work available in the DB from a previous
	// attempt.
	signal(scansAvailable)
	signal(dagScansAvailable)
	signal(nodeUploadsAvailable)
	signal(closedShardsAvailable)
	close(scansAvailable)

	log.Debugf("Waiting for workers to finish for upload %s", uploadID)
	workersErr := eg.Wait()

	var badFSEntriesErr types.BadFSEntriesError
	var badNodesErr types.BadNodesError
	var blobUploadErrors types.BlobUploadErrors
	// Clean up after errors that need it
	switch {

	case errors.As(workersErr, &badFSEntriesErr):
		err := a.handleBadFSEntries(ctx, uploadID, badFSEntriesErr)
		if err != nil {
			return cid.Undef, fmt.Errorf("handling bad FS entries worker error [%w]: %w", workersErr, err)
		}
	case errors.As(workersErr, &blobUploadErrors):
		err := a.handleBadBlobUploads(ctx, uploadID, spaceDID, blobUploadErrors)
		if err != nil {
			return cid.Undef, fmt.Errorf("handling bad shard uploads worker error [%w]: %w", workersErr, err)
		}
	case errors.As(workersErr, &badNodesErr):
		err := a.handleBadNodes(ctx, uploadID, spaceDID, badNodesErr)
		if err != nil {
			return cid.Undef, fmt.Errorf("handling bad nodes worker error [%w]: %w", workersErr, err)
		}
	}

	// Reload the upload to get the latest state from the DB.
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return cid.Undef, fmt.Errorf("reloading upload: %w", err)
	}

	return upload.RootCID(), workersErr
}

func (a API) handleBadFSEntries(ctx context.Context, uploadID id.UploadID, badFSEntriesErr types.BadFSEntriesError) error {
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("getting upload %s after finding bad FS entry: %w", uploadID, err)
	}

	for _, badFSEntryErr := range badFSEntriesErr.Errs() {
		log.Debug("Removing bad FS entry from upload ", uploadID, ": ", badFSEntryErr.FsEntryID())
		err = a.RemoveBadFSEntry(ctx, upload.SpaceDID(), badFSEntryErr.FsEntryID())
		if err != nil {
			return fmt.Errorf("removing bad FS entry for upload %s: %w", uploadID, err)
		}
	}
	log.Debug("Invalidating upload ", uploadID, " after removing bad FS entries")

	err = upload.Invalidate()
	if err != nil {
		return fmt.Errorf("invalidating upload %s after removing bad FS entry: %w", uploadID, err)
	}

	err = a.Repo.UpdateUpload(ctx, upload)
	if err != nil {
		return fmt.Errorf("updating upload %s after removing bad FS entry: %w", uploadID, err)
	}
	return nil
}

func (a API) handleBadBlobUploads(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, blobUploadErrors types.BlobUploadErrors) error {
	// when there's a bad shard upload, it's not based on a problem locally usually, unless bad nodes were read during upload
	for _, e := range blobUploadErrors.Errs() {
		// bad nodes error can happen from reading car during upload
		var badNodesErr types.BadNodesError
		if errors.As(e.Unwrap(), &badNodesErr) {
			err := a.handleBadNodes(ctx, uploadID, spaceDID, badNodesErr)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a API) handleBadNodes(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, badNodesErr types.BadNodesError) error {
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("getting upload %s after finding bad nodes: %w", uploadID, err)
	}

	var cids []cid.Cid
	for _, e := range badNodesErr.Errs() {
		cids = append(cids, e.CID())
	}
	log.Debug("Removing bad nodes from upload ", uploadID, ": ", cids)
	err = a.RemoveBadNodes(ctx, upload.SpaceDID(), cids)
	if err != nil {
		return fmt.Errorf("removing bad nodes for upload %s: %w", uploadID, err)
	}

	log.Debug("Removing bad shard for upload ", uploadID, ": ", badNodesErr.ShardID())
	err = a.RemoveShard(ctx, badNodesErr.ShardID())
	if err != nil {
		return fmt.Errorf("removing bad shard %s for upload %s: %w", badNodesErr.ShardID(), uploadID, err)
	}
	// Good nodes will have their shard_id set to NULL (via ON DELETE SET NULL FK),
	// so they'll be picked up again by the sharding worker.

	err = upload.Invalidate()
	if err != nil {
		return fmt.Errorf("invalidating upload %s after removing bad nodes: %w", uploadID, err)
	}

	err = a.Repo.UpdateUpload(ctx, upload)
	if err != nil {
		return fmt.Errorf("updating upload %s after removing bad nodes: %w", uploadID, err)
	}

	return nil
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
// and creates node_upload records for each node.
func runDAGScanWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, dagScansAvailable <-chan struct{}, nodeUploadsAvailable chan<- struct{}) error {
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
				log.Debugf("Creating node upload for node %s in upload %s", node.CID(), uploadID)

				// Create node upload record (node is now tracked for this upload)
				created, err := api.Repo.FindOrCreateNodeUpload(ctx, uploadID, node.CID(), spaceDID)
				if err != nil {
					return fmt.Errorf("creating node upload: %w", err)
				}

				if created {
					// Signal that new nodes are available for sharding
					signal(nodeUploadsAvailable)
				}

				return nil
			})

			if err != nil {
				return fmt.Errorf("running dag scans for upload %s: %w", uploadID, err)
			}

			return nil
		},

		// finalize
		func() error {
			// Set root CID now that all nodes are scanned
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

			// Signal completion and close the nodeUploadsAvailable channel
			close(nodeUploadsAvailable)
			return nil
		},
	)
}

// runShardingWorker runs the worker that assigns nodes to shards.
func runShardingWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, nodeUploadsAvailable <-chan struct{}, closedShardsAvailable chan<- struct{}, closedIndexesAvailable chan<- struct{}) error {
	ctx, span := tracer.Start(ctx, "sharding-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("Sharding worker for upload %s exiting", uploadID)
	defer span.End()

	handleClosedShard := func(shard *blobsmodel.Shard) error {
		signal(closedShardsAvailable)

		err := api.AddShardToUploadIndexes(ctx, uploadID, shard.ID(), nil)
		if err != nil {
			return fmt.Errorf("adding shard to upload index: %w", err)
		}

		return nil
	}

	return Worker(
		ctx,
		nodeUploadsAvailable,

		// doWork
		func() error {
			err := api.AddNodesToUploadShards(ctx, uploadID, spaceDID, handleClosedShard)
			if err != nil {
				return fmt.Errorf("adding nodes to shards for upload %s: %w", uploadID, err)
			}
			return nil
		},

		// finalize
		func() error {
			// Close any remaining open shards
			err := api.CloseUploadShards(ctx, uploadID, handleClosedShard)
			if err != nil {
				return fmt.Errorf("closing upload shards for upload %s: %w", uploadID, err)
			}

			// Close any remaining open indexes
			err = api.CloseUploadIndexes(ctx, uploadID, nil)
			if err != nil {
				return fmt.Errorf("closing upload indexes for upload %s: %w", uploadID, err)
			}

			close(closedShardsAvailable)

			// We can't add indexes until the root CID is set, so we signal way down
			// here. This means that in practice, the index worker only runs once,
			// after all the shards are closed (but not necessarily added).
			signal(closedIndexesAvailable)
			close(closedIndexesAvailable)

			return nil
		},
	)
}

// runShardWorker runs the worker that adds shards to Storacha.
func runShardWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, closedShardsAvailable <-chan struct{}, uploadedShardsAvailable chan<- struct{}) error {
	ctx, span := tracer.Start(ctx, "shard-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("Shard worker for upload %s exiting", uploadID)
	defer span.End()

	return Worker(
		ctx,
		closedShardsAvailable,

		// doWork
		func() error {
			err := api.AddShardsForUpload(ctx, uploadID, spaceDID, func(shard *blobsmodel.Shard) error {
				signal(uploadedShardsAvailable)
				return nil
			})
			if err != nil {
				return fmt.Errorf("`space/blob/add`ing shards for upload %s: %w", uploadID, err)
			}

			return nil
		},

		// finalize
		func() error {
			close(uploadedShardsAvailable)
			return nil
		},
	)
}

func runPostProcessShardWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, uploadedShardsAvailable <-chan struct{}) error {
	ctx, span := tracer.Start(ctx, "post-process-shard-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("Post-process shard worker for upload %s exiting", uploadID)
	defer span.End()

	return Worker(
		ctx,
		uploadedShardsAvailable,

		// doWork
		func() error {
			err := api.PostProcessUploadedShards(ctx, uploadID, spaceDID)
			if err != nil {
				return fmt.Errorf("`post-processing shards for upload %s: %w", uploadID, err)
			}
			return nil
		},

		// finalize
		func() error {
			err := api.AddStorachaUploadForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				return fmt.Errorf("`upload/add`ing upload %s: %w", uploadID, err)
			}

			return nil
		},
	)
}

// runIndexWorker runs the worker that adds indexes to Storacha.
func runIndexWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, closedIndexesAvailable <-chan struct{}, uploadedIndexesAvailable chan<- struct{}) error {
	ctx, span := tracer.Start(ctx, "index-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("Index worker for upload %s exiting", uploadID)
	defer span.End()

	return Worker(
		ctx,
		closedIndexesAvailable,

		// doWork
		func() error {
			err := api.AddIndexesForUpload(ctx, uploadID, spaceDID, func(index *blobsmodel.Index) error {
				signal(uploadedIndexesAvailable)
				return nil
			})
			if err != nil {
				return fmt.Errorf("`space/blob/add`ing indexes for upload %s: %w", uploadID, err)
			}
			return nil
		},

		// finalize
		func() error {
			close(uploadedIndexesAvailable)
			return nil
		},
	)
}

func runPostProcessIndexWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, uploadedIndexesAvailable <-chan struct{}) error {
	ctx, span := tracer.Start(ctx, "post-process-index-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("Post-process index worker for upload %s exiting", uploadID)
	defer span.End()

	return Worker(
		ctx,
		uploadedIndexesAvailable,

		// doWork
		func() error {
			err := api.PostProcessUploadedIndexes(ctx, uploadID, spaceDID)
			if err != nil {
				return fmt.Errorf("`post-processing indexes for upload %s: %w", uploadID, err)
			}
			return nil
		},

		// finalize
		nil,
	)
}
