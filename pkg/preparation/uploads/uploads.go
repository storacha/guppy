package uploads

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/storacha/guppy/pkg/bus"
	"github.com/storacha/guppy/pkg/bus/events"
	"github.com/storacha/guppy/pkg/preparation/bettererrgroup"
	blobsmodel "github.com/storacha/guppy/pkg/preparation/blobs/model"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/internal/worker"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var (
	log    = logging.Logger("preparation/uploads")
	tracer = otel.Tracer("preparation/uploads")
)

type ExecuteScanFunc func(ctx context.Context, uploadID id.UploadID, nodeCB func(node scanmodel.FSEntry) error) error
type ExecuteDagScansForUploadFunc func(ctx context.Context, uploadID id.UploadID, nodeCB func(node dagmodel.Node, data []byte) error) error
type AddNodeToUploadShardsFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, nodeCID cid.Cid, data []byte, shardCB func(shard *blobsmodel.Shard) error) error
type AddShardsToUploadIndexesFunc func(ctx context.Context, uploadID id.UploadID, indexCB func(index *blobsmodel.Index) error) error
type CloseUploadShardsFunc func(ctx context.Context, uploadID id.UploadID, shardCB func(shard *blobsmodel.Shard) error) error
type CloseUploadIndexesFunc func(ctx context.Context, uploadID id.UploadID, indexCB func(index *blobsmodel.Index) error) error
type FindShardAddTasksForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) ([]types.IDTask, error)
type FindIndexAddTasksForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) ([]types.IDTask, error)
type AddNodesToUploadShardsFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, shardCB func(shard *blobsmodel.Shard) error) error
type FindShardPostProcessTasksForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) ([]types.IDTask, error)
type FindIndexPostProcessTasksForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) ([]types.IDTask, error)
type AddStorachaUploadForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error
type RemoveBadFSEntryFunc func(ctx context.Context, spaceDID did.DID, fsEntryID id.FSEntryID) error
type RemoveBadNodesFunc func(ctx context.Context, spaceDID did.DID, nodeCIDs []cid.Cid) error
type RemoveShardFunc func(ctx context.Context, shardID id.ShardID) error

type API struct {
	Repo                               Repo
	ExecuteScan                        ExecuteScanFunc
	ExecuteDagScansForUpload           ExecuteDagScansForUploadFunc
	BlobUploadParallelism              int
	FindShardAddTasksForUpload         FindShardAddTasksForUploadFunc
	FindIndexAddTasksForUpload         FindIndexAddTasksForUploadFunc
	FindShardPostProcessTasksForUpload FindShardPostProcessTasksForUploadFunc
	FindIndexPostProcessTasksForUpload FindIndexPostProcessTasksForUploadFunc
	AddStorachaUploadForUpload         AddStorachaUploadForUploadFunc
	RemoveBadFSEntry                   RemoveBadFSEntryFunc
	RemoveBadNodes                     RemoveBadNodesFunc
	RemoveShard                        RemoveShardFunc

	// AddNodesToUploadShards assigns all unsharded nodes for an upload to shards.
	AddNodesToUploadShards AddNodesToUploadShardsFunc

	// AddShardsToUploadIndexes adds the contents of shards to the upload's indexes, creating new
	// indexes if necessary. If an index is closed as a result, the provided
	// `indexCB` callback is called with the closed index. `indexCB` may be nil.
	AddShardsToUploadIndexes AddShardsToUploadIndexesFunc

	// CloseUploadShards closes any remaining open shard for the upload.
	CloseUploadShards CloseUploadShardsFunc

	// CloseUploadIndexes closes any remaining open index for the upload.
	CloseUploadIndexes CloseUploadIndexesFunc

	// Publishes events observation
	Publisher bus.Publisher

	// AssumeUnchangedSources skips the filesystem scan when a completed scan
	// already exists, assuming the filesystem hasn't changed since the last run.
	AssumeUnchangedSources bool
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

func multiplexSignal(input <-chan struct{}, outputs ...chan<- struct{}) {
	// Multiplex closed shards to both indexing and uploading workers
	go func() {
		for range input {
			for _, out := range outputs {
				signal(out)
			}
		}

		for _, out := range outputs {
			close(out)
		}
	}()
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

	var (
		shardsNeedIndexing  = make(chan struct{}, 1)
		shardsNeedUploading = make(chan struct{}, 1)
	)

	// Multiplex closed shards to both indexing and uploading workers
	multiplexSignal(closedShardsAvailable, shardsNeedIndexing, shardsNeedUploading)

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
		err := runShardingWorker(wCtx, a, uploadID, spaceDID, nodeUploadsAvailable, closedShardsAvailable)
		if err != nil {
			return fmt.Errorf("sharding worker: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		err := runIndexingWorker(wCtx, a, uploadID, spaceDID, shardsNeedIndexing, closedIndexesAvailable)
		if err != nil {
			return fmt.Errorf("indexing worker: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := runShardUploadWorker(wCtx, a, uploadID, spaceDID, shardsNeedUploading, uploadedShardsAvailable)
		if err != nil {
			return fmt.Errorf("shard worker: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := runIndexUploadWorker(wCtx, a, uploadID, spaceDID, closedIndexesAvailable, uploadedIndexesAvailable)
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
	signal(closedIndexesAvailable)
	signal(uploadedShardsAvailable)
	signal(uploadedIndexesAvailable)
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
		err := a.handleBadBlobUploads(ctx, uploadID, blobUploadErrors)
		if err != nil {
			return cid.Undef, fmt.Errorf("handling bad shard uploads worker error [%w]: %w", workersErr, err)
		}
	case errors.As(workersErr, &badNodesErr):
		err := a.handleBadNodes(ctx, uploadID, badNodesErr)
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

func (a API) handleBadBlobUploads(ctx context.Context, uploadID id.UploadID, blobUploadErrors types.BlobUploadErrors) error {
	// when there's a bad shard upload, it's not based on a problem locally usually, unless bad nodes were read during upload
	for _, e := range blobUploadErrors.Unwrap() {
		// bad nodes error can happen from reading car during upload
		var badNodesErr types.BadNodesError
		if errors.As(e, &badNodesErr) {
			err := a.handleBadNodes(ctx, uploadID, badNodesErr)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a API) handleBadNodes(ctx context.Context, uploadID id.UploadID, badNodesErr types.BadNodesError) error {
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

func runScanWorker(
	ctx context.Context,
	api API,
	uploadID id.UploadID,
	spaceDID did.DID,
	scansAvailable <-chan struct{},
	dagScansAvailable chan<- struct{},
) (err error) {
	ctx, span := tracer.Start(ctx, "scan-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))

	const eventName = "Scan-FS"
	api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
		Name:   eventName,
		Status: events.Running,
	})
	defer func() {
		status := events.Stopped
		if err != nil {
			status = events.Failed
		}
		api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
			Name:   eventName,
			Status: status,
			Error:  err,
		})
		log.Debugf("Scan worker for upload %s exiting", uploadID)
		span.End()
	}()

	if te := worker.Run(
		ctx,
		scansAvailable,
		1,

		// findWork
		func(ctx context.Context) ([]worker.Task, error) {
			return []worker.Task{
				func(ctx context.Context) worker.TaskError {
					if api.AssumeUnchangedSources {
						upload, err := api.Repo.GetUploadByID(ctx, uploadID)
						if err != nil {
							return worker.NewFatalError(fmt.Errorf("checking upload for existing scan: %w", err))
						}
						if upload.HasRootFSEntryID() {
							log.Infow("Skipping FS rescan (--assume-unchanged-sources): scan already exists", "upload", uploadID)
							return nil
						}
						log.Infow("No existing scan found, performing FS scan despite --assume-unchanged-sources", "upload", uploadID)
					}

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
						return worker.NewFatalError(fmt.Errorf("running scans: %w", err))
					}

					return nil
				},
			}, nil
		},

		// finalize
		func() error {
			close(dagScansAvailable)
			return nil
		},
	); te != nil {
		err = te
	}
	return err
}

// runDAGScanWorker runs the worker that scans files and directories into blocks,
// and creates node_upload records for each node.
func runDAGScanWorker(
	ctx context.Context,
	api API,
	uploadID id.UploadID,
	spaceDID did.DID,
	dagScansAvailable <-chan struct{},
	nodeUploadsAvailable chan<- struct{},
) (err error) {
	ctx, span := tracer.Start(ctx, "dag-scan-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	const eventName = "Scan-DAG"
	api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
		Name:   eventName,
		Status: events.Running,
	})
	defer func() {
		status := events.Stopped
		if err != nil {
			status = events.Failed
		}
		api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
			Name:   eventName,
			Status: status,
			Error:  err,
		})
		log.Debugf("DAG scan worker for upload %s exiting", uploadID)
		span.End()
	}()

	if te := worker.Run(
		ctx,
		dagScansAvailable,
		1,

		// findWork
		func(ctx context.Context) ([]worker.Task, error) {
			return []worker.Task{
				func(ctx context.Context) worker.TaskError {
					err := api.ExecuteDagScansForUpload(ctx, uploadID, func(node dagmodel.Node, data []byte) error {
						signal(nodeUploadsAvailable)
						return nil
					})

					if err != nil {
						return worker.NewFatalError(fmt.Errorf("running dag scans for upload %s: %w", uploadID, err))
					}

					return nil
				},
			}, nil
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
	); te != nil {
		err = te
	}
	return err
}

// runShardingWorker runs the worker that assigns nodes to shards.
func runShardingWorker(
	ctx context.Context,
	api API,
	uploadID id.UploadID,
	spaceDID did.DID,
	nodeUploadsAvailable <-chan struct{},
	closedShardsAvailable chan<- struct{},
) (err error) {
	ctx, span := tracer.Start(ctx, "sharding-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	const eventName = "Scan-Shard"
	api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
		Name:   eventName,
		Status: events.Running,
	})
	defer func() {
		status := events.Stopped
		if err != nil {
			status = events.Failed
		}
		api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
			Name:   eventName,
			Status: status,
			Error:  err,
		})
		log.Debugf("Sharding worker for upload %s exiting", uploadID)
		span.End()
	}()

	handleClosedShard := func(shard *blobsmodel.Shard) error {
		signal(closedShardsAvailable)
		return nil
	}

	if te := worker.Run(
		ctx,
		nodeUploadsAvailable,
		1,

		// findWork
		func(ctx context.Context) ([]worker.Task, error) {
			return []worker.Task{
				func(ctx context.Context) worker.TaskError {
					err := api.AddNodesToUploadShards(ctx, uploadID, spaceDID, handleClosedShard)
					if err != nil {
						return worker.NewFatalError(fmt.Errorf("adding nodes to shards for upload %s: %w", uploadID, err))
					}
					return nil
				},
			}, nil
		},

		// finalize
		func() error {
			// Close any remaining open shards
			err := api.CloseUploadShards(ctx, uploadID, handleClosedShard)
			if err != nil {
				return fmt.Errorf("closing upload shards for upload %s: %w", uploadID, err)
			}
			close(closedShardsAvailable)

			return nil
		},
	); te != nil {
		err = te
	}
	return err
}

func runIndexingWorker(
	ctx context.Context,
	api API,
	uploadID id.UploadID,
	spaceDID did.DID,
	shardsNeedIndexing <-chan struct{},
	closedIndexesAvailable chan<- struct{},
) (err error) {
	ctx, span := tracer.Start(ctx, "indexing-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	const eventName = "Index"
	api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
		Name:   eventName,
		Status: events.Running,
	})
	defer func() {
		status := events.Stopped
		if err != nil {
			status = events.Failed
		}
		api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
			Name:   eventName,
			Status: status,
			Error:  err,
		})
		log.Debugf("Indexing worker for upload %s exiting", uploadID)
		span.End()
	}()

	handleClosedIndex := func(index *blobsmodel.Index) error {
		signal(closedIndexesAvailable)
		return nil
	}

	if te := worker.Run(
		ctx,
		shardsNeedIndexing,
		1,

		// findWork
		func(ctx context.Context) ([]worker.Task, error) {
			return []worker.Task{
				func(ctx context.Context) worker.TaskError {
					err := api.AddShardsToUploadIndexes(ctx, uploadID, handleClosedIndex)
					if err != nil {
						return worker.NewFatalError(fmt.Errorf("adding shards to indexes for upload %s: %w", uploadID, err))
					}
					return nil
				},
			}, nil
		},

		// finalize
		func() error {
			// Close any remaining open indexes
			err := api.CloseUploadIndexes(ctx, uploadID, handleClosedIndex)
			if err != nil {
				return fmt.Errorf("closing upload indexes for upload %s: %w", uploadID, err)
			}
			close(closedIndexesAvailable)

			return nil
		},
	); te != nil {
		err = te
	}
	return err
}

// runShardUploadWorker runs the worker that adds shards to Storacha.
func runShardUploadWorker(
	ctx context.Context,
	api API,
	uploadID id.UploadID,
	spaceDID did.DID,
	closedShardsAvailable <-chan struct{},
	uploadedShardsAvailable chan<- struct{},
) (err error) {
	ctx, span := tracer.Start(ctx, "shard-upload-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	const eventName = "Upload-Shard"
	api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
		Name:   eventName,
		Status: events.Running,
	})
	defer func() {
		status := events.Stopped
		if err != nil {
			status = events.Failed
		}
		api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
			Name:   eventName,
			Status: status,
			Error:  err,
		})
		log.Debugf("Shard upload worker for upload %s exiting", uploadID)
		span.End()
	}()

	var inFlightShards sync.Map

	te := worker.Run(
		ctx,
		closedShardsAvailable,
		api.BlobUploadParallelism,

		// findWork
		func(ctx context.Context) ([]worker.Task, error) {
			rawTasks, err := api.FindShardAddTasksForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				return nil, err
			}
			var tasks []worker.Task
			for _, raw := range rawTasks {
				// Ignore tasks that are already in flight.
				if _, loaded := inFlightShards.LoadOrStore(raw.ID, struct{}{}); loaded {
					continue
				}
				tasks = append(tasks, func(ctx context.Context) worker.TaskError {
					defer inFlightShards.Delete(raw.ID)
					taskErr := raw.Run(ctx)
					if taskErr == nil {
						signal(uploadedShardsAvailable)
					}
					return taskErr
				})
			}
			return tasks, nil
		},

		// finalize
		func() error {
			close(uploadedShardsAvailable)
			return nil
		},
	)

	var nonFatals []error
	var fatal error
	if te != nil {
		nonFatals = te.NonFatalErrors()
		fatal = te.FatalError()
	}
	err = errors.Join(fatal, types.NewBlobUploadErrors(nonFatals))
	return err
}

func runPostProcessShardWorker(
	ctx context.Context,
	api API,
	uploadID id.UploadID,
	spaceDID did.DID,
	uploadedShardsAvailable <-chan struct{},
) (err error) {
	ctx, span := tracer.Start(ctx, "post-process-shard-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	const eventName = "Process-Shard"
	api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
		Name:   eventName,
		Status: events.Running,
	})
	defer func() {
		status := events.Stopped
		if err != nil {
			status = events.Failed
		}
		api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
			Name:   eventName,
			Status: status,
			Error:  err,
		})
		log.Debugf("Post-process shard worker for upload %s exiting", uploadID)
		span.End()
	}()

	var inFlightShards sync.Map

	if te := worker.Run(
		ctx,
		uploadedShardsAvailable,
		api.BlobUploadParallelism,

		// findWork
		func(ctx context.Context) ([]worker.Task, error) {
			rawTasks, err := api.FindShardPostProcessTasksForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				return nil, err
			}
			var tasks []worker.Task
			for _, raw := range rawTasks {
				if _, loaded := inFlightShards.LoadOrStore(raw.ID, struct{}{}); loaded {
					continue
				}
				tasks = append(tasks, func(ctx context.Context) worker.TaskError {
					defer inFlightShards.Delete(raw.ID)
					return raw.Run(ctx)
				})
			}
			return tasks, nil
		},

		// finalize
		func() error {
			err := api.AddStorachaUploadForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				return fmt.Errorf("`upload/add`ing upload %s: %w", uploadID, err)
			}
			return nil
		},
	); te != nil {
		err = te
	}
	return err
}

// runIndexUploadWorker runs the worker that adds indexes to Storacha.
func runIndexUploadWorker(
	ctx context.Context,
	api API,
	uploadID id.UploadID,
	spaceDID did.DID,
	closedIndexesAvailable <-chan struct{},
	uploadedIndexesAvailable chan<- struct{},
) (err error) {
	const eventName = "Upload-Index"
	ctx, span := tracer.Start(ctx, "index-upload-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
		Name:   eventName,
		Status: events.Running,
	})
	defer func() {
		status := events.Stopped
		if err != nil {
			status = events.Failed
		}
		api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
			Name:   eventName,
			Status: status,
			Error:  err,
		})
		log.Debugf("Index upload worker for upload %s exiting", uploadID)
		span.End()
	}()

	var inFlightIndexes sync.Map

	te := worker.Run(
		ctx,
		closedIndexesAvailable,
		api.BlobUploadParallelism,

		// findWork
		func(ctx context.Context) ([]worker.Task, error) {
			rawTasks, err := api.FindIndexAddTasksForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				return nil, err
			}
			var tasks []worker.Task
			for _, raw := range rawTasks {
				// Ignore tasks that are already in flight.
				if _, loaded := inFlightIndexes.LoadOrStore(raw.ID, struct{}{}); loaded {
					continue
				}
				tasks = append(tasks, func(ctx context.Context) worker.TaskError {
					defer inFlightIndexes.Delete(raw.ID)
					taskErr := raw.Run(ctx)
					if taskErr == nil {
						signal(uploadedIndexesAvailable)
					}
					return taskErr
				})
			}
			return tasks, nil
		},

		// finalize
		func() error {
			close(uploadedIndexesAvailable)
			return nil
		},
	)

	var nonFatals []error
	var fatal error
	if te != nil {
		nonFatals = te.NonFatalErrors()
		fatal = te.FatalError()
	}
	err = errors.Join(fatal, types.NewBlobUploadErrors(nonFatals))
	return err
}

func runPostProcessIndexWorker(
	ctx context.Context,
	api API,
	uploadID id.UploadID,
	spaceDID did.DID,
	uploadedIndexesAvailable <-chan struct{},
) (err error) {
	const eventName = "Process-Index"
	ctx, span := tracer.Start(ctx, "post-process-index-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
		Name:   eventName,
		Status: events.Running,
	})
	defer func() {
		status := events.Stopped
		if err != nil {
			status = events.Failed
		}
		api.Publisher.Publish(events.TopicWorker(uploadID), events.UploadWorkerEvent{
			Name:   eventName,
			Status: status,
			Error:  err,
		})
		log.Debugf("Post-process index worker for upload %s exiting", uploadID)
		span.End()
	}()

	var inFlightIndexes sync.Map

	if te := worker.Run(
		ctx,
		uploadedIndexesAvailable,
		api.BlobUploadParallelism,

		// findWork
		func(ctx context.Context) ([]worker.Task, error) {
			rawTasks, err := api.FindIndexPostProcessTasksForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				return nil, err
			}
			var tasks []worker.Task
			for _, raw := range rawTasks {
				if _, loaded := inFlightIndexes.LoadOrStore(raw.ID, struct{}{}); loaded {
					continue
				}
				tasks = append(tasks, func(ctx context.Context) worker.TaskError {
					defer inFlightIndexes.Delete(raw.ID)
					return raw.Run(ctx)
				})
			}
			return tasks, nil
		},

		// finalize
		nil,
	); te != nil {
		err = te
	}
	return err
}
