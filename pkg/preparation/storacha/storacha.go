package storacha

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	filecoincap "github.com/storacha/go-libstoracha/capabilities/filecoin"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/bus"
	"github.com/storacha/guppy/pkg/bus/events"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/internal/util"
	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	"github.com/storacha/guppy/pkg/preparation/internal/meteredwriter"
	gtypes "github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	log    = logging.Logger("preparation/storacha")
	tracer = otel.Tracer("preparation/storacha")
)

// Client is an interface for working with a Storacha space. It's typically
// implemented by [client.Client].
type Client interface {
	SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (client.AddedBlob, error)
	SpaceIndexAdd(ctx context.Context, indexCID cid.Cid, indexSize uint64, rootCID cid.Cid, space did.DID) error
	FilecoinOffer(ctx context.Context, space did.DID, content ipld.Link, piece ipld.Link, opts ...client.FilecoinOfferOption) (filecoincap.OfferOk, error)
	UploadAdd(ctx context.Context, space did.DID, root ipld.Link, shards []ipld.Link) (upload.AddOk, error)
	SpaceBlobReplicate(ctx context.Context, space did.DID, blob types.Blob, replicaCount uint, locationCommitment delegation.Delegation) (spaceblobcap.ReplicateOk, fx.Effects, error)
}

var _ Client = (*client.Client)(nil)

type ReaderForShardFunc func(ctx context.Context, shardID id.ShardID) (io.ReadCloser, error)
type ReaderForIndexFunc func(ctx context.Context, indexID id.IndexID) (io.ReadCloser, error)

// API provides methods to interact with Storacha.
type API struct {
	Repo           Repo
	Client         Client
	ReaderForShard ReaderForShardFunc
	ReaderForIndex ReaderForIndexFunc

	// TK: Rm
	BlobUploadParallelism int
	Bus                   bus.Publisher
	Replicas              uint
	BlobAddOptions        []client.SpaceBlobAddOption
}

var _ uploads.FindShardAddTasksForUploadFunc = API{}.FindShardAddTasksForUpload
var _ uploads.FindIndexAddTasksForUploadFunc = API{}.FindIndexAddTasksForUpload
var _ uploads.FindShardPostProcessTasksForUploadFunc = API{}.FindShardPostProcessTasksForUpload
var _ uploads.FindIndexPostProcessTasksForUploadFunc = API{}.FindIndexPostProcessTasksForUpload
var _ uploads.AddStorachaUploadForUploadFunc = API{}.AddStorachaUploadForUpload

func (a API) FindShardAddTasksForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) ([]gtypes.IDTask, error) {
	ctx, span := tracer.Start(ctx, "find-shard-add-tasks-for-upload")
	defer span.End()

	closedShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.BlobStateClosed)
	if err != nil {
		return nil, fmt.Errorf("failed to get closed shards for upload %s: %w", uploadID, err)
	}
	span.AddEvent("found closed shards", trace.WithAttributes(attribute.Int("shards", len(closedShards))))

	tasks := make([]gtypes.IDTask, 0, len(closedShards))
	for _, shard := range closedShards {
		tasks = append(tasks, gtypes.IDTask{
			ID: shard.ID(),
			Run: func(ctx context.Context) (error, error) {
				if err := a.addBlob(ctx, shard, spaceDID); err != nil {
					err = fmt.Errorf("failed to add shard %s: %w", shard, err)
					// [gtypes.BlobUploadError]s are non-fatal.
					var errBlobUpload gtypes.BlobUploadError
					if errors.As(err, &errBlobUpload) {
						return err, nil
					}
					log.Errorf("%v", err)
					return nil, err
				}
				return nil, nil
			},
		})
	}
	return tasks, nil
}

func (a API) FindIndexAddTasksForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) ([]gtypes.IDTask, error) {
	ctx, span := tracer.Start(ctx, "find-index-add-tasks-for-upload")
	defer span.End()

	closedIndexes, err := a.Repo.IndexesForUploadByState(ctx, uploadID, model.BlobStateClosed)
	if err != nil {
		return nil, fmt.Errorf("failed to get closed indexes for upload %s: %w", uploadID, err)
	}
	span.AddEvent("found closed indexes", trace.WithAttributes(attribute.Int("indexes", len(closedIndexes))))

	tasks := make([]gtypes.IDTask, 0, len(closedIndexes))
	for _, index := range closedIndexes {
		tasks = append(tasks, gtypes.IDTask{
			ID: index.ID(),
			Run: func(ctx context.Context) (error, error) {
				if err := a.addBlob(ctx, index, spaceDID); err != nil {
					err = fmt.Errorf("failed to add index %s: %w", index, err)
					// [gtypes.BlobUploadError]s are non-fatal.
					var errBlobUpload gtypes.BlobUploadError
					if errors.As(err, &errBlobUpload) {
						return err, nil
					}
					log.Errorf("%v", err)
					return nil, err
				}
				return nil, nil
			},
		})
	}
	return tasks, nil
}

func (a API) FindShardPostProcessTasksForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) ([]gtypes.IDTask, error) {
	ctx, span := tracer.Start(ctx, "find-shard-post-process-tasks-for-upload")
	defer span.End()

	uploadedShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.BlobStateUploaded)
	if err != nil {
		return nil, fmt.Errorf("failed to get uploaded shards for upload %s: %w", uploadID, err)
	}
	span.AddEvent("found uploaded shards", trace.WithAttributes(attribute.Int("shards", len(uploadedShards))))

	tasks := make([]gtypes.IDTask, 0, len(uploadedShards))
	for _, shard := range uploadedShards {
		tasks = append(tasks, gtypes.IDTask{
			ID: shard.ID(),
			Run: func(ctx context.Context) (error, error) {
				err := a.postProcessBlob(ctx, shard, spaceDID, func(blob model.Blob) error {
					var opts []client.FilecoinOfferOption
					if blob.PDPAccept() != nil {
						opts = append(opts, client.WithPDPAcceptInvocation(blob.PDPAccept()))
					}
					if err := a.filecoinOffer(ctx, blob, spaceDID, opts...); err != nil {
						return fmt.Errorf("failed to `filecoin/offer` shard %s: %w", blob, err)
					}
					return nil
				})
				if err != nil {
					log.Errorf("failed to post-process shard %s: %v", shard, err)
					return nil, fmt.Errorf("failed to post-process shard %s: %w", shard, err)
				}
				log.Infof("Successfully post-processed shard %s", shard.ID())
				return nil, nil
			},
		})
	}
	return tasks, nil
}

func (a API) readerForBlob(ctx context.Context, blob model.Blob) (io.ReadCloser, error) {
	switch blob := blob.(type) {
	case *model.Shard:
		return a.ReaderForShard(ctx, blob.ID())
	case *model.Index:
		return a.ReaderForIndex(ctx, blob.ID())
	default:
		return nil, fmt.Errorf("unexpected blob type %T", blob)
	}
}

func (a API) updateBlob(ctx context.Context, blob model.Blob) error {
	switch blob := blob.(type) {
	case *model.Shard:
		return a.Repo.UpdateShard(ctx, blob)
	case *model.Index:
		return a.Repo.UpdateIndex(ctx, blob)
	default:
		return fmt.Errorf("unexpected blob type %T", blob)
	}
}

func (a API) addBlob(ctx context.Context, blob model.Blob, spaceDID did.DID) error {
	start := time.Now()
	log.Infow("adding blob", "cid", blob.CID().String(), "blob", blob)
	ctx, span := tracer.Start(ctx, "add-blob", trace.WithAttributes(
		attribute.String("blob.id", blob.ID().String()),
		attribute.Int64("blob.size", int64(blob.Size())),
		attribute.String("blob.digest", blob.Digest().String()),
		attribute.String("blob.cid", blob.CID().String())))
	defer func() {
		log.Infow("added blob", "cid", blob.CID().String(), "blob", blob, "duration", time.Since(start))
		span.End()
	}()

	blobReader, err := a.readerForBlob(ctx, blob)
	if err != nil {
		return fmt.Errorf("failed to get reader for blob %s: %w", blob, err)
	}
	// make sure to close the shard reader before returning, even if we return early.
	defer blobReader.Close()

	addReader, addWriter := io.Pipe()
	defer addReader.Close()
	go func() {
		meteredAddWriter := meteredwriter.New(ctx, addWriter, "add-writer")
		defer meteredAddWriter.Close()
		_, err := io.Copy(meteredAddWriter, blobReader)
		if err != nil {
			addWriter.CloseWithError(fmt.Errorf("failed to copy blob bytes to pipe: %w", err))
		}
	}()
	// If we don't have a location commitment yet, we have yet to successfully
	// `space/blob/add`. (Note that `shard.PDPAccept()` is optional and may be
	// legitimately nil even if the `space/blob/add` succeeded.)
	if blob.Location() == nil {
		log.Infof("adding blob %s to space %s via `space/blob/add`", blob, spaceDID)

		var opts []client.SpaceBlobAddOption
		if blob.Digest() != nil && blob.Size() != 0 {
			opts = append(opts, client.WithPrecomputedDigest(blob.Digest(), blob.Size()))
		}
		opts = append(opts, client.WithPutProgress(func(uploaded int64) {
			a.Bus.Publish(events.TopicClientPut(blob.UploadID()), events.PutProgress{
				BlobID:   blob.ID(),
				Uploaded: uploaded,
				Total:    blob.Size(),
			})
		}))
		addedBlob, err := a.spaceBlobAdd(ctx, addReader, spaceDID, opts...)
		if err != nil {
			return gtypes.NewBlobUploadError(blob.ID(), fmt.Errorf("failed to add blob %s to space %s: %w", blob, spaceDID, err))
		}

		if err := blob.SpaceBlobAdded(addedBlob); err != nil {
			return fmt.Errorf("failed to record `space/blob/add` for blob %s: %w", blob, err)
		}
	} else {
		// If we have a location, the blob has been uploaded. If we called this function,
		// the blob record is probably not marked as BlobStateUploaded like it
		// should be, so mark it now.
		log.Infof("blob %s already has location; skipping `space/blob/add` and updating blob record", blob.ID())
		if err := blob.SpaceBlobAdded(client.AddedBlob{Location: blob.Location(), PDPAccept: blob.PDPAccept(),
			Digest: blob.Digest(), Size: blob.Size()}); err != nil {
			return fmt.Errorf("failed to record `space/blob/add` for blob %s: %w", blob, err)
		}
	}

	if err := a.updateBlob(context.WithoutCancel(ctx), blob); err != nil {
		return fmt.Errorf("failed to update blob %s after `space/blob/add`: %w", blob, err)
	}
	return nil
}

func (a API) postProcessBlob(ctx context.Context, blob model.Blob, spaceDID did.DID, afterAdded func(blob model.Blob) error) error {
	if err := a.spaceBlobReplicate(ctx, blob, spaceDID, blob.Location()); err != nil {
		return gtypes.NewBlobUploadError(blob.ID(), fmt.Errorf("failed to replicate blob %s: %w", blob, err))
	}

	if afterAdded != nil {
		if err := afterAdded(blob); err != nil {
			return fmt.Errorf("failed to call afterAdded for blob %s: %w", blob, err)
		}
	}

	if err := blob.Added(); err != nil {
		return fmt.Errorf("failed to mark blob %s as added: %w", blob, err)
	}
	if err := a.updateBlob(context.WithoutCancel(ctx), blob); err != nil {
		return fmt.Errorf("failed to update blob %s after adding to space: %w", blob, err)
	}

	return nil
}

func (a API) spaceBlobAdd(ctx context.Context, content io.Reader, spaceDID did.DID, opts ...client.SpaceBlobAddOption) (client.AddedBlob, error) {
	ctx, span := tracer.Start(ctx, "space-blob-add")
	defer span.End()

	return a.Client.SpaceBlobAdd(ctx, content, spaceDID, append(a.BlobAddOptions, opts...)...)
}

func (a API) spaceBlobReplicate(ctx context.Context, blob model.Blob, spaceDID did.DID, locationCommitment delegation.Delegation) error {
	ctx, span := tracer.Start(ctx, "space-blob-replicate")
	defer span.End()

	// if the replication count is 1 (or less) then there is nothing to do
	replicas := a.Replicas
	if replicas <= 1 {
		return nil
	}

	_, _, err := a.Client.SpaceBlobReplicate(
		ctx,
		spaceDID,
		types.Blob{
			Digest: blob.Digest(),
			Size:   blob.Size(),
		},
		replicas,
		locationCommitment,
	)
	return err
}

func (a API) filecoinOffer(ctx context.Context, blob model.Blob, spaceDID did.DID, opts ...client.FilecoinOfferOption) error {
	ctx, span := tracer.Start(ctx, "filecoin-offer")
	defer span.End()

	// On shards too small to compute a CommP, just skip the `filecoin/offer`.
	switch {
	case blob.Size() == 0:
		return fmt.Errorf("blob %s has no set size yet", blob)
	case blob.Size() < gtypes.MinPiecePayload:
		log.Warnf("skipping `filecoin/offer` for blob %s: size %d is below minimum %d", blob, blob.Size(), gtypes.MinPiecePayload)
		return nil
	case blob.Size() > commp.MaxPiecePayload:
		log.Warnf("skipping `filecoin/offer` for blob %s: size %d is above maximum %d", blob, blob.Size(), commp.MaxPiecePayload)
		return nil
	}

	if blob.PieceCID() == cid.Undef {
		return fmt.Errorf("blob %s missing piece CID for filecoin offer", blob)
	}

	_, err := a.Client.FilecoinOffer(ctx, spaceDID, cidlink.Link{Cid: blob.CID()}, cidlink.Link{Cid: blob.PieceCID()}, opts...)
	if err != nil {
		return fmt.Errorf("failed to offer blob %s: %w", blob.CID(), err)
	}

	return nil
}

func (a API) FindIndexPostProcessTasksForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) ([]gtypes.IDTask, error) {
	ctx, span := tracer.Start(ctx, "find-index-post-process-tasks-for-upload")
	defer span.End()

	uploadedIndexes, err := a.Repo.IndexesForUploadByState(ctx, uploadID, model.BlobStateUploaded)
	if err != nil {
		return nil, fmt.Errorf("failed to get uploaded indexes for upload %s: %w", uploadID, err)
	}
	span.AddEvent("found uploaded indexes", trace.WithAttributes(attribute.Int("indexes", len(uploadedIndexes))))

	tasks := make([]gtypes.IDTask, 0, len(uploadedIndexes))
	for _, index := range uploadedIndexes {
		tasks = append(tasks, gtypes.IDTask{
			ID: index.ID(),
			Run: func(ctx context.Context) (error, error) {
				err := a.postProcessBlob(ctx, index, spaceDID, func(blob model.Blob) error {
					// Use a placeholder for the root because it doesn't matter what it is,
					// and we don't want to wait for it to be known. It shouldn't really be
					// something the index knows at all.
					return a.Client.SpaceIndexAdd(ctx, blob.CID(), blob.Size(), util.PlaceholderCID, spaceDID)
				})
				if err != nil {
					log.Errorf("failed to post-process index %s: %v", index, err)
					return nil, fmt.Errorf("failed to post-process index %s: %w", index, err)
				}
				log.Infof("Successfully post-processed index %s", index.ID())
				return nil, nil
			},
		})
	}
	return tasks, nil
}

func (a API) AddStorachaUploadForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload %s: %w", uploadID, err)
	}

	shards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.BlobStateAdded)
	if err != nil {
		return fmt.Errorf("failed to get shards for upload %s: %w", uploadID, err)
	}

	var shardLinks []ipld.Link
	for _, shard := range shards {
		shardCID := cid.NewCidV1(uint64(multicodec.Car), shard.Digest())
		shardLinks = append(shardLinks, cidlink.Link{Cid: shardCID})
	}

	_, err = a.Client.UploadAdd(ctx, spaceDID, cidlink.Link{Cid: upload.RootCID()}, shardLinks)
	if err != nil {
		return fmt.Errorf("failed to add upload %s to space %s: %w", uploadID, spaceDID, err)
	}

	return nil
}
