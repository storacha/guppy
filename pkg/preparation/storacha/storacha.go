package storacha

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	"github.com/storacha/guppy/pkg/preparation/internal/meteredwriter"
	gtypes "github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
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
	Repo                  Repo
	Client                Client
	ReaderForShard        ReaderForShardFunc
	ReaderForIndex        ReaderForIndexFunc
	BlobUploadParallelism int
}

var _ uploads.AddShardsForUploadFunc = API{}.AddShardsForUpload
var _ uploads.AddIndexesForUploadFunc = API{}.AddIndexesForUpload
var _ uploads.AddStorachaUploadForUploadFunc = API{}.AddStorachaUploadForUpload

func (a API) AddShardsForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, shardUploadedCb func(shard *model.Shard) error) error {
	ctx, span := tracer.Start(ctx, "add-shards-for-upload")
	defer span.End()
	getClosedShards := func(ctx context.Context) ([]*model.Shard, error) {
		closedShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.BlobStateClosed)

		if err != nil {
			return nil, fmt.Errorf("failed to get closed shards for upload %s: %w", uploadID, err)
		}
		span.AddEvent("found closed shards", trace.WithAttributes(attribute.Int("shards", len(closedShards))))

		return closedShards, nil
	}
	addShard := func(ctx context.Context, shard *model.Shard) error {
		if err := a.addBlob(ctx, shard, spaceDID); err != nil {
			return fmt.Errorf("failed to add shard %s: %w", shard.ID(), err)
		}
		if shardUploadedCb != nil {
			if err := shardUploadedCb(shard); err != nil {
				return fmt.Errorf("failed to call after uploaded callback for shard %s: %w", shard.ID(), err)
			}
		}
		log.Infof("Successfully added shard %s for upload %s", shard.ID(), uploadID)
		return nil
	}
	return parallelProcess(ctx, getClosedShards, addShard, a.BlobUploadParallelism)
}

func (a API) PostProcessUploadedShards(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	ctx, span := tracer.Start(ctx, "post-process-uploaded-shards")
	defer span.End()
	getUploadedShards := func(ctx context.Context) ([]*model.Shard, error) {
		uploadedShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.BlobStateUploaded)
		if err != nil {
			return nil, fmt.Errorf("failed to get uploaded shards for post processing %s: %w", uploadID, err)
		}
		span.AddEvent("found uploaded shards", trace.WithAttributes(attribute.Int("shards", len(uploadedShards))))
		return uploadedShards, nil
	}
	postProcessShard := func(ctx context.Context, shard *model.Shard) error {
		err := a.postProcessBlob(ctx, shard, spaceDID, func(blob model.Blob) error {
			var opts []client.FilecoinOfferOption
			if blob.PDPAccept() != nil {
				opts = append(opts, client.WithPDPAcceptInvocation(blob.PDPAccept()))
			}
			if err := a.filecoinOffer(ctx, blob, spaceDID, opts...); err != nil {
				return gtypes.NewBlobUploadError(blob.ID(), err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to post process shard %s: %w", shard.ID(), err)
		}
		log.Infof("Successfully post-processed shard %s for upload %s", shard.ID(), uploadID)
		return nil
	}
	return parallelProcess(ctx, getUploadedShards, postProcessShard, a.BlobUploadParallelism)
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
		addedBlob, err := a.spaceBlobAdd(ctx, addReader, spaceDID, opts...)
		if err != nil {
			return gtypes.NewBlobUploadError(blob.ID(), fmt.Errorf("failed to add blob %s to space %s: %w", blob, spaceDID, err))
		}

		if err := blob.SpaceBlobAdded(addedBlob); err != nil {
			return fmt.Errorf("failed to record `space/blob/add` for blob %s: %w", blob, err)
		}

		if err := a.updateBlob(ctx, blob); err != nil {
			return fmt.Errorf("failed to update blob %s after `space/blob/add`: %w", blob, err)
		}
	} else {
		// this is a just a legacy case where the blob was added to the space but didn't record the state change
		log.Infof("blob %s already has location and PDP accept, skipping `space/blob/add`", blob.ID())
		blob.SpaceBlobAdded(client.AddedBlob{Location: blob.Location(), PDPAccept: blob.PDPAccept(), Digest: blob.Digest(), Size: blob.Size()})
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
	if err := a.updateBlob(ctx, blob); err != nil {
		return fmt.Errorf("failed to update blob %s after adding to space: %w", blob, err)
	}

	return nil
}

func (a API) spaceBlobAdd(ctx context.Context, content io.Reader, spaceDID did.DID, opts ...client.SpaceBlobAddOption) (client.AddedBlob, error) {
	ctx, span := tracer.Start(ctx, "space-blob-add")
	defer span.End()

	return a.Client.SpaceBlobAdd(ctx, content, spaceDID, opts...)
}

func (a API) spaceBlobReplicate(ctx context.Context, blob model.Blob, spaceDID did.DID, locationCommitment delegation.Delegation) error {
	ctx, span := tracer.Start(ctx, "space-blob-replicate")
	defer span.End()

	_, _, err := a.Client.SpaceBlobReplicate(
		ctx,
		spaceDID,
		types.Blob{
			Digest: blob.Digest(),
			Size:   blob.Size(),
		},
		3,
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

// AddIndexesForUpload adds the given indexes to the space, in parallel. The
// upload must have a root CID set.
func (a API) AddIndexesForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, indexCB func(index *model.Index) error) error {
	ctx, span := tracer.Start(ctx, "add-indexes-for-upload")
	defer span.End()

	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload %s: %w", uploadID, err)
	}
	rootCID := upload.RootCID()
	if rootCID == cid.Undef {
		return fmt.Errorf("tried to add index, but upload %s has no root CID yet", uploadID)
	}

	getClosedIndexes := func(ctx context.Context) ([]*model.Index, error) {
		closedIndexes, err := a.Repo.IndexesForUploadByState(ctx, uploadID, model.BlobStateClosed)
		if err != nil {
			return nil, fmt.Errorf("failed to get closed indexes for upload %s: %w", uploadID, err)
		}
		span.AddEvent("found closed indexes", trace.WithAttributes(attribute.Int("indexes", len(closedIndexes))))
		return closedIndexes, nil
	}
	uploadIndex := func(ctx context.Context, index *model.Index) error {
		if err := a.addBlob(ctx, index, spaceDID); err != nil {
			return fmt.Errorf("failed to add shard %s: %w", index.ID(), err)
		}
		if indexCB != nil {
			if err := indexCB(index); err != nil {
				return fmt.Errorf("failed to call after upload callback for index %s: %w", index.ID(), err)
			}
		}
		log.Infof("Successfully added index %s for upload %s", index.ID(), uploadID)
		return nil
	}
	return parallelProcess(ctx, getClosedIndexes, uploadIndex, a.BlobUploadParallelism)
}

// PostProcessUploadedIndexes runs post-processing for uploaded indexes, including
// adding them to the space via `space/index/add`.
func (a API) PostProcessUploadedIndexes(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	ctx, span := tracer.Start(ctx, "post-process-uploaded-indexes")
	defer span.End()

	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload %s: %w", uploadID, err)
	}
	rootCID := upload.RootCID()
	if rootCID == cid.Undef {
		return fmt.Errorf("tried to add index, but upload %s has no root CID yet", uploadID)
	}

	getUploadedIndexes := func(ctx context.Context) ([]*model.Index, error) {
		uploadedIndexes, err := a.Repo.IndexesForUploadByState(ctx, uploadID, model.BlobStateUploaded)
		if err != nil {
			return nil, fmt.Errorf("failed to get uploaded indexes for upload %s: %w", uploadID, err)
		}
		span.AddEvent("found uploaded indexes", trace.WithAttributes(attribute.Int("indexes", len(uploadedIndexes))))
		return uploadedIndexes, nil
	}
	postProcessIndex := func(ctx context.Context, index *model.Index) error {
		err := a.postProcessBlob(ctx, index, spaceDID, func(blob model.Blob) error {
			return a.Client.SpaceIndexAdd(ctx, blob.CID(), blob.Size(), rootCID, spaceDID)
		})
		if err != nil {
			return fmt.Errorf("failed to post process index %s: %w", index.ID(), err)
		}
		log.Infof("Successfully post-processed index %s for upload %s", index.ID(), uploadID)
		return nil
	}
	return parallelProcess(ctx, getUploadedIndexes, postProcessIndex, a.BlobUploadParallelism)
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

func consumeChannelWithContext[T any](ctx context.Context, incoming <-chan T, handle func(T) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item, ok := <-incoming:
			if !ok {
				return nil
			}
			if err := handle(item); err != nil {
				return err
			}
		}
	}
}

func signal(work chan<- struct{}) {
	select {
	case work <- struct{}{}:
	default:
		// channel is full, no need to signal
	}
}

// parallelProcess processes items of type T in parallel, using the provided
// getCurrentItems function to fetch items and processItem function to process
// each item. The parallelism parameter controls the number of concurrent
// processing goroutines.
// getCurrentItems should return the current list of items to process. It will be
// called initially and then again whenever an item is processed, to check for
// new items that may have appeared since the last call.
// the item type T must implement an ID() U method, where U is a comparable type,
// to uniquely identify items and avoid processing the same item multiple times.
func parallelProcess[U comparable, T interface{ ID() U }](ctx context.Context, getCurrentItems func(ctx context.Context) ([]T, error), processItem func(ctx context.Context, item T) error, parallelism int) error {
	// Ensure at least 1 parallelism
	if parallelism < 1 {
		parallelism = 1
	}

	newItems := make(chan T, parallelism)
	checkForNewItems := make(chan struct{}, 1)
	var blobUploadErrLk sync.Mutex
	var blobUploadErrs []gtypes.BlobUploadError
	seen := make(map[U]struct{})

	// Initial trigger to fetch items
	checkForNewItems <- struct{}{}

	eg, gctx := errgroup.WithContext(ctx)

	// Item fetcher goroutine
	eg.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case <-checkForNewItems:
				items, err := getCurrentItems(gctx)
				if err != nil {
					return fmt.Errorf("failed to get more items: %w", err)
				}
				// remove seen items no longer present - memory leak prevention
				for id := range seen {
					found := false
					for _, item := range items {
						if item.ID() == id {
							found = true
							break
						}
					}
					if !found {
						delete(seen, id)
					}
				}
				// filter out seen items
				itemsNotSeen := make([]T, 0, len(items))
				for _, item := range items {
					if _, ok := seen[item.ID()]; !ok {
						seen[item.ID()] = struct{}{}
						itemsNotSeen = append(itemsNotSeen, item)
					}
				}
				if len(itemsNotSeen) == 0 {
					close(newItems)
					return nil
				}
				for _, item := range itemsNotSeen {
					select {
					case <-gctx.Done():
						return gctx.Err()
					case newItems <- item:
					}
				}
			}
		}
	})

	// Worker goroutines
	for i := 0; i < parallelism; i++ {
		eg.Go(func() error {
			return consumeChannelWithContext(gctx, newItems, func(item T) error {
				if err := processItem(gctx, item); err != nil {
					var errBlobUpload gtypes.BlobUploadError
					if errors.As(err, &errBlobUpload) {
						blobUploadErrLk.Lock()
						blobUploadErrs = append(blobUploadErrs, errBlobUpload)
						blobUploadErrLk.Unlock()
						signal(checkForNewItems)
						return nil
					}
					return err
				}
				signal(checkForNewItems)
				return nil
			})
		})
	}

	terminalErr := eg.Wait()

	if terminalErr != nil {
		return terminalErr
	}

	if len(blobUploadErrs) > 0 {
		return gtypes.NewBlobUploadErrors(blobUploadErrs)
	}

	return nil
}
