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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/storacha/guppy/pkg/bus"
	"github.com/storacha/guppy/pkg/bus/events"
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
	Bus                   bus.Publisher
}

var _ uploads.AddShardsForUploadFunc = API{}.AddShardsForUpload
var _ uploads.AddIndexesForUploadFunc = API{}.AddIndexesForUpload
var _ uploads.AddStorachaUploadForUploadFunc = API{}.AddStorachaUploadForUpload

func (a API) AddShardsForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, shardUploadedCb func(shard *model.Shard) error) error {
	ctx, span := tracer.Start(ctx, "add-shards-for-upload")
	defer span.End()
	closedShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.BlobStateClosed)
	if err != nil {
		return fmt.Errorf("failed to get closed shards for upload %s: %w", uploadID, err)
	}
	span.AddEvent("found closed shards", trace.WithAttributes(attribute.Int("shards", len(closedShards))))

	blobs := make([]model.Blob, len(closedShards))
	for i, shard := range closedShards {
		blobs[i] = shard
	}
	return a.addBlobs(ctx, blobs, spaceDID, func(blob model.Blob) error {
		if shardUploadedCb != nil {
			return shardUploadedCb(blob.(*model.Shard))
		}
		return nil
	})
}

func (a API) PostProcessUploadedShards(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	ctx, span := tracer.Start(ctx, "post-process-uploaded-shards")
	defer span.End()
	uploadedShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.BlobStateUploaded)
	if err != nil {
		return fmt.Errorf("failed to get uploaded shards for post processing %s: %w", uploadID, err)
	}
	span.AddEvent("found uploaded shards", trace.WithAttributes(attribute.Int("shards", len(uploadedShards))))
	blobs := make([]model.Blob, len(uploadedShards))
	for i, shard := range uploadedShards {
		blobs[i] = shard
	}
	return a.postProcessBlobs(ctx, blobs, spaceDID, func(blob model.Blob) error {
		var opts []client.FilecoinOfferOption
		if blob.PDPAccept() != nil {
			opts = append(opts, client.WithPDPAcceptInvocation(blob.PDPAccept()))
		}
		if err := a.filecoinOffer(ctx, blob, spaceDID, opts...); err != nil {
			return gtypes.NewBlobUploadError(blob.ID(), err)
		}

		return nil
	})
}

// addBlobs adds the given blobs to the space, in parallel. For each blob, it
// will `space/blob/add` if it hasn't been added yet, then call the `afterUploaded` callback if successful.
// `SpaceBlobAdded()` will be called after `space/blob/add`. `Added()` will be
// called at the very end. If any of these steps fail, an error will be
// returned.
func (a API) addBlobs(ctx context.Context, blobs []model.Blob, spaceDID did.DID, afterUploaded func(blob model.Blob) error) error {
	// Ensure at least 1 parallelism
	if a.BlobUploadParallelism < 1 {
		a.BlobUploadParallelism = 1
	}

	sem := make(chan struct{}, a.BlobUploadParallelism)
	blobUploadErrorCh := make(chan gtypes.BlobUploadError, len(blobs))
	eg, gctx := errgroup.WithContext(ctx)
	for _, blob := range blobs {
		sem <- struct{}{}
		eg.Go(func() error {
			defer func() { <-sem }()
			if err := a.addBlob(gctx, blob, spaceDID); err != nil {
				err = fmt.Errorf("failed to add blob %s: %w", blob, err)
				var errBlobUpload gtypes.BlobUploadError
				if errors.As(err, &errBlobUpload) {
					blobUploadErrorCh <- errBlobUpload
					return nil
				}
				log.Errorf("%v", err)
				return err
			}
			if afterUploaded != nil {
				if err := afterUploaded(blob); err != nil {
					return fmt.Errorf("failed to call after uploaded callback for blob %s: %w", blob.ID(), err)
				}
			}
			log.Infof("Successfully added blob %s", blob.ID())
			return nil
		})
	}

	terminalErr := eg.Wait()
	close(blobUploadErrorCh)

	if terminalErr != nil {
		return terminalErr
	}

	var blobUploadErrors []gtypes.BlobUploadError
	for err := range blobUploadErrorCh {
		blobUploadErrors = append(blobUploadErrors, err)
	}
	if len(blobUploadErrors) > 0 {
		return gtypes.NewBlobUploadErrors(blobUploadErrors)
	}
	return nil
}

func (a API) postProcessBlobs(ctx context.Context, blobs []model.Blob, spaceDID did.DID, afterAdded func(blob model.Blob) error) error {
	// Ensure at least 1 parallelism
	if a.BlobUploadParallelism < 1 {
		a.BlobUploadParallelism = 1
	}

	sem := make(chan struct{}, a.BlobUploadParallelism)
	blobUploadErrorCh := make(chan gtypes.BlobUploadError, len(blobs))
	eg, gctx := errgroup.WithContext(ctx)
	for _, blob := range blobs {
		sem <- struct{}{}
		eg.Go(func() error {
			defer func() { <-sem }()
			if err := a.postProcessBlob(gctx, blob, spaceDID, afterAdded); err != nil {
				err = fmt.Errorf("failed to add blob %s: %w", blob, err)
				var errBlobUpload gtypes.BlobUploadError
				if errors.As(err, &errBlobUpload) {
					blobUploadErrorCh <- errBlobUpload
					return nil
				}
				log.Errorf("%v", err)
				return err
			}
			log.Infof("Successfully post-processed blob %s", blob.ID())
			return nil
		})
	}

	terminalErr := eg.Wait()
	close(blobUploadErrorCh)

	if terminalErr != nil {
		return terminalErr
	}

	var blobUploadErrors []gtypes.BlobUploadError
	for err := range blobUploadErrorCh {
		blobUploadErrors = append(blobUploadErrors, err)
	}
	if len(blobUploadErrors) > 0 {
		return gtypes.NewBlobUploadErrors(blobUploadErrors)
	}
	return nil
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

		if err := a.updateBlob(ctx, blob); err != nil {
			return fmt.Errorf("failed to update blob %s after `space/blob/add`: %w", blob, err)
		}
	} else {
		// this is a just a legacy case where the blob was added to the space but didn't record the state change
		log.Infof("blob %s already has location and PDP accept, skipping `space/blob/add`", blob.ID())
		if err := blob.SpaceBlobAdded(client.AddedBlob{Location: blob.Location(), PDPAccept: blob.PDPAccept(),
			Digest: blob.Digest(), Size: blob.Size()}); err != nil {
			return fmt.Errorf("failed to record `space/blob/add` for blob %s: %w", blob, err)
		}
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

	closedIndexes, err := a.Repo.IndexesForUploadByState(ctx, uploadID, model.BlobStateClosed)
	if err != nil {
		return fmt.Errorf("failed to get closed indexes for upload %s: %w", uploadID, err)
	}
	span.AddEvent("found closed indexes", trace.WithAttributes(attribute.Int("indexes", len(closedIndexes))))

	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload %s: %w", uploadID, err)
	}
	rootCID := upload.RootCID()
	if rootCID == cid.Undef {
		return fmt.Errorf("tried to add index, but upload %s has no root CID yet", uploadID)
	}

	blobs := make([]model.Blob, len(closedIndexes))
	for i, shard := range closedIndexes {
		blobs[i] = shard
	}
	return a.addBlobs(ctx, blobs, spaceDID, func(blob model.Blob) error {
		if indexCB != nil {
			return indexCB(blob.(*model.Index))
		}
		return nil
	})
}

// PostProcessUploadedIndexes runs post-processing for uploaded indexes, including
// adding them to the space via `space/index/add`.
func (a API) PostProcessUploadedIndexes(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	ctx, span := tracer.Start(ctx, "post-process-uploaded-indexes")
	defer span.End()

	uploadedIndexes, err := a.Repo.IndexesForUploadByState(ctx, uploadID, model.BlobStateUploaded)
	if err != nil {
		return fmt.Errorf("failed to get uploaded indexes for upload %s: %w", uploadID, err)
	}
	span.AddEvent("found uploaded indexes", trace.WithAttributes(attribute.Int("indexes", len(uploadedIndexes))))

	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload %s: %w", uploadID, err)
	}
	rootCID := upload.RootCID()
	if rootCID == cid.Undef {
		return fmt.Errorf("tried to add index, but upload %s has no root CID yet", uploadID)
	}

	blobs := make([]model.Blob, len(uploadedIndexes))
	for i, shard := range uploadedIndexes {
		blobs[i] = shard
	}
	return a.postProcessBlobs(ctx, blobs, spaceDID, func(blob model.Blob) error {
		return a.Client.SpaceIndexAdd(ctx, blob.CID(), blob.Size(), rootCID, spaceDID)
	})
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
