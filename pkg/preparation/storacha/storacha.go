package storacha

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/minio/sha256-simd"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
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
	"github.com/storacha/guppy/pkg/preparation/internal/meteredwriter"
	indexesmodel "github.com/storacha/guppy/pkg/preparation/indexes/model"
	shardsmodel "github.com/storacha/guppy/pkg/preparation/shards/model"
	gtypes "github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
)

var (
	log    = logging.Logger("preparation/storacha")
	tracer = otel.Tracer("preparation/storacha")
)

// computeIndexHash computes the SHA-256 digest and Piece CID for index data
func computeIndexHash(data []byte) (multihash.Multihash, cid.Cid, error) {
	// Compute SHA-256 digest
	sha256Hash := sha256.Sum256(data)
	digest, err := multihash.Encode(sha256Hash[:], multihash.SHA2_256)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("encoding digest: %w", err)
	}

	// Compute piece CID if size is large enough
	size := uint64(len(data))
	if size < gtypes.MinPiecePayload {
		return digest, cid.Undef, nil
	}

	// Compute commp
	commpCalc := &commp.Calc{}
	_, err = commpCalc.Write(data)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("computing commp: %w", err)
	}
	pieceDigest := commpCalc.Sum(nil)

	pieceCID, err := commcid.DataCommitmentToPieceCidv2(pieceDigest, size)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("computing piece CID: %w", err)
	}

	return digest, pieceCID, nil
}

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

type ReaderForShardFunc func(ctx context.Context, shardID id.ShardID) (io.Reader, error)
type ReaderForIndexFunc func(ctx context.Context, indexID id.IndexID) (io.Reader, error)

// type IndexesForUploadFunc func(ctx context.Context, upload *uploadsmodel.Upload) ([]io.Reader, error)

// API provides methods to interact with Storacha.
type API struct {
	Repo           Repo
	Client         Client
	ReaderForShard ReaderForShardFunc
	ReaderForIndex ReaderForIndexFunc
	// IndexesForUpload      IndexesForUploadFunc
	BlobUploadParallelism int
}

var _ uploads.AddShardsForUploadFunc = API{}.AddShardsForUpload
var _ uploads.AddIndexesForUploadFunc = API{}.AddIndexesForUpload
var _ uploads.AddStorachaUploadForUploadFunc = API{}.AddStorachaUploadForUpload

func (a API) AddShardsForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	ctx, span := tracer.Start(ctx, "add-shards-for-upload")
	defer span.End()
	closedShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, shardsmodel.ShardStateClosed)
	if err != nil {
		return fmt.Errorf("failed to get closed shards for upload %s: %w", uploadID, err)
	}
	span.AddEvent("found closed shards", trace.WithAttributes(attribute.Int("shards", len(closedShards))))

	return a.addBlobs(ctx, closedShards, spaceDID)
}

func (a API) addBlobs(ctx context.Context, blobs []*shardsmodel.Shard, spaceDID did.DID) error {
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
			log.Infof("Successfully added blob %s", blob)
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

func (a API) readerForBlob(ctx context.Context, blob *shardsmodel.Shard) (io.Reader, error) {
	return a.ReaderForShard(ctx, blob.ID())
}

func (a API) traceAttributesForBlob(blob *shardsmodel.Shard) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("shard.id", blob.ID().String()),
		attribute.Int64("shard.size", int64(blob.Size())),
		attribute.String("shard.digest", blob.Digest().String()),
		attribute.String("shard.cid", blob.CID().String()),
	}
}

func (a API) addBlob(ctx context.Context, blob *shardsmodel.Shard, spaceDID did.DID) error {
	start := time.Now()
	log.Infow("adding blob", "cid", blob.CID().String(), "id", blob.ID())
	ctx, span := tracer.Start(ctx, "add-blob", trace.WithAttributes(a.traceAttributesForBlob(blob)...))
	defer func() {
		log.Infow("added blob", "cid", blob.CID().String(), "id", blob.ID(), "duration", time.Since(start))
		span.End()
	}()

	car, err := a.readerForBlob(ctx, blob)
	if err != nil {
		return fmt.Errorf("failed to get reader for blob %s: %w", blob.ID(), err)
	}

	addReader, addWriter := io.Pipe()

	go func() {
		meteredAddWriter := meteredwriter.New(ctx, addWriter, "add-writer")
		defer meteredAddWriter.Close()
		_, err := io.Copy(meteredAddWriter, car)
		if err != nil {
			addWriter.CloseWithError(fmt.Errorf("failed to copy blob bytes to pipe: %w", err))
		}
	}()

	location := blob.Location()
	pdpAccept := blob.PDPAccept()

	// If we don't have a location commitment yet, we have yet to successfully
	// `space/blob/add`. (Note that `shard.PDPAccept()` is optional and may be
	// legitimately nil even if the `space/blob/add` succeeded.)
	if location == nil {
		log.Infof("adding shard %s to space %s via `space/blob/add`", blob.ID(), spaceDID)
		addedBlob, err := a.spaceBlobAdd(ctx, addReader, spaceDID, client.WithPrecomputedDigest(blob.Digest(), blob.Size()))
		if err != nil {
			return gtypes.NewBlobUploadError(blob.String(), fmt.Errorf("failed to add blob %s to space %s: %w", blob.ID(), spaceDID, err))
		}

		if addedBlob.Digest.B58String() != blob.Digest().B58String() {
			return fmt.Errorf("added blob %s digest mismatch: expected %x, got %x", blob.ID(), blob.Digest(), addedBlob.Digest)
		}

		blob.SpaceBlobAdded(addedBlob.Location, addedBlob.PDPAccept)
		if err := a.Repo.UpdateShard(ctx, blob); err != nil {
			return fmt.Errorf("failed to update blob %s after `space/blob/add`: %w", blob.ID(), err)
		}

		location = addedBlob.Location
		pdpAccept = addedBlob.PDPAccept
	} else {
		log.Infof("blob %s already has location and PDP accept, skipping `space/blob/add`", blob.ID())
	}

	err = a.spaceBlobReplicate(ctx, blob, spaceDID, location)
	if err != nil {
		return gtypes.NewBlobUploadError(blob.String(), fmt.Errorf("failed to replicate blob %s: %w", blob, err))
	}

	var opts []client.FilecoinOfferOption
	if pdpAccept != nil {
		opts = append(opts, client.WithPDPAcceptInvocation(pdpAccept))
	}
	err = a.filecoinOffer(ctx, blob, spaceDID, opts...)
	if err != nil {
		return gtypes.NewBlobUploadError(blob.String(), err)
	}

	err = blob.Added()
	if err != nil {
		return fmt.Errorf("failed to mark blob %s as added: %w", blob.String(), err)
	}
	if err := a.Repo.UpdateShard(ctx, blob); err != nil {
		return fmt.Errorf("failed to update blob %s after adding to space: %w", blob.String(), err)
	}

	return nil
}

func (a API) spaceBlobAdd(ctx context.Context, content io.Reader, spaceDID did.DID, opts ...client.SpaceBlobAddOption) (client.AddedBlob, error) {
	ctx, span := tracer.Start(ctx, "space-blob-add")
	defer span.End()

	return a.Client.SpaceBlobAdd(ctx, content, spaceDID, opts...)
}

func (a API) spaceBlobReplicate(ctx context.Context, shard *shardsmodel.Shard, spaceDID did.DID, locationCommitment delegation.Delegation) error {
	ctx, span := tracer.Start(ctx, "space-blob-replicate")
	defer span.End()

	_, _, err := a.Client.SpaceBlobReplicate(
		ctx,
		spaceDID,
		types.Blob{
			Digest: shard.Digest(),
			Size:   shard.Size(),
		},
		3,
		locationCommitment,
	)
	return err
}

func (a API) spaceBlobReplicateForIndex(ctx context.Context, index *indexesmodel.Index, spaceDID did.DID, locationCommitment delegation.Delegation) error {
	ctx, span := tracer.Start(ctx, "space-blob-replicate-index")
	defer span.End()

	_, _, err := a.Client.SpaceBlobReplicate(
		ctx,
		spaceDID,
		types.Blob{
			Digest: index.Digest(),
			Size:   index.Size(),
		},
		3,
		locationCommitment,
	)
	return err
}

func (a API) filecoinOffer(ctx context.Context, shard *shardsmodel.Shard, spaceDID did.DID, opts ...client.FilecoinOfferOption) error {
	ctx, span := tracer.Start(ctx, "filecoin-offer")
	defer span.End()

	// On shards too small to compute a CommP, just skip the `filecoin/offer`.
	switch {
	case shard.Size() < gtypes.MinPiecePayload:
		log.Warnf("skipping `filecoin/offer` for shard %s: size %d is below minimum %d", shard.ID(), shard.Size(), gtypes.MinPiecePayload)
		return nil
	case shard.Size() > commp.MaxPiecePayload:
		log.Warnf("skipping `filecoin/offer` for shard %s: size %d is above maximum %d", shard.ID(), shard.Size(), commp.MaxPiecePayload)
		return nil
	}

	if shard.PieceCID() == cid.Undef {
		return fmt.Errorf("shard %s missing piece CID for filecoin offer", shard.ID())
	}

	_, err := a.Client.FilecoinOffer(ctx, spaceDID, cidlink.Link{Cid: shard.CID()}, cidlink.Link{Cid: shard.PieceCID()}, opts...)
	if err != nil {
		return fmt.Errorf("failed to offer shard %s: %w", shard.CID(), err)
	}

	return nil
}

func (a API) addIndexBlobs(ctx context.Context, indexes []*indexesmodel.Index, uploadID id.UploadID, spaceDID did.DID) error {
	// Ensure at least 1 parallelism
	if a.BlobUploadParallelism < 1 {
		a.BlobUploadParallelism = 1
	}

	sem := make(chan struct{}, a.BlobUploadParallelism)
	blobUploadErrorCh := make(chan gtypes.BlobUploadError, len(indexes))
	eg, gctx := errgroup.WithContext(ctx)
	for _, index := range indexes {
		sem <- struct{}{}
		eg.Go(func() error {
			defer func() { <-sem }()
			if err := a.addIndexBlob(gctx, index, uploadID, spaceDID); err != nil {
				err = fmt.Errorf("failed to add index blob %s: %w", index, err)
				var errBlobUpload gtypes.BlobUploadError
				if errors.As(err, &errBlobUpload) {
					blobUploadErrorCh <- errBlobUpload
					return nil
				}
				log.Errorf("%v", err)
				return err
			}
			log.Infof("Successfully added index blob %s", index)
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

func (a API) addIndexBlob(ctx context.Context, index *indexesmodel.Index, uploadID id.UploadID, spaceDID did.DID) error {
	start := time.Now()
	log.Infow("adding index blob", "id", index.ID())
	ctx, span := tracer.Start(ctx, "add-index-blob", trace.WithAttributes(
		attribute.String("index.id", index.ID().String()),
	))
	defer func() {
		if index.CID() != cid.Undef {
			log.Infow("added index blob", "cid", index.CID().String(), "id", index.ID(), "duration", time.Since(start))
		} else {
			log.Infow("added index blob", "id", index.ID(), "duration", time.Since(start))
		}
		span.End()
	}()

	// Generate the index data and compute its digest
	indexReader, err := a.ReaderForIndex(ctx, index.ID())
	if err != nil {
		return fmt.Errorf("failed to get reader for index %s: %w", index.ID(), err)
	}

	// Read all the index data and compute digest/pieceCID
	indexData, err := io.ReadAll(indexReader)
	if err != nil {
		return fmt.Errorf("failed to read index %s data: %w", index.ID(), err)
	}

	indexDigest, pieceCID, err := computeIndexHash(indexData)
	if err != nil {
		return fmt.Errorf("failed to compute hash for index %s: %w", index.ID(), err)
	}

	// Set the digest, pieceCID, and size on the index
	if err := index.SetDigestAndSize(indexDigest, pieceCID, uint64(len(indexData))); err != nil {
		return fmt.Errorf("failed to set digest for index %s: %w", index.ID(), err)
	}
	if err := a.Repo.UpdateIndex(ctx, index); err != nil {
		return fmt.Errorf("failed to update index %s after computing digest: %w", index.ID(), err)
	}

	// Update span attributes now that we have the digest
	span.SetAttributes(
		attribute.Int64("index.size", int64(index.Size())),
		attribute.String("index.digest", index.Digest().String()),
		attribute.String("index.cid", index.CID().String()),
	)

	// Create a new reader from the data we already read
	indexReader = io.NopCloser(bytes.NewReader(indexData))

	addReader, addWriter := io.Pipe()

	go func() {
		meteredAddWriter := meteredwriter.New(ctx, addWriter, "add-writer")
		defer meteredAddWriter.Close()
		_, err := io.Copy(meteredAddWriter, indexReader)
		if err != nil {
			addWriter.CloseWithError(fmt.Errorf("failed to copy index bytes to pipe: %w", err))
		}
	}()

	location := index.Location()

	// If we don't have a location commitment yet, we have yet to successfully
	// `space/blob/add`.
	if location == nil {
		log.Infof("adding index %s to space %s via `space/blob/add`", index.ID(), spaceDID)
		addedBlob, err := a.spaceBlobAdd(ctx, addReader, spaceDID, client.WithPrecomputedDigest(index.Digest(), index.Size()))
		if err != nil {
			return gtypes.NewBlobUploadError(index.String(), fmt.Errorf("failed to add index %s to space %s: %w", index.ID(), spaceDID, err))
		}

		if addedBlob.Digest.B58String() != index.Digest().B58String() {
			return fmt.Errorf("added index %s digest mismatch: expected %x, got %x", index.ID(), index.Digest(), addedBlob.Digest)
		}

		index.SpaceBlobAdded(addedBlob.Location, addedBlob.PDPAccept)
		if err := a.Repo.UpdateIndex(ctx, index); err != nil {
			return fmt.Errorf("failed to update index %s after `space/blob/add`: %w", index.ID(), err)
		}

		location = addedBlob.Location
	} else {
		log.Infof("index %s already has location, skipping `space/blob/add`", index.ID())
	}

	err = a.spaceBlobReplicateForIndex(ctx, index, spaceDID, location)
	if err != nil {
		return gtypes.NewBlobUploadError(index.String(), fmt.Errorf("failed to replicate index %s: %w", index, err))
	}

	// Add the index link to the upload
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload %s: %w", uploadID, err)
	}

	indexCID := cid.NewCidV1(uint64(multicodec.Car), index.Digest())
	err = a.Client.SpaceIndexAdd(ctx, indexCID, index.Size(), upload.RootCID(), spaceDID)
	if err != nil {
		return gtypes.NewBlobUploadError(index.String(), fmt.Errorf("failed to add index link for %s: %w", index, err))
	}

	err = index.Added()
	if err != nil {
		return fmt.Errorf("failed to mark index %s as added: %w", index.String(), err)
	}
	if err := a.Repo.UpdateIndex(ctx, index); err != nil {
		return fmt.Errorf("failed to update index %s after adding to space: %w", index.String(), err)
	}

	return nil
}

func (a API) AddIndexesForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	ctx, span := tracer.Start(ctx, "add-indexes-for-upload")
	defer span.End()

	openIndexes, err := a.Repo.IndexesForUploadByState(ctx, uploadID, indexesmodel.IndexStateOpen)
	if err != nil {
		return fmt.Errorf("failed to get open indexes for upload %s: %w", uploadID, err)
	}
	span.AddEvent("found open indexes", trace.WithAttributes(attribute.Int("indexes", len(openIndexes))))

	return a.addIndexBlobs(ctx, openIndexes, uploadID, spaceDID)
}

func (a API) AddStorachaUploadForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload %s: %w", uploadID, err)
	}

	shards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, shardsmodel.ShardStateAdded)
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
