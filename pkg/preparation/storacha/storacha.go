package storacha

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
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
	shardsmodel "github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var (
	log    = logging.Logger("preparation/storacha")
	tracer = otel.Tracer("preparation/storacha")
)

// MaxConcurrentShardAdds caps concurrent shard uploads to avoid overwhelming the remote services.
const MaxConcurrentShardAdds = 4

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

type CarForShardFunc func(ctx context.Context, shardID id.ShardID) (io.Reader, error)
type IndexesForUploadFunc func(ctx context.Context, upload *uploadsmodel.Upload) ([]io.Reader, error)

// API provides methods to interact with Storacha.
type API struct {
	Repo             Repo
	Client           Client
	CarForShard      CarForShardFunc
	IndexesForUpload IndexesForUploadFunc
}

var _ uploads.AddShardsForUploadFunc = API{}.AddShardsForUpload
var _ uploads.AddShardForUploadFunc = API{}.AddShardForUpload
var _ uploads.AddIndexesForUploadFunc = API{}.AddIndexesForUpload
var _ uploads.AddStorachaUploadForUploadFunc = API{}.AddStorachaUploadForUpload

func (a API) AddShardsForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	ctx, span := tracer.Start(ctx, "add-shards-for-upload")
	defer span.End()
	closedShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, shardsmodel.ShardStateClosed)
	if err != nil {
		return fmt.Errorf("failed to get closed shards for upload %s: %w", uploadID, err)
	}
	span.AddEvent("closed shards", trace.WithAttributes(attribute.Int("shards", len(closedShards))))

	sem := make(chan struct{}, MaxConcurrentShardAdds)
	eg, gctx := errgroup.WithContext(ctx)

	for _, shard := range closedShards {
		shard := shard
		sem <- struct{}{}
		eg.Go(func() error {
			defer func() { <-sem }()
			if err := a.addShard(gctx, shard, spaceDID); err != nil {
				return fmt.Errorf("failed to add shard %s for upload %s: %w", shard.ID(), uploadID, err)
			}
			return nil
		})
	}

	return eg.Wait()
}

// AddShardForUpload adds a single shard to Storacha.
func (a API) AddShardForUpload(ctx context.Context, shard *shardsmodel.Shard, spaceDID did.DID) error {
	return a.addShard(ctx, shard, spaceDID)
}

func (a API) addShard(ctx context.Context, shard *shardsmodel.Shard, spaceDID did.DID) error {
	ctx, span := tracer.Start(ctx, "add-shard", trace.WithAttributes(
		attribute.String("shard.id", shard.ID().String()),
		attribute.Int64("shard.size", int64(shard.Size())),
	))
	defer span.End()

	if err := a.ensureShardDigests(ctx, shard); err != nil {
		return fmt.Errorf("preparing digests for shard %s: %w", shard.ID(), err)
	}

	car, err := a.CarForShard(ctx, shard.ID())
	if err != nil {
		return fmt.Errorf("failed to get CAR reader for shard %s: %w", shard.ID(), err)
	}

	addReader, addWriter := io.Pipe()

	go func() {
		meteredAddWriter := meteredwriter.New(ctx, addWriter, "add-writer")
		defer meteredAddWriter.Close()
		_, err := io.Copy(meteredAddWriter, car)
		if err != nil {
			addWriter.CloseWithError(fmt.Errorf("failed to copy CAR to pipe: %w", err))
		}
	}()

	addedBlob, err := a.spaceBlobAdd(ctx, addReader, spaceDID, client.WithPrecomputedDigest(shard.Digest(), shard.Size()))
	if err != nil {
		return fmt.Errorf("failed to add shard %s to space %s: %w", shard.ID(), spaceDID, err)
	}

	err = shard.Added(addedBlob.Digest)
	if err != nil {
		return fmt.Errorf("failed to mark shard %s as added: %w", shard.ID(), err)
	}

	span.SetAttributes(attribute.String("shard.digest", shard.Digest().String()))
	span.SetAttributes(attribute.String("shard.cid", shard.CID().String()))

	err = a.spaceBlobReplicate(ctx, shard, spaceDID, addedBlob.Location)
	if err != nil {
		return fmt.Errorf("failed to replicate shard %s: %w", shard.ID(), err)
	}

	var opts []client.FilecoinOfferOption
	if addedBlob.PDPAccept != nil {
		opts = append(opts, client.WithPDPAcceptInvocation(addedBlob.PDPAccept))
	}
	err = a.filecoinOffer(ctx, shard, spaceDID, opts...)
	if err != nil {
		return err
	}

	if err := a.Repo.UpdateShard(ctx, shard); err != nil {
		return fmt.Errorf("failed to update shard %s after adding to space: %w", shard.CID(), err)
	}

	return nil
}

func (a API) ensureShardDigests(ctx context.Context, shard *shardsmodel.Shard) error {
	if shard.Digest() != nil && len(shard.Digest()) > 0 && shard.PieceCID() != cid.Undef {
		return nil
	}

	car, err := a.CarForShard(ctx, shard.ID())
	if err != nil {
		return fmt.Errorf("failed to get CAR for shard %s while ensuring digests: %w", shard.ID(), err)
	}

	carHash := sha256.New()
	commpCalc := &commp.Calc{}
	if _, err := io.Copy(io.MultiWriter(carHash, commpCalc), car); err != nil {
		return fmt.Errorf("failed to hash shard %s: %w", shard.ID(), err)
	}

	carDigest, err := multihash.Encode(carHash.Sum(nil), multihash.SHA2_256)
	if err != nil {
		return fmt.Errorf("failed to encode shard digest for %s: %w", shard.ID(), err)
	}

	pieceDigest, _, err := commpCalc.Digest()
	if err != nil {
		return fmt.Errorf("failed to get piece digest for shard %s: %w", shard.ID(), err)
	}

	pieceCID, err := commcid.DataCommitmentToPieceCidv2(pieceDigest, shard.Size())
	if err != nil {
		return fmt.Errorf("failed to get piece CID for shard %s: %w", shard.ID(), err)
	}

	if err := shard.SetDigests(carDigest, pieceCID); err != nil {
		return fmt.Errorf("failed to set digests on shard %s: %w", shard.ID(), err)
	}

	return a.Repo.UpdateShard(ctx, shard)
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

func (a API) filecoinOffer(ctx context.Context, shard *shardsmodel.Shard, spaceDID did.DID, opts ...client.FilecoinOfferOption) error {
	ctx, span := tracer.Start(ctx, "filecoin-offer")
	defer span.End()

	// On shards too small to compute a CommP, just skip the `filecoin/offer`.
	switch {
	case shard.Size() < commp.MinPiecePayload:
		log.Warnf("skipping `filecoin/offer` for shard %s: size %d is below minimum %d", shard.ID(), shard.Size(), commp.MinPiecePayload)
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

func (a API) AddIndexesForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload %s: %w", uploadID, err)
	}
	if upload.RootCID() == cid.Undef {
		return fmt.Errorf("no root CID set yet on upload %s", upload.ID())
	}

	indexReaders, err := a.IndexesForUpload(ctx, upload)
	if err != nil {
		return fmt.Errorf("failed to get index for upload %s: %w", uploadID, err)
	}
	for _, indexReader := range indexReaders {
		indexBytes, err := io.ReadAll(indexReader)
		if err != nil {
			return fmt.Errorf("failed to read index for upload %s: %w", uploadID, err)
		}

		addedBlob, err := a.Client.SpaceBlobAdd(ctx, bytes.NewReader(indexBytes), spaceDID)
		if err != nil {
			return fmt.Errorf("failed to add index to space %s: %w", spaceDID, err)
		}

		_, _, err = a.Client.SpaceBlobReplicate(
			ctx,
			spaceDID,
			types.Blob{
				Digest: addedBlob.Digest,
				Size:   uint64(len(indexBytes)),
			},
			3,
			addedBlob.Location,
		)
		if err != nil {
			return fmt.Errorf("failed to replicate index: %w", err)
		}

		indexCID := cid.NewCidV1(uint64(multicodec.Car), addedBlob.Digest)
		err = a.Client.SpaceIndexAdd(ctx, indexCID, uint64(len(indexBytes)), upload.RootCID(), spaceDID)
		if err != nil {
			return fmt.Errorf("failed to add index link to space %s: %w", spaceDID, err)
		}
	}

	return nil
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
