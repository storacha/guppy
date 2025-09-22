package storacha

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
	shardsmodel "github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

// Client is an interface for working with a Storacha space. It's typically
// implemented by [client.Client].
type Client interface {
	SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (multihash.Multihash, delegation.Delegation, error)
	SpaceIndexAdd(ctx context.Context, indexLink ipld.Link, space did.DID) error
	UploadAdd(ctx context.Context, space did.DID, root ipld.Link, shards []ipld.Link) (upload.AddOk, error)
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

var _ uploads.SpaceBlobAddShardsForUploadFunc = API{}.SpaceBlobAddShardsForUpload
var _ uploads.AddIndexesForUploadFunc = API{}.AddIndexesForUpload
var _ uploads.AddStorachaUploadForUploadFunc = API{}.AddStorachaUploadForUpload

func (a API) SpaceBlobAddShardsForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	closedShards, err := a.Repo.ShardsForUploadByStatus(ctx, uploadID, shardsmodel.ShardStateClosed)
	if err != nil {
		return fmt.Errorf("failed to get closed shards for upload %s: %w", uploadID, err)
	}

	for _, shard := range closedShards {
		car, err := a.CarForShard(ctx, shard.ID())
		if err != nil {
			return fmt.Errorf("failed to get CAR reader for shard %s: %w", shard.ID(), err)
		}

		digest, _, err := a.Client.SpaceBlobAdd(ctx, car, spaceDID)
		if err != nil {
			return fmt.Errorf("failed to add shard %s to space %s: %w", shard.ID(), spaceDID, err)
		}
		shard.Added(digest)
		if err := a.Repo.UpdateShard(ctx, shard); err != nil {
			return fmt.Errorf("failed to update shard %s after adding to space: %w", shard.ID(), err)
		}
	}

	return nil
}

func (a API) AddIndexesForUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error {
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload %s: %w", uploadID, err)
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

		indexDigest, _, err := a.Client.SpaceBlobAdd(ctx, bytes.NewReader(indexBytes), spaceDID)
		if err != nil {
			return fmt.Errorf("failed to add index to space %s: %w", spaceDID, err)
		}

		indexCID := cid.NewCidV1(uint64(multicodec.Car), indexDigest)
		err = a.Client.SpaceIndexAdd(ctx, cidlink.Link{Cid: indexCID}, spaceDID)
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

	shards, err := a.Repo.ShardsForUploadByStatus(ctx, uploadID, shardsmodel.ShardStateAdded)
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
