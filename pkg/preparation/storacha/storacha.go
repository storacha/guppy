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
}

var _ Client = (*client.Client)(nil)

type CarForShardFunc func(ctx context.Context, shardID id.ShardID) (io.Reader, error)
type IndexForUploadFunc func(ctx context.Context, upload *uploadsmodel.Upload) (io.Reader, error)

// API provides methods to interact with Storacha.
type API struct {
	Repo           Repo
	Client         Client
	Space          did.DID
	CarForShard    CarForShardFunc
	IndexForUpload IndexForUploadFunc
}

var _ uploads.SpaceBlobAddShardsForUploadFunc = API{}.SpaceBlobAddShardsForUpload
var _ uploads.AddIndexForUploadFunc = API{}.AddIndexForUpload

func (a API) SpaceBlobAddShardsForUpload(ctx context.Context, uploadID id.UploadID) error {
	closedShards, err := a.Repo.ShardsForUploadByStatus(ctx, uploadID, shardsmodel.ShardStateClosed)
	if err != nil {
		return fmt.Errorf("failed to get closed shards for upload %s: %w", uploadID, err)
	}

	for _, shard := range closedShards {
		car, err := a.CarForShard(ctx, shard.ID())
		if err != nil {
			return fmt.Errorf("failed to get CAR reader for shard %s: %w", shard.ID(), err)
		}

		digest, _, err := a.Client.SpaceBlobAdd(ctx, car, a.Space)
		if err != nil {
			return fmt.Errorf("failed to add shard %s to space %s: %w", shard.ID(), a.Space, err)
		}
		shard.Added(digest)
		if err := a.Repo.UpdateShard(ctx, shard); err != nil {
			return fmt.Errorf("failed to update shard %s after adding to space: %w", shard.ID(), err)
		}
	}

	return nil
}

func (a API) AddIndexForUpload(ctx context.Context, uploadID id.UploadID) error {
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload %s: %w", uploadID, err)
	}
	indexReader, err := a.IndexForUpload(ctx, upload)
	indexBytes, err := io.ReadAll(indexReader)
	if err != nil {
		return fmt.Errorf("failed to read index for upload %s: %w", uploadID, err)
	}

	indexDigest, _, err := a.Client.SpaceBlobAdd(ctx, bytes.NewReader(indexBytes), a.Space)
	if err != nil {
		return fmt.Errorf("failed to add index to space %s: %w", a.Space, err)
	}

	indexCID := cid.NewCidV1(uint64(multicodec.Car), indexDigest)
	err = a.Client.SpaceIndexAdd(ctx, cidlink.Link{Cid: indexCID}, a.Space)
	if err != nil {
		return fmt.Errorf("failed to add index link to space %s: %w", a.Space, err)
	}

	return nil
}
