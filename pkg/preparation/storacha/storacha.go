package storacha

import (
	"context"
	"fmt"
	"io"

	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
)

// SpaceBlobAdder is an interface for adding shards to a space blob. It's
// typically implemented by [client.Client].
type SpaceBlobAdder interface {
	SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (multihash.Multihash, delegation.Delegation, error)
}

var _ SpaceBlobAdder = (*client.Client)(nil)

type CarForShardFunc func(ctx context.Context, shard *model.Shard) (io.Reader, error)

// API provides methods to interact with Storacha.
type API struct {
	Repo        Repo
	Client      SpaceBlobAdder
	Space       did.DID
	CarForShard CarForShardFunc
}

var _ uploads.SpaceBlobAddShardsForUploadFunc = API{}.SpaceBlobAddShardsForUpload

func (a API) SpaceBlobAddShardsForUpload(ctx context.Context, uploadID id.UploadID) error {
	closedShards, err := a.Repo.ShardsForUploadByStatus(ctx, uploadID, model.ShardStateClosed)
	if err != nil {
		return fmt.Errorf("failed to get closed shards for upload %s: %w", uploadID, err)
	}

	for _, shard := range closedShards {
		reader, err := a.CarForShard(ctx, shard)
		if err != nil {
			return fmt.Errorf("failed to get CAR reader for shard %s: %w", shard.ID(), err)
		}

		_, _, err = a.Client.SpaceBlobAdd(ctx, reader, a.Space)
		if err != nil {
			return fmt.Errorf("failed to add shard %s to space %s: %w", shard.ID(), a.Space, err)
		}
		shard.Added()
		if err := a.Repo.UpdateShard(ctx, shard); err != nil {
			return fmt.Errorf("failed to update shard %s after adding to space: %w", shard.ID(), err)
		}
	}

	return nil
}
