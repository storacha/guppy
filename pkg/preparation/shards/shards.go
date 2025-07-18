package shards

import (
	"context"
	"fmt"
	"net/url"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
)

// API provides methods to interact with the Shards in the repository.
type API struct {
	Repo        Repo
	Client      client.Client
	Space       did.DID
	ReceiptsURL *url.URL
}

func (a API) AddNodeToUploadShards(ctx context.Context, uploadID id.UploadID, nodeCID cid.Cid) error {
	return a.Repo.AddNodeToUploadShards(ctx, uploadID, nodeCID)
}

var _ uploads.AddNodeToUploadShardsFn = API{}.AddNodeToUploadShards

func (a API) UploadShardWorker(ctx context.Context, work <-chan struct{}, uploadID id.UploadID) error {
	// TK: Need to restart these?
	// err := a.RestartScansForUpload(ctx, uploadID)
	// if err != nil {
	// 	return fmt.Errorf("restarting scans for upload %s: %w", uploadID, err)
	// }
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // Exit if the context is canceled
		case _, ok := <-work:
			if !ok {
				return nil // Channel closed, exit the loop
			}
			// space/blob/add all closed, not-yet-added shards for the given upload.
			if err := a.AddShardsForUpload(ctx, uploadID); err != nil {
				return fmt.Errorf("adding shards for upload %s: %w", uploadID, err)
			}
		}
	}
}

var _ uploads.UploadShardWorkerFn = API{}.UploadShardWorker

// AddShardsForUpload `space/blob/add`s all shards ready to add.
func (a API) AddShardsForUpload(ctx context.Context, uploadID id.UploadID) error {
	for {
		shards, err := a.Repo.ShardsForUploadByStatus(ctx, uploadID, model.ShardStateClosed)
		if err != nil {
			return fmt.Errorf("getting shards for upload %s: %w", uploadID, err)
		}
		if len(shards) == 0 {
			return nil // No closed shards found, exit the loop
		}
		for _, shard := range shards {
			if err := a.AddShard(ctx, shard); err != nil {
				return fmt.Errorf("adding shard %s: %w", shard.ID(), err)
			}
		}
	}
}

// AddShard adds a shard to the space
func (a API) AddShard(ctx context.Context, shard *model.Shard) error {
	mh, locCom, err := a.Client.SpaceBlobAdd(ctx, shard.Bytes(), a.Space, a.ReceiptsURL)
	if err != nil {
		return fmt.Errorf("adding shard %s to space %s: %w", shard.ID(), a.Space, err)
	}

	_ = mh     // TODO: Use the multihash if needed
	_ = locCom // TODO: Use the location comment if needed
	return nil
}
