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
	config, err := a.Repo.GetConfigurationByUploadID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get configuration for upload %s: %w", uploadID, err)
	}
	openShards, err := a.Repo.ShardsForUploadByStatus(ctx, uploadID, model.ShardStateOpen)
	if err != nil {
		return fmt.Errorf("failed to get open shards for upload %s: %w", uploadID, err)
	}

	var shard *model.Shard

	// Look for an open shard that has room for the node, and close any that don't
	// have room. (There should only be at most one open shard, but there's no
	// harm handling multiple if they exist.)
	for _, s := range openShards {
		hasRoom, err := a.Repo.RoomInShard(ctx, s, nodeCID, config)
		if err != nil {
			return fmt.Errorf("failed to check room in shard %s for node %s: %w", s.ID(), nodeCID, err)
		}
		if hasRoom {
			shard = s
			break
		}
		s.Close()
		if err := a.Repo.UpdateShard(ctx, s); err != nil {
			return fmt.Errorf("updating scan: %w", err)
		}
	}

	// If no such shard exists, create a new one
	if shard == nil {
		shard, err = a.Repo.CreateShard(ctx, uploadID)
		if err != nil {
			return fmt.Errorf("failed to add node %s to shards for upload %s: %w", nodeCID, uploadID, err)
		}
	}

	err = a.Repo.AddNodeToShard(ctx, shard.ID(), nodeCID)
	if err != nil {
		return fmt.Errorf("failed to add node %s to shard %s for upload %s: %w", nodeCID, shard.ID(), uploadID, err)
	}
	return nil
}

var _ uploads.AddNodeToUploadShardsFn = API{}.AddNodeToUploadShards

func (a API) UploadShardWorker(ctx context.Context, work <-chan struct{}, uploadID id.UploadID) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // Exit if the context is canceled
		case _, ok := <-work:
			if !ok {
				return nil // Channel closed, exit the loop
			}
			// space/blob/add all closed, not-yet-added shards for the given upload.
			if err := a.addShardsForUpload(ctx, uploadID); err != nil {
				return fmt.Errorf("adding shards for upload %s: %w", uploadID, err)
			}
		}
	}
}

var _ uploads.UploadShardWorkerFn = API{}.UploadShardWorker

// addShardsForUpload `space/blob/add`s all shards ready to add.
func (a API) addShardsForUpload(ctx context.Context, uploadID id.UploadID) error {
	for {
		shards, err := a.Repo.ShardsForUploadByStatus(ctx, uploadID, model.ShardStateClosed)
		if err != nil {
			return fmt.Errorf("getting shards for upload %s: %w", uploadID, err)
		}
		if len(shards) == 0 {
			return nil // No closed shards found, exit the loop
		}
		for _, shard := range shards {
			if err := a.addShard(ctx, shard); err != nil {
				return fmt.Errorf("adding shard %s: %w", shard.ID(), err)
			}
		}
	}
}

// addShard adds a shard to the space. This probably won't live here when it's
// actually implemented.
func (a API) addShard(ctx context.Context, shard *model.Shard) error {
	mh, locCom, err := a.Client.SpaceBlobAdd(ctx, shard.Bytes(), a.Space, a.ReceiptsURL)
	if err != nil {
		return fmt.Errorf("adding shard %s to space %s: %w", shard.ID(), a.Space, err)
	}

	_ = mh     // TODO: Use the multihash if needed
	_ = locCom // TODO: Use the location comment if needed
	return nil
}
