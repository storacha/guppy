package shards

import (
	"context"
	"fmt"
	"net/url"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-varint"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
	configmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
)

// Byte length of a CBOR encoded CAR header with zero roots.
const noRootsHeaderLen = 17

var log = logging.Logger("preparation/shards")

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
		hasRoom, err := a.roomInShard(ctx, s, nodeCID, config)
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

func (a *API) roomInShard(ctx context.Context, shard *model.Shard, nodeCID cid.Cid, config *configmodel.Configuration) (bool, error) {
	node, err := a.Repo.FindNodeByCid(ctx, nodeCID)
	if err != nil {
		return false, fmt.Errorf("failed to find node %s: %w", nodeCID, err)
	}
	if node == nil {
		return false, fmt.Errorf("node %s not found", nodeCID)
	}
	nodeSize := nodeEncodingLength(nodeCID, node.Size())

	currentSize, err := a.currentSizeOfShard(ctx, shard.ID())
	if err != nil {
		return false, fmt.Errorf("failed to get current size of shard %s: %w", shard.ID(), err)
	}

	if currentSize+nodeSize > config.ShardSize() {
		return false, nil // No room in the shard
	}

	return true, nil
}

func (a *API) currentSizeOfShard(ctx context.Context, shardID id.ShardID) (uint64, error) {
	var totalSize uint64 = noRootsHeaderLen

	err := a.Repo.ForEachNodeCIDAndSize(ctx, shardID, func(cid cid.Cid, size uint64) {
		totalSize += nodeEncodingLength(cid, size)
	})
	if err != nil {
		return 0, fmt.Errorf("failed to iterate over nodes in shard %s: %w", shardID, err)
	}

	return totalSize, nil
}

func nodeEncodingLength(cid cid.Cid, blockSize uint64) uint64 {
	pllen := uint64(len(cidlink.Link{Cid: cid}.Binary())) + blockSize
	vilen := uint64(varint.UvarintSize(uint64(pllen)))
	return pllen + vilen
}

var _ uploads.AddNodeToUploadShardsFn = API{}.AddNodeToUploadShards

func (a API) UploadShardWorker(ctx context.Context, work <-chan struct{}, uploadID id.UploadID) error {
	log.Debugf("Starting upload shard worker for upload ID: %s", uploadID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // Exit if the context is canceled
		case _, ok := <-work:
			if !ok {
				return nil // Channel closed, exit the loop
			}
			log.Debugf("Would process shard here for %s", uploadID)
		}
	}
}

var _ uploads.UploadShardWorkerFn = API{}.UploadShardWorker
