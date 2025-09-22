package shards

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-varint"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
)

// Byte length of a CBOR encoded CAR header with zero roots.
const noRootsHeaderLen = 17

var log = logging.Logger("preparation/shards")

// API provides methods to interact with the Shards in the repository.
type API struct {
	Repo Repo
}

var _ uploads.AddNodeToUploadShardsFunc = API{}.AddNodeToUploadShards
var _ uploads.CloseUploadShardsFunc = API{}.CloseUploadShards

func (a API) AddNodeToUploadShards(ctx context.Context, uploadID id.UploadID, nodeCID cid.Cid) (bool, error) {
	space, err := a.Repo.GetSpaceByUploadID(ctx, uploadID)
	if err != nil {
		return false, fmt.Errorf("failed to get space for upload %s: %w", uploadID, err)
	}
	openShards, err := a.Repo.ShardsForUploadByStatus(ctx, uploadID, model.ShardStateOpen)
	if err != nil {
		return false, fmt.Errorf("failed to get open shards for upload %s: %w", uploadID, err)
	}

	var shard *model.Shard
	var closed bool

	// Look for an open shard that has room for the node, and close any that don't
	// have room. (There should only be at most one open shard, but there's no
	// harm handling multiple if they exist.)
	for _, s := range openShards {
		hasRoom, err := a.roomInShard(ctx, s, nodeCID, space)
		if err != nil {
			return false, fmt.Errorf("failed to check room in shard %s for node %s: %w", s.ID(), nodeCID, err)
		}
		if hasRoom {
			shard = s
			break
		}
		s.Close()
		if err := a.Repo.UpdateShard(ctx, s); err != nil {
			return false, fmt.Errorf("updating scan: %w", err)
		}
		closed = true
	}

	// If no such shard exists, create a new one
	if shard == nil {
		shard, err = a.Repo.CreateShard(ctx, uploadID)
		if err != nil {
			return false, fmt.Errorf("failed to add node %s to shards for upload %s: %w", nodeCID, uploadID, err)
		}
	}

	err = a.Repo.AddNodeToShard(ctx, shard.ID(), nodeCID, space.DID())
	if err != nil {
		return false, fmt.Errorf("failed to add node %s to shard %s for upload %s: %w", nodeCID, shard.ID(), uploadID, err)
	}
	return closed, nil
}

func (a *API) roomInShard(ctx context.Context, shard *model.Shard, nodeCID cid.Cid, space *spacesmodel.Space) (bool, error) {
	node, err := a.Repo.FindNodeByCidAndSpaceDID(ctx, nodeCID, space.DID())
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

	if currentSize+nodeSize > space.ShardSize() {
		return false, nil // No room in the shard
	}

	return true, nil
}

func (a *API) currentSizeOfShard(ctx context.Context, shardID id.ShardID) (uint64, error) {
	var totalSize uint64 = noRootsHeaderLen

	err := a.Repo.ForEachNode(ctx, shardID, func(node dagsmodel.Node) error {
		totalSize += nodeEncodingLength(node.CID(), node.Size())
		return nil
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

func (a API) CloseUploadShards(ctx context.Context, uploadID id.UploadID) (bool, error) {
	openShards, err := a.Repo.ShardsForUploadByStatus(ctx, uploadID, model.ShardStateOpen)
	if err != nil {
		return false, fmt.Errorf("failed to get open shards for upload %s: %w", uploadID, err)
	}

	var closed bool

	for _, s := range openShards {
		s.Close()
		if err := a.Repo.UpdateShard(ctx, s); err != nil {
			return false, fmt.Errorf("updating shard %s for upload %s: %w", s.ID(), uploadID, err)
		}
		closed = true
	}

	return closed, nil
}
