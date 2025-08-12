package indexes

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/indexes/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// ShardInfo contains basic shard information
type ShardInfo struct {
	ID  id.ShardID
	CID cid.Cid
}

// Repo defines the interface for interacting with index manifests and shard blocks
type Repo interface {
	// Track block positions in shards
	AddShardBlock(ctx context.Context, shardID id.ShardID, blockCID cid.Cid, offset, size uint64) error
	GetShardBlocks(ctx context.Context, shardID id.ShardID) ([]*model.ShardBlock, error)

	// Index manifest operations
	SaveIndexManifest(ctx context.Context, uploadID id.UploadID, manifestJSON []byte) error
	GetIndexManifest(ctx context.Context, uploadID id.UploadID) (*model.IndexManifest, error)

	// Get shards for an upload
	GetShardsForUpload(ctx context.Context, uploadID id.UploadID) ([]ShardInfo, error)
}
