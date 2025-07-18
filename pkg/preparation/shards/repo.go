package shards

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Repo defines the interface for interacting with DAG scans, nodes, and links in the repository.
type Repo interface {
	AddNodeToUploadShard(ctx context.Context, uploadID id.UploadID, nodeCID cid.Cid) error
	ShardsForUploadByStatus(ctx context.Context, uploadID id.UploadID, state model.ShardState) ([]*model.Shard, error)
}
