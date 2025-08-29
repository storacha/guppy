package shards

import (
	"context"

	"github.com/ipfs/go-cid"
	configurationsmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Repo defines the interface for interacting with DAG scans, nodes, and links in the repository.
type Repo interface {
	CreateShard(ctx context.Context, uploadID id.UploadID, size uint64) (*model.Shard, error)
	UpdateShard(ctx context.Context, shard *model.Shard) error
	ShardsForUploadByStatus(ctx context.Context, uploadID id.UploadID, state model.ShardState) ([]*model.Shard, error)
	GetConfigurationByUploadID(ctx context.Context, uploadID id.UploadID) (*configurationsmodel.Configuration, error)
	AddNodeToShard(ctx context.Context, shardID id.ShardID, nodeCID cid.Cid, offset uint64) error
	FindNodeByCid(ctx context.Context, c cid.Cid) (dagsmodel.Node, error)
	ForEachNode(ctx context.Context, shardID id.ShardID, yield func(node dagsmodel.Node, shardOffset uint64) error) error
}
