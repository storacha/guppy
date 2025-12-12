package storacha

import (
	"context"

	indexesmodel "github.com/storacha/guppy/pkg/preparation/indexes/model"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

// Repo defines the interface for interacting with DAG scans, nodes, and links in the repository.
type Repo interface {
	UpdateShard(ctx context.Context, shard *model.Shard) error
	ShardsForUploadByState(ctx context.Context, uploadID id.UploadID, state model.ShardState) ([]*model.Shard, error)
	GetUploadByID(ctx context.Context, uploadID id.UploadID) (*uploadmodel.Upload, error)

	// Index methods
	UpdateIndex(ctx context.Context, index *indexesmodel.Index) error
	IndexesForUploadByState(ctx context.Context, uploadID id.UploadID, state indexesmodel.IndexState) ([]*indexesmodel.Index, error)
}
