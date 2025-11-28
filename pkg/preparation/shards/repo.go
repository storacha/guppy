package shards

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Repo defines the interface for interacting with DAG scans, nodes, and links in the repository.
type Repo interface {
	CreateShard(ctx context.Context, uploadID id.UploadID, size uint64) (*model.Shard, error)
	UpdateShard(ctx context.Context, shard *model.Shard) error
	ShardsForUploadByState(ctx context.Context, uploadID id.UploadID, state model.ShardState) ([]*model.Shard, error)
	AddNodeToShard(ctx context.Context, shardID id.ShardID, nodeCID cid.Cid, spaceDID did.DID, offset uint64) error
	FindNodeByCIDAndSpaceDID(ctx context.Context, c cid.Cid, spaceDID did.DID) (dagsmodel.Node, error)
	ForEachNode(ctx context.Context, shardID id.ShardID, yield func(node dagsmodel.Node, shardOffset uint64) error) error
	// NodesByShard fetches all the nodes for a given shard, returned in the order
	// they should appear in the shard.
	NodesByShard(ctx context.Context, shardID id.ShardID) ([]dagsmodel.Node, error)
	GetSpaceByDID(ctx context.Context, spaceDID did.DID) (*spacesmodel.Space, error)
	DeleteShard(ctx context.Context, shardID id.ShardID) error
}

// ShardEncoder is the interface for shard implementations.
type ShardEncoder interface {
	// WriteHeader writes a header to the provided writer.
	WriteHeader(ctx context.Context, w io.Writer) error
	// WriteNode writes a node to the provided writer.
	WriteNode(ctx context.Context, node dagsmodel.Node, data []byte, w io.Writer) error
	// NodeEncodingLength determines the number of bytes a node will occupy when
	// encoded in a shard.
	NodeEncodingLength(node dagsmodel.Node) uint64
	// HeaderEncodingLength returns the number of bytes of preamble that will be
	// written when a shard is encoded. Note: this may be 0.
	HeaderEncodingLength() uint64
}
