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
	CreateShard(ctx context.Context, uploadID id.UploadID, size uint64, digestState, pieceCidState []byte) (*model.Shard, error)
	UpdateShard(ctx context.Context, shard *model.Shard) error
	ShardsForUploadByState(ctx context.Context, uploadID id.UploadID, state model.ShardState) ([]*model.Shard, error)
	AddNodeToShard(ctx context.Context, shardID id.ShardID, nodeCID cid.Cid, spaceDID did.DID, offset uint64, options ...AddNodeToShardOption) error
	FindNodeByCIDAndSpaceDID(ctx context.Context, c cid.Cid, spaceDID did.DID) (dagsmodel.Node, error)
	ForEachNode(ctx context.Context, shardID id.ShardID, yield func(node dagsmodel.Node, shardOffset uint64) error) error
	// NodesByShard fetches all the nodes for a given shard, returned in the order
	// they should appear in the shard.
	NodesByShard(ctx context.Context, shardID id.ShardID, startOffset uint64) ([]dagsmodel.Node, error)
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
	// HeaderDigestState returns the digest state bytes used to jumpstart digest calculation
	HeaderDigestState() []byte
	// HeaderPieceCIDState returns the piece CID state bytes used to jumpstart piece cid calculation
	HeaderPieceCIDState() []byte
}

type (
	DigestStateUpdate struct {
		digestStateUpTo uint64
		digestState     []byte
		pieceCIDState   []byte
	}

	AddNodeConfig struct {
		digestStateUpdate *DigestStateUpdate
	}
)

func (d *DigestStateUpdate) DigestStateUpTo() uint64 {
	return d.digestStateUpTo
}
func (d *DigestStateUpdate) DigestState() []byte {
	return d.digestState
}
func (d *DigestStateUpdate) PieceCIDState() []byte {
	return d.pieceCIDState
}

func (a *AddNodeConfig) HasDigestStateUpdate() bool {
	return a.digestStateUpdate != nil
}
func (a *AddNodeConfig) DigestStateUpdate() *DigestStateUpdate {
	return a.digestStateUpdate
}

type AddNodeToShardOption func(cfg *AddNodeConfig)

func WithDigestStateUpdate(digestStateUpTo uint64, digestState []byte, pieceCIDState []byte) AddNodeToShardOption {
	return func(cfg *AddNodeConfig) {
		cfg.digestStateUpdate = &DigestStateUpdate{
			digestStateUpTo: digestStateUpTo,
			digestState:     digestState,
			pieceCIDState:   pieceCIDState,
		}
	}
}

func NewAddNodeConfig(options ...AddNodeToShardOption) *AddNodeConfig {
	cfg := &AddNodeConfig{}
	for _, option := range options {
		option(cfg)
	}
	return cfg
}
