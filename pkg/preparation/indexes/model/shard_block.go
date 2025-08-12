package model

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// ShardBlock represents a block's position within a shard (main model)
type ShardBlock struct {
	cid    cid.Cid
	offset uint64 // Byte offset within the CAR file
	size   uint64 // Size of the block in bytes
}

// NewShardBlock creates a new ShardBlock
func NewShardBlock(blockCID cid.Cid, offset, size uint64) (*ShardBlock, error) {
	block := &ShardBlock{
		cid:    blockCID,
		offset: offset,
		size:   size,
	}
	if err := validateShardBlock(block); err != nil {
		return nil, fmt.Errorf("failed to create ShardBlock: %w", err)
	}
	return block, nil
}

func validateShardBlock(sb *ShardBlock) error {
	if sb.cid == cid.Undef {
		return types.ErrEmpty{Field: "block CID"}
	}
	if sb.size == 0 {
		return types.ErrEmpty{Field: "block size"}
	}
	return nil
}

// Accessors
func (sb *ShardBlock) CID() cid.Cid {
	return sb.cid
}

func (sb *ShardBlock) Offset() uint64 {
	return sb.offset
}

func (sb *ShardBlock) Size() uint64 {
	return sb.size
}

// JSON-serializable version (for the index manifest)
type ShardBlockJSON struct {
	CID    string `json:"cid"`
	Offset uint64 `json:"offset"`
	Size   uint64 `json:"size"`
}

// Convert to JSON-serializable format
func (sb *ShardBlock) ToJSON() ShardBlockJSON {
	return ShardBlockJSON{
		CID:    sb.cid.String(),
		Offset: sb.offset,
		Size:   sb.size,
	}
}

// Database serialization functions
type ShardBlockWriter func(shardID id.ShardID, blockCID cid.Cid, offset, size uint64) error

func WriteShardBlockToDatabase(shardID id.ShardID, block *ShardBlock, writer ShardBlockWriter) error {
	return writer(shardID, block.cid, block.offset, block.size)
}

type ShardBlockScanner func(blockCID *cid.Cid, offset, size *uint64) error

func ReadShardBlockFromDatabase(scanner ShardBlockScanner) (*ShardBlock, error) {
	var blockCID cid.Cid
	var offset, size uint64

	if err := scanner(&blockCID, &offset, &size); err != nil {
		return nil, fmt.Errorf("reading shard block from database: %w", err)
	}

	return NewShardBlock(blockCID, offset, size)
}
