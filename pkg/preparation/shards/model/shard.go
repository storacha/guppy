package model

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// ShardState represents the state of a shard.
type ShardState string

const (
	// ShardStateOpen indicates that a shard is still accepting new nodes.
	ShardStateOpen ShardState = "open"
	// ShardStateClosed indicates that a shard is no longer accepting nodes, but
	// is not yet added to the space.
	ShardStateClosed ShardState = "closed"
	// ShardStateAdded indicates that a shard has been added to the space.
	ShardStateAdded ShardState = "added"
)

func validShardState(state ShardState) bool {
	switch state {
	case ShardStateOpen, ShardStateClosed, ShardStateAdded:
		return true
	default:
		return false
	}
}

type Shard struct {
	id       id.ShardID
	uploadID id.UploadID
	cid      cid.Cid
	state    ShardState
}

// NewShard creates a new Shard with the given fsEntryID.
func NewShard(uploadID id.UploadID) (*Shard, error) {
	s := &Shard{
		id:       id.New(),
		uploadID: uploadID,
		cid:      cid.Undef,
		state:    ShardStateOpen,
	}
	if _, err := validateShard(s); err != nil {
		return nil, fmt.Errorf("failed to create Shard: %w", err)
	}
	return s, nil
}

// validation conditions -- should not be callable externally, all Shards outside this module MUST be valid
func validateShard(s *Shard) (*Shard, error) {
	if s.uploadID == id.Nil {
		return nil, types.ErrEmpty{Field: "uploadID"}
	}
	if !validShardState(s.state) {
		return nil, fmt.Errorf("invalid shard state: %s", s.state)
	}
	return s, nil
}

func (s *Shard) Close() error {
	if s.state != ShardStateOpen {
		return fmt.Errorf("cannot close shard in state %s", s.state)
	}
	s.state = ShardStateClosed
	return nil
}

type ShardScanner func(
	id *id.ShardID,
	uploadID *id.UploadID,
	cid *cid.Cid,
	state *ShardState,
) error

func ReadShardFromDatabase(scanner ShardScanner) (*Shard, error) {
	shard := &Shard{}
	err := scanner(
		&shard.id,
		&shard.uploadID,
		&shard.cid,
		&shard.state,
	)
	if err != nil {
		return nil, fmt.Errorf("reading shard from database: %w", err)
	}
	return validateShard(shard)
}

// ShardWriter is a function type for writing a Shard to the database.
type ShardWriter func(id id.ShardID, uploadID id.UploadID, cid cid.Cid, state ShardState) error

// WriteShardToDatabase writes a Shard to the database using the provided writer function.
func WriteShardToDatabase(shard *Shard, writer ShardWriter) error {
	return writer(
		shard.id,
		shard.uploadID,
		shard.cid,
		shard.state,
	)
}

func (s *Shard) ID() id.ShardID {
	return s.id
}

func (s *Shard) Bytes() io.Reader {
	return nil // TK: Replace with actual byte reader
}
