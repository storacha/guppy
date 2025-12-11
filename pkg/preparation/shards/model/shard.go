package model

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"
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
	id              id.ShardID
	uploadID        id.UploadID
	size            uint64
	sliceCount      uint64
	digest          multihash.Multihash
	pieceCID        cid.Cid
	digestStateUpTo uint64
	digestState     []byte
	pieceCIDState   []byte
	state           ShardState
	location        invocation.Invocation
	pdpAccept       invocation.Invocation
}

// NewShard creates a new Shard with the given initial size.
func NewShard(uploadID id.UploadID, size uint64, digestState []byte, pieceCIDState []byte) (*Shard, error) {
	s := &Shard{
		id:              id.New(),
		uploadID:        uploadID,
		size:            size,
		digest:          multihash.Multihash{},
		pieceCID:        cid.Undef,
		digestStateUpTo: size,
		digestState:     digestState,
		pieceCIDState:   pieceCIDState,
		state:           ShardStateOpen,
	}
	if _, err := validateShard(s); err != nil {
		return nil, fmt.Errorf("failed to create Shard: %w", err)
	}
	return s, nil
}

// validation conditions -- should not be callable externally, all Shards outside this module MUST be valid
func validateShard(s *Shard) (*Shard, error) {
	if s.uploadID == id.Nil {
		return nil, types.EmptyError{Field: "uploadID"}
	}
	if !validShardState(s.state) {
		return nil, fmt.Errorf("invalid shard state: %s", s.state)
	}
	return s, nil
}

// State changes
func (s *Shard) Close(digest multihash.Multihash, pieceCID cid.Cid) error {
	if s.state != ShardStateOpen {
		return fmt.Errorf("cannot close shard in state %s", s.state)
	}
	if digest == nil {
		return fmt.Errorf("CAR digest cannot be nil when closing shard")
	}

	s.digest = digest
	s.pieceCID = pieceCID

	s.state = ShardStateClosed
	return nil
}

func (s *Shard) Added() error {
	if s.state != ShardStateClosed {
		return fmt.Errorf("cannot add shard in state %s", s.state)
	}
	s.state = ShardStateAdded
	return nil
}

type ShardScanner func(
	id *id.ShardID,
	uploadID *id.UploadID,
	size *uint64,
	sliceCount *uint64,
	digest *multihash.Multihash,
	pieceCID *cid.Cid,
	digestStateUpTo *uint64,
	digestState *[]byte,
	pieceCIDState *[]byte,
	state *ShardState,
	location *invocation.Invocation,
	pdpAccept *invocation.Invocation,
) error

func ReadShardFromDatabase(scanner ShardScanner) (*Shard, error) {
	shard := &Shard{}
	err := scanner(
		&shard.id,
		&shard.uploadID,
		&shard.size,
		&shard.sliceCount,
		&shard.digest,
		&shard.pieceCID,
		&shard.digestStateUpTo,
		&shard.digestState,
		&shard.pieceCIDState,
		&shard.state,
		&shard.location,
		&shard.pdpAccept,
	)
	if err != nil {
		return nil, fmt.Errorf("reading shard from database: %w", err)
	}
	return validateShard(shard)
}

// ShardWriter is a function type for writing a Shard to the database.
type ShardWriter func(
	id id.ShardID,
	uploadID id.UploadID,
	size uint64,
	sliceCount uint64,
	digest multihash.Multihash,
	pieceCID cid.Cid,
	digestStateUpTo uint64,
	digestState []byte,
	pieceCIDState []byte,
	state ShardState,
	location invocation.Invocation,
	pdpAccept invocation.Invocation,
) error

// WriteShardToDatabase writes a Shard to the database using the provided writer function.
func WriteShardToDatabase(shard *Shard, writer ShardWriter) error {
	return writer(
		shard.id,
		shard.uploadID,
		shard.size,
		shard.sliceCount,
		shard.digest,
		shard.pieceCID,
		shard.digestStateUpTo,
		shard.digestState,
		shard.pieceCIDState,
		shard.state,
		shard.location,
		shard.pdpAccept,
	)
}

func (s *Shard) ID() id.ShardID {
	return s.id
}

func (s *Shard) State() ShardState {
	return s.state
}

func (s *Shard) Size() uint64 {
	return s.size
}

func (s *Shard) SliceCount() uint64 {
	return s.sliceCount
}

func (s *Shard) Digest() multihash.Multihash {
	return s.digest
}

func (s *Shard) PieceCID() cid.Cid {
	return s.pieceCID
}

func (s *Shard) DigestStateUpTo() uint64 {
	return s.digestStateUpTo
}

func (s *Shard) DigestState() []byte {
	return s.digestState
}

func (s *Shard) PieceCIDState() []byte {
	return s.pieceCIDState
}

func (s *Shard) CID() cid.Cid {
	if s.digest == nil {
		return cid.Undef
	}
	return cid.NewCidV1(uint64(multicodec.Car), s.digest)
}

func (s *Shard) Location() invocation.Invocation {
	return s.location
}

func (s *Shard) PDPAccept() invocation.Invocation {
	return s.pdpAccept
}

// SpaceBlobAdded records the location and PDP accept invocations for the shard
// after a successful `space/blob/add`. The shard must be in `ShardStateClosed`,
// or it should not have been `space/blob/add`ed to begin with. `location` must
// be non-nil, because it represents the fact that the shard has truly been
// added. The `pdpAccept` may be nil.
func (s *Shard) SpaceBlobAdded(location invocation.Invocation, pdpAccept invocation.Invocation) error {
	if s.state != ShardStateClosed {
		return fmt.Errorf("cannot add shard in state %s", s.state)
	}
	if location == nil {
		return fmt.Errorf("location invocation cannot be nil")
	}
	s.location = location
	s.pdpAccept = pdpAccept
	return nil
}
