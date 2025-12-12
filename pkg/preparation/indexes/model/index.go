package model

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// IndexState represents the state of a index.
type IndexState string

const (
	// IndexStateOpen indicates that a index is still accepting new indexes.
	IndexStateOpen IndexState = "open"
	// IndexStateClosed indicates that a index is no longer accepting nodes, but
	// is not yet added to the space.
	IndexStateClosed IndexState = "closed"
	// IndexStateAdded indicates that a index has been added to the space.
	IndexStateAdded IndexState = "added"
)

func validIndexState(state IndexState) bool {
	switch state {
	case IndexStateOpen, IndexStateClosed, IndexStateAdded:
		return true
	default:
		return false
	}
}

// TK: Extract commonalities with Index?
type Index struct {
	id         id.IndexID
	uploadID   id.UploadID
	digest     multihash.Multihash
	pieceCID   cid.Cid
	size       uint64
	sliceCount int
	state      IndexState
	location   invocation.Invocation
	pdpAccept  invocation.Invocation
}

var _ types.Blob = (*Index)(nil)

// NewIndex creates a new Index with the given initial size.
func NewIndex(uploadID id.UploadID) (*Index, error) {
	s := &Index{
		id:       id.New(),
		uploadID: uploadID,
		digest:   multihash.Multihash{},
		pieceCID: cid.Undef,
		state:    IndexStateOpen,
	}
	if _, err := validateIndex(s); err != nil {
		return nil, fmt.Errorf("failed to create Index: %w", err)
	}
	return s, nil
}

// validation conditions -- should not be callable externally, all Indexes outside this module MUST be valid
func validateIndex(s *Index) (*Index, error) {
	if s.uploadID == id.Nil {
		return nil, types.EmptyError{Field: "uploadID"}
	}
	if !validIndexState(s.state) {
		return nil, fmt.Errorf("invalid index state: %s", s.state)
	}
	return s, nil
}

// State changes

func (i *Index) Close() error {
	if i.state != IndexStateOpen {
		return fmt.Errorf("cannot set digest for index %s in state %s", i.id, i.state)
	}
	i.state = IndexStateClosed
	return nil
}

func (i *Index) Added() error {
	if i.state != IndexStateClosed {
		return fmt.Errorf("cannot mark index %s as added in state %s", i.id, i.state)
	}
	i.state = IndexStateAdded
	return nil
}

type IndexScanner func(
	id *id.IndexID,
	uploadID *id.UploadID,
	size *uint64,
	sliceCount *int,
	digest *multihash.Multihash,
	pieceCID *cid.Cid,
	state *IndexState,
	location *invocation.Invocation,
	pdpAccept *invocation.Invocation,
) error

func ReadIndexFromDatabase(scanner IndexScanner) (*Index, error) {
	index := &Index{}
	err := scanner(
		&index.id,
		&index.uploadID,
		&index.size,
		&index.sliceCount,
		&index.digest,
		&index.pieceCID,
		&index.state,
		&index.location,
		&index.pdpAccept,
	)
	if err != nil {
		return nil, fmt.Errorf("reading index from database: %w", err)
	}
	return validateIndex(index)
}

// IndexWriter is a function type for writing a Index to the database.
type IndexWriter func(
	id id.IndexID,
	uploadID id.UploadID,
	size uint64,
	sliceCount int,
	digest multihash.Multihash,
	pieceCID cid.Cid,
	state IndexState,
	location invocation.Invocation,
	pdpAccept invocation.Invocation,
) error

// WriteIndexToDatabase writes a Index to the database using the provided writer function.
func WriteIndexToDatabase(index *Index, writer IndexWriter) error {
	return writer(
		index.id,
		index.uploadID,
		index.size,
		index.sliceCount,
		index.digest,
		index.pieceCID,
		index.state,
		index.location,
		index.pdpAccept,
	)
}

func (i *Index) ID() id.IndexID {
	return i.id
}

func (i *Index) UploadID() id.UploadID {
	return i.uploadID
}

func (i *Index) State() IndexState {
	return i.state
}

func (i *Index) SliceCount() int {
	return i.sliceCount
}

func (i *Index) Digest() multihash.Multihash {
	return i.digest
}

func (i *Index) PieceCID() cid.Cid {
	return i.pieceCID
}

func (i *Index) Size() uint64 {
	return i.size
}

func (i *Index) CID() cid.Cid {
	if i.digest == nil {
		return cid.Undef
	}
	return cid.NewCidV1(uint64(multicodec.Car), i.digest)
}

func (i *Index) Location() invocation.Invocation {
	return i.location
}

func (i *Index) PDPAccept() invocation.Invocation {
	return i.pdpAccept
}

func (i *Index) String() string {
	if i.CID() != cid.Undef {
		return fmt.Sprintf("Index[id=%s, cid=%s]", i.id, i.CID())
	}
	return fmt.Sprintf("Index[id=%s]", i.id)
}

// SpaceBlobAdded records the result of a successful `space/blob/add`. The shard
// must be in `ShardStateClosed`, or it should not have been `space/blob/add`ed
// to begin with. `location` must be non-nil, because it represents the fact
// that the shard has truly been added. The `pdpAccept` may be nil. The `digest`
// and `size` will be recorded as well.
func (i *Index) SpaceBlobAdded(addedBlob client.AddedBlob) error {
	if i.state != IndexStateClosed {
		return fmt.Errorf("cannot add index in state %s", i.state)
	}
	if addedBlob.Location == nil {
		return fmt.Errorf("location invocation cannot be nil")
	}
	i.digest = addedBlob.Digest
	i.size = addedBlob.Size
	i.location = addedBlob.Location
	i.pdpAccept = addedBlob.PDPAccept
	return nil
}
