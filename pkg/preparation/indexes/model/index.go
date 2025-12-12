package model

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// IndexWriter is a function that writes index data to storage
type IndexWriter func(
	id id.IndexID,
	uploadID id.UploadID,
	digest multihash.Multihash,
	pieceCID cid.Cid,
	size uint64,
	sliceCount uint64,
	state IndexState,
	location invocation.Invocation,
	pdpAccept invocation.Invocation,
) error

// IndexState represents the state of a index.
type IndexState string

const (
	// IndexStateOpen indicates that a index is still accepting new nodes.
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
	sliceCount uint64
	state      IndexState
	location   invocation.Invocation
	pdpAccept  invocation.Invocation
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

func (i *Index) SliceCount() uint64 {
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

func (i *Index) Close(indexDigest multihash.Multihash, pieceCID cid.Cid, size uint64) error {
	if i.state != IndexStateOpen {
		return fmt.Errorf("cannot close index %s in state %s", i.id, i.state)
	}
	i.digest = indexDigest
	i.pieceCID = pieceCID
	i.size = size
	i.state = IndexStateClosed
	return nil
}

func (i *Index) SpaceBlobAdded(location invocation.Invocation, pdpAccept invocation.Invocation) {
	i.location = location
	i.pdpAccept = pdpAccept
}

func (i *Index) Added() error {
	if i.state != IndexStateClosed {
		return fmt.Errorf("cannot mark index %s as added in state %s", i.id, i.state)
	}
	i.state = IndexStateAdded
	return nil
}

func NewIndex(uploadID id.UploadID) (*Index, error) {
	idx := &Index{
		id:         id.New(),
		uploadID:   uploadID,
		digest:     multihash.Multihash{},
		pieceCID:   cid.Undef,
		size:       0,
		sliceCount: 0,
		state:      IndexStateOpen,
	}
	return idx, nil
}

func WriteIndexToDatabase(index *Index, writer IndexWriter) error {
	return writer(
		index.id,
		index.uploadID,
		index.digest,
		index.pieceCID,
		index.size,
		index.sliceCount,
		index.state,
		index.location,
		index.pdpAccept,
	)
}

// ReadIndexFromDatabase creates an Index from database values
func ReadIndexFromDatabase(
	id id.IndexID,
	uploadID id.UploadID,
	digest multihash.Multihash,
	pieceCID cid.Cid,
	size uint64,
	sliceCount uint64,
	state IndexState,
	location invocation.Invocation,
	pdpAccept invocation.Invocation,
) (*Index, error) {
	if !validIndexState(state) {
		return nil, fmt.Errorf("invalid index state: %s", state)
	}

	return &Index{
		id:         id,
		uploadID:   uploadID,
		digest:     digest,
		pieceCID:   pieceCID,
		size:       size,
		sliceCount: sliceCount,
		state:      state,
		location:   location,
		pdpAccept:  pdpAccept,
	}, nil
}
