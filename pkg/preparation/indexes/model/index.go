package model

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

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
	id              id.IndexID
	uploadID        id.UploadID
	digest          multihash.Multihash
	pieceCID        cid.Cid
	digestStateUpTo uint64
	digestState     []byte
	pieceCIDState   []byte
	state           IndexState
	location        invocation.Invocation
	pdpAccept       invocation.Invocation
}

func (i *Index) ID() id.IndexID {
	return i.id
}

func (i *Index) State() IndexState {
	return i.state
}

// func (i *Index) SliceCount() uint64 {
// 	return i.sliceCount
// }

func (i *Index) Digest() multihash.Multihash {
	return i.digest
}

func (i *Index) PieceCID() cid.Cid {
	return i.pieceCID
}

func (i *Index) DigestStateUpTo() uint64 {
	return i.digestStateUpTo
}

func (i *Index) DigestState() []byte {
	return i.digestState
}

func (i *Index) PieceCIDState() []byte {
	return i.pieceCIDState
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
