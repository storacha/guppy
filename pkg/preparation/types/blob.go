package types

import (
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

type Blob interface {
	ID() id.ID
	CID() cid.Cid
	Digest() multihash.Multihash
	PieceCID() cid.Cid
	Size() uint64
	Location() invocation.Invocation
	PDPAccept() invocation.Invocation
	SpaceBlobAdded(client.AddedBlob) error
	Added() error
}
