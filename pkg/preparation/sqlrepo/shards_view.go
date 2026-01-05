package sqlrepo

import (
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"

	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

type ShardView struct {
	ID        id.ShardID
	UploadID  id.UploadID
	Size      uint64
	Digest    multihash.Multihash
	PieceCID  cid.Cid
	State     model.BlobState
	Location  invocation.Invocation
	PDPAccept invocation.Invocation
}
