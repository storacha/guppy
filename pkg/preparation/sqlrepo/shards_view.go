package sqlrepo

import (
	"time"

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

type DAGScanView struct {
	FSEntryID id.FSEntryID
	Created   time.Time
	Updated   time.Time
	CID       cid.Cid
}

type FSScanView struct {
	Path      string
	IsDir     bool
	Size      uint64
	FSEntryID id.FSEntryID
}
