package events

import (
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"

	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

const (
	fsEntryTopic   = "event.fsentry"
	dagScanTopic   = "event.dagscan"
	shardTopic     = "event.shard"
	clientPutTopic = "event.client-put"
)

func TopicFsEntry(sid id.SourceID) string {
	return fmt.Sprintf("%s:%s", fsEntryTopic, sid.String())
}

func TopicDagScan(uid id.UploadID) string {
	return fmt.Sprintf("%s:%s", dagScanTopic, uid)
}

func TopicShard(uid id.UploadID) string {
	return fmt.Sprintf("%s:%s", shardTopic, uid)
}

func TopicClientPut(uid id.UploadID) string {
	return fmt.Sprintf("%s:%s", clientPutTopic, uid)
}

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

// PutProgress represents progress of an in-flight PUT upload.
type PutProgress struct {
	BlobID   id.ID
	Uploaded int64
	Total    uint64
}
