package jsonout

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/did"
	"github.com/stretchr/testify/require"

	"github.com/storacha/guppy/pkg/bus"
	"github.com/storacha/guppy/pkg/bus/events"
	blobsmodel "github.com/storacha/guppy/pkg/preparation/blobs/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

func TestJSONEmitter_Emit(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewJSONEmitter(&buf)

	emitter.Emit(map[string]string{"type": "test", "value": "hello"})

	var got map[string]string
	err := json.Unmarshal(buf.Bytes(), &got)
	require.NoError(t, err)
	require.Equal(t, "test", got["type"])
	require.Equal(t, "hello", got["value"])
}

func TestJSONEmitter_ThreadSafety(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewJSONEmitter(&buf)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			emitter.Emit(map[string]int{"n": n})
		}(i)
	}
	wg.Wait()

	// Each line should be valid JSON
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	require.Len(t, lines, 100)
	for _, line := range lines {
		var got map[string]int
		err := json.Unmarshal(line, &got)
		require.NoError(t, err)
	}
}

func testUpload(t *testing.T) *uploadsmodel.Upload {
	t.Helper()
	spaceDID, err := did.Parse("did:web:test.storacha.network")
	require.NoError(t, err)
	u, err := uploadsmodel.NewUpload(spaceDID, id.New())
	require.NoError(t, err)
	return u
}

func TestSubscribe_FSScan(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewJSONEmitter(&buf)
	eb := bus.New()
	u := testUpload(t)

	err := Subscribe(emitter, eb, []*uploadsmodel.Upload{u})
	require.NoError(t, err)

	fsEntryID := id.New()
	eb.Publish(events.TopicFsEntry(u.SourceID()), events.FSScanView{
		Path:      "/data/file.bin",
		IsDir:     false,
		Size:      1024,
		FSEntryID: fsEntryID,
	})

	// Give the synchronous handler time to write
	time.Sleep(10 * time.Millisecond)

	var got FSScanEvent
	err = json.Unmarshal(buf.Bytes(), &got)
	require.NoError(t, err)
	require.Equal(t, "fs_scan", got.Type)
	require.Equal(t, u.SourceID().String(), got.SourceID)
	require.Equal(t, u.ID().String(), got.UploadID)
	require.Equal(t, fsEntryID.String(), got.FSEntryID)
	require.Equal(t, "/data/file.bin", got.Path)
	require.False(t, got.IsDir)
	require.Equal(t, uint64(1024), got.Size)
}

func TestSubscribe_DAGScan_WithCID(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewJSONEmitter(&buf)
	eb := bus.New()
	u := testUpload(t)

	err := Subscribe(emitter, eb, []*uploadsmodel.Upload{u})
	require.NoError(t, err)

	// Create a test CID
	mh, err := multihash.Sum([]byte("test"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	testCID := cid.NewCidV1(cid.Raw, mh)

	now := time.Now().UTC()
	eb.Publish(events.TopicDagScan(u.ID()), events.DAGScanView{
		FSEntryID: id.New(),
		Created:   now,
		Updated:   now,
		CID:       testCID,
	})

	time.Sleep(10 * time.Millisecond)

	var got DAGScanEvent
	err = json.Unmarshal(buf.Bytes(), &got)
	require.NoError(t, err)
	require.Equal(t, "dag_scan", got.Type)
	require.NotNil(t, got.CID)
	require.Equal(t, testCID.String(), *got.CID)
}

func TestSubscribe_DAGScan_UndefinedCID(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewJSONEmitter(&buf)
	eb := bus.New()
	u := testUpload(t)

	err := Subscribe(emitter, eb, []*uploadsmodel.Upload{u})
	require.NoError(t, err)

	now := time.Now().UTC()
	eb.Publish(events.TopicDagScan(u.ID()), events.DAGScanView{
		FSEntryID: id.New(),
		Created:   now,
		Updated:   now,
		CID:       cid.Undef,
	})

	time.Sleep(10 * time.Millisecond)

	var got DAGScanEvent
	err = json.Unmarshal(buf.Bytes(), &got)
	require.NoError(t, err)
	require.Equal(t, "dag_scan", got.Type)
	require.Nil(t, got.CID)
}

func TestSubscribe_Shard(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewJSONEmitter(&buf)
	eb := bus.New()
	u := testUpload(t)

	err := Subscribe(emitter, eb, []*uploadsmodel.Upload{u})
	require.NoError(t, err)

	mh, err := multihash.Sum([]byte("shard-data"), multihash.SHA2_256, -1)
	require.NoError(t, err)

	shardID := id.New()
	eb.Publish(events.TopicShard(u.ID()), events.ShardView{
		ID:       shardID,
		UploadID: u.ID(),
		Size:     4194304,
		Digest:   mh,
		State:    blobsmodel.BlobStateClosed,
		// Location and PDPAccept left nil
	})

	time.Sleep(10 * time.Millisecond)

	var got ShardEvent
	err = json.Unmarshal(buf.Bytes(), &got)
	require.NoError(t, err)
	require.Equal(t, "shard", got.Type)
	require.Equal(t, shardID.String(), got.ShardID)
	require.Equal(t, u.ID().String(), got.UploadID)
	require.Equal(t, uint64(4194304), got.Size)
	require.NotNil(t, got.Digest)
	require.Equal(t, "closed", got.State)
	require.Nil(t, got.Location)
	require.Nil(t, got.PDPAccept)
}

func TestSubscribe_PutProgress(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewJSONEmitter(&buf)
	eb := bus.New()
	u := testUpload(t)

	err := Subscribe(emitter, eb, []*uploadsmodel.Upload{u})
	require.NoError(t, err)

	blobID := id.New()
	eb.Publish(events.TopicClientPut(u.ID()), events.PutProgress{
		BlobID:   blobID,
		Uploaded: 2048,
		Total:    4096,
	})

	time.Sleep(10 * time.Millisecond)

	var got PutProgressEvent
	err = json.Unmarshal(buf.Bytes(), &got)
	require.NoError(t, err)
	require.Equal(t, "put_progress", got.Type)
	require.Equal(t, u.ID().String(), got.UploadID)
	require.Equal(t, blobID.String(), got.BlobID)
	require.Equal(t, int64(2048), got.Uploaded)
	require.Equal(t, uint64(4096), got.Total)
}

func TestSubscribe_Worker(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewJSONEmitter(&buf)
	eb := bus.New()
	u := testUpload(t)

	err := Subscribe(emitter, eb, []*uploadsmodel.Upload{u})
	require.NoError(t, err)

	eb.Publish(events.TopicWorker(u.ID()), events.UploadWorkerEvent{
		Name:   "Scan-FS",
		Status: events.Running,
	})

	time.Sleep(10 * time.Millisecond)

	var got WorkerEvent
	err = json.Unmarshal(buf.Bytes(), &got)
	require.NoError(t, err)
	require.Equal(t, "worker", got.Type)
	require.Equal(t, u.ID().String(), got.UploadID)
	require.Equal(t, "Scan-FS", got.Name)
	require.Equal(t, "Running", got.Status)
	require.Nil(t, got.Error)
}

func TestSubscribe_Worker_WithError(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewJSONEmitter(&buf)
	eb := bus.New()
	u := testUpload(t)

	err := Subscribe(emitter, eb, []*uploadsmodel.Upload{u})
	require.NoError(t, err)

	eb.Publish(events.TopicWorker(u.ID()), events.UploadWorkerEvent{
		Name:   "Upload-Shard",
		Status: events.Failed,
		Error:  errors.New("connection refused"),
	})

	time.Sleep(10 * time.Millisecond)

	var got WorkerEvent
	err = json.Unmarshal(buf.Bytes(), &got)
	require.NoError(t, err)
	require.Equal(t, "worker", got.Type)
	require.Equal(t, "Failed", got.Status)
	require.NotNil(t, got.Error)
	require.Equal(t, "connection refused", *got.Error)
}

func TestEmitLifecycleEvents(t *testing.T) {
	var buf bytes.Buffer
	emitter := NewJSONEmitter(&buf)
	u := testUpload(t)

	emitter.EmitUploadStart(u)
	emitter.EmitUploadComplete(u.ID(), "bafytest123")
	emitter.EmitUploadError(u.ID(), errors.New("something broke"), 3)

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	require.Len(t, lines, 3)

	var start UploadStartEvent
	require.NoError(t, json.Unmarshal(lines[0], &start))
	require.Equal(t, "upload_start", start.Type)
	require.Equal(t, u.ID().String(), start.UploadID)
	require.Equal(t, u.SourceID().String(), start.SourceID)

	var complete UploadCompleteEvent
	require.NoError(t, json.Unmarshal(lines[1], &complete))
	require.Equal(t, "upload_complete", complete.Type)
	require.Equal(t, "bafytest123", complete.RootCID)

	var errEvt UploadErrorEvent
	require.NoError(t, json.Unmarshal(lines[2], &errEvt))
	require.Equal(t, "upload_error", errEvt.Type)
	require.Equal(t, "something broke", errEvt.Error)
	require.Equal(t, 3, errEvt.Attempt)
}
