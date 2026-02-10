package jsonout

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/storacha/go-libstoracha/digestutil"

	"github.com/storacha/guppy/pkg/bus"
	"github.com/storacha/guppy/pkg/bus/events"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

// JSONEmitter writes JSON events as newline-delimited JSON (NDJSON) to a writer.
// It is safe for concurrent use.
type JSONEmitter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewJSONEmitter creates a new JSONEmitter that writes to w.
func NewJSONEmitter(w io.Writer) *JSONEmitter {
	return &JSONEmitter{enc: json.NewEncoder(w)}
}

// Emit writes v as a single JSON line.
func (e *JSONEmitter) Emit(v any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = e.enc.Encode(v)
}

// EmitUploadStart emits a lifecycle event when an upload begins.
func (e *JSONEmitter) EmitUploadStart(u *uploadsmodel.Upload) {
	e.Emit(UploadStartEvent{
		Type:     "upload_start",
		UploadID: u.ID().String(),
		SourceID: u.SourceID().String(),
	})
}

// EmitUploadComplete emits a lifecycle event when an upload finishes successfully.
func (e *JSONEmitter) EmitUploadComplete(uploadID id.UploadID, rootCID string) {
	e.Emit(UploadCompleteEvent{
		Type:     "upload_complete",
		UploadID: uploadID.String(),
		RootCID:  rootCID,
	})
}

// EmitUploadError emits a lifecycle event when an upload fails.
func (e *JSONEmitter) EmitUploadError(uploadID id.UploadID, err error, attempt int) {
	e.Emit(UploadErrorEvent{
		Type:     "upload_error",
		UploadID: uploadID.String(),
		Error:    err.Error(),
		Attempt:  attempt,
	})
}

// Subscribe registers bus event handlers that emit NDJSON for each event.
func Subscribe(emitter *JSONEmitter, sub bus.Subscriber, uploads []*uploadsmodel.Upload) error {
	for _, u := range uploads {
		uid := u.ID()
		uidStr := uid.String()
		srcIDStr := u.SourceID().String()

		if err := sub.Subscribe(events.TopicFsEntry(u.SourceID()), func(evt events.FSScanView) {
			emitter.Emit(FSScanEvent{
				Type:      "fs_scan",
				SourceID:  srcIDStr,
				UploadID:  uidStr,
				FSEntryID: evt.FSEntryID.String(),
				Path:      evt.Path,
				IsDir:     evt.IsDir,
				Size:      evt.Size,
			})
		}); err != nil {
			return fmt.Errorf("subscribing to fs entry events: %w", err)
		}

		if err := sub.Subscribe(events.TopicDagScan(uid), func(evt events.DAGScanView) {
			var cidStr *string
			if evt.CID.Defined() {
				s := evt.CID.String()
				cidStr = &s
			}
			emitter.Emit(DAGScanEvent{
				Type:      "dag_scan",
				UploadID:  uidStr,
				FSEntryID: evt.FSEntryID.String(),
				Created:   evt.Created.Format(time.RFC3339Nano),
				Updated:   evt.Updated.Format(time.RFC3339Nano),
				CID:       cidStr,
			})
		}); err != nil {
			return fmt.Errorf("subscribing to dag scan events: %w", err)
		}

		if err := sub.Subscribe(events.TopicShard(uid), func(evt events.ShardView) {
			se := ShardEvent{
				Type:     "shard",
				ShardID:  evt.ID.String(),
				UploadID: evt.UploadID.String(),
				Size:     evt.Size,
				State:    string(evt.State),
			}
			if len(evt.Digest) > 0 {
				s := digestutil.Format(evt.Digest)
				se.Digest = &s
			}
			if evt.PieceCID.Defined() {
				s := evt.PieceCID.String()
				se.PieceCID = &s
			}
			if evt.Location != nil {
				s := evt.Location.Link().String()
				se.Location = &s
			}
			if evt.PDPAccept != nil {
				s := evt.PDPAccept.Link().String()
				se.PDPAccept = &s
			}
			emitter.Emit(se)
		}); err != nil {
			return fmt.Errorf("subscribing to shard events: %w", err)
		}

		if err := sub.Subscribe(events.TopicClientPut(uid), func(evt events.PutProgress) {
			emitter.Emit(PutProgressEvent{
				Type:     "put_progress",
				UploadID: uidStr,
				BlobID:   evt.BlobID.String(),
				Uploaded: evt.Uploaded,
				Total:    evt.Total,
			})
		}); err != nil {
			return fmt.Errorf("subscribing to put progress events: %w", err)
		}

		if err := sub.Subscribe(events.TopicWorker(uid), func(evt events.UploadWorkerEvent) {
			we := WorkerEvent{
				Type:     "worker",
				UploadID: uidStr,
				Name:     evt.Name,
				Status:   string(evt.Status),
			}
			if evt.Error != nil {
				s := evt.Error.Error()
				we.Error = &s
			}
			emitter.Emit(we)
		}); err != nil {
			return fmt.Errorf("subscribing to worker events: %w", err)
		}
	}
	return nil
}

// Event types for NDJSON output. Each has a "type" discriminator field.

type FSScanEvent struct {
	Type      string `json:"type"`
	SourceID  string `json:"source_id"`
	UploadID  string `json:"upload_id"`
	FSEntryID string `json:"fs_entry_id"`
	Path      string `json:"path"`
	IsDir     bool   `json:"is_dir"`
	Size      uint64 `json:"size"`
}

type DAGScanEvent struct {
	Type      string  `json:"type"`
	UploadID  string  `json:"upload_id"`
	FSEntryID string  `json:"fs_entry_id"`
	Created   string  `json:"created"`
	Updated   string  `json:"updated"`
	CID       *string `json:"cid"`
}

type ShardEvent struct {
	Type      string  `json:"type"`
	ShardID   string  `json:"shard_id"`
	UploadID  string  `json:"upload_id"`
	Size      uint64  `json:"size"`
	Digest    *string `json:"digest,omitempty"`
	PieceCID  *string `json:"piece_cid,omitempty"`
	State     string  `json:"state"`
	Location  *string `json:"location,omitempty"`
	PDPAccept *string `json:"pdp_accept,omitempty"`
}

type PutProgressEvent struct {
	Type     string `json:"type"`
	UploadID string `json:"upload_id"`
	BlobID   string `json:"blob_id"`
	Uploaded int64  `json:"uploaded"`
	Total    uint64 `json:"total"`
}

type WorkerEvent struct {
	Type     string  `json:"type"`
	UploadID string  `json:"upload_id"`
	Name     string  `json:"name"`
	Status   string  `json:"status"`
	Error    *string `json:"error,omitempty"`
}

type UploadStartEvent struct {
	Type     string `json:"type"`
	UploadID string `json:"upload_id"`
	SourceID string `json:"source_id"`
}

type UploadCompleteEvent struct {
	Type     string `json:"type"`
	UploadID string `json:"upload_id"`
	RootCID  string `json:"root_cid"`
}

type UploadErrorEvent struct {
	Type     string `json:"type"`
	UploadID string `json:"upload_id"`
	Error    string `json:"error"`
	Attempt  int    `json:"attempt"`
}
