package ui

import (
	"context"
	"sort"
	"sync"

	"github.com/ipfs/go-cid"

	"github.com/storacha/guppy/pkg/bus"
	"github.com/storacha/guppy/pkg/bus/events"
	blobsmodel "github.com/storacha/guppy/pkg/preparation/blobs/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

type UploadObserver struct {
	sub  bus.Subscriber
	repo *sqlrepo.Repo

	states   map[id.UploadID]*UploadState
	statesMu sync.Mutex

	initOnceFn sync.Once
}

type UploadState struct {
	Model     *uploadsmodel.Upload
	SourceDir string

	PutProgress map[id.ID]PutProgress

	Shards map[id.ShardID]events.ShardView

	DagsFsMap   map[id.FSEntryID]cid.Cid
	DagsTotal   uint64
	DagsScanned uint64

	FsEntries     map[id.FSEntryID]FSEntry
	FsEntriesSize uint64

	WorkerStates map[string]events.UploadWorkerEvent
}

type FSEntry struct {
	Path  string
	IsDir bool
	Size  uint64
}

type PutProgress struct {
	Uploaded uint64
	Total    uint64
}

func NewUploadObserver(sub bus.Subscriber, repo *sqlrepo.Repo, uploads ...*uploadsmodel.Upload) *UploadObserver {
	states := make(map[id.UploadID]*UploadState, len(uploads))
	for _, upload := range uploads {
		states[upload.ID()] = &UploadState{
			Model:        upload,
			PutProgress:  make(map[id.ID]PutProgress),
			Shards:       make(map[id.ShardID]events.ShardView),
			DagsFsMap:    make(map[id.FSEntryID]cid.Cid),
			FsEntries:    make(map[id.FSEntryID]FSEntry),
			WorkerStates: make(map[string]events.UploadWorkerEvent),
		}
	}
	return &UploadObserver{
		sub:    sub,
		states: states,
		repo:   repo,
	}
}

type Observation struct {
	Model     *uploadsmodel.Upload
	SourceDir string

	OpenShards     []events.ShardView
	ClosedShards   []events.ShardView
	UploadedShards []events.ShardView
	AddedShards    []events.ShardView
	BytesRead      uint64
	FilesFound     uint64
	TotalDags      uint64
	ProcessedDags  uint64
	// ID is a blob ID
	ClientUploadProgress map[id.ID]PutProgress
	WorkerStates         map[string]events.UploadWorkerEvent
}

func (o *UploadObserver) Observe(ctx context.Context) []Observation {
	o.initOnceFn.Do(func() {
		if err := o.init(ctx); err != nil {
			panic(err)
		}
	})
	o.statesMu.Lock()
	defer o.statesMu.Unlock()

	out := make([]Observation, 0, len(o.states))
	for _, state := range o.states {
		progressCopy := make(map[id.ID]PutProgress, len(state.PutProgress))
		for k, v := range state.PutProgress {
			progressCopy[k] = v
		}
		workerStatesCopy := make(map[string]events.UploadWorkerEvent, len(state.WorkerStates))
		for k, v := range state.WorkerStates {
			workerStatesCopy[k] = v
		}

		out = append(out, Observation{
			Model:                state.Model,
			SourceDir:            state.SourceDir,
			OpenShards:           shardsByState(state.Shards, blobsmodel.BlobStateOpen),
			ClosedShards:         shardsByState(state.Shards, blobsmodel.BlobStateClosed),
			UploadedShards:       shardsByState(state.Shards, blobsmodel.BlobStateUploaded),
			AddedShards:          shardsByState(state.Shards, blobsmodel.BlobStateAdded),
			BytesRead:            state.FsEntriesSize,
			FilesFound:           uint64(len(state.FsEntries)),
			TotalDags:            state.DagsTotal,
			ProcessedDags:        state.DagsScanned,
			ClientUploadProgress: progressCopy,
			WorkerStates:         workerStatesCopy,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Model.ID().String() > out[j].Model.ID().String()
	})

	return out
}

func (o *UploadObserver) init(ctx context.Context) error {
	for _, state := range o.states {
		shards, err := o.repo.ShardsForUpload(ctx, state.Model.ID())
		if err != nil {
			return err
		}
		for _, shard := range shards {
			state.Shards[shard.ID()] = events.ShardView{
				ID:        shard.ID(),
				UploadID:  shard.UploadID(),
				Size:      shard.Size(),
				Digest:    shard.Digest(),
				PieceCID:  shard.PieceCID(),
				State:     shard.State(),
				Location:  shard.Location(),
				PDPAccept: shard.PDPAccept(),
			}
		}
		dagsCompleted, err := o.repo.CompleteDAGScansForUpload(ctx, state.Model.ID())
		if err != nil {
			return err
		}
		dagsToScan, err := o.repo.IncompleteDAGScansForUpload(ctx, state.Model.ID())
		if err != nil {
			return err
		}
		state.DagsTotal = uint64(len(dagsCompleted) + len(dagsToScan))
		state.DagsScanned = uint64(len(dagsToScan))
		uploadSource, err := o.repo.GetSourceByID(ctx, state.Model.SourceID())
		if err != nil {
			return err
		}
		state.SourceDir = uploadSource.Path()
	}
	return nil
}

func (o *UploadObserver) Subscribe() error {
	for uid, state := range o.states {
		// subscribe to client upload events (guppy -> piri(s))
		if err := o.sub.Subscribe(events.TopicClientPut(uid), func(evt events.PutProgress) {
			o.statesMu.Lock()
			defer o.statesMu.Unlock()
			state.PutProgress[evt.BlobID] = PutProgress{
				Uploaded: uint64(evt.Uploaded),
				Total:    evt.Total,
			}
		}); err != nil {
			return err
		}

		// subscribe to shard updates
		if err := o.sub.Subscribe(events.TopicShard(uid), func(evt events.ShardView) {
			o.statesMu.Lock()
			defer o.statesMu.Unlock()
			// TODO probably don't want to hold all shard in memory, once they close
			// we could remove them and just maintain a list of closed once, and
			// remove them from this "active shard" list.
			state.Shards[evt.ID] = evt

		}); err != nil {
			return err
		}

		// sub to dag updates, running counts of total seen and total scanned
		if err := o.sub.Subscribe(events.TopicDagScan(uid), func(evt events.DAGScanView) {
			o.statesMu.Lock()
			defer o.statesMu.Unlock()
			if evt.CID != cid.Undef {
				state.DagsScanned++
			}
			state.DagsFsMap[evt.FSEntryID] = evt.CID
			state.DagsTotal = uint64(len(state.DagsFsMap))

		}); err != nil {
			return err
		}

		if err := o.sub.Subscribe(events.TopicFsEntry(state.Model.SourceID()), func(evt events.FSScanView) {
			o.statesMu.Lock()
			defer o.statesMu.Unlock()
			state.FsEntries[evt.FSEntryID] = FSEntry{
				Path:  evt.Path,
				IsDir: evt.IsDir,
				Size:  evt.Size,
			}
			state.FsEntriesSize += evt.Size

		}); err != nil {
			return err
		}

		if err := o.sub.Subscribe(events.TopicWorker(state.Model.ID()), func(evt events.UploadWorkerEvent) {
			o.statesMu.Lock()
			defer o.statesMu.Unlock()
			state.WorkerStates[evt.Name] = evt
		}); err != nil {
			return err
		}
	}

	return nil
}

func shardsByState(
	shards map[id.ShardID]events.ShardView,
	state blobsmodel.BlobState,
) []events.ShardView {
	// Collect matching shards
	out := make([]events.ShardView, 0, len(shards))
	for _, v := range shards {
		if v.State == state {
			out = append(out, v)
		}
	}

	// Sort: size descending, ID asc as tie-break
	sort.Slice(out, func(i, j int) bool {
		if out[i].Size != out[j].Size {
			return out[i].Size > out[j].Size // larger first
		}
		return out[i].ID.String() < out[j].ID.String()
	})

	return out
}
