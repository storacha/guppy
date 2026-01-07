package ui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	"github.com/mattn/go-isatty"
	"github.com/samber/lo"

	"github.com/storacha/guppy/pkg/bus"
	"github.com/storacha/guppy/pkg/bus/events"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

type Canceled struct{}

func (c Canceled) Error() string {
	return "upload canceled"
}

type uploadModel struct {
	// Configuration
	ctx     context.Context
	cancel  context.CancelFunc
	repo    *sqlrepo.Repo
	api     preparation.API
	uploads []*uploadsmodel.Upload
	rng     *rand.Rand

	observer            *UploadObserver
	currentObservations []Observation

	clientPutProgress map[id.UploadID]map[id.ID]progress.Model
	dagProgress       map[id.UploadID]progress.Model
	uploadTotals      map[id.UploadID]float64
	uploadCompleted   map[id.UploadID]float64
	progressSamples   map[id.UploadID]progressSample

	// Aggregated Totals
	addedShardsSize  uint64
	closedShardsSize uint64
	openShardsSize   uint64
	dagScans         uint64
	retry            bool

	// Internal tracking for totals
	openShardsStats map[id.UploadID]uint64
	dagScanStats    map[id.UploadID]uint64

	results map[id.UploadID]cid.Cid

	// Bubbles (Nested map: UploadID -> ShardID -> Spinner)
	closedShardSpinners map[id.UploadID]map[id.ShardID]spinner.Model

	// Output
	err              error
	lastRetriableErr *types.RetriableError
}

type progressSample struct {
	bytes float64
	rate  float64
	t     time.Time
}

func (m uploadModel) Init() tea.Cmd {
	cmds := make([]tea.Cmd, 0, len(m.uploads)*2)
	for _, u := range m.uploads {
		cmds = append(cmds, executeUpload(m.ctx, m.api, u))
		cmds = append(cmds, tea.Tick(time.Second, func(t time.Time) tea.Msg {
			return m.observer.Observe(m.ctx)
		}))
	}
	return tea.Batch(cmds...)
}

func (m uploadModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.cancel()
			return m, nil
		default:
			return m, nil
		}

	case rootCIDMsg:
		m.results[msg.id] = msg.cid

		// Only quit if ALL uploads are finished
		if len(m.results) == len(m.uploads) {
			time.Sleep(time.Second)
			return m, tea.Quit
		}
		return m, nil

	case []Observation:
		var cmds []tea.Cmd
		m.currentObservations = msg

		for _, obs := range m.currentObservations {
			uid := obs.Model.ID()
			if _, ok := m.clientPutProgress[uid]; !ok {
				m.clientPutProgress[uid] = make(map[id.ID]progress.Model)
			}
			if _, ok := m.dagProgress[uid]; !ok {
				m.dagProgress[uid] = progress.New(progress.WithDefaultGradient())
			}
			var uploadedBytes float64
			for bid, pgrs := range obs.ClientUploadProgress {
				cpb, ok := m.clientPutProgress[uid][bid]
				if !ok {
					cpb = progress.New(progress.WithDefaultGradient())
				}
				if pgrs.Total > 0 {
					if cmd := cpb.SetPercent(float64(pgrs.Uploaded) / float64(pgrs.Total)); cmd != nil {
						cmds = append(cmds, cmd)
					}
				}
				m.clientPutProgress[uid][bid] = cpb
				uploadedBytes += float64(pgrs.Uploaded)
			}

			// Update DAG progress for this upload.
			pb := m.dagProgress[uid]
			dagPercent := 1.0
			if obs.TotalDags > 0 {
				dagPercent = float64(obs.ProcessedDags) / float64(obs.TotalDags)
			} else if obs.ProcessedDags > 0 {
				dagPercent = 1
			}
			if obs.ProcessedDags == 0 {
				if cmd := pb.SetPercent(1); cmd != nil {
					cmds = append(cmds, cmd)
				}
			} else {
				if cmd := pb.SetPercent(dagPercent); cmd != nil {
					cmds = append(cmds, cmd)
				}
			}
			m.dagProgress[uid] = pb

			// Track aggregate upload bytes and throughput for ETA/progress.
			m.uploadTotals[uid] = float64(obs.BytesRead)
			m.uploadCompleted[uid] = uploadedBytes
			now := time.Now()
			prev := m.progressSamples[uid]
			sample := progressSample{bytes: uploadedBytes, t: now, rate: prev.rate}
			if !prev.t.IsZero() && uploadedBytes >= prev.bytes {
				deltaBytes := uploadedBytes - prev.bytes
				deltaT := now.Sub(prev.t).Seconds()
				if deltaT > 0 {
					instRate := deltaBytes / deltaT
					if prev.rate > 0 {
						instRate = 0.7*prev.rate + 0.3*instRate
					}
					sample.rate = instRate
				}
			}
			m.progressSamples[uid] = sample
		}

		cmds = append(cmds, tea.Tick(time.Second, func(t time.Time) tea.Msg {
			return m.observer.Observe(m.ctx)
		}))
		return m, tea.Sequence(cmds...)

	case error:

		m.err = msg
		return m, tea.Quit

	case uploadErrMsg:
		var retriableError types.RetriableError
		if m.retry && errors.As(msg.err, &retriableError) {
			m.lastRetriableErr = &retriableError
			var upload *uploadsmodel.Upload
			for _, u := range m.uploads {
				if u.ID() == msg.id {
					upload = u
					break
				}
			}
			if upload == nil {
				m.err = fmt.Errorf("could not find upload %s to retry after retriable error", msg.id)
				return m, tea.Quit
			}
			return m, tea.Batch(executeUpload(m.ctx, m.api, upload))
		}
		m.err = msg.err
		return m, tea.Quit

	default:
		var cmds []tea.Cmd
		for uid, blobBars := range m.clientPutProgress {
			for bid, pb := range blobBars {
				npb, cmd := pb.Update(msg)
				pb = npb.(progress.Model)
				m.clientPutProgress[uid][bid] = pb
				cmds = append(cmds, cmd)
			}
		}
		for uid, pb := range m.dagProgress {
			npb, cmd := pb.Update(msg)
			pb = npb.(progress.Model)
			m.dagProgress[uid] = pb
			cmds = append(cmds, cmd)
		}
		for uid, shardMap := range m.closedShardSpinners {
			for sid, sp := range shardMap {
				updatedSpinner, cmd := sp.Update(msg)
				m.closedShardSpinners[uid][sid] = updatedSpinner
				cmds = append(cmds, cmd)
			}
		}
		return m, tea.Sequence(cmds...)
	}
}

func renderListItem(style lipgloss.Style, text string, size uint64) string {
	return "• " + style.Render(text) + " " + style.Faint(true).Render(humanize.Bytes(size)) + "\n"
}

func formatETA(total, uploaded float64, sample progressSample) string {
	if total <= 0 {
		return ""
	}
	remaining := total - uploaded
	if remaining <= 0 {
		return ""
	}
	if sample.rate <= 0 {
		return ""
	}
	eta := time.Duration(remaining/sample.rate) * time.Second
	if eta < time.Second {
		return "<1s"
	}
	etaStr := eta.Round(time.Second).String()

	// Convert bytes/sec -> bits/sec and display Mbps or Gbps.
	bps := sample.rate * 8
	throughput := ""
	switch {
	case bps >= 1_000_000_000:
		throughput = fmt.Sprintf(" @ %.2f Gbps", bps/1_000_000_000)
	case bps > 0:
		throughput = fmt.Sprintf(" @ %.2f Mbps", bps/1_000_000)
	}

	return etaStr + throughput
}

func (m uploadModel) View() string {
	//addedShardColor := lipgloss.Color("#0176CE")
	//closedShardColor := lipgloss.Color("#7CABCF")
	//shardedColor := lipgloss.Color("#FFE299")
	//scannedColor := lipgloss.Color("#E88B8D")

	var output strings.Builder

	style := lipgloss.NewStyle()

	if m.retry {
		output.WriteString(style.Bold(true).Render("Uploading with auto-retry enabled...\n\n"))
	}

	for _, u := range m.currentObservations {
		output.WriteString(fmt.Sprintf("Upload ID: %s\n", u.Model.ID()))
		// Aggregate shard totals and build rows.
		var totalShardSize uint64
		type shardRow struct {
			view  events.ShardView
			state string
			order int
		}
		rows := make([]shardRow, 0, len(u.OpenShards)+len(u.ClosedShards)+len(u.UploadedShards)+len(u.AddedShards))

		addRows := func(shards []events.ShardView, state string, order int, _ bool) {
			for _, s := range shards {
				rows = append(rows, shardRow{view: s, state: state, order: order})
				totalShardSize += s.Size
			}
		}

		addRows(u.OpenShards, "Packing", 1, false)
		addRows(u.ClosedShards, "Queued", 2, false)
		addRows(u.UploadedShards, "Uploaded", 3, true)
		addRows(u.AddedShards, "Complete", 4, true)

		blobBars := m.clientPutProgress[u.Model.ID()]
		var completedShardBytes float64
		for i := range rows {
			row := &rows[i]
			if _, ok := blobBars[row.view.ID]; ok {
				row.state = "Uploading"
				completedShardBytes += float64(row.view.Size) * m.clientPutProgress[u.Model.ID()][row.view.ID].Percent()
				continue
			}
			if row.state == "Uploaded" || row.state == "Complete" {
				completedShardBytes += float64(row.view.Size)
			}
		}

		if pb, ok := m.dagProgress[u.Model.ID()]; ok {
			output.WriteString("Scan Progress:\t\t")
			output.WriteString(pb.View() + "\n")
			output.WriteString(fmt.Sprintf("\tFiles Scanned: %d\n", u.ProcessedDags))
		}

		totalBytes := m.uploadTotals[u.Model.ID()]
		overallPercent := 0.0
		if totalBytes == 0 {
			totalBytes = float64(totalShardSize)
		}
		completedBytes := completedShardBytes
		if totalBytes > 0 {
			overallPercent = completedBytes / totalBytes
			if overallPercent > 1 {
				overallPercent = 1
			}
		}

		shardProgressBar := progress.New(progress.WithDefaultGradient())
		eta := formatETA(totalBytes, completedBytes, m.progressSamples[u.Model.ID()])
		output.WriteString("Upload Progress:\t")
		output.WriteString(shardProgressBar.ViewAs(overallPercent))
		if eta != "" {
			output.WriteString("\n\tETA: " + eta)
		}
		output.WriteString(fmt.Sprintf(" (%s of %s)", humanize.Bytes(uint64(completedBytes)),
			humanize.Bytes(uint64(totalBytes))))
		output.WriteString("\n")

		output.WriteString("\nShard Progress:\n")
		output.WriteString(fmt.Sprintf("%-36s %-9s %-8s %s\n", "Shard", "State", "Size", "Put Progress"))

		sort.Slice(rows, func(i, j int) bool {
			if rows[i].order != rows[j].order {
				return rows[i].order < rows[j].order
			}
			return rows[i].view.ID.String() < rows[j].view.ID.String()
		})

		completeBar := progress.New(progress.WithDefaultGradient())
		for _, row := range rows {
			pbStr := "--"
			if pb, ok := blobBars[row.view.ID]; ok {
				pbStr = pb.View()
			} else if row.state == "Uploaded" || row.state == "Complete" {
				pbStr = completeBar.ViewAs(1)
			}
			output.WriteString(fmt.Sprintf("%-36s %-9s %-8s %s\n",
				row.view.ID.String(),
				row.state,
				humanize.Bytes(row.view.Size),
				pbStr,
			))
		}

		output.WriteString("\n\n")

	}

	if m.lastRetriableErr != nil {
		output.WriteString("\n\n")
		output.WriteString(style.Bold(true).Foreground(lipgloss.Color("#FFA500")).Render("Last retriable error: "))
		if listErr, ok := m.lastRetriableErr.Unwrap().(interface{ Unwrap() []error }); ok {
			output.WriteString("Multiple errors...\n")
			for i, e := range listErr.Unwrap() {
				if i >= 3 {
					output.WriteString(style.Italic(true).Render(fmt.Sprintf("...and %d more errors\n", len(listErr.Unwrap())-i)))
					break
				}
				output.WriteString(e.Error() + "\n")
			}
		} else {
			output.WriteString(m.lastRetriableErr.Unwrap().Error() + "\n")
		}
	}

	output.WriteString("\n\nPress q to quit.\n")

	return output.String()
}

type rootCIDMsg struct {
	id  id.UploadID
	cid cid.Cid
}

type uploadErrMsg struct {
	id  id.UploadID
	err error
}

func executeUpload(ctx context.Context, api preparation.API, upload *uploadsmodel.Upload) tea.Cmd {
	return func() tea.Msg {
		rootCID, err := api.ExecuteUpload(ctx, upload)
		if err != nil {
			return uploadErrMsg{id: upload.ID(), err: fmt.Errorf("command failed to execute upload: %w", err)}
		}
		return rootCIDMsg{id: upload.ID(), cid: rootCID}
	}
}

type fileDagStatusSample struct {
	Path string
	Size uint64
}

type fileDagStatusBucket struct {
	Count   int
	Samples []fileDagStatusSample
}

type fileDagStatusSummary struct {
	Shared   fileDagStatusBucket
	Pending  fileDagStatusBucket
	Unshared fileDagStatusBucket
}

type uploadObserver struct {
	Repo      *sqlrepo.Repo
	Uploads   map[id.UploadID]*uploadView
	uploadsMu sync.RWMutex
}

type uploadView struct {
	// The ID of the upload this view is for
	UploadID id.UploadID
	// The shards for the upload
	Shards   map[id.ShardID]*events.ShardView
	shardsMu sync.RWMutex

	DagScans   map[id.FSEntryID]*events.DAGScanView
	dagScansMu sync.RWMutex

	FSScans  map[id.FSEntryID]*events.FSScanView
	fsScanMu sync.RWMutex

	putProgress map[id.ID]events.PutProgress
	putMu       sync.RWMutex
}

/*
func (o *uploadObserver) subscribe(ctx context.Context) error {
	if err := o.Repo.Subscriber().Subscribe("put_progress", func(progresses events.PutProgress) {
		for _, progress := range progresses {
			o.uploadsMu.RLock()
			upload, ok := o.Uploads[progress.UploadID]
			o.uploadsMu.RUnlock()
			if !ok {
				continue
			}
			upload.putMu.Lock()
			if upload.putProgress == nil {
				upload.putProgress = make(map[id.ID]client.PutProgress)
			}
			upload.putProgress[progress.BlobID] = progress
			upload.putMu.Unlock()
		}
	}); err != nil {
		return err
	}

	o.uploadsMu.Lock()
	for uploadID := range o.Uploads {
		// get the current state one time from the DB, since the subscriber will only get _new_ DB changes
		// if there is existing state, from a resumed upload, or duplicate upload this logic loads it
		shards, err := o.Repo.ShardsForUpload(ctx, uploadID)
		if err != nil {
			return err
		}
		shardView := make(map[id.ShardID]*events.ShardView, len(shards))
		for _, shard := range shards {
			shardView[shard.ID()] = &events.ShardView{
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

		// preload DAG scans and FS scans from the database for resumed runs
		dagScans := make(map[id.FSEntryID]*events.DAGScanView)
		fsScans := make(map[id.FSEntryID]*events.FSScanView)

		loadDAGScan := func(ds dagmodel.DAGScan) {
			dagScans[ds.FsEntryID()] = &events.DAGScanView{
				FSEntryID: ds.FsEntryID(),
				Created:   ds.CreatedAt(),
				Updated:   ds.UpdatedAt(),
				CID:       ds.CID(),
			}
		}

		incomplete, err := o.Repo.IncompleteDAGScansForUpload(ctx, uploadID)
		if err != nil {
			return err
		}
		for _, ds := range incomplete {
			loadDAGScan(ds)
		}

		complete, err := o.Repo.CompleteDAGScansForUpload(ctx, uploadID)
		if err != nil {
			return err
		}
		for _, ds := range complete {
			loadDAGScan(ds)
		}

		fsEntryIDs := make(map[id.FSEntryID]struct{}, len(dagScans))
		for fsID := range dagScans {
			fsEntryIDs[fsID] = struct{}{}
		}
		for fsID := range fsEntryIDs {
			file, err := o.Repo.GetFileByID(ctx, fsID)
			if err != nil {
				if strings.Contains(err.Error(), "found entry is not a file") {
					// directories may have DAG scans but aren't needed for file status summaries
					continue
				}
				return err
			}
			if file == nil {
				continue // likely a directory; directories are size 0 and ignored in status
			}
			fsScans[fsID] = &events.FSScanView{
				Path:      file.Path(),
				IsDir:     false,
				Size:      file.Size(),
				FSEntryID: file.ID(),
			}
		}

		o.Uploads[uploadID] = &uploadView{
			UploadID:    uploadID,
			Shards:      shardView,
			DagScans:    dagScans,
			FSScans:     fsScans,
			putProgress: make(map[id.ID]client.PutProgress),
		}

		// now subscribe to receive all new state
		if err := o.Repo.Subscriber().Subscribe(fmt.Sprintf("shard:%s", uploadID), func(shard *events.ShardView) {
			upload := o.Uploads[uploadID]
			upload.shardsMu.Lock()
			upload.Shards[shard.ID] = shard
			upload.shardsMu.Unlock()
		}); err != nil {
			return err
		}
		if err := o.Repo.Subscriber().Subscribe(fmt.Sprintf("dag_scan:%s", uploadID), func(scan *events.DAGScanView) {
			upload := o.Uploads[uploadID]
			upload.dagScansMu.Lock()
			upload.DagScans[scan.FSEntryID] = scan
			upload.dagScansMu.Unlock()
		}); err != nil {
			return err
		}
		if err := o.Repo.Subscriber().Subscribe("fsentry", func(fsentry *events.FSScanView) {
			upload := o.Uploads[uploadID]
			upload.fsScanMu.Lock()
			upload.FSScans[fsentry.FSEntryID] = fsentry
			upload.fsScanMu.Unlock()
		}); err != nil {
			return err
		}
	}
	o.uploadsMu.Unlock()
	return nil
}

*/

func (o *uploadObserver) FilesScanned(ctx context.Context, uploadID id.UploadID) ([]*events.FSScanView, error) {
	_ = ctx

	o.uploadsMu.RLock()
	upload, ok := o.Uploads[uploadID]
	o.uploadsMu.RUnlock()
	if !ok {
		return []*events.FSScanView{}, nil
	}

	upload.dagScansMu.RLock()
	dagScans := make(map[id.FSEntryID]*events.DAGScanView, len(upload.DagScans))
	for k, v := range upload.DagScans {
		dagScans[k] = v
	}
	upload.dagScansMu.RUnlock()

	upload.fsScanMu.RLock()
	defer upload.fsScanMu.RUnlock()
	out := make([]*events.FSScanView, 0, len(upload.FSScans))
	for _, v := range upload.FSScans {
		if v.IsDir {
			continue
		}
		ds, ok := dagScans[v.FSEntryID]
		if !ok || ds.CID == cid.Undef {
			out = append(out, v)
		}
	}
	return out, nil
}

func (o *uploadObserver) BytesScanned(ctx context.Context, uploadID id.UploadID) (uint64, error) {
	o.uploadsMu.RLock()
	defer o.uploadsMu.RUnlock()

	upload, ok := o.Uploads[uploadID]
	if !ok {
		return 0, nil
	}
	upload.fsScanMu.RLock()
	defer upload.fsScanMu.RUnlock()
	return lo.Sum(
		lo.MapToSlice(upload.FSScans, func(_ id.ShardID, v *events.FSScanView) uint64 {
			return v.Size
		}),
	), nil
}

/*
func (o *uploadObserver) FileDagStatus(ctx context.Context, uploadID id.UploadID, sampleLimit int) (fileDagStatusSummary, error) {
	_ = ctx

	o.uploadsMu.RLock()
	upload, ok := o.Uploads[uploadID]
	o.uploadsMu.RUnlock()
	if !ok {
		return fileDagStatusSummary{}, nil
	}

	upload.dagScansMu.RLock()
	dagScans := make(map[id.FSEntryID]*sqlrepo.DAGScanView, len(upload.DagScans))
	for k, v := range upload.DagScans {
		dagScans[k] = v
	}
	upload.dagScansMu.RUnlock()

	upload.fsScanMu.RLock()
	fsScans := make(map[id.FSEntryID]*sqlrepo.FSScanView, len(upload.FSScans))
	for k, v := range upload.FSScans {
		fsScans[k] = v
	}
	upload.fsScanMu.RUnlock()

	summary := fileDagStatusSummary{}

	for _, fs := range fsScans {
		if fs.IsDir {
			continue
		}

		scan, ok := dagScans[fs.FSEntryID]
		switch {
		case ok && scan.CID != cid.Undef:
			summary.Shared.Count++
			if len(summary.Shared.Samples) < sampleLimit {
				summary.Shared.Samples = append(summary.Shared.Samples, fileDagStatusSample{Path: fs.Path, Size: fs.Size})
			}
		case ok:
			summary.Pending.Count++
			if len(summary.Pending.Samples) < sampleLimit {
				summary.Pending.Samples = append(summary.Pending.Samples, fileDagStatusSample{Path: fs.Path, Size: fs.Size})
			}
		default:
			summary.Unshared.Count++
			if len(summary.Unshared.Samples) < sampleLimit {
				summary.Unshared.Samples = append(summary.Unshared.Samples, fileDagStatusSample{Path: fs.Path, Size: fs.Size})
			}
		}
	}
	sort.Slice(summary.Shared.Samples, func(i, j int) bool { return summary.Shared.Samples[i].Path < summary.Shared.Samples[j].Path })
	sort.Slice(summary.Pending.Samples, func(i, j int) bool { return summary.Pending.Samples[i].Path < summary.Pending.Samples[j].Path })
	sort.Slice(summary.Unshared.Samples, func(i, j int) bool { return summary.Unshared.Samples[i].Path < summary.Unshared.Samples[j].Path })

	return summary, nil
}

*/

func newUploadModel(
	ctx context.Context,
	repo *sqlrepo.Repo,
	api preparation.API,
	uploads []*uploadsmodel.Upload,
	retry bool,
	eb bus.Subscriber,
) uploadModel {
	if len(uploads) == 0 {
		panic("no uploads provided to upload model")
	}

	ctx, cancel := context.WithCancel(ctx)

	obs := NewUploadObserver(eb, repo, uploads...)
	if err := obs.Subscribe(); err != nil {
		panic(err)
	}

	return uploadModel{
		ctx:     ctx,
		cancel:  cancel,
		repo:    repo,
		api:     api,
		uploads: uploads,
		results: make(map[id.UploadID]cid.Cid),
		rng:     rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),

		observer:          obs,
		clientPutProgress: make(map[id.UploadID]map[id.ID]progress.Model),
		dagProgress:       make(map[id.UploadID]progress.Model),
		uploadTotals:      make(map[id.UploadID]float64),
		uploadCompleted:   make(map[id.UploadID]float64),
		progressSamples:   make(map[id.UploadID]progressSample),

		retry: retry,
	}
}

type bar struct {
	color lipgloss.Color
	value int
}

var partials = []rune{'▏', '▎', '▍', '▌', '▋', '▊', '▉'}

func partialBlock(n int) string {
	if n < 1 || n > len(partials) {
		panic(fmt.Sprintf("invalid partial block: %d", n))
	}
	return string(partials[n-1])
}

func renderBars(bars []bar, width int) string {
	var b strings.Builder

	var total int
	for _, bar := range bars {
		total += bar.value
	}

	var remainder int
	var previousColor lipgloss.Color
	for _, bar := range bars {
		var eigthBlocks = 0
		if bar.value != 0 {
			eigthBlocks = int(math.Round(float64(bar.value*width*8) / float64(total)))
		}

		var pb string
		if remainder > 0 {
			pb = partialBlock(remainder)
			eigthBlocks -= (8 - remainder)
		}

		fullBlocks := eigthBlocks / 8

		b.WriteString(
			lipgloss.NewStyle().
				Foreground(previousColor).
				Background(bar.color).
				Render(pb + strings.Repeat(" ", fullBlocks)),
		)

		remainder = eigthBlocks % 8
		previousColor = bar.color
	}

	return b.String()

}

func RunUploadUI(
	ctx context.Context,
	repo *sqlrepo.Repo,
	api preparation.API,
	uploads []*uploadsmodel.Upload,
	retry bool,
	eb bus.Subscriber,
) error {
	if len(uploads) == 0 {
		return fmt.Errorf("no uploads provided to upload UI")
	}

	var teaOpts []tea.ProgramOption
	// if no tty present, don't expect one (e.g. when a debugger is attached)
	if !isatty.IsTerminal(os.Stdout.Fd()) {
		teaOpts = append(teaOpts,
			tea.WithoutRenderer(),
			tea.WithInput(io.NopCloser(strings.NewReader(""))),
			tea.WithOutput(io.Discard),
		)
	}
	m, err := tea.NewProgram(newUploadModel(ctx, repo, api, uploads, retry, eb), teaOpts...).Run()
	if err != nil {
		return fmt.Errorf("command failed to run upload UI: %w", err)
	}
	um := m.(uploadModel)

	if um.err != nil {
		var errRetriable types.RetriableError

		if errors.As(um.err, &errRetriable) {
			var sb strings.Builder
			underlyingErr := errRetriable.Unwrap()
			sb.WriteString("\nUpload failed due to out-of-date scan:\n")
			if listErr, ok := underlyingErr.(interface{ Unwrap() []error }); ok {
				for i, e := range listErr.Unwrap() {
					if i >= 3 {
						sb.WriteString(fmt.Sprintf("...and %d more errors\n", len(listErr.Unwrap())-i))
						break
					}
					sb.WriteString(e.Error() + "\n")
				}
			} else {
				sb.WriteString(underlyingErr.Error() + "\n")
			}
			sb.WriteString("\nYou can retry the upload to re-scan and continue.\n")
			fmt.Print(sb.String())
			return nil
		}

		if errors.Is(um.err, context.Canceled) || errors.Is(um.err, Canceled{}) {
			fmt.Println("\nUpload canceled.")
			return nil
		}

		return fmt.Errorf("upload failed: %w", um.err)
	}

	fmt.Println("Upload complete!")
	for _, u := range um.uploads {
		if c, ok := um.results[u.ID()]; ok {
			// Fetch Source Path to display
			sourceName := u.SourceID().String()
			source, err := repo.GetSourceByID(ctx, u.SourceID())
			if err == nil && source != nil {
				sourceName = source.Path()
			}
			fmt.Printf("Root CID for %s: %s\n", sourceName, c.String())
		}
	}
	return nil
}
