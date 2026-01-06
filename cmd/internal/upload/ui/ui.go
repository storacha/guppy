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

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	"github.com/mattn/go-isatty"
	"github.com/samber/lo"
	"github.com/storacha/go-libstoracha/digestutil"

	"github.com/storacha/guppy/internal/largeupload/bubbleup"
	"github.com/storacha/guppy/pkg/preparation"
	blobsmodel "github.com/storacha/guppy/pkg/preparation/blobs/model"
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

	observer *uploadObserver

	// State (Maps for multi-upload support)
	recentAddedShards  map[id.UploadID][]*sqlrepo.ShardView
	recentClosedShards map[id.UploadID][]*sqlrepo.ShardView
	filesToDAGScan     map[id.UploadID][]*sqlrepo.FSScanView
	shardedFiles       map[id.UploadID][]sqlrepo.FileInfo

	// Aggregated Totals
	addedShardsSize  uint64
	closedShardsSize uint64
	openShardsSize   uint64
	dagScans         uint64
	retry            bool

	// Internal tracking for totals
	openShardsStats map[id.UploadID]uint64
	dagScanStats    map[id.UploadID]uint64

	dagsScanned      uint64
	dagsToScan       uint64
	fileBytesScanned uint64

	results map[id.UploadID]cid.Cid

	// Bubbles (Nested map: UploadID -> ShardID -> Spinner)
	closedShardSpinners map[id.UploadID]map[id.ShardID]spinner.Model

	// Output
	err              error
	lastRetriableErr *types.RetriableError
}

func (m uploadModel) Init() tea.Cmd {
	cmds := make([]tea.Cmd, 0, len(m.uploads)*2)
	for _, u := range m.uploads {
		cmds = append(cmds, executeUpload(m.ctx, m.api, u))
		cmds = append(cmds, checkStats(m.ctx, m.observer, m.repo, u.ID()))
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
			return m, tea.Quit
		}
		return m, nil

	case statsMsg:
		var cmds []tea.Cmd

		// 1. Store data for THIS specific upload
		m.recentAddedShards[msg.uploadID] = msg.addedShards
		m.recentClosedShards[msg.uploadID] = msg.closedShards
		m.filesToDAGScan[msg.uploadID] = msg.filesToDAGScan
		m.shardedFiles[msg.uploadID] = msg.shardedFiles
		m.dagsToScan = msg.dagsToScan
		m.dagsScanned = msg.dagsScanned
		m.fileBytesScanned = msg.fileBytesScanned

		// 2. Update stats for THIS specific upload
		openSize := uint64(0)
		for _, s := range msg.openShards {
			openSize += s.Size
		}
		m.openShardsStats[msg.uploadID] = openSize
		m.dagScanStats[msg.uploadID] = msg.bytesToDAGScan

		// 3. Handle Spinners
		if m.closedShardSpinners[msg.uploadID] == nil {
			m.closedShardSpinners[msg.uploadID] = make(map[id.ShardID]spinner.Model)
		}
		currentSpinners := m.closedShardSpinners[msg.uploadID]

		newSpinners := make(map[id.ShardID]spinner.Model)
		for _, s := range msg.closedShards {
			if sp, ok := currentSpinners[s.ID]; ok {
				newSpinners[s.ID] = sp
			} else {
				newSpinner := spinner.New(spinner.WithSpinner(bubbleup.Spinner(m.rng)))
				newSpinners[s.ID] = newSpinner
				cmds = append(cmds, newSpinner.Tick)
			}
		}
		m.closedShardSpinners[msg.uploadID] = newSpinners

		// 4. Recalculate Global Totals
		m.addedShardsSize = 0
		m.closedShardsSize = 0
		m.openShardsSize = 0
		m.dagScans = 0

		for _, shards := range m.recentAddedShards {
			for _, s := range shards {
				m.addedShardsSize += s.Size
			}
		}
		for _, shards := range m.recentClosedShards {
			for _, s := range shards {
				m.closedShardsSize += s.Size
			}
		}
		for _, size := range m.openShardsStats {
			m.openShardsSize += size
		}
		for _, size := range m.dagScanStats {
			m.dagScans += size
		}

		return m, tea.Batch(append(cmds, checkStats(m.ctx, m.observer, m.repo, msg.uploadID))...)

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
		for uid, shardMap := range m.closedShardSpinners {
			for sid, sp := range shardMap {
				updatedSpinner, cmd := sp.Update(msg)
				m.closedShardSpinners[uid][sid] = updatedSpinner
				cmds = append(cmds, cmd)
			}
		}
		return m, tea.Batch(cmds...)
	}
}

func renderListItem(style lipgloss.Style, text string, size uint64) string {
	return "• " + style.Render(text) + " " + style.Faint(true).Render(humanize.Bytes(size)) + "\n"
}

func (m uploadModel) View() string {
	addedShardColor := lipgloss.Color("#0176CE")
	closedShardColor := lipgloss.Color("#7CABCF")
	shardedColor := lipgloss.Color("#FFE299")
	scannedColor := lipgloss.Color("#E88B8D")

	var output strings.Builder

	style := lipgloss.NewStyle()

	if m.retry {
		output.WriteString(style.Bold(true).Render("Uploading with auto-retry enabled...\n\n"))
	}

	output.WriteString(fmt.Sprintf("Dags To Scan: %d\n", m.dagsToScan))
	output.WriteString(fmt.Sprintf("Dags Scanned: %d\n", m.dagsScanned))
	output.WriteString(fmt.Sprintf("File Bytes Scanned: %s\n", humanize.Bytes(m.fileBytesScanned)))
	output.WriteString("Shards:\n")
	// Iterate uploads to show shards for everyone
	for _, u := range m.uploads {
		uid := u.ID()
		if shards, ok := m.recentAddedShards[uid]; ok {
			for _, s := range shards {
				output.WriteString(renderListItem(style.Foreground(addedShardColor), digestutil.Format(s.Digest), s.Size))
			}
		}
		if shards, ok := m.recentClosedShards[uid]; ok {
			for _, s := range shards {
				spView := ""
				if spMap, ok := m.closedShardSpinners[uid]; ok {
					if sp, ok := spMap[s.ID]; ok {
						spView = sp.View() + " "
					}
				}
				output.WriteString(renderListItem(style.Foreground(closedShardColor), spView+s.ID.String(), s.Size))
			}
		}
	}
	output.WriteString("\n")

	output.WriteString("Added to Shards:\n")
	for _, u := range m.uploads {
		if files, ok := m.shardedFiles[u.ID()]; ok {
			for i, fi := range files {
				if i < 3 {
					output.WriteString(renderListItem(style.Foreground(shardedColor), fi.Path, fi.Size))
				}
			}
		}
	}

	output.WriteString("\n")

	output.WriteString("Scanning Files:\n")
	for _, u := range m.uploads {
		if files, ok := m.filesToDAGScan[u.ID()]; ok {
			for i, fi := range files {
				if i < 3 {
					output.WriteString(renderListItem(style.Foreground(scannedColor), fi.Path, fi.Size))
				}
			}
		}
	}

	output.WriteString("\n")

	output.WriteString(renderBars([]bar{
		{color: addedShardColor, value: int(m.addedShardsSize)},
		{color: closedShardColor, value: int(m.closedShardsSize)},
		{color: shardedColor, value: int(m.openShardsSize)},
		{color: scannedColor, value: int(m.dagScans)},
	}, 80))

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

type statsMsg struct {
	uploadID         id.UploadID
	addedShards      []*sqlrepo.ShardView
	closedShards     []*sqlrepo.ShardView
	openShards       []*sqlrepo.ShardView
	bytesToDAGScan   uint64
	filesToDAGScan   []*sqlrepo.FSScanView
	shardedFiles     []sqlrepo.FileInfo
	dagsToScan       uint64
	dagsScanned      uint64
	fileBytesScanned uint64
}

func newUploadObserver(repo *sqlrepo.Repo, uploadIDs []id.UploadID) *uploadObserver {
	uploads := make(map[id.UploadID]*uploadView, len(uploadIDs))
	for _, uid := range uploadIDs {
		uploads[uid] = &uploadView{
			UploadID: uid,
			Shards:   make(map[id.ShardID]*sqlrepo.ShardView),
			DagScans: make(map[id.FSEntryID]*sqlrepo.DAGScanView),
			FSScans:  make(map[id.FSEntryID]*sqlrepo.FSScanView),
		}
	}
	return &uploadObserver{
		Uploads: uploads,
		Repo:    repo,
	}

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
	Shards   map[id.ShardID]*sqlrepo.ShardView
	shardsMu sync.RWMutex

	DagScans   map[id.FSEntryID]*sqlrepo.DAGScanView
	dagScansMu sync.RWMutex

	FSScans  map[id.FSEntryID]*sqlrepo.FSScanView
	fsScanMu sync.RWMutex
}

func (o *uploadObserver) subscribe(ctx context.Context) error {
	o.uploadsMu.Lock()
	for uploadID := range o.Uploads {
		// get the current state one time from the DB, since the subscriber will only get _new_ DB changes
		// if there is existing state, from a resumed upload, or duplicate upload this logic loads it
		shards, err := o.Repo.ShardsForUpload(ctx, uploadID)
		if err != nil {
			return err
		}
		shardView := make(map[id.ShardID]*sqlrepo.ShardView, len(shards))
		for _, shard := range shards {
			shardView[shard.ID()] = &sqlrepo.ShardView{
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

		o.Uploads[uploadID] = &uploadView{
			UploadID: uploadID,
			Shards:   shardView,
			DagScans: make(map[id.FSEntryID]*sqlrepo.DAGScanView),
			FSScans:  make(map[id.FSEntryID]*sqlrepo.FSScanView),
		}

		// now subscribe to receive all new state
		if err := o.Repo.Subscriber().Subscribe(fmt.Sprintf("shard:%s", uploadID), func(shard *sqlrepo.ShardView) {
			upload := o.Uploads[uploadID]
			upload.shardsMu.Lock()
			upload.Shards[shard.ID] = shard
			upload.shardsMu.Unlock()
		}); err != nil {
			return err
		}
		if err := o.Repo.Subscriber().Subscribe(fmt.Sprintf("dag_scan:%s", uploadID), func(scan *sqlrepo.DAGScanView) {
			upload := o.Uploads[uploadID]
			upload.dagScansMu.Lock()
			upload.DagScans[scan.FSEntryID] = scan
			upload.dagScansMu.Unlock()
		}); err != nil {
			return err
		}
		if err := o.Repo.Subscriber().Subscribe("fsentry", func(fsentry *sqlrepo.FSScanView) {
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

func (o *uploadObserver) ShardsForUploadByState(
	ctx context.Context,
	uploadID id.UploadID,
	state blobsmodel.BlobState,
) ([]*sqlrepo.ShardView, error) {
	o.uploadsMu.RLock()
	defer o.uploadsMu.RUnlock()

	upload, ok := o.Uploads[uploadID]
	if !ok {
		return []*sqlrepo.ShardView{}, nil
	}

	upload.shardsMu.RLock()
	defer upload.shardsMu.RUnlock()

	// Collect matching shards
	out := make([]*sqlrepo.ShardView, 0, len(upload.Shards))
	for _, v := range upload.Shards {
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

	return out, nil
}

func (o *uploadObserver) FilesScanned(ctx context.Context, uploadID id.UploadID) ([]*sqlrepo.FSScanView, error) {
	o.uploadsMu.RLock()
	defer o.uploadsMu.RUnlock()

	upload, ok := o.Uploads[uploadID]
	if !ok {
		return []*sqlrepo.FSScanView{}, nil
	}
	upload.fsScanMu.RLock()
	defer upload.fsScanMu.RUnlock()
	out := make([]*sqlrepo.FSScanView, 0, len(upload.FSScans))
	for _, v := range upload.FSScans {
		out = append(out, v)
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
		lo.MapToSlice(upload.FSScans, func(_ id.ShardID, v *sqlrepo.FSScanView) uint64 {
			return v.Size
		}),
	), nil
}

func (o *uploadObserver) DagsToScan(ctx context.Context, uploadID id.UploadID) (uint64, error) {
	o.uploadsMu.RLock()
	defer o.uploadsMu.RUnlock()

	upload, ok := o.Uploads[uploadID]
	if !ok {
		return 0, nil
	}

	upload.dagScansMu.RLock()
	defer upload.dagScansMu.RUnlock()

	total := uint64(0)
	for _, v := range upload.DagScans {
		if v.CID == cid.Undef {
			total++
		}
	}
	return total, nil
}

func (o *uploadObserver) DagsCompleted(ctx context.Context, uploadID id.UploadID) (uint64, error) {
	o.uploadsMu.RLock()
	defer o.uploadsMu.RUnlock()

	upload, ok := o.Uploads[uploadID]
	if !ok {
		return 0, nil
	}

	upload.dagScansMu.RLock()
	defer upload.dagScansMu.RUnlock()

	total := uint64(0)
	for _, v := range upload.DagScans {
		if v.CID != cid.Undef {
			total++
		}
	}
	return total, nil

}

func checkStats(ctx context.Context, observer *uploadObserver, repo *sqlrepo.Repo, uploadID id.UploadID) tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		addedShards, err := observer.ShardsForUploadByState(ctx, uploadID, blobsmodel.BlobStateAdded)
		if err != nil {
			return fmt.Errorf("getting added shards for upload %s: %w", uploadID, err)
		}

		closedShards, err := observer.ShardsForUploadByState(ctx, uploadID, blobsmodel.BlobStateClosed)
		if err != nil {
			return fmt.Errorf("getting closed shards for upload %s: %w", uploadID, err)
		}

		openShards, err := observer.ShardsForUploadByState(ctx, uploadID, blobsmodel.BlobStateOpen)
		if err != nil {
			return fmt.Errorf("getting open shards for upload %s: %w", uploadID, err)
		}

		bytesToDAGScan, err := observer.BytesScanned(ctx, uploadID)
		if err != nil {
			return fmt.Errorf("getting total bytes to scan for upload %s: %w", uploadID, err)
		}

		filesToDAGScan, err := observer.FilesScanned(ctx, uploadID)
		if err != nil {
			return fmt.Errorf("getting info on remaining files for upload %s: %w", uploadID, err)
		}

		shardedFiles, err := repo.ShardedFiles(ctx, uploadID, 6)
		if err != nil {
			return fmt.Errorf("getting info on sharded files for upload %s: %w", uploadID, err)
		}

		scanTodo, err := observer.DagsToScan(ctx, uploadID)
		if err != nil {
			return err
		}

		scanDone, err := observer.DagsCompleted(ctx, uploadID)
		if err != nil {
			return err
		}
		bytesScanned, err := observer.BytesScanned(ctx, uploadID)
		if err != nil {
			return err
		}

		return statsMsg{
			uploadID:         uploadID,
			addedShards:      addedShards,
			closedShards:     closedShards,
			openShards:       openShards,
			bytesToDAGScan:   bytesToDAGScan,
			filesToDAGScan:   filesToDAGScan,
			shardedFiles:     shardedFiles,
			dagsScanned:      scanDone,
			dagsToScan:       scanTodo,
			fileBytesScanned: bytesScanned,
		}
	})
}

func newUploadModel(ctx context.Context, repo *sqlrepo.Repo, api preparation.API, uploads []*uploadsmodel.Upload, retry bool) uploadModel {
	if len(uploads) == 0 {
		panic("no uploads provided to upload model")
	}

	ctx, cancel := context.WithCancel(ctx)

	uids := make([]id.UploadID, 0, len(uploads))
	for _, u := range uploads {
		uids = append(uids, u.ID())
	}
	obs := newUploadObserver(repo, uids)
	if err := obs.subscribe(ctx); err != nil {
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

		observer: obs,

		// Initialize Maps
		recentAddedShards:   make(map[id.UploadID][]*sqlrepo.ShardView),
		recentClosedShards:  make(map[id.UploadID][]*sqlrepo.ShardView),
		filesToDAGScan:      make(map[id.UploadID][]*sqlrepo.FSScanView),
		shardedFiles:        make(map[id.UploadID][]sqlrepo.FileInfo),
		openShardsStats:     make(map[id.UploadID]uint64),
		dagScanStats:        make(map[id.UploadID]uint64),
		closedShardSpinners: make(map[id.UploadID]map[id.ShardID]spinner.Model),
		retry:               retry,
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

func RunUploadUI(ctx context.Context, repo *sqlrepo.Repo, api preparation.API, uploads []*uploadsmodel.Upload, retry bool) error {
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
	m, err := tea.NewProgram(newUploadModel(ctx, repo, api, uploads, retry), teaOpts...).Run()
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
