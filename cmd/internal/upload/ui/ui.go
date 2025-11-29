package ui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	"github.com/mattn/go-isatty"

	"github.com/storacha/guppy/internal/largeupload/bubbleup"
	"github.com/storacha/guppy/pkg/preparation"
	shardsmodel "github.com/storacha/guppy/pkg/preparation/shards/model"
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

	// State (Maps for multi-upload support)
	recentAddedShards  map[id.UploadID][]*shardsmodel.Shard
	recentClosedShards map[id.UploadID][]*shardsmodel.Shard
	filesToDAGScan     map[id.UploadID][]sqlrepo.FileInfo
	shardedFiles       map[id.UploadID][]sqlrepo.FileInfo

	// Aggregated Totals
	addedShardsSize  uint64
	closedShardsSize uint64
	openShardsSize   uint64
	dagScans         uint64

	// Internal tracking for totals
	openShardsStats map[id.UploadID]uint64
	dagScanStats    map[id.UploadID]uint64

	results map[id.UploadID]cid.Cid

	// Bubbles (Nested map: UploadID -> ShardID -> Spinner)
	closedShardSpinners map[id.UploadID]map[id.ShardID]spinner.Model

	// Output
	err error
}

func (m uploadModel) Init() tea.Cmd {
	cmds := make([]tea.Cmd, 0, len(m.uploads)*2)
	for _, u := range m.uploads {
		cmds = append(cmds, executeUpload(m.ctx, m.api, u))
		cmds = append(cmds, checkStats(m.ctx, m.repo, u.ID()))
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

		// 2. Update stats for THIS specific upload
		openSize := uint64(0)
		for _, s := range msg.openShards {
			openSize += s.Size()
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
			if sp, ok := currentSpinners[s.ID()]; ok {
				newSpinners[s.ID()] = sp
			} else {
				newSpinner := spinner.New(spinner.WithSpinner(bubbleup.Spinner(m.rng)))
				newSpinners[s.ID()] = newSpinner
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
				m.addedShardsSize += s.Size()
			}
		}
		for _, shards := range m.recentClosedShards {
			for _, s := range shards {
				m.closedShardsSize += s.Size()
			}
		}
		for _, size := range m.openShardsStats {
			m.openShardsSize += size
		}
		for _, size := range m.dagScanStats {
			m.dagScans += size
		}

		return m, tea.Batch(append(cmds, checkStats(m.ctx, m.repo, msg.uploadID))...)

	case error:
		m.err = msg
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

	output.WriteString("Shards:\n")
	// Iterate uploads to show shards for everyone
	for _, u := range m.uploads {
		uid := u.ID()
		if shards, ok := m.recentAddedShards[uid]; ok {
			for _, s := range shards {
				output.WriteString(renderListItem(style.Foreground(addedShardColor), s.Digest().B58String(), s.Size()))
			}
		}
		if shards, ok := m.recentClosedShards[uid]; ok {
			for _, s := range shards {
				spView := ""
				if spMap, ok := m.closedShardSpinners[uid]; ok {
					if sp, ok := spMap[s.ID()]; ok {
						spView = sp.View() + " "
					}
				}
				output.WriteString(renderListItem(style.Foreground(closedShardColor), spView+s.ID().String(), s.Size()))
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

	output.WriteString("\n\nPress q to quit.\n")

	return output.String()
}

type rootCIDMsg struct {
	id  id.UploadID
	cid cid.Cid
}

func executeUpload(ctx context.Context, api preparation.API, upload *uploadsmodel.Upload) tea.Cmd {
	return func() tea.Msg {
		rootCID, err := api.ExecuteUpload(ctx, upload)
		if err != nil {
			return fmt.Errorf("command failed to execute upload: %w", err)
		}
		return rootCIDMsg{id: upload.ID(), cid: rootCID}
	}
}

type statsMsg struct {
	uploadID       id.UploadID
	addedShards    []*shardsmodel.Shard
	closedShards   []*shardsmodel.Shard
	openShards     []*shardsmodel.Shard
	bytesToDAGScan uint64
	filesToDAGScan []sqlrepo.FileInfo
	shardedFiles   []sqlrepo.FileInfo
}

func checkStats(ctx context.Context, repo *sqlrepo.Repo, uploadID id.UploadID) tea.Cmd {
	return tea.Tick(10*time.Millisecond, func(t time.Time) tea.Msg {
		addedShards, err := repo.ShardsForUploadByState(ctx, uploadID, shardsmodel.ShardStateAdded)
		if err != nil {
			return fmt.Errorf("getting added shards for upload %s: %w", uploadID, err)
		}

		closedShards, err := repo.ShardsForUploadByState(ctx, uploadID, shardsmodel.ShardStateClosed)
		if err != nil {
			return fmt.Errorf("getting closed shards for upload %s: %w", uploadID, err)
		}

		openShards, err := repo.ShardsForUploadByState(ctx, uploadID, shardsmodel.ShardStateOpen)
		if err != nil {
			return fmt.Errorf("getting open shards for upload %s: %w", uploadID, err)
		}

		bytesToDAGScan, err := repo.TotalBytesToScan(ctx, uploadID)
		if err != nil {
			return fmt.Errorf("getting total bytes to scan for upload %s: %w", uploadID, err)
		}

		filesToDAGScan, err := repo.FilesToDAGScan(ctx, uploadID, 6)
		if err != nil {
			return fmt.Errorf("getting info on remaining files for upload %s: %w", uploadID, err)
		}

		shardedFiles, err := repo.ShardedFiles(ctx, uploadID, 6)
		if err != nil {
			return fmt.Errorf("getting info on sharded files for upload %s: %w", uploadID, err)
		}

		return statsMsg{
			uploadID:       uploadID,
			addedShards:    addedShards,
			closedShards:   closedShards,
			openShards:     openShards,
			bytesToDAGScan: bytesToDAGScan,
			filesToDAGScan: filesToDAGScan,
			shardedFiles:   shardedFiles,
		}
	})
}

func newUploadModel(ctx context.Context, repo *sqlrepo.Repo, api preparation.API, uploads []*uploadsmodel.Upload) uploadModel {
	if len(uploads) == 0 {
		panic("no uploads provided to upload model")
	}

	ctx, cancel := context.WithCancel(ctx)

	return uploadModel{
		ctx:     ctx,
		cancel:  cancel,
		repo:    repo,
		api:     api,
		uploads: uploads,
		results: make(map[id.UploadID]cid.Cid),
		rng:     rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
		
		// Initialize Maps
		recentAddedShards:   make(map[id.UploadID][]*shardsmodel.Shard),
		recentClosedShards:  make(map[id.UploadID][]*shardsmodel.Shard),
		filesToDAGScan:      make(map[id.UploadID][]sqlrepo.FileInfo),
		shardedFiles:        make(map[id.UploadID][]sqlrepo.FileInfo),
		openShardsStats:     make(map[id.UploadID]uint64),
		dagScanStats:        make(map[id.UploadID]uint64),
		closedShardSpinners: make(map[id.UploadID]map[id.ShardID]spinner.Model),
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

func RunUploadUI(ctx context.Context, repo *sqlrepo.Repo, api preparation.API, uploads []*uploadsmodel.Upload) error {
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
	m, err := tea.NewProgram(newUploadModel(ctx, repo, api, uploads), teaOpts...).Run()
	if err != nil {
		return fmt.Errorf("command failed to run upload UI: %w", err)
	}
	um := m.(uploadModel)

	if um.err != nil {
		var errBadNodes types.ErrBadNodes
		if errors.As(um.err, &errBadNodes) {
			var sb strings.Builder
			sb.WriteString("\nUpload failed due to out-of-date scan:\n")
			for i, ebn := range errBadNodes.Errs() {
				if i >= 3 {
					sb.WriteString(fmt.Sprintf("...and %d more errors\n", len(errBadNodes.Errs())-i))
					break
				}
				sb.WriteString(fmt.Sprintf(" - %s: %v\n", ebn.CID(), ebn.Unwrap()))
			}
			sb.WriteString("\nYou can resume the upload to re-scan and continue.\n")

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
