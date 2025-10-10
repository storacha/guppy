package ui

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
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

	// State
	recentAddedShards  []*shardsmodel.Shard
	addedShardsSize    uint64
	recentClosedShards []*shardsmodel.Shard
	closedShardsSize   uint64
	openShardsSize     uint64
	dagScans           uint64
	filesToDAGScan     []sqlrepo.FileInfo
	shardedFiles       []sqlrepo.FileInfo
	rootCID            cid.Cid

	// Bubbles
	closedShardSpinners map[id.ShardID]spinner.Model

	// Output
	err error
}

func (m uploadModel) Init() tea.Cmd {
	cmds := make([]tea.Cmd, 0, len(m.uploads)+1)
	for _, u := range m.uploads {
		cmds = append(cmds, executeUpload(m.ctx, m.api, u))
	}
	// TK: Handle stats for more than one upload. Right now, we're just looking at
	// the first one to make the UI easier, since that's almost always what we
	// have.
	cmds = append(cmds, checkStats(m.ctx, m.repo, m.uploads[0].ID()))
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
		m.rootCID = cid.Cid(msg)
		return m, tea.Quit

	case statsMsg:
		var cmds []tea.Cmd

		m.recentAddedShards = msg.addedShards
		m.addedShardsSize = 0
		for _, s := range msg.addedShards {
			m.addedShardsSize += s.Size()
		}

		m.recentClosedShards = msg.closedShards
		m.closedShardsSize = 0
		for _, s := range msg.closedShards {
			m.closedShardsSize += s.Size()
		}

		previousSpinners := m.closedShardSpinners
		m.closedShardSpinners = make(map[id.ShardID]spinner.Model)
		for _, s := range msg.closedShards {
			if sp, ok := previousSpinners[s.ID()]; ok {
				m.closedShardSpinners[s.ID()] = sp
			} else {
				newSpinner := spinner.New(spinner.WithSpinner(bubbleup.Spinner(m.rng)))
				m.closedShardSpinners[s.ID()] = newSpinner
				cmds = append(cmds, newSpinner.Tick)
			}
		}

		m.openShardsSize = 0
		for _, s := range msg.openShards {
			m.openShardsSize += s.Size()
		}

		m.dagScans = msg.bytesToDAGScan
		m.filesToDAGScan = msg.filesToDAGScan
		m.shardedFiles = msg.shardedFiles

		return m, tea.Batch(append(cmds, checkStats(m.ctx, m.repo, msg.uploadID))...)

	case error:
		m.err = msg
		return m, tea.Quit

	default:
		var cmds []tea.Cmd
		for id, sp := range m.closedShardSpinners {
			updatedSpinner, cmd := sp.Update(msg)
			m.closedShardSpinners[id] = updatedSpinner
			cmds = append(cmds, cmd)
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
	for _, s := range m.recentAddedShards {
		output.WriteString(renderListItem(style.Foreground(addedShardColor), s.Digest().B58String(), s.Size()))
	}

	for _, s := range m.recentClosedShards {
		output.WriteString(renderListItem(style.Foreground(closedShardColor), m.closedShardSpinners[s.ID()].View()+" "+s.ID().String(), s.Size()))
	}

	output.WriteString("\n")

	output.WriteString("Added to Shards:\n")
	for i, fi := range m.shardedFiles {
		if i < 5 {
			output.WriteString(renderListItem(style.Foreground(shardedColor), fi.Path, fi.Size))
		} else {
			output.WriteString(style.Foreground(shardedColor).Italic(true).Render("... and more\n"))
		}
	}

	output.WriteString("\n")

	output.WriteString("Scanning Files:\n")
	for i, fi := range m.filesToDAGScan {
		if i < 5 {
			output.WriteString(renderListItem(style.Foreground(scannedColor), fi.Path, fi.Size))
		} else {
			output.WriteString(style.Foreground(scannedColor).Italic(true).Render("... and more\n"))
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

type rootCIDMsg cid.Cid

func executeUpload(ctx context.Context, api preparation.API, upload *uploadsmodel.Upload) tea.Cmd {
	return func() tea.Msg {
		rootCID, err := api.ExecuteUpload(ctx, upload)
		if err != nil {
			return fmt.Errorf("command failed to execute upload: %w", err)
		}

		return rootCIDMsg(rootCID)
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
		addedShards, err := repo.ShardsForUploadByStatus(ctx, uploadID, shardsmodel.ShardStateAdded)
		if err != nil {
			return fmt.Errorf("getting added shards for upload %s: %w", uploadID, err)
		}

		closedShards, err := repo.ShardsForUploadByStatus(ctx, uploadID, shardsmodel.ShardStateClosed)
		if err != nil {
			return fmt.Errorf("getting closed shards for upload %s: %w", uploadID, err)
		}

		openShards, err := repo.ShardsForUploadByStatus(ctx, uploadID, shardsmodel.ShardStateOpen)
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
		rng:     rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
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
		eigthBlocks := int(math.Round(float64(bar.value*width*8) / float64(total)))

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

type multiBar struct {
	bars  []bar
	width int
}

func (mb multiBar) Init() tea.Cmd {
	return nil
}

func (mb multiBar) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	return mb, nil
}

func (mb multiBar) View() string {
	return renderBars(mb.bars, mb.width)
}

func RunUploadUI(ctx context.Context, repo *sqlrepo.Repo, api preparation.API, uploads []*uploadsmodel.Upload) error {
	if len(uploads) == 0 {
		return fmt.Errorf("no uploads provided to upload UI")
	}

	m, err := tea.NewProgram(newUploadModel(ctx, repo, api, uploads)).Run()
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

	return nil
}
