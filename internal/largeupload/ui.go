package largeupload

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation"
	shardsmodel "github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

type uploadModel struct {
	ctx    context.Context
	repo   *sqlrepo.Repo
	api    preparation.API
	upload *uploadsmodel.Upload

	recentAddedShards  []*shardsmodel.Shard
	addedShardsSize    uint64
	recentClosedShards []*shardsmodel.Shard
	closedShardsSize   uint64
	openShardsSize     uint64
	dagScans           uint64
	filesToDAGScan     []sqlrepo.FileInfo
	shardedFiles       []sqlrepo.FileInfo
	rootCID            cid.Cid

	spinner spinner.Model
}

func (m uploadModel) Init() tea.Cmd {
	return tea.Batch(
		executeUpload(m.ctx, m.api, m.upload),
		checkStats(m.ctx, m.repo, m.upload.ID()),
		m.spinner.Tick,
	)
}

func (m uploadModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		default:
			return m, nil
		}

	case rootCIDMsg:
		m.rootCID = cid.Cid(msg)
		return m, tea.Quit

	case statsMsg:
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

		m.openShardsSize = 0
		for _, s := range msg.openShards {
			m.openShardsSize += s.Size()
		}

		m.dagScans = msg.bytesToDAGScan
		m.filesToDAGScan = msg.filesToDAGScan
		m.shardedFiles = msg.shardedFiles

		return m, checkStats(m.ctx, m.repo, m.upload.ID())

	case error:
		fmt.Println("Error:", msg)
		return m, tea.Quit

	default:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}
}

func renderListItem(style lipgloss.Style, text string, size uint64) string {
	return "• " + style.Render(text) + " " + style.Faint(true).Render(humanize.Bytes(size)) + "\n"
}

func (m uploadModel) View() string {
	red := lipgloss.Color("#D63328")
	yellow := lipgloss.Color("#F7CA5B")
	blue := lipgloss.Color("#3373C9")
	lightblue := lipgloss.Color("#C4DFFC")

	var output strings.Builder

	output.WriteString("Uploading " + m.upload.ID().String() + "\n\n")

	style := lipgloss.NewStyle()

	output.WriteString("Shards:\n")
	for _, s := range m.recentAddedShards {
		output.WriteString(renderListItem(style.Foreground(red), s.Digest().B58String(), s.Size()))
	}

	for _, s := range m.recentClosedShards {
		output.WriteString(renderListItem(style.Foreground(yellow), m.spinner.View()+" "+s.ID().String(), s.Size()))
	}

	output.WriteString("\n")

	output.WriteString("Added to Shards:\n")
	for i, fi := range m.shardedFiles {
		if i < 5 {
			output.WriteString(renderListItem(style.Foreground(blue), fi.Path, fi.Size))
		} else {
			output.WriteString(style.Foreground(blue).Italic(true).Render("... and more\n"))
		}
	}

	output.WriteString("\n")

	output.WriteString("Scanning Files:\n")
	for i, fi := range m.filesToDAGScan {
		if i < 5 {
			output.WriteString(renderListItem(style.Foreground(lightblue), fi.Path, fi.Size))
		} else {
			output.WriteString(style.Foreground(lightblue).Italic(true).Render("... and more\n"))
		}
	}

	output.WriteString("\n")

	output.WriteString(renderBars([]bar{
		{color: red, value: int(m.addedShardsSize)},
		{color: yellow, value: int(m.closedShardsSize)},
		{color: blue, value: int(m.openShardsSize)},
		{color: lightblue, value: int(m.dagScans)},
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
			panic(fmt.Errorf("getting added shards for upload %s: %w", uploadID, err))
		}

		closedShards, err := repo.ShardsForUploadByStatus(ctx, uploadID, shardsmodel.ShardStateClosed)
		if err != nil {
			panic(fmt.Errorf("getting closed shards for upload %s: %w", uploadID, err))
		}

		openShards, err := repo.ShardsForUploadByStatus(ctx, uploadID, shardsmodel.ShardStateOpen)
		if err != nil {
			panic(fmt.Errorf("getting open shards for upload %s: %w", uploadID, err))
		}

		bytesToDAGScan, err := repo.TotalBytesToScan(ctx, uploadID)
		if err != nil {
			panic(fmt.Errorf("getting total bytes to scan for upload %s: %w", uploadID, err))
		}

		filesToDAGScan, err := repo.FilesToDAGScan(ctx, uploadID, 6)
		if err != nil {
			panic(fmt.Errorf("getting info on remaining files for upload %s: %w", uploadID, err))
		}

		shardedFiles, err := repo.ShardedFiles(ctx, uploadID, 6)
		if err != nil {
			panic(fmt.Errorf("getting info on sharded files for upload %s: %w", uploadID, err))
		}

		return statsMsg{
			addedShards:    addedShards,
			closedShards:   closedShards,
			openShards:     openShards,
			bytesToDAGScan: bytesToDAGScan,
			filesToDAGScan: filesToDAGScan,
			shardedFiles:   shardedFiles,
		}
	})
}

var brailleSpinner = spinner.Spinner{
	Frames: []string{
		"⣸",
		"⡼",
		"⡞",
		"⢏",
		"⢣",
		"⢱",
		"⡸",
		"⡜",
		"⡎",
		"⣇",
		"⢧",
		"⢳",
		"⡹",
		"⡜",
		"⡎",
		"⢇",
		"⢣",
		"⢱",
	},
	FPS: time.Second / 10,
}

func newUploadModel(ctx context.Context, repo *sqlrepo.Repo, api preparation.API, upload *uploadsmodel.Upload) uploadModel {
	return uploadModel{
		ctx:     ctx,
		repo:    repo,
		api:     api,
		upload:  upload,
		spinner: spinner.New(spinner.WithSpinner(brailleSpinner)),
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
