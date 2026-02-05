package ui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/paginator"
	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	"github.com/mattn/go-isatty"
	"github.com/muesli/termenv"
	"github.com/storacha/go-libstoracha/digestutil"

	"github.com/storacha/guppy/pkg/bus"
	"github.com/storacha/guppy/pkg/bus/events"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var (
	activeRed   = lipgloss.Color("#dc3030")
	inactiveRed = lipgloss.Color("#873b3b")

	tableStyle = table.Styles{
		Header:   lipgloss.NewStyle().Bold(true).Padding(0, 0),
		Selected: lipgloss.NewStyle().Padding(0, 0),
		Cell:     lipgloss.NewStyle().Padding(0, 0),
	}

	inactiveTabBorder = tabBorderWithBottom("┴", "─", "┴")
	activeTabBorder   = tabBorderWithBottom("┘", " ", "└")
	tabActiveStyle    = lipgloss.NewStyle().
				Border(activeTabBorder, true).
				BorderForeground(activeRed).
				BorderBottom(true).
				Bold(true)

	tabInactiveStyle = lipgloss.NewStyle().
				Border(inactiveTabBorder, true).
				BorderForeground(inactiveRed)

	uploadTabActiveStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(activeRed).
				Bold(true).
				Padding(0, 1)
	uploadTabInactiveStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(inactiveRed).
				Padding(0, 1)

	panelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(inactiveRed)

	shardStates = []string{"Packing", "Queued", "Uploading", "Complete"}
)

func tabBorderWithBottom(left, middle, right string) lipgloss.Border {
	border := lipgloss.RoundedBorder()
	border.BottomLeft = left
	border.Bottom = middle
	border.BottomRight = right
	return border
}

func NewProgressBar() progress.Model {
	return progress.New(progress.WithWidth(40), progress.WithColorProfile(termenv.Ascii))
}

type Canceled struct{}

func (c Canceled) Error() string {
	return "upload canceled"
}

type uploadModel struct {
	updateMu sync.Mutex
	// Configuration
	ctx     context.Context
	cancel  context.CancelFunc
	repo    *sqlrepo.Repo
	api     preparation.API
	uploads []*uploadsmodel.Upload

	observer            *UploadObserver
	currentObservations []Observation
	observationsByID    map[id.UploadID]Observation

	clientPutProgress map[id.UploadID]map[id.ID]progress.Model
	dagProgress       map[id.UploadID]progress.Model
	uploadTotals      map[id.UploadID]float64
	uploadCompleted   map[id.UploadID]float64
	progressSamples   map[id.UploadID]progressSample
	tabTitles         []string
	tabIndex          int

	shardTabIndex   map[id.UploadID]int
	shardPaginators map[id.UploadID]map[string]paginator.Model
	shardsByState   map[id.UploadID]map[string][]events.ShardView

	workerStates map[id.UploadID]map[string]*workerStateSpinner

	retry bool

	results map[id.UploadID]cid.Cid

	// Output
	err              error
	lastRetriableErr *types.RetriableError
}

type workerStateSpinner struct {
	state   events.UploadWorkerEvent
	spinner spinner.Model
}

type progressSample struct {
	bytes float64
	rate  float64
	t     time.Time
}

func (m *uploadModel) Init() tea.Cmd {
	cmds := make([]tea.Cmd, 0, len(m.uploads)*2)
	// Setup tab titles based on uploads.
	m.tabTitles = make([]string, len(m.uploads))
	for i, u := range m.uploads {
		us, err := m.repo.GetSourceByID(m.ctx, u.SourceID())
		if err != nil {
			panic(err)
		}
		m.tabTitles[i] = us.Path()
	}
	m.tabIndex = 0

	for _, u := range m.uploads {
		cmds = append(cmds, executeUpload(m.ctx, m.api, u))
		cmds = append(cmds, tea.Tick(time.Second, func(t time.Time) tea.Msg {
			return m.observer.Observe(m.ctx)
		}))
	}

	// init shard tab/paginators
	m.shardTabIndex = make(map[id.UploadID]int, len(m.uploads))
	m.shardPaginators = make(map[id.UploadID]map[string]paginator.Model, len(m.uploads))
	m.shardsByState = make(map[id.UploadID]map[string][]events.ShardView, len(m.uploads))
	m.workerStates = make(map[id.UploadID]map[string]*workerStateSpinner, len(m.uploads))
	for _, u := range m.uploads {
		m.shardTabIndex[u.ID()] = 0
		m.shardPaginators[u.ID()] = make(map[string]paginator.Model)
	}

	return tea.Batch(cmds...)
}

func (m *uploadModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	m.updateMu.Lock()
	defer m.updateMu.Unlock()
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.cancel()
			return m, nil
		case "left", "h":
			if m.activeUploadID() != nil {
				uid := *m.activeUploadID()
				if m.shardTabIndex[uid] > 0 {
					m.shardTabIndex[uid]--
				}
			}
			return m, nil
		case "right", "l":
			if m.activeUploadID() != nil {
				uid := *m.activeUploadID()
				if m.shardTabIndex[uid] < len(shardStates)-1 {
					m.shardTabIndex[uid]++
				}
			}
			return m, nil
		case "shift+up", "shift+k":
			if uid := m.activeUploadID(); uid != nil {
				state := shardStates[m.shardTabIndex[*uid]]
				p := m.ensurePaginator(*uid, state, len(m.shardsByState[*uid][state]))
				p.PrevPage()
				m.shardPaginators[*uid][state] = p
			}
			return m, nil
		case "shift+down", "shift+j":
			if uid := m.activeUploadID(); uid != nil {
				state := shardStates[m.shardTabIndex[*uid]]
				p := m.ensurePaginator(*uid, state, len(m.shardsByState[*uid][state]))
				p.NextPage()
				m.shardPaginators[*uid][state] = p
			}
			return m, nil
		case "up", "k":
			if m.tabIndex > 0 {
				m.tabIndex--
			}
			return m, nil
		case "down", "j":
			if m.tabIndex < len(m.tabTitles)-1 {
				m.tabIndex++
			}
			return m, nil
		case "1", "2", "3", "4", "5", "6", "7", "8", "9":
			i := int(msg.String()[0] - '1')
			if i >= 0 && i < len(m.tabTitles) {
				m.tabIndex = i
			}
			return m, nil
		default:
			return m, nil
		}

	case uploadComplete:
		return m, tea.Quit

	case rootCIDMsg:
		var cmds []tea.Cmd
		m.results[msg.id] = msg.cid

		for _, spnr := range m.workerStates[msg.id] {
			if spnr.state.Status == events.Running {
				spnr.state.Status = events.Stopped
			}
		}

		cmds = append(cmds, tea.Tick(time.Second, func(t time.Time) tea.Msg { return nil }))

		// Only quit if ALL uploads are finished
		if len(m.results) == len(m.uploads) {
			cmds = append(cmds, tea.Tick(time.Second, func(t time.Time) tea.Msg {
				return uploadComplete{results: m.results}
			}))
		}
		return m, tea.Sequence(cmds...)

	case []Observation:
		var cmds []tea.Cmd
		m.currentObservations = msg
		for _, obs := range msg {
			m.observationsByID[obs.Model.ID()] = obs
		}

		for _, obs := range m.currentObservations {
			uid := obs.Model.ID()
			if _, ok := m.clientPutProgress[uid]; !ok {
				m.clientPutProgress[uid] = make(map[id.ID]progress.Model)
			}
			if _, ok := m.dagProgress[uid]; !ok {
				m.dagProgress[uid] = NewProgressBar()
			}
			var uploadedBytes float64
			for bid, pgrs := range obs.ClientUploadProgress {
				cpb, ok := m.clientPutProgress[uid][bid]
				if !ok {
					cpb = NewProgressBar()
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
				if deltaT > 0 && deltaBytes > 0 {
					instRate := deltaBytes / deltaT
					if prev.rate > 0 {
						// Clamp instantaneous rate change to ±20% of previous to reduce jitter.
						lower := prev.rate * 0.8
						upper := prev.rate * 1.2
						if instRate < lower {
							instRate = lower
						} else if instRate > upper {
							instRate = upper
						}
						// Smooth with heavier weight on prior rate.
						instRate = 0.8*prev.rate + 0.2*instRate
					}
					sample.rate = instRate
				} else {
					sample.rate = prev.rate
				}
			}
			m.progressSamples[uid] = sample

			// Build shard state slices and paginators.
			stateSlices := map[string][]events.ShardView{
				"Queued":    {},
				"Packing":   {},
				"Uploading": {},
				"Complete":  {},
			}
			stateSlices["Packing"] = append(stateSlices["Packing"], obs.OpenShards...)
			stateSlices["Queued"] = append(stateSlices["Queued"], obs.ClosedShards...)
			stateSlices["Uploading"] = append(stateSlices["Uploading"], obs.UploadedShards...)
			stateSlices["Complete"] = append(stateSlices["Complete"], obs.AddedShards...)
			// If a queued shard has upload progress, treat it as uploading.
			if bars, ok := m.clientPutProgress[uid]; ok {
				stillQueued := make([]events.ShardView, 0, len(stateSlices["Queued"]))
				for _, sv := range stateSlices["Queued"] {
					if _, uploading := bars[sv.ID]; uploading {
						stateSlices["Uploading"] = append(stateSlices["Uploading"], sv)
					} else {
						stillQueued = append(stillQueued, sv)
					}
				}
				stateSlices["Queued"] = stillQueued
			}
			m.shardsByState[uid] = stateSlices
			for _, st := range shardStates {
				p := m.ensurePaginator(uid, st, len(stateSlices[st]))
				m.shardPaginators[uid][st] = p
			}

			for _, state := range obs.WorkerStates {
				spnrs := m.workerStates[uid]
				if spnrs == nil {
					spnrs = make(map[string]*workerStateSpinner)
					m.workerStates[uid] = spnrs
				}
				_, ok := spnrs[state.Name]
				if !ok {
					spnrs[state.Name] = &workerStateSpinner{
						state: state,
						spinner: spinner.New(spinner.WithSpinner(spinner.Spinner{
							Frames: []string{"∙∙∙", "●∙∙", "∙●∙", "∙∙●"},
							FPS:    250 * time.Millisecond,
						})),
					}
					m.workerStates[uid] = spnrs
				}
				sp := spnrs[state.Name].spinner
				cmds = append(cmds, tea.Tick(3*time.Second, func(t time.Time) tea.Msg {
					return sp.Tick()
				}))
			}
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
		for uid, spnrs := range m.workerStates {
			for evt, s := range spnrs {
				var cmd tea.Cmd
				s.spinner, cmd = s.spinner.Update(msg)
				s.spinner.Tick()
				m.workerStates[uid][evt] = s
				cmds = append(cmds, cmd)
			}
		}
		return m, tea.Sequence(cmds...)
	}
}

func (m *uploadModel) activeUploadID() *id.UploadID {
	if len(m.uploads) == 0 {
		return nil
	}
	if m.tabIndex < 0 {
		m.tabIndex = 0
	}
	if m.tabIndex >= len(m.uploads) {
		m.tabIndex = len(m.uploads) - 1
	}
	uid := m.uploads[m.tabIndex].ID()
	return &uid
}

func (m *uploadModel) ensurePaginator(uid id.UploadID, state string, totalItems int) paginator.Model {
	if _, ok := m.shardPaginators[uid]; !ok {
		m.shardPaginators[uid] = make(map[string]paginator.Model)
	}
	p, ok := m.shardPaginators[uid][state]
	if !ok {
		p = paginator.New()
		p.PerPage = 10
	}
	p.SetTotalPages(totalItems)
	if p.TotalPages < 1 {
		p.Page = 0
	} else if p.Page >= p.TotalPages {
		p.Page = p.TotalPages - 1
	}
	m.shardPaginators[uid][state] = p
	return p
}

func (m *uploadModel) overallProgress(obs Observation) (completedBytes float64, totalBytes float64) {
	uid := obs.Model.ID()

	var totalShardSize uint64
	blobBars := m.clientPutProgress[uid]

	// Track which shards are fully done.
	added := make(map[id.ShardID]struct{}, len(obs.AddedShards))
	for _, s := range obs.AddedShards {
		added[s.ID] = struct{}{}
	}
	uploaded := make(map[id.ShardID]struct{}, len(obs.UploadedShards))
	for _, s := range obs.UploadedShards {
		uploaded[s.ID] = struct{}{}
	}

	allShards := make([]events.ShardView, 0, len(obs.OpenShards)+len(obs.ClosedShards)+len(obs.UploadedShards)+len(obs.AddedShards))
	allShards = append(allShards, obs.OpenShards...)
	allShards = append(allShards, obs.ClosedShards...)
	allShards = append(allShards, obs.UploadedShards...)
	allShards = append(allShards, obs.AddedShards...)

	for _, s := range allShards {
		totalShardSize += s.Size
		if _, ok := added[s.ID]; ok {
			completedBytes += float64(s.Size)
			continue
		}
		if _, ok := uploaded[s.ID]; ok {
			completedBytes += float64(s.Size)
			continue
		}
		if pb, ok := blobBars[s.ID]; ok {
			completedBytes += float64(s.Size) * pb.Percent()
		}
	}

	totalBytes = m.uploadTotals[uid]
	if totalBytes == 0 {
		totalBytes = float64(totalShardSize)
	}
	if totalBytes > 0 && completedBytes > totalBytes {
		completedBytes = totalBytes
	}
	return
}

func (m *uploadModel) buildUploadLabels() []string {
	labels := make([]string, len(m.tabTitles))
	for i, upload := range m.uploads {
		labelBase := m.tabTitles[i]
		if obs, ok := m.observationsByID[upload.ID()]; ok {
			completedBytes, totalBytes := m.overallProgress(obs)
			pct := 0.0
			if totalBytes > 0 {
				pct = completedBytes / totalBytes
				if pct > 1 {
					pct = 1
				}
			}
			if _, ok := m.results[upload.ID()]; ok {
				labels[i] = fmt.Sprintf("%s %s", labelBase, "✔")
			} else {
				eta := formatETA(totalBytes, completedBytes, m.progressSamples[upload.ID()])
				labels[i] = fmt.Sprintf("%s %s %3.0f%% %s", labelBase, renderMiniBar(pct, 8), pct*100, eta)
			}
		} else {
			labels[i] = labelBase
		}
	}
	return labels
}

func (m *uploadModel) renderSummary(obs Observation, activeIdx int) string {
	rootCidStr := "Calculating..."
	rootCid, uploadDone := m.results[obs.Model.ID()]
	if uploadDone {
		rootCidStr = rootCid.String()
	}
	completedBytes, totalBytes := m.overallProgress(obs)
	summaryCols := []table.Column{
		{Title: "", Width: 25},
		{Title: "", Width: 80},
	}
	summaryRows := []table.Row{
		{"Upload ID:", obs.Model.ID().String()},
		{"Space:", obs.Model.SpaceDID().String()},
		{"Root CID:", rootCidStr},
		{"", ""},
		{"Directory:", m.tabTitles[activeIdx]},
		{"Size:", humanize.IBytes(uint64(totalBytes))},
		{"Bytes Uploaded:", humanize.IBytes(uint64(completedBytes))},
		{"Files Found:", fmt.Sprintf("%d", obs.TotalDags)},
		{"DAGs Created:", fmt.Sprintf("%d", func() uint64 {
			if obs.ProcessedDags != 0 {
				return obs.ProcessedDags
			}
			return obs.TotalDags
		}())},
		{"", ""},
	}
	if pb, ok := m.dagProgress[obs.Model.ID()]; ok {
		if pb.Percent() == 1 {
			summaryRows = append(summaryRows, table.Row{"Scan Progress:", "✔"})
		} else {
			summaryRows = append(summaryRows, table.Row{"Scan Progress:", pb.View()})
		}
	}

	if spinners, ok := m.workerStates[obs.Model.ID()]; ok {
		names := make([]string, 0, len(spinners))
		for name := range spinners {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			ws := spinners[name]
			status := ""
			switch ws.state.Status {
			case events.Running:
				status = ws.spinner.View()
			case events.Stopped:
				status = "✔"
			case events.Failed:
				status = fmt.Sprintf("X: %s", ws.state.Error)
			default:
				status = string(ws.state.Status)
			}
			summaryRows = append(summaryRows, table.Row{
				fmt.Sprintf("%s Worker:", name),
				status,
			})
		}
	}

	summaryStyles := table.DefaultStyles()
	summaryStyles.Header = lipgloss.NewStyle().UnsetPadding().UnsetBorderStyle().Height(0)
	summaryStyles.Cell = lipgloss.NewStyle().UnsetPadding()
	summaryStyles.Selected = summaryStyles.Cell

	summaryTable := table.New(
		table.WithColumns(summaryCols),
		table.WithRows(summaryRows),
		table.WithStyles(summaryStyles),
		table.WithHeight(len(summaryRows)+2),
		table.WithFocused(false),
	)
	return summaryTable.View()
}

func (m *uploadModel) renderShardTable(state string, shards []events.ShardView, blobBars map[id.ID]progress.Model) string {
	var cols []table.Column
	var rowsData []table.Row

	switch state {
	case "Packing":
		cols = []table.Column{
			{Title: "ID", Width: 37},
			{Title: "Size", Width: 9},
		}
		for _, sv := range shards {
			rowsData = append(rowsData, table.Row{
				sv.ID.String(),
				humanize.IBytes(sv.Size),
			})
		}

	case "Queued":
		cols = []table.Column{
			{Title: "ID", Width: 37},
			{Title: "Size", Width: 9},
			{Title: "Digest", Width: 52},
		}
		for _, sv := range shards {
			rowsData = append(rowsData, table.Row{
				sv.ID.String(),
				humanize.IBytes(sv.Size),
				digestutil.Format(sv.Digest),
			})
		}

	case "Uploading":
		cols = []table.Column{
			{Title: "ID", Width: 37},
			{Title: "Size", Width: 9},
			{Title: "Digest", Width: 52},
			{Title: "Put Progress", Width: 40},
		}
		for _, sv := range shards {
			pbStr := "--"
			if pb, ok := blobBars[sv.ID]; ok {
				pbStr = pb.View()
			}
			rowsData = append(rowsData, table.Row{
				sv.ID.String(),
				humanize.IBytes(sv.Size),
				digestutil.Format(sv.Digest),
				pbStr,
			})
		}

	case "Complete":
		cols = []table.Column{
			{Title: "ID", Width: 37},
			{Title: "Size", Width: 9},
			{Title: "Digest", Width: 52},
			{Title: "PieceCID", Width: 72},
		}
		for _, sv := range shards {
			rowsData = append(rowsData, table.Row{
				sv.ID.String(),
				humanize.IBytes(sv.Size),
				digestutil.Format(sv.Digest),
				sv.PieceCID.String(),
			})
		}
	}

	t := table.New(
		table.WithColumns(cols),
		table.WithRows(rowsData),
		table.WithStyles(tableStyle),
		table.WithFocused(false),
	)
	return t.View()
}

func (m *uploadModel) View() string {
	var body strings.Builder
	style := lipgloss.NewStyle()

	if m.retry {
		body.WriteString(style.Bold(true).Render("Uploading with auto-retry enabled...\n\n"))
	}

	if len(m.currentObservations) == 0 {
		return body.String()
	}

	active := m.tabIndex
	if active >= len(m.tabTitles) {
		active = len(m.tabTitles) - 1
	}
	if active < 0 {
		active = 0
	}

	tabsRendered := renderUploadTabs(m.buildUploadLabels(), active)

	activeID := m.uploads[active].ID()
	u, ok := m.observationsByID[activeID]
	if !ok {
		body.WriteString("No data yet for this upload...\n")
		return lipgloss.JoinVertical(lipgloss.Top, tabsRendered, panelStyle.Render(body.String()))
	}

	body.WriteString(m.renderSummary(u, active))

	// Aggregate shard totals and build rows.
	type shardRow struct {
		view  events.ShardView
		state string
		order int
	}
	rows := make([]shardRow, 0, len(u.OpenShards)+len(u.ClosedShards)+len(u.UploadedShards)+len(u.AddedShards))

	addRows := func(shards []events.ShardView, state string, order int) {
		for _, s := range shards {
			rows = append(rows, shardRow{view: s, state: state, order: order})
		}
	}

	addRows(u.OpenShards, "Packing", 4)
	addRows(u.ClosedShards, "Queued", 3)
	addRows(u.UploadedShards, "Uploading", 2)
	addRows(u.AddedShards, "Complete", 1)

	blobBars := m.clientPutProgress[u.Model.ID()]
	for i := range rows {
		row := &rows[i]
		if _, ok := blobBars[row.view.ID]; ok {
			row.state = "Uploading"
			row.order = 2
			continue
		}
	}

	body.WriteString("\nShard Progress:\n")
	uid := u.Model.ID()
	stateIdx := m.shardTabIndex[uid]
	if stateIdx < 0 {
		stateIdx = 0
	}
	if stateIdx >= len(shardStates) {
		stateIdx = len(shardStates) - 1
	}
	activeState := shardStates[stateIdx]
	stateMap := m.shardsByState[uid]
	var shardTabSegments []string
	for i, st := range shardStates {
		count := 0
		if stateMap != nil {
			count = len(stateMap[st])
		}
		label := fmt.Sprintf("%s (%d)", st, count)
		if i == stateIdx {
			shardTabSegments = append(shardTabSegments, tabActiveStyle.Render(label))
		} else {
			shardTabSegments = append(shardTabSegments, tabInactiveStyle.Render(label))
		}
	}
	body.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, shardTabSegments...))
	body.WriteString("\n")

	activeShards := stateMap[activeState]
	p := m.ensurePaginator(uid, activeState, len(activeShards))
	start, end := p.GetSliceBounds(len(activeShards))
	pageShards := activeShards[start:end]

	body.WriteString(m.renderShardTable(activeState, pageShards, blobBars))
	body.WriteString(fmt.Sprintf("Page %d/%d\n", p.Page+1, p.TotalPages))

	if m.lastRetriableErr != nil {
		body.WriteString("\n\n")
		body.WriteString(style.Bold(true).Foreground(lipgloss.Color("#FFA500")).Render("Last retriable error: "))
		if listErr, ok := m.lastRetriableErr.Unwrap().(interface{ Unwrap() []error }); ok {
			body.WriteString("Multiple errors...\n")
			for i, e := range listErr.Unwrap() {
				if i >= 3 {
					body.WriteString(style.Italic(true).Render(fmt.Sprintf("...and %d more errors\n", len(listErr.Unwrap())-i)))
					break
				}
				body.WriteString(e.Error() + "\n")
			}
		} else {
			body.WriteString(m.lastRetriableErr.Unwrap().Error() + "\n")
		}
	}

	body.WriteString("Use numbers or ↑/↓ to switch uploads • ←/→ shard tabs • Shift+↑/↓ page\n")
	body.WriteString("Press q to quit.\n")

	return lipgloss.JoinVertical(lipgloss.Top, tabsRendered, panelStyle.Render(body.String()))
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

	bytesFrac := fmt.Sprintf(" [%s of %s]", humanize.IBytes(uint64(uploaded)), humanize.IBytes(uint64(total)))

	return etaStr + throughput + bytesFrac
}

func renderMiniBar(pct float64, width int) string {
	if width <= 0 {
		return ""
	}
	if pct < 0 {
		pct = 0
	}
	if pct > 1 {
		pct = 1
	}
	filled := int(math.Round(pct * float64(width)))
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func renderUploadTabs(titles []string, active int) string {
	if len(titles) == 0 {
		return ""
	}

	var renderedTabs []string
	for i, t := range titles {
		label := fmt.Sprintf("[%d] %s", i+1, t)
		isFirst, isLast, isActive := i == 0, i == len(titles)-1, i == active
		style := uploadTabInactiveStyle
		if isActive {
			style = uploadTabActiveStyle
		}
		border, _, _, _, _ := style.GetBorder()
		if isFirst && isActive {
			border.BottomLeft = "│"
		} else if isFirst && !isActive {
			border.BottomLeft = "├"
		} else if isLast && isActive {
			border.BottomRight = "│"
		} else if isLast && !isActive {
			border.BottomRight = "┤"
		}
		style = style.Border(border)
		renderedTabs = append(renderedTabs, style.Render(label))
	}
	return lipgloss.JoinVertical(lipgloss.Top, renderedTabs...)
}

type rootCIDMsg struct {
	id  id.UploadID
	cid cid.Cid
}

type uploadErrMsg struct {
	id  id.UploadID
	err error
}

type uploadComplete struct {
	results map[id.UploadID]cid.Cid
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

func newUploadModel(
	ctx context.Context,
	repo *sqlrepo.Repo,
	api preparation.API,
	uploads []*uploadsmodel.Upload,
	retry bool,
	eb bus.Subscriber,
) *uploadModel {
	if len(uploads) == 0 {
		panic("no uploads provided to upload model")
	}

	ctx, cancel := context.WithCancel(ctx)

	obs := NewUploadObserver(eb, repo, uploads...)
	if err := obs.Subscribe(); err != nil {
		panic(err)
	}

	return &uploadModel{
		ctx:     ctx,
		cancel:  cancel,
		repo:    repo,
		api:     api,
		uploads: uploads,
		results: make(map[id.UploadID]cid.Cid),

		observer:          obs,
		clientPutProgress: make(map[id.UploadID]map[id.ID]progress.Model),
		dagProgress:       make(map[id.UploadID]progress.Model),
		uploadTotals:      make(map[id.UploadID]float64),
		uploadCompleted:   make(map[id.UploadID]float64),
		progressSamples:   make(map[id.UploadID]progressSample),
		observationsByID:  make(map[id.UploadID]Observation),

		retry: retry,
	}
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
	um := m.(*uploadModel)

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
