// Package ticker provides a simple ticker model for Bubble Tea. It's
// essentially the insides of a spinner; in fact, the spinner bubble could be
// implemented using it. It probably belongs upstream, eventually.
package ticker

import (
	"errors"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// Internal ID management. Used during animating to ensure that frame messages
// are received only by spinner components that sent them.
var lastID int64

func nextID() int {
	return int(atomic.AddInt64(&lastID, 1))
}

// Model contains the state for the spinner. Use New to create new models
// rather than using Model as a struct literal.
type Model struct {
	Value int
	D     time.Duration
	id    int
	tag   int
}

// ID returns the spinner's unique ID.
func (m Model) ID() int {
	return m.id
}

// New returns a model with default values.
func New(d time.Duration) Model {
	m := Model{
		D:  d,
		id: nextID(),
	}

	return m
}

// TickMsg indicates that the timer has ticked and we should render a frame.
type TickMsg struct {
	tag int
	id  int
}

func (m Model) Update(msg tea.Msg) (Model, tea.Cmd) {
	switch msg := msg.(type) {
	case TickMsg:
		// If this is not the next tick for this ticker, ignore it.
		if msg.id != m.id || msg.tag != m.tag {
			return m, nil
		}

		m.Value++
		m.tag++
		return m, m.tick(m.id, m.tag)

	default:
		return m, nil
	}
}

func (m Model) Tick() tea.Msg {
	if m.id == 0 {
		return errors.New("Tick called on uninitialized Ticker")
	}

	return TickMsg{
		id:  m.id,
		tag: m.tag,
	}
}

func (m Model) tick(id, tag int) tea.Cmd {
	return tea.Tick(m.D, func(t time.Time) tea.Msg {
		return TickMsg{
			id:  id,
			tag: tag,
		}
	})
}
