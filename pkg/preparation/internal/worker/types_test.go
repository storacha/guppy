package worker_test

import (
	"errors"
	"testing"

	"github.com/storacha/guppy/pkg/preparation/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestNewFatalError(t *testing.T) {
	t.Run("returns nil when err is nil", func(t *testing.T) {
		require.Nil(t, worker.NewFatalError(nil))
	})

	t.Run("wraps a non-nil error as fatal", func(t *testing.T) {
		err := errors.New("boom")
		te := worker.NewFatalError(err)
		require.NotNil(t, te)
		require.True(t, te.IsFatal())
		require.ErrorIs(t, te.FatalError(), err)
		require.Empty(t, te.NonFatalErrors())
	})
}

func TestNewNonFatalError(t *testing.T) {
	t.Run("returns nil when no errors are supplied", func(t *testing.T) {
		require.Nil(t, worker.NewNonFatalError())
	})

	t.Run("returns nil when all supplied errors are nil", func(t *testing.T) {
		require.Nil(t, worker.NewNonFatalError(nil, nil))
	})

	t.Run("drops nil entries and keeps non-nil ones", func(t *testing.T) {
		a := errors.New("a")
		b := errors.New("b")
		te := worker.NewNonFatalError(nil, a, nil, b)
		require.NotNil(t, te)
		require.False(t, te.IsFatal())
		require.Nil(t, te.FatalError())
		require.Len(t, te.NonFatalErrors(), 2)
		require.ErrorIs(t, te.NonFatalErrors()[0], a)
		require.ErrorIs(t, te.NonFatalErrors()[1], b)
	})
}

func TestTaskErrorUnwrap(t *testing.T) {
	t.Run("exposes non-fatal and fatal errors via errors.Is", func(t *testing.T) {
		nonFatal := errors.New("non-fatal")
		fatal := errors.New("fatal")
		te := worker.Join(
			worker.NewNonFatalError(nonFatal),
			worker.NewFatalError(fatal),
		)
		require.NotNil(t, te)
		require.ErrorIs(t, te, nonFatal)
		require.ErrorIs(t, te, fatal)
	})
}

func TestJoin(t *testing.T) {
	t.Run("returns nil when both operands are nil", func(t *testing.T) {
		require.Nil(t, worker.Join(nil, nil))
	})

	t.Run("returns the non-nil operand when the other is nil", func(t *testing.T) {
		err := errors.New("err")
		te := worker.NewFatalError(err)
		require.Equal(t, te, worker.Join(te, nil))
		require.Equal(t, te, worker.Join(nil, te))
	})

	t.Run("concatenates non-fatal errors and joins fatals", func(t *testing.T) {
		nonFatalA := errors.New("non-fatal A")
		nonFatalB := errors.New("non-fatal B")
		fatalA := errors.New("fatal A")
		fatalB := errors.New("fatal B")

		a := worker.Join(worker.NewNonFatalError(nonFatalA), worker.NewFatalError(fatalA))
		b := worker.Join(worker.NewNonFatalError(nonFatalB), worker.NewFatalError(fatalB))

		joined := worker.Join(a, b)
		require.NotNil(t, joined)
		require.True(t, joined.IsFatal())

		require.Len(t, joined.NonFatalErrors(), 2)
		require.ErrorIs(t, joined.NonFatalErrors()[0], nonFatalA)
		require.ErrorIs(t, joined.NonFatalErrors()[1], nonFatalB)

		require.ErrorIs(t, joined.FatalError(), fatalA)
		require.ErrorIs(t, joined.FatalError(), fatalB)
	})
}
