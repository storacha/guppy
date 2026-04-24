package worker_test

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/storacha/guppy/pkg/preparation/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestGroupLaunchSkipsIfCancelled(t *testing.T) {
	t.Run("task is skipped if group ctx is already cancelled before launch", func(t *testing.T) {
		fatalErr := errors.New("fatal")

		g, gctx := worker.WithContext(t.Context())

		// First task cancels the group by returning a fatal.
		g.Go(func() worker.TaskError {
			return worker.NewFatalError(fatalErr)
		})

		// Wait for cancellation to propagate before enqueuing the second
		// task, so the second task's goroutine sees a cancelled ctx on entry.
		<-gctx.Done()

		var ranSecond atomic.Bool
		g.Go(func() worker.TaskError {
			ranSecond.Store(true)
			return worker.NewFatalError(errors.New("should not be reported"))
		})

		res := g.Wait()
		require.False(t, ranSecond.Load(), "task enqueued after cancellation should be skipped")
		require.NotNil(t, res)
		require.True(t, res.IsFatal())
		require.ErrorIs(t, res.FatalError(), fatalErr)
		require.NotContains(t, res.FatalError().Error(), "should not be reported")
	})

	t.Run("in-flight task can still report errors after cancellation", func(t *testing.T) {
		firstFatal := errors.New("first fatal")
		secondFatal := errors.New("second fatal from in-flight task")

		g, gctx := worker.WithContext(t.Context())

		started := make(chan struct{})
		release := make(chan struct{})

		// In-flight task: signals it has started, waits to be released, then
		// returns a fatal. It's already executing when the group cancels.
		g.Go(func() worker.TaskError {
			close(started)
			<-release
			return worker.NewFatalError(secondFatal)
		})

		// Wait for the in-flight task to be running.
		<-started

		// Fire a fatal from another task to cancel the group.
		g.Go(func() worker.TaskError {
			return worker.NewFatalError(firstFatal)
		})

		// Wait for gctx to be cancelled.
		<-gctx.Done()

		// Release the in-flight task so it can complete.
		close(release)

		res := g.Wait()
		require.NotNil(t, res)
		require.True(t, res.IsFatal())
		require.ErrorIs(t, res.FatalError(), firstFatal, "first fatal should be recorded")
		require.ErrorIs(t, res.FatalError(), secondFatal, "in-flight task's fatal should also be recorded")
	})
}
