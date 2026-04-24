package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/storacha/guppy/pkg/preparation/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// e is a shorthand helper function that uses [require.EventuallyWithT] with
// a standard timeout and interval, to keep noise out of the tests.
func e(t *testing.T, condition func(collect *assert.CollectT)) {
	t.Helper()
	require.EventuallyWithT(t, condition, time.Second, 10*time.Millisecond)
}

// fatalTask wraps a fn returning error into a Task that reports any error as
// fatal.
func fatalTask(fn func() error) worker.Task {
	return func(ctx context.Context) worker.TaskError {
		return worker.NewFatalError(fn())
	}
}

func TestWorker(t *testing.T) {
	t.Run("runs the work function for every signal received, then the finalize function when the channel closes", func(t *testing.T) {
		signalChan := make(chan struct{}, 1)
		resultChan := make(chan worker.TaskError, 1)
		var runs int
		var finalizes int

		go func() {
			defer close(resultChan)
			resultChan <- worker.Run(t.Context(), signalChan, 1,
				func(ctx context.Context) ([]worker.Task, error) {
					runs++
					return nil, nil
				},
				func() error {
					finalizes++
					return nil
				},
			)
		}()

		require.Equal(t, 0, runs, "worker should not run before signal")
		signalChan <- struct{}{}
		e(t, func(t *assert.CollectT) { require.Equal(t, 1, runs, "worker should run once after signal") })
		signalChan <- struct{}{}
		e(t, func(t *assert.CollectT) { require.Equal(t, 2, runs, "worker should run again after second signal") })

		require.Equal(t, 0, finalizes, "finalize function should not be called until the channel closes")
		close(signalChan)
		e(t, func(t *assert.CollectT) {
			require.Equal(t, 1, finalizes, "finalize function should be called once the channel closes")
		})

		res := <-resultChan
		require.Nil(t, res, "result should be nil after a clean run")
	})

	t.Run("immediately responds with any work error, skipping the finalizer", func(t *testing.T) {
		workerErr := errors.New("error in doWork")
		signalChan := make(chan struct{}, 3)
		resultChan := make(chan worker.TaskError, 1)
		var runs int
		var finalizes int

		go func() {
			defer close(resultChan)
			resultChan <- worker.Run(t.Context(), signalChan, 1,
				func(ctx context.Context) ([]worker.Task, error) {
					runs++
					if runs == 2 {
						return []worker.Task{fatalTask(func() error { return workerErr })}, nil
					}
					return nil, nil
				},
				func() error {
					finalizes++
					return nil
				},
			)
		}()

		// Send three signals; the second should cause an error, the third should not run
		signalChan <- struct{}{}
		signalChan <- struct{}{}
		signalChan <- struct{}{}

		res := <-resultChan
		require.NotNil(t, res)
		require.True(t, res.IsFatal(), "a fatal task error should make the result fatal")
		require.ErrorIs(t, res.FatalError(), workerErr)
		require.LessOrEqual(t, runs, 3, "worker should have stopped after encountering an error")
		require.Equal(t, 0, finalizes, "finalize function should not have been called")
	})

	t.Run("collects non-fatal errors and continues dispatching", func(t *testing.T) {
		nonFatalErr := errors.New("non-fatal failure")
		signalChan := make(chan struct{}, 3)
		resultChan := make(chan worker.TaskError, 1)
		var runs int
		var finalizes int

		go func() {
			defer close(resultChan)
			resultChan <- worker.Run(t.Context(), signalChan, 1,
				func(ctx context.Context) ([]worker.Task, error) {
					runs++
					return []worker.Task{
						func(ctx context.Context) worker.TaskError {
							return worker.NewNonFatalError(nonFatalErr)
						},
					}, nil
				},
				func() error {
					finalizes++
					return nil
				},
			)
		}()

		signalChan <- struct{}{}
		signalChan <- struct{}{}
		signalChan <- struct{}{}
		close(signalChan)

		res := <-resultChan
		require.NotNil(t, res)
		require.False(t, res.IsFatal(), "non-fatal errors should not make the result fatal")
		require.Len(t, res.NonFatalErrors(), 3, "every non-fatal error should be collected")
		for _, err := range res.NonFatalErrors() {
			require.ErrorIs(t, err, nonFatalErr)
		}
		require.Equal(t, 3, runs, "worker should have kept dispatching after non-fatal errors")
		require.Equal(t, 1, finalizes, "finalize should still run when only non-fatal errors occurred")
	})

	t.Run("responds with any finalize error", func(t *testing.T) {
		finalizerErr := errors.New("error in finalize")
		signalChan := make(chan struct{}, 3)
		resultChan := make(chan worker.TaskError, 1)
		var runs int

		go func() {
			defer close(resultChan)
			resultChan <- worker.Run(t.Context(), signalChan, 1,
				func(ctx context.Context) ([]worker.Task, error) {
					runs++
					return nil, nil
				},
				func() error {
					return finalizerErr
				},
			)
		}()

		// Send three signals; all should run
		signalChan <- struct{}{}
		signalChan <- struct{}{}
		signalChan <- struct{}{}
		close(signalChan)

		res := <-resultChan
		require.NotNil(t, res)
		require.True(t, res.IsFatal(), "finalize error should make the result fatal")
		require.ErrorContains(t, res.FatalError(), "worker finalize encountered an error: error in finalize")
		require.ErrorIs(t, res.FatalError(), finalizerErr)
		require.Equal(t, 3, runs, "worker should have run all three times")
	})

	t.Run("ignores a nil finalizer", func(t *testing.T) {
		signalChan := make(chan struct{}, 1)
		resultChan := make(chan worker.TaskError, 1)
		var ran bool

		go func() {
			defer close(resultChan)
			resultChan <- worker.Run(t.Context(), signalChan, 1,
				func(ctx context.Context) ([]worker.Task, error) {
					ran = true
					return nil, nil
				},
				nil,
			)
		}()

		require.False(t, ran, "worker should not run before signal")
		signalChan <- struct{}{}
		e(t, func(t *assert.CollectT) { require.True(t, ran, "worker should run after signal") })
		close(signalChan)
		res := <-resultChan
		require.Nil(t, res, "result should be nil after a clean run with no finalizer")
	})
}
