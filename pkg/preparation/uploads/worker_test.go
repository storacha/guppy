package uploads_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/storacha/guppy/pkg/preparation/uploads"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type unwrappableError interface {
	error
	Unwrap() error
}

// e is a shorthand helper function that uses [require.EventuallyWithT] with
// a standard timeout and interval, to keep noise out of the tests.
func e(t *testing.T, condition func(collect *assert.CollectT)) {
	t.Helper()
	require.EventuallyWithT(t, condition, time.Second, 10*time.Millisecond)
}

func TestWorker(t *testing.T) {
	t.Run("runs the work function for every signal received, then the finalize function when the channel closes", func(t *testing.T) {
		signalChan := make(chan struct{}, 1)
		resultChan := make(chan error, 1)
		var runs atomic.Int32
		var finalizes atomic.Int32

		go func() {
			defer close(resultChan)

			resultChan <- uploads.Worker(t.Context(), signalChan, func() error {
				runs.Add(1)
				return nil
			}, func() error {
				finalizes.Add(1)
				return nil
			})
		}()

		require.Equal(t, int32(0), runs.Load(), "worker should not run before signal")
		signalChan <- struct{}{}
		e(t, func(t *assert.CollectT) { require.Equal(t, int32(1), runs.Load(), "worker should run once after signal") })
		signalChan <- struct{}{}
		e(t, func(t *assert.CollectT) { require.Equal(t, int32(2), runs.Load(), "worker should run again after second signal") })

		require.Equal(t, int32(0), finalizes.Load(), "finalize function should be called until the channel closes")
		close(signalChan)
		e(t, func(t *assert.CollectT) {
			require.Equal(t, int32(1), finalizes.Load(), "finalize function should be called once the channel closes")
		})

		result := <-resultChan
		require.Nil(t, result, "result should be nil after successful runs")
	})

	t.Run("immediately responds with any work error, skipping the finalizer", func(t *testing.T) {
		workerErr := errors.New("error in doWork")
		signalChan := make(chan struct{}, 3)
		resultChan := make(chan error, 1)
		var runs atomic.Int32
		var finalizes atomic.Int32

		go func() {
			defer close(resultChan)
			resultChan <- uploads.Worker(t.Context(), signalChan, func() error {
				n := runs.Add(1)
				// Fail on the second run
				if n == 2 {
					return workerErr
				}
				return nil
			}, func() error {
				finalizes.Add(1)
				return nil
			})
		}()

		// Send three signals; the second should cause an error, the third should not run
		signalChan <- struct{}{}
		signalChan <- struct{}{}
		signalChan <- struct{}{}

		result, ok := (<-resultChan).(unwrappableError)
		require.True(t, ok, "result should be a wrapped error")
		require.ErrorContains(t, result, "worker encountered an error: error in doWork")
		require.Equal(t, workerErr, result.Unwrap(), "worker should send back the error it encountered, wrapped")
		require.Equal(t, int32(2), runs.Load(), "worker should have stopped after encountering an error")
		require.Equal(t, int32(0), finalizes.Load(), "finalize function should not have be called")
	})

	t.Run("responds with any finalize error", func(t *testing.T) {
		finalizerErr := errors.New("error in finalize")
		signalChan := make(chan struct{}, 3)
		resultChan := make(chan error, 1)
		var runs atomic.Int32

		go func() {
			defer close(resultChan)
			resultChan <- uploads.Worker(t.Context(), signalChan, func() error {
				runs.Add(1)
				return nil
			}, func() error {
				return finalizerErr
			})
		}()

		// Send three signals; all should run
		signalChan <- struct{}{}
		signalChan <- struct{}{}
		signalChan <- struct{}{}
		close(signalChan)

		result, ok := (<-resultChan).(unwrappableError)
		require.True(t, ok, "result should be a wrapped error")
		require.ErrorContains(t, result, "worker finalize encountered an error: error in finalize")
		require.Equal(t, finalizerErr, result.Unwrap(), "worker should send back the error it encountered, wrapped")
		require.Equal(t, int32(3), runs.Load(), "worker should have run all three times")
	})

	t.Run("ignores a nil finalizer", func(t *testing.T) {
		signalChan := make(chan struct{}, 1)
		resultChan := make(chan error, 1)
		var ran atomic.Bool

		go func() {
			defer close(resultChan)

			resultChan <- uploads.Worker(t.Context(), signalChan, func() error {
				ran.Store(true)
				return nil
			}, nil)
		}()

		require.False(t, ran.Load(), "worker should not run before signal")
		signalChan <- struct{}{}
		e(t, func(t *assert.CollectT) { require.True(t, ran.Load(), "worker should run after signal") })
		close(signalChan)
		result := <-resultChan
		require.Nil(t, result, "result should be nil after successful runs and no finalizer")
	})
}
