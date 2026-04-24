package worker

import (
	"context"
	"fmt"

	"github.com/storacha/guppy/internal/ctxutil"
)

// Run executes a worker loop. The loop runs tasks in parallel. Run blocks until
// the `workAvailable` channel closes and all tasks are complete.
//
// The loop waits for a signal on the `workAvailable` channel, then calls
// `findWork` to get a batch of tasks to run and runs them with a maximum
// parallelism of `parallelism`, queuing any additional tasks. Any time the
// queue becomes empty, the loop waits for the next signal to find more work,
// until the `workAvailable` channel is closed. When the channel is closed, and
// all work is complete, the loop calls `finalize` and returns.
//
// If any task returns a [TaskError] whose [TaskError.IsFatal] is true, all
// running tasks are cancelled and no new tasks are started.
//
// Returns nil iff no task reported any errors, fatal or non-fatal. Otherwise,
// returns a [TaskError] describing what accumulated.
func Run(
	ctx context.Context,
	workAvailable <-chan struct{},
	parallelism int,
	findWork func(ctx context.Context) ([]Task, error),
	finalize func() error,
) TaskError {
	g, gctx := WithContext(ctx)
	g.SetLimit(parallelism)

	for {
		select {
		case <-ctx.Done():
			// External cancellation. The group has already been cancelled, so wait
			// and return, but include the cancellation cause as a fatal error.
			return Join(g.Wait(), NewFatalError(ctxutil.Cause(ctx)))

		case <-gctx.Done():
			// Internal cancellation. Wait, and return the result.
			return g.Wait()

		case _, ok := <-workAvailable:
			if !ok {
				result := g.Wait()
				if result != nil && result.IsFatal() {
					return result
				}
				if finalize != nil {
					if ferr := finalize(); ferr != nil {
						return Join(result, NewFatalError(fmt.Errorf("worker finalize encountered an error: %w", ferr)))
					}
				}
				return result
			}

			tasks, err := findWork(ctx)
			if err != nil {
				// Cancel in-flight siblings and drain before surfacing the
				// fatal.
				result := g.Wait()
				return Join(result, NewFatalError(fmt.Errorf("worker findWork encountered an error: %w", err)))
			}

			for _, task := range tasks {
				g.Go(func() TaskError {
					select {
					case <-gctx.Done():
						// If the context is already cancelled, skip starting the task.
						return nil
					default:
						return task(gctx)
					}
				})
			}
		}
	}
}
