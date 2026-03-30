package uploads

import (
	"context"
	"fmt"
	"sync"

	"github.com/storacha/guppy/internal/ctxutil"
	"golang.org/x/sync/errgroup"
)

func Worker(
	ctx context.Context,
	workAvailable <-chan struct{},
	parallelism int,
	findWork func(ctx context.Context) ([]func(context.Context) (error, error), error),
	finalize func() error,
) ([]error, error) {
	var (
		queue            []func(context.Context) (error, error)
		nonFatalErrorsMu sync.Mutex
		nonFatalErrors   []error
	)

	// gctx is cancelled when any task returns a fatal error, allowing the outer
	// loop to detect failure and stop dispatching. Tasks receive the outer ctx
	// (not gctx) so sibling tasks are not cancelled when one fails.
	sem := make(chan struct{}, parallelism)
	eg, gctx := errgroup.WithContext(ctx)

	dispatchNext := func() bool {
		if len(queue) == 0 {
			return false
		}
		task := queue[0]
		queue = queue[1:]
		select {
		case sem <- struct{}{}:
		case <-gctx.Done():
			return false
		}
		eg.Go(func() error {
			defer func() { <-sem }()
			nonFatal, fatal := task(ctx)
			if fatal != nil {
				return fmt.Errorf("worker task encountered a fatal error: %w", fatal)
			}
			if nonFatal != nil {
				nonFatalErrorsMu.Lock()
				nonFatalErrors = append(nonFatalErrors, nonFatal)
				nonFatalErrorsMu.Unlock()
			}
			return nil
		})
		return true
	}

	for {
		select {
		case <-gctx.Done():
			// gctx is cancelled either because ctx was cancelled (external stop)
			// or because a task returned a fatal error (internal failure). Wait
			// for all in-flight tasks to finish before determining which it was.
			fatalErr := eg.Wait()
			if fatalErr != nil {
				return nonFatalErrors, fatalErr
			}
			// No task error — must be an external cancellation.
			return nonFatalErrors, ctxutil.Cause(ctx)
		case _, ok := <-workAvailable:
			if !ok {
				// Drain the queue before finalizing.
				for dispatchNext() {
				}
				if fatalErr := eg.Wait(); fatalErr != nil {
					return nonFatalErrors, fatalErr
				}
				if finalize != nil {
					if err := finalize(); err != nil {
						return nonFatalErrors, fmt.Errorf("worker finalize encountered an error: %w", err)
					}
				}
				if len(nonFatalErrors) > 0 {
					return nonFatalErrors, nil
				}
				return nil, nil
			}

			tasks, err := findWork(ctx)
			if err != nil {
				_ = eg.Wait()
				return nonFatalErrors, fmt.Errorf("worker findWork encountered an error: %w", err)
			}
			queue = append(queue, tasks...)

			// Fill available parallelism slots.
			for dispatchNext() {
			}
		}
	}
}
