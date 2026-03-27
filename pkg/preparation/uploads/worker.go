package uploads

import (
	"context"
	"fmt"
	"sync"

	"github.com/storacha/guppy/internal/ctxutil"
	"golang.org/x/sync/errgroup"
)

func Worker(ctx context.Context, in <-chan struct{}, doWork func() error, finalize func() error) error {
	for {
		select {
		case <-ctx.Done():
			return ctxutil.Cause(ctx)
		case _, ok := <-in:
			if !ok {
				if finalize != nil {
					if err := finalize(); err != nil {
						return fmt.Errorf("worker finalize encountered an error: %w", err)
					}
				}
				return nil
			}
			if err := doWork(); err != nil {
				return fmt.Errorf("worker encountered an error: %w", err)
			}
		}
	}
}

func Worker2[Task any](
	ctx context.Context,
	tasksAvailable <-chan struct{},
	parallelism int,
	findWork func(ctx context.Context) ([]Task, error),
	doWork func(ctx context.Context, task Task) (error, error),
	finalize func() error,
) ([]error, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctxutil.Cause(ctx)
		case _, ok := <-tasksAvailable:
			if !ok {
				if finalize != nil {
					if err := finalize(); err != nil {
						return nil, fmt.Errorf("worker finalize encountered an error: %w", err)
					}
				}
				return nil, nil
			}

			tasks, err := findWork(ctx)
			if err != nil {
				return nil, fmt.Errorf("worker findWork encountered an error: %w", err)
			}

			sem := make(chan struct{}, parallelism)
			eg, gctx := errgroup.WithContext(ctx)
			var (
				nonFatalErrorsMu sync.Mutex
				nonFatalErrors   []error
			)
			for _, task := range tasks {
				sem <- struct{}{}
				task := task
				eg.Go(func() error {
					defer func() { <-sem }()
					nonFatal, fatal := doWork(gctx, task)
					if fatal != nil {
						return fmt.Errorf("worker doWork encountered a fatal error: %w", fatal)
					}
					if nonFatal != nil {
						nonFatalErrorsMu.Lock()
						nonFatalErrors = append(nonFatalErrors, nonFatal)
						nonFatalErrorsMu.Unlock()
					}
					return nil
				})
			}

			fatalErr := eg.Wait()
			if fatalErr != nil || len(nonFatalErrors) > 0 {
				return nonFatalErrors, fatalErr
			}
		}
	}
}
