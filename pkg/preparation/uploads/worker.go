package uploads

import (
	"context"
	"fmt"
)

func Worker(ctx context.Context, in <-chan struct{}, doWork func() error, finalize func() error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
