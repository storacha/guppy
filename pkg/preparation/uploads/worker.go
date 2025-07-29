package uploads

import (
	"context"
	"fmt"
)

func Worker(ctx context.Context, in <-chan struct{}, out chan<- error, doWork func() error, finalize func() error) {
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-in:
				if !ok {
					if err := finalize(); err != nil {
						out <- fmt.Errorf("worker finalize encountered an error: %w", err)
					}
					return
				}
				if err := doWork(); err != nil {
					out <- fmt.Errorf("worker encountered an error: %w", err)
					return
				}
			}
		}
	}()
}
