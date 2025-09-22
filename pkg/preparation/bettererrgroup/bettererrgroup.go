package bettererrgroup

import (
	"context"
	"fmt"
	"runtime/debug"

	"golang.org/x/sync/errgroup"
)

// Group is a [errgroup.Group] that also captures panics from its goroutines and
// reports them with a stack trace. The original [errgroup.Group] reports the
// stack trace of the `Wait()` call, which is much less useful.
type Group struct {
	*errgroup.Group
}

func WithContext(ctx context.Context) (*Group, context.Context) {
	group, ctx := errgroup.WithContext(ctx)
	return &Group{Group: group}, ctx
}

type PanicError struct {
	recovered any
	stack     string
}

func (e PanicError) Error() string {
	return fmt.Sprintf("panic: %v\n%s", e.recovered, e.stack)
}

func (e PanicError) Unwrap() error {
	wrappedError, ok := e.recovered.(error)
	if !ok {
		return nil
	}
	return wrappedError
}

func (e PanicError) Recovered() any {
	return e.recovered
}

func (e PanicError) Stack() string {
	return e.stack
}

func (g *Group) Go(f func() error) {
	g.Group.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = PanicError{recovered: r, stack: string(debug.Stack())}
			}
		}()
		return f()
	})
}
