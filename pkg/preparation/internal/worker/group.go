package worker

import (
	"context"
	"sync"
)

// Group runs a collection of tasks that return [TaskError]. Non-fatal errors
// are accumulated; a fatal error from any task cancels the context derived by
// [WithContext] so siblings watching that context can exit early. It's
// analogous to [golang.org/x/sync/errgroup.Group], with two deliberate
// differences: (1) it distinguishes fatal from non-fatal errors, and (2) it
// accumulates every task's errors rather than keeping only the first.
//
// If the group is cancelled, the cause will be the fatal error.
type Group struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup
	sem    chan struct{}

	mu  sync.Mutex
	err taskError
}

// WithContext returns a new [Group] and a derived context. The context is
// cancelled the first time a task reports a fatal error, or the first time
// [Group.Wait] returns, whichever occurs first.
func WithContext(ctx context.Context) (*Group, context.Context) {
	ctx, cancel := context.WithCancelCause(ctx)
	return &Group{ctx: ctx, cancel: cancel}, ctx
}

// SetLimit limits the number of concurrently-running tasks to n. Must be
// called before any call to [Group.Go]. A value <= 0 removes any limit.
func (g *Group) SetLimit(n int) {
	if n <= 0 {
		g.sem = nil
		return
	}
	g.sem = make(chan struct{}, n)
}

// Go runs the given task in a new goroutine. If a concurrency limit is set, Go
// blocks until a slot is available.  The task's [TaskError] is folded into the
// group's accumulated result; if the task returns a fatal error, the derived
// context is cancelled.
//
// If the group's context is cancelled when Go is called or while waiting for a
// slot, the task is not started and Go returns immediately.
func (g *Group) Go(task func() TaskError) {
	if g.sem != nil {
		select {
		case g.sem <- struct{}{}:
		case <-g.ctx.Done():
			return
		}
	}
	g.launch(task)
}

// TryGo runs the given task in a new goroutine if a slot is available. It
// returns true if the task was started, false otherwise. If no concurrency
// limit is set, TryGo always starts the task and returns true.
func (g *Group) TryGo(task func() TaskError) bool {
	if g.sem != nil {
		select {
		case g.sem <- struct{}{}:
		default:
			return false
		}
	}
	g.launch(task)
	return true
}

func (g *Group) launch(task func() TaskError) {
	g.wg.Add(1)
	go func() {
		defer func() {
			if g.sem != nil {
				<-g.sem
			}
			g.wg.Done()
		}()
		select {
		case <-g.ctx.Done():
			// Group has cancelled before this task started. Skip it silently:
			// the real fatal is already in the accumulator, and an un-started
			// task has nothing to report. Tasks already executing when cancel
			// fires still run to completion and report whatever they want.
			return
		default:
		}
		g.collect(task())
	}()
}

// Wait blocks until all goroutines started with [Group.Go] or [Group.TryGo]
// have returned, then returns the accumulated [TaskError]. It returns nil iff
// no task reported a fatal error and no task reported any non-fatal errors.
func (g *Group) Wait() TaskError {
	g.wg.Wait()

	// Clean up resources: a `cancel` must always be called eventually. We've just
	// `Wait()`ed for all tasks to finish, so this won't stop anything.
	g.cancel(nil)

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err.isEmpty() {
		return nil
	}
	result := g.err
	return &result
}

func (g *Group) collect(result TaskError) {
	if result == nil {
		return
	}
	g.mu.Lock()
	g.err.add(result)
	g.mu.Unlock()
	if result.FatalError() != nil {
		g.cancel(result.FatalError())
	}
}
