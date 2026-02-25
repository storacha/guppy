package ctxutil

import (
	"context"
	"errors"
	"fmt"
)

// Cause returns the error and cause from the context as a single error with a
// nice message. This is an ergonomic convenience. `ctx.Err()` predates causes,
// and doesn't report them. `context.Cause(ctx)` returns the cause, but its
// message doesn't state that it's a cause. This function will return either:
//
//   - nil, if the context is not done
//   - the error from `ctx.Err()` if there is no separate cause, which will be
//     either [context.Canceled] or [context.DeadlineExceeded]
//   - an error stating that the [context.Canceled] or
//     [context.DeadlineExceeded] was caused by the cause, if there is a cause
//
// For a context with a cause, the returned error will wrap both the error and
// the cause, so you can use `errors.Is`, `errors.As`, and `errors.Unwrap`.
func Cause(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	cause := context.Cause(ctx)
	if cause == err {
		return err
	}
	return fmt.Errorf("%w, cause: %w", err, cause)
}

// ErrorWithCause takes an error which should be returned from a function that
// also takes a context, and the context itself. If the context was canceled or
// its deadline was exceeded *with a cause*, and the error returned was the
// `ctx.Err()` without including the cause, then this function will return a new
// error that wraps both. This is an ergonomic convenience to adapt functions
// which respond to `ctx.Done()` but don't include the cause in their returned
// error, likely because they were written before context causes were added.
func ErrorWithCause(err error, ctx context.Context) error {
	cause := context.Cause(ctx)
	if cause != nil && errors.Is(err, ctx.Err()) && !errors.Is(err, cause) {
		return fmt.Errorf("%w, cause: %w", err, cause)
	}
	return err
}
