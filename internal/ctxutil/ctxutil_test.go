package ctxutil_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/storacha/guppy/internal/ctxutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCausedError(t *testing.T) {
	t.Run("returns nil when context is not done", func(t *testing.T) {
		ctx := context.Background()
		assert.Nil(t, ctxutil.CausedError(ctx))
	})

	t.Run("returns context.Canceled when canceled with no explicit cause", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := ctxutil.CausedError(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, "context canceled", err.Error())
	})

	t.Run("returns error wrapping both canceled and cause", func(t *testing.T) {
		cause := errors.New("server shutting down")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		err := ctxutil.CausedError(ctx)
		require.NotNil(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.ErrorIs(t, err, cause)
		assert.Equal(t, "context canceled -- Caused by: server shutting down", err.Error())
	})

	t.Run("returns context.DeadlineExceeded when deadline exceeded with no explicit cause", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		<-ctx.Done()
		err := ctxutil.CausedError(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, "context deadline exceeded", err.Error())
	})

	t.Run("returns error wrapping both deadline exceeded and cause", func(t *testing.T) {
		cause := errors.New("query too slow")
		// Simulate deadline exceeded by using WithDeadlineCause
		ctx, cancel := context.WithDeadlineCause(context.Background(), time.Now().Add(-time.Second), cause)
		defer cancel()
		err := ctxutil.CausedError(ctx)
		require.NotNil(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.ErrorIs(t, err, cause)
		assert.Equal(t, "context deadline exceeded -- Caused by: query too slow", err.Error())
	})
}

func TestErrorWithCause(t *testing.T) {
	t.Run("returns nil when err is nil and context is not done", func(t *testing.T) {
		ctx := context.Background()
		assert.Nil(t, ctxutil.ErrorWithCause(nil, ctx))
	})

	t.Run("returns nil when err is nil and context is canceled with cause", func(t *testing.T) {
		cause := errors.New("shutting down")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		assert.Nil(t, ctxutil.ErrorWithCause(nil, ctx))
	})

	t.Run("returns err unchanged when canceled with no explicit cause", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := context.Canceled
		result := ctxutil.ErrorWithCause(err, ctx)
		assert.Equal(t, err, result)
	})

	t.Run("wraps context.Canceled with cause when canceled with explicit cause", func(t *testing.T) {
		cause := errors.New("server shutting down")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		err := context.Canceled
		result := ctxutil.ErrorWithCause(err, ctx)
		require.NotNil(t, result)
		assert.ErrorIs(t, result, context.Canceled)
		assert.ErrorIs(t, result, cause)
		assert.Equal(t, "context canceled -- Caused by: server shutting down", result.Error())
	})

	t.Run("wraps error that wraps context.Canceled", func(t *testing.T) {
		cause := errors.New("server shutting down")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		err := fmt.Errorf("query failed: %w", context.Canceled)
		result := ctxutil.ErrorWithCause(err, ctx)
		require.NotNil(t, result)
		assert.ErrorIs(t, result, context.Canceled)
		assert.ErrorIs(t, result, cause)
		assert.Equal(t, "query failed: context canceled -- Caused by: server shutting down", result.Error())
	})

	t.Run("returns err unchanged when it already wraps both ctx.Err and cause", func(t *testing.T) {
		cause := errors.New("server shutting down")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		err := fmt.Errorf("%w: %w", context.Canceled, cause)
		result := ctxutil.ErrorWithCause(err, ctx)
		assert.Equal(t, err, result)
	})

	t.Run("returns unrelated error unchanged when context is canceled", func(t *testing.T) {
		cause := errors.New("server shutting down")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		err := errors.New("disk full")
		result := ctxutil.ErrorWithCause(err, ctx)
		assert.Equal(t, err, result)
	})

	t.Run("wraps context.DeadlineExceeded with cause", func(t *testing.T) {
		cause := errors.New("query too slow")
		ctx, cancel := context.WithDeadlineCause(context.Background(), time.Now().Add(-time.Second), cause)
		defer cancel()
		err := context.DeadlineExceeded
		result := ctxutil.ErrorWithCause(err, ctx)
		require.NotNil(t, result)
		assert.ErrorIs(t, result, context.DeadlineExceeded)
		assert.ErrorIs(t, result, cause)
		assert.Equal(t, "context deadline exceeded -- Caused by: query too slow", result.Error())
	})

	t.Run("is idempotent: calling twice does not double-wrap", func(t *testing.T) {
		cause := errors.New("server shutting down")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		err := context.Canceled
		first := ctxutil.ErrorWithCause(err, ctx)
		second := ctxutil.ErrorWithCause(first, ctx)
		assert.Equal(t, first, second)
	})
}
