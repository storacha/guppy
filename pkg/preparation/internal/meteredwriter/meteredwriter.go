package meteredwriter

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// MeteredWriter is an [io.Writer] that records metrics about write operations
// as attributes on the current OpenTelemetry span. It is safe for concurrent
// use. Among other things, you can use this to isolate the performance of a
// specific writer from within a [io.MultiWriter].
//
// Attributes are set on the span when the writer is [Close()]ed. If the writer
// is not closed, attributes will not be set automatically. However, you can
// call [AttachSummaryToSpan()] to set the attributes to the latest values at
// any time.
type MeteredWriter struct {
	w             io.Writer
	ctx           context.Context
	prefix        string
	totalBytes    atomic.Int64
	totalDuration atomic.Int64
	writeCount    atomic.Int64
	closed        atomic.Bool
}

var _ io.WriteCloser = (*MeteredWriter)(nil)

// New creates a new [MeteredWriter] that writes to the provided [io.Writer]. It
// will record metrics on the span found in the provided context.
//
// The attribute names will begin with the given [prefix]. For example, if
// [prefix] is "upload", the attributes will be named "upload.total_bytes",
// "upload.total_duration_ms", etc.
//
// If the provided writer also implements [io.Closer], its Close method will be
// called when the MeteredWriter is closed.
func New(ctx context.Context, w io.Writer, prefix string) *MeteredWriter {
	return &MeteredWriter{
		w:      w,
		ctx:    ctx,
		prefix: prefix,
	}
}

func (m *MeteredWriter) Write(p []byte) (n int, err error) {
	start := time.Now()
	n, err = m.w.Write(p)
	duration := time.Since(start)

	m.totalBytes.Add(int64(n))
	m.totalDuration.Add(duration.Nanoseconds())
	m.writeCount.Add(1)

	return n, err
}

func (m *MeteredWriter) Close() error {
	// Ensure we only attach once
	if !m.closed.CompareAndSwap(false, true) {
		return nil
	}

	m.AttachSummaryToSpan()

	// Close underlying writer if possible
	if closer, ok := m.w.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (m *MeteredWriter) AttachSummaryToSpan() {
	span := trace.SpanFromContext(m.ctx)
	if !span.IsRecording() {
		return
	}

	totalNs := m.totalDuration.Load()
	totalBytes := m.totalBytes.Load()
	count := m.writeCount.Load()

	span.SetAttributes(
		attribute.Int64(m.prefix+".total_bytes", totalBytes),
		attribute.Int64(m.prefix+".total_duration_ms", totalNs/1e6),
		attribute.Int64(m.prefix+".count", count),
	)

	if count > 0 && totalNs > 0 {
		avgMs := float64(totalNs/count) / 1e6
		mbps := float64(totalBytes) / (1024 * 1024) / (float64(totalNs) / 1e9)

		span.SetAttributes(
			attribute.Float64(m.prefix+".avg_duration_ms", avgMs),
			attribute.Float64(m.prefix+".throughput_mbps", mbps),
		)
	}
}
