package telemetry

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
)

const (
	// DefaultTracesEndpoint is the default OTLP HTTP endpoint for Storacha telemetry.
	DefaultTracesEndpoint = "telemetry.storacha.network:443"
)

// Config configures OTLP tracing.
type Config struct {
	Enabled  bool
	Endpoint string
	Insecure bool
}

// Setup bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func Setup(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = DefaultTracesEndpoint
	}

	traceExporter, err := otlptracehttp.New(ctx, traceExporterOptions(endpoint, cfg.Insecure)...)
	if err != nil {
		return nil, err
	}

	// Set up resource.
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", "guppy"),
			attribute.String("service.version", "0.0.0"),
		),
	)
	if err != nil {
		return nil, err
	}

	tracerProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(res),
	)

	prop := newPropagator()
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(prop)

	return shutdownFunc(tracerProvider), nil
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func traceExporterOptions(endpoint string, insecure bool) []otlptracehttp.Option {
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(endpoint),
	}
	if insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	return opts
}

func shutdownFunc(tp *trace.TracerProvider) func(context.Context) error {
	return func(ctx context.Context) error {
		return errors.Join(tp.ForceFlush(ctx), tp.Shutdown(ctx))
	}
}
