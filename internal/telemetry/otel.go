package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	otelpyroscope "github.com/grafana/otel-profiling-go"
	"github.com/grafana/pyroscope-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
)

const (
	// DefaultTracesEndpoint is the default OTLP HTTP endpoint for Storacha telemetry.
	DefaultTracesEndpoint = "telemetry.storacha.network:443"

	// DefaultProfilesEndpoint is the default OTLP HTTP endpoint for sending profiles.
	DefaultProfilesEndpoint = DefaultTracesEndpoint
)

// Config configures OTLP tracing.
type Config struct {
	Enabled   bool
	Endpoint  string
	Insecure  bool
	Profiling ProfilingConfig
}

// ProfilingConfig configures OTLP profiling.
type ProfilingConfig struct {
	Enabled       bool
	Endpoint      string
	Insecure      bool
	Application   string
	BasicAuthUser string
	BasicAuthPass string
}

// Setup bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func Setup(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if !cfg.Enabled && !cfg.Profiling.Enabled {
		return func(context.Context) error { return nil }, nil
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

	var shutdowns []func(context.Context) error

	var tracerProvider *trace.TracerProvider
	if cfg.Enabled {
		traceEndpoint := cfg.Endpoint
		if traceEndpoint == "" {
			traceEndpoint = DefaultTracesEndpoint
		}

		traceExporter, err := otlptracehttp.New(ctx, traceExporterOptions(traceEndpoint, cfg.Insecure)...)
		if err != nil {
			return nil, err
		}

		tracerProvider = trace.NewTracerProvider(
			trace.WithBatcher(traceExporter),
			trace.WithResource(res),
		)
		shutdowns = append(shutdowns, shutdownFunc(tracerProvider))
	} else {
		tracerProvider = trace.NewTracerProvider(trace.WithResource(res))
	}

	var meterProvider *metric.MeterProvider
	if cfg.Enabled {
		metricEndpoint := cfg.Endpoint
		if metricEndpoint == "" {
			metricEndpoint = DefaultTracesEndpoint
		}

		metricExporter, err := otlpmetrichttp.New(ctx, metricExporterOptions(metricEndpoint, cfg.Insecure)...)
		if err != nil {
			return nil, err
		}

		meterProvider = metric.NewMeterProvider(
			metric.WithReader(metric.NewPeriodicReader(metricExporter)),
			metric.WithResource(res),
		)
		shutdowns = append(shutdowns, shutdownFunc(meterProvider))
	} else {
		meterProvider = metric.NewMeterProvider(metric.WithResource(res))
	}

	prop := newPropagator()
	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetTextMapPropagator(prop)

	if cfg.Profiling.Enabled {
		otel.SetTracerProvider(otelpyroscope.NewTracerProvider(otel.GetTracerProvider()))

		profileEndpoint := profileEndpointWithScheme(cfg.Profiling.Endpoint, cfg.Profiling.Insecure)
		profiler, err := pyroscope.Start(pyroscope.Config{
			ApplicationName:   profileApp(cfg.Profiling.Application),
			ServerAddress:     profileEndpoint,
			BasicAuthUser:     cfg.Profiling.BasicAuthUser,
			BasicAuthPassword: cfg.Profiling.BasicAuthPass,
		})
		if err != nil {
			return nil, fmt.Errorf("start profiling: %w", err)
		}
		shutdowns = append(shutdowns, func(context.Context) error {
			return profiler.Stop()
		})
	}

	return func(ctx context.Context) error {
		var err error
		for i := len(shutdowns) - 1; i >= 0; i-- {
			err = errors.Join(err, shutdowns[i](ctx))
		}
		return err
	}, nil
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

func metricExporterOptions(endpoint string, insecure bool) []otlpmetrichttp.Option {
	opts := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(endpoint),
	}
	if insecure {
		opts = append(opts, otlpmetrichttp.WithInsecure())
	}
	return opts
}

func profileEndpointWithScheme(endpoint string, insecure bool) string {
	if endpoint == "" {
		endpoint = DefaultProfilesEndpoint
	}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint
	}
	scheme := "https://"
	if insecure {
		scheme = "http://"
	}
	return fmt.Sprintf("%s%s", scheme, endpoint)
}

func profileApp(app string) string {
	if app == "" {
		return "guppy"
	}
	return app
}

type shutdowner interface {
	Shutdown(context.Context) error
	ForceFlush(context.Context) error
}

func shutdownFunc(s shutdowner) func(context.Context) error {
	return func(ctx context.Context) error {
		return errors.Join(s.ForceFlush(ctx), s.Shutdown(ctx))
	}
}
