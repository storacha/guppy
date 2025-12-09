package main

import (
	"context"
	"errors"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	logging "github.com/ipfs/go-log/v2"

	"github.com/storacha/guppy/cmd"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/internal/telemetry"
)

var log = logging.Logger("main")

func main() {
	var err error
	defer func() {
		if err != nil {
			var handledCliError cmdutil.HandledCliError
			if errors.As(err, &handledCliError) {
				os.Exit(1)
			}
			log.Fatalln(err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		return
	}

	if os.Getenv("GUPPY_PPROF") != "" {
		startPprofServer()
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	err = cmd.ExecuteContext(ctx)
}

func startPprofServer() {
	const addr = "localhost:8081"
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Warnw("pprof server exited", "addr", addr, "err", err)
		}
	}()
}

func setupOTelSDK(ctx context.Context) (func(ctx context.Context) error, error) {
	cfg, enabled := telemetryConfigFromEnv()
	if !enabled {
		return func(_ context.Context) error {
			return nil
		}, nil
	}

	return telemetry.Setup(ctx, cfg)
}

func telemetryConfigFromEnv() (telemetry.Config, bool) {
	traceEndpoint := os.Getenv("GUPPY_TRACES_ENDPOINT")
	traceEnabled := traceEndpoint != "" || os.Getenv("GUPPY_TRACING_ENABLED") != ""
	traceInsecure := os.Getenv("GUPPY_TRACES_INSECURE") != ""

	profileEndpoint := os.Getenv("GUPPY_PROFILING_ENDPOINT")
	profileEnabled := profileEndpoint != "" || os.Getenv("GUPPY_PROFILING_ENABLED") != ""
	profileInsecure := os.Getenv("GUPPY_PROFILING_INSECURE") != ""

	cfg := telemetry.Config{
		Enabled:  traceEnabled,
		Endpoint: traceEndpoint,
		Insecure: traceInsecure,
		Profiling: telemetry.ProfilingConfig{
			Enabled:       profileEnabled,
			Endpoint:      profileEndpoint,
			Insecure:      profileInsecure,
			Application:   os.Getenv("GUPPY_PROFILING_APP_NAME"),
			BasicAuthUser: os.Getenv("GUPPY_PROFILING_BASIC_AUTH_USER"),
			BasicAuthPass: os.Getenv("GUPPY_PROFILING_BASIC_AUTH_PASSWORD"),
		},
	}

	if cfg.Enabled && cfg.Endpoint == "" {
		cfg.Endpoint = telemetry.DefaultTracesEndpoint
	}

	if cfg.Profiling.Enabled {
		if cfg.Profiling.Endpoint == "" {
			cfg.Profiling.Endpoint = telemetry.DefaultProfilesEndpoint
		}
		if cfg.Profiling.Application == "" {
			cfg.Profiling.Application = "guppy"
		}
	}

	if !cfg.Enabled && !cfg.Profiling.Enabled {
		return telemetry.Config{}, false
	}
	return cfg, true
}
