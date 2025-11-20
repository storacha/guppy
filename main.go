package main

import (
	"context"
	"errors"
	"os"

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
			if _, ok := err.(cmdutil.HandledCliError); ok {
				os.Exit(1)
			}
			log.Fatalln(err)
		}
	}()

	ctx := context.Background()

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		return
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	err = cmd.ExecuteContext(ctx)
}

func setupOTelSDK(ctx context.Context) (func(ctx context.Context) error, error) {
	cfg, enabled := tracingConfigFromEnv()
	if !enabled {
		return func(_ context.Context) error {
			return nil
		}, nil
	}

	return telemetry.Setup(ctx, cfg)
}

func tracingConfigFromEnv() (telemetry.Config, bool) {
	// allows a custom endpoint to be set
	endpoint := os.Getenv("GUPPY_TRACES_ENDPOINT")
	// enable tracing with the default endpoing
	envEnabled := os.Getenv("GUPPY_TRACING_ENABLED")
	// if you want insecure
	insecure := os.Getenv("GUPPY_TRACES_INSECURE")

	enabled := endpoint != "" || envEnabled != ""
	if !enabled {
		return telemetry.Config{}, false
	}

	if endpoint == "" {
		endpoint = telemetry.DefaultTracesEndpoint
	}

	cfg := telemetry.Config{
		Enabled:  true,
		Endpoint: endpoint,
	}
	if insecure != "" {
		cfg.Insecure = true
	}
	return cfg, true
}
