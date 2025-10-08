package main

import (
	"context"
	"errors"
	"log"

	"github.com/storacha/guppy/cmd"
)

func main() {
	var err error
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

	defer func() {
		if err != nil {
			log.Fatalln(err)
		}
	}()

	cmd.ExecuteContext(ctx)
}
