package main

import (
	"context"
	"errors"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/guppy/cmd"
)

var log = logging.Logger("main")

func main() {
	var err error
	defer func() {
		if err != nil {
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
