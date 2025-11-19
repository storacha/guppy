package main

import (
	"context"
	"errors"
	"os"



	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/guppy/cmd"
	"github.com/storacha/guppy/internal/cmdutil"
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
