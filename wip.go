//go:build wip

// Subcommands which are not yet ready for production use. Use `-tags=wip` to
// enable them for development and testing.

package main

import (
	_ "embed"

	"github.com/storacha/guppy/internal/largeupload"
	"github.com/urfave/cli/v2"
	_ "modernc.org/sqlite"
)

func init() {
	commands = append(commands,
		&cli.Command{
			Name:   "large-upload",
			Usage:  "WIP - Upload a large amount of data to the service",
			Action: largeupload.Action,
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "space",
					Value: "",
					Usage: "DID of space to upload to.",
				},
			},
		},
		&cli.Command{
			Name:   "large-upload-demo",
			Action: largeupload.Demo,
		},
	)
}
