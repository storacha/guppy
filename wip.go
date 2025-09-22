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
				&cli.BoolFlag{
					Name:  "resume",
					Usage: "Resume a previously interrupted upload.",
				},
			},
		},
		&cli.Command{
			Name:   "large-upload-demo",
			Action: largeupload.Demo,
			Flags: []cli.Flag{
				&cli.BoolFlag{
					Name:  "resume",
					Usage: "Resume a previously interrupted upload.",
				},
				&cli.BoolFlag{
					Name:  "alter-metadata",
					Usage: "Cause an error during DAG generation by changing the modification time in a file on the \"filesystem\" between file opens.",
				},
				&cli.BoolFlag{
					Name:  "alter-data",
					Usage: "Cause an error during shard generation by changing the data in a file on the \"filesystem\" between file opens.",
				},
			},
		},
	)
}
