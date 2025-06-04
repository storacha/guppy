package main

import (
	"fmt"
	"log"
	"os"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/guppy/cmd/util"
	"github.com/storacha/guppy/pkg/capability/uploadlist"
	"github.com/storacha/guppy/pkg/client"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "w3",
		Usage: "interact with the web3.storage API",
		Commands: []*cli.Command{
			{
				Name:   "whoami",
				Usage:  "Print information about the current agent.",
				Action: whoami,
			},
			{
				Name:    "up",
				Aliases: []string{"upload"},
				Usage:   "Store a file(s) to the service and register an upload.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "space",
						Value: "",
						Usage: "DID of space to upload to.",
					},
					&cli.StringFlag{
						Name:  "proof",
						Value: "",
						Usage: "Path to file containing UCAN proof(s) for the operation.",
					},
					&cli.StringFlag{
						Name:    "car",
						Aliases: []string{"c"},
						Value:   "",
						Usage:   "Path to CAR file to upload.",
					},
					&cli.BoolFlag{
						Name:    "hidden",
						Aliases: []string{"H"},
						Value:   false,
						Usage:   "Include paths that start with \".\".",
					},
					&cli.BoolFlag{
						Name:    "json",
						Aliases: []string{"j"},
						Value:   false,
						Usage:   "Format as newline delimited JSON",
					},
					&cli.BoolFlag{
						Name:    "verbose",
						Aliases: []string{"v"},
						Value:   false,
						Usage:   "Output more details.",
					},
					&cli.BoolFlag{
						Name:  "wrap",
						Value: true,
						Usage: "Wrap single input file in a directory. Has no effect on directory or CAR uploads. Pass --no-wrap to disable.",
					},
					&cli.IntFlag{
						Name:  "shard-size",
						Value: 0,
						Usage: "Shard uploads into CAR files of approximately this size in bytes.",
					},
				},
				Action: upload,
			},
			{
				Name:    "ls",
				Aliases: []string{"list"},
				Usage:   "List uploads in the current space.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "space",
						Value: "",
						Usage: "DID of space to list uploads from.",
					},
					&cli.StringFlag{
						Name:  "proof",
						Value: "",
						Usage: "Path to file containing UCAN proof(s) for the operation.",
					},
					&cli.BoolFlag{
						Name:  "shards",
						Value: false,
						Usage: "Display shard CID(s) for each upload root.",
					},
				},
				Action: ls,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func whoami(cCtx *cli.Context) error {
	s := util.MustGetSigner()
	fmt.Println(s.DID())
	return nil
}

func ls(cCtx *cli.Context) error {
	signer := util.MustGetSigner()
	conn := util.MustGetConnection()
	space := util.MustParseDID(cCtx.String("space"))
	proof := util.MustGetProof(cCtx.String("proof"))

	rcpt, err := client.UploadList(
		signer,
		space,
		uploadlist.Caveat{},
		client.WithConnection(conn),
		client.WithProofs([]delegation.Delegation{proof}),
	)
	if err != nil {
		return err
	}

	lsSuccess, lsFailure := result.Unwrap(rcpt.Out())
	if lsFailure != nil {
		return fmt.Errorf("%+v", lsFailure)
	}

	for _, r := range lsSuccess.Results {
		fmt.Printf("%s\n", r.Root)
		if cCtx.Bool("shards") {
			for _, s := range r.Shards {
				fmt.Printf("\t%s\n", s)
			}
		}
	}

	return nil
}
