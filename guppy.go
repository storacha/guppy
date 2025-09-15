package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/briandowns/spinner"
	logging "github.com/ipfs/go-log/v2"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/internal/output"
	"github.com/storacha/guppy/internal/upload"
	"github.com/storacha/guppy/pkg/didmailto"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("guppy/main")

var commands = []*cli.Command{
	{
		Name:   "whoami",
		Usage:  "Print information about the current agent.",
		Action: whoami,
	},
	{
		Name:      "login",
		Usage:     "Authenticate this agent with your email address to gain access to all capabilities that have been delegated to it.",
		UsageText: "login <email>",
		Action:    login,
	},
	{
		Name:      "reset",
		Usage:     "Remove all proofs/delegations from the store but retain the agent DID.",
		UsageText: "reset",
		Action:    reset,
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
		Action: upload.Upload,
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
}

func main() {
	app := &cli.App{
		Name:     "guppy",
		Usage:    "interact with the Storacha Network",
		Commands: commands,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "json",
				Aliases: []string{"j"},
				Usage:   "Output in JSON format",
				Value:   false,
			},
		},
	}

	// set up a context that is canceled when a command is interrupted
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// set up a signal handler to cancel the context
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)

		select {
		case <-interrupt:
			fmt.Println()
			log.Info("received interrupt signal")
			cancel()
		case <-ctx.Done():
		}

		// Allow any further SIGTERM or SIGINT to kill process
		signal.Stop(interrupt)
	}()

	if err := app.RunContext(ctx, os.Args); err != nil {
		log.Fatal(err)
	}
}

func whoami(cCtx *cli.Context) error {
	c := cmdutil.MustGetClient()
	did := c.DID()
	
	if cCtx.Bool("json") {
		return output.JSON(map[string]string{"did": did})
	}
	output.Success("Current agent DID: %s", did)
	return nil
}

func login(cCtx *cli.Context) error {
	email := cCtx.Args().First()
	if email == "" {
		return output.JSONError(fmt.Errorf("email address is required"))
	}

	accountDid, err := didmailto.FromEmail(email)
	if err != nil {
		return output.JSONError(fmt.Errorf("invalid email address: %w", err))
	}

	c := cmdutil.MustGetClient()

	authOk, err := c.RequestAccess(cCtx.Context, accountDid.String())
	if err != nil {
		return output.JSONError(fmt.Errorf("requesting access: %w", err))
	}

	resultChan := c.PollClaim(cCtx.Context, authOk)

	if !cCtx.Bool("json") {
		s := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
		s.Suffix = fmt.Sprintf(" 🔗 please click the link sent to %s to authorize this agent", email)
		s.Start()
		defer s.Stop()
	}

	claimedDels, err := result.Unwrap(<-resultChan)

	if cCtx.Context.Err() != nil {
		return output.JSONError(fmt.Errorf("login canceled: %w", cCtx.Context.Err()))
	}

	if err != nil {
		return output.JSONError(fmt.Errorf("claiming access: %w", err))
	}

	c.AddProofs(claimedDels...)

	if cCtx.Bool("json") {
		return output.JSON(map[string]interface{}{
			"message": "Successfully logged in",
			"delegations": claimedDels,
		})
	}
	
	output.Success("Successfully logged in!")
	return nil
}

func reset(cCtx *cli.Context) error {
	c := cmdutil.MustGetClient()
	if err := c.Reset(); err != nil {
		return output.JSONError(err)
	}

	if cCtx.Bool("json") {
		return output.JSON(map[string]string{
			"message": "Successfully reset agent",
		})
	}

	output.Success("Successfully reset agent")
	return nil
}

func ls(cCtx *cli.Context) error {
	space := cmdutil.MustParseDID(cCtx.String("space"))

	proofs := []delegation.Delegation{}
	if cCtx.String("proof") != "" {
		proof := cmdutil.MustGetProof(cCtx.String("proof"))
		proofs = append(proofs, proof)
	}

	c := cmdutil.MustGetClient(proofs...)

	listOk, err := c.UploadList(
		cCtx.Context,
		space,
		uploadcap.ListCaveats{})
	if err != nil {
		return output.JSONError(err)
	}

	if cCtx.Bool("json") {
		return output.JSON(map[string]interface{}{
			"space": space.String(),
			"uploads": listOk.Results,
		})
	}

	if len(listOk.Results) == 0 {
		output.Success("No uploads found in space %s", space)
		return nil
	}

	// Prepare table data
	var data [][]string
	data = append(data, []string{"ROOT", "SHARDS"})
	
	for _, r := range listOk.Results {
		if cCtx.Bool("shards") {
			shards := ""
			for i, s := range r.Shards {
				if i > 0 {
					shards += ", "
				}
				shards += s.String()
			}
			data = append(data, []string{r.Root.String(), shards})
		} else {
			data = append(data, []string{r.Root.String(), "-"})
		}
	}

	output.Table(data)
	return nil
}
