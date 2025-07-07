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
	fmt.Println(c.DID())
	return nil
}

func login(cCtx *cli.Context) error {
	email := cCtx.Args().First()
	if email == "" {
		return fmt.Errorf("email address is required")
	}

	accountDid, err := didmailto.FromEmail(email)
	if err != nil {
		return fmt.Errorf("invalid email address: %w", err)
	}

	c := cmdutil.MustGetClient()

	authOk, err := c.RequestAccess(cCtx.Context, accountDid.String())
	if err != nil {
		return fmt.Errorf("requesting access: %w", err)
	}

	resultChan := c.PollClaim(cCtx.Context, authOk)

	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond) // Spinner: ⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏
	s.Suffix = fmt.Sprintf(" 🔗 please click the link sent to %s to authorize this agent", email)
	s.Start()
	claimedDels, err := result.Unwrap(<-resultChan)
	s.Stop()

	if cCtx.Context.Err() != nil {
		return fmt.Errorf("login canceled: %w", cCtx.Context.Err())
	}

	if err != nil {
		return fmt.Errorf("claiming access: %w", err)
	}

	fmt.Println("Successfully logged in!", claimedDels)
	c.AddProofs(claimedDels...)

	return nil
}

func reset(cCtx *cli.Context) error {
	c := cmdutil.MustGetClient()
	return c.Reset()
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
		return err
	}

	for _, r := range listOk.Results {
		fmt.Printf("%s\n", r.Root)
		if cCtx.Bool("shards") {
			for _, s := range r.Shards {
				fmt.Printf("\t%s\n", s)
			}
		}
	}

	return nil
}
