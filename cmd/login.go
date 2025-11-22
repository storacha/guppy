package cmd

import (
	"fmt"
	"time"

	"github.com/briandowns/spinner"
	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/result"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/didmailto"
	"github.com/storacha/guppy/pkg/repo"
)

var loginCmd = &cobra.Command{
	Use:   "login <account>",
	Short: "Authenticate with a Storacha account",
	Long: wordwrap.WrapString(
		"Authenticates this agent with an email address to gain access to all "+
			"capabilities that have been delegated to it. This command will ask "+
			"Storacha to send an authorization email and then wait for that "+
			"authorization to be confirmed."+
			"\n\n"+
			"You can rerun this command at any time to gain access to any new "+
			"spaces created since your last login. Your agent can authorize with "+
			"multiple Storacha accounts at once; your agent will simply store "+
			"delegations received from each account.",
		80),
	Example: fmt.Sprintf("  %s login racha@storacha.network", rootCmd.Name()),
	Args:    cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		email := cmd.Flags().Arg(0)

		accountDid, err := didmailto.FromEmail(email)
		if err != nil {
			return fmt.Errorf("parsing email: %w", err)
		}

		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		repo, err := repo.Open(cfg.Repo)
		if err != nil {
			return fmt.Errorf("opening repo: %w", err)
		}

		c, err := cmdutil.NewClient(cfg.Network, repo)
		if err != nil {
			return fmt.Errorf("creating client: %w", err)
		}

		authOk, err := c.RequestAccess(ctx, accountDid.String())
		if err != nil {
			return fmt.Errorf("requesting access: %w", err)
		}

		s := spinner.New(spinner.CharSets[14], 100*time.Millisecond, spinner.WithWriter(cmd.ErrOrStderr())) // Spinner: ⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏
		s.Suffix = fmt.Sprintf(" 🔗 please click the link sent to %s to authorize this agent", email)
		s.Start()
		defer s.Stop()

		select {
		case <-ctx.Done():
			return fmt.Errorf("login canceled: %w", ctx.Err())
		case res := <-c.PollClaim(ctx, authOk):
			claimedDels, err := result.Unwrap(res)
			if err != nil {
				return fmt.Errorf("claiming access: %w", err)
			}
			if err := c.AddProofs(claimedDels...); err != nil {
				return fmt.Errorf("storing claimed access: %w", err)
			}
			cmd.Printf("Successfully logged in as %s!", email)
		}
		return nil
	},
}
