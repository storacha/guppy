package cmd

import (
	"fmt"
	"time"

	"github.com/briandowns/spinner"
	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/didmailto"
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
		email := cmd.Flags().Arg(0)

		accountDid, err := didmailto.FromEmail(email)
		if err != nil {
			return err
		}

		c := cmdutil.MustGetClient()

		authOk, err := c.RequestAccess(cmd.Context(), accountDid.String())
		if err != nil {
			return fmt.Errorf("requesting access: %w", err)
		}

		resultChan := c.PollClaim(cmd.Context(), authOk)

		s := spinner.New(spinner.CharSets[14], 100*time.Millisecond) // Spinner: ⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏
		s.Suffix = fmt.Sprintf(" 🔗 please click the link sent to %s to authorize this agent", email)
		s.Start()
		claimedDels, err := result.Unwrap(<-resultChan)
		s.Stop()

		if cmd.Context().Err() != nil {
			return fmt.Errorf("login canceled: %w", cmd.Context().Err())
		}

		if err != nil {
			return fmt.Errorf("claiming access: %w", err)
		}

		fmt.Printf("Successfully logged in as %s!", email)
		c.AddProofs(claimedDels...)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(loginCmd)
}
