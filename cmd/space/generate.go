package space

import (
	"fmt"
	"slices"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/didmailto"
)

var generateFlags struct {
	grantTo     string
	provisionTo string
}

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate a new space",
	Long: wordwrap.WrapString(
		"Generates a new Storacha space, provisions it to the logged-in account, "+
			"grants space access to the logged-in account, and stores it in the "+
			"local store.",
		80),
	RunE: func(cmd *cobra.Command, args []string) error {
		space, err := signer.Generate()
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("generating signer for space: %w", err)
		}

		c := cmdutil.MustGetClient(*StorePathP)
		accounts := c.Accounts()

		var provisionAccount did.DID
		var grantAccount did.DID

		// Get provision account
		if generateFlags.provisionTo != "" {
			provisionAccount, err = didmailto.FromInput(generateFlags.provisionTo)
			if err != nil {
				return fmt.Errorf("parsing `--provision-to` account %q: %w", generateFlags.provisionTo, err)
			}
			if !slices.Contains(accounts, provisionAccount) {
				return fmt.Errorf("account %s is not logged in yet. Use `guppy login %s` to log in.", generateFlags.provisionTo, generateFlags.provisionTo)
			}
		} else {
			switch {
			case len(accounts) == 0:
				return fmt.Errorf("No accounts are logged in yet. Use `guppy login <account>` to log in.")
			case len(accounts) == 1:
				provisionAccount = accounts[0]
			default:
				return fmt.Errorf("Multiple accounts are logged in. Specify an account with `--provision-to`.")
			}
		}

		// Get grant account
		if generateFlags.grantTo != "" {
			grantAccount, err = didmailto.FromInput(generateFlags.grantTo)
			if err != nil {
				return fmt.Errorf("parsing `--grant-to` account %q: %w", generateFlags.grantTo, err)
			}
			if !slices.Contains(accounts, grantAccount) {
				return fmt.Errorf("account %s is not logged in yet. Use `guppy login %s` to log in.", generateFlags.grantTo, generateFlags.grantTo)
			}
		} else {
			switch {
			case len(accounts) == 0:
				return fmt.Errorf("No accounts are logged in yet. Use `guppy login <account>` to log in.")
			case len(accounts) == 1:
				grantAccount = accounts[0]
			default:
				return fmt.Errorf("Multiple accounts are logged in. Specify an account with `--grant-to`.")
			}
		}

		if provisionAccount == did.Undef {
			return fmt.Errorf("no account found to provision space to")
		}
		if grantAccount == did.Undef {
			return fmt.Errorf("no account found to grant space access to")
		}

		fmt.Printf("Provisioning %s to %s...\n", space.DID(), provisionAccount)
		_, err = c.ProviderAdd(cmd.Context(), provisionAccount, c.Connection().ID().DID(), space.DID())
		if err != nil {
			return fmt.Errorf("provisioning space: %w", err)
		}

		fmt.Printf("Granting access on %s to %s...\n", space.DID(), grantAccount)
		_, err = grant(cmd.Context(), c, space, grantAccount)
		if err != nil {
			return fmt.Errorf("granting capabilities: %w", err)
		}

		fmt.Printf("Adding key for space %s to local store...\n", space.DID())
		err = c.AddSpace(space)
		if err != nil {
			return fmt.Errorf("adding space to client: %w", err)
		}

		fmt.Printf("Generated space %s!\n", space.DID())

		return nil
	},
}

func init() {
	SpaceCmd.AddCommand(generateCmd)
	generateCmd.Flags().StringVar(&generateFlags.grantTo, "grant-to", "", "Account DID to grant space access to. Must be logged in already. (optional when exactly one account is logged in)")
	generateCmd.Flags().StringVar(&generateFlags.provisionTo, "provision-to", "", "Account DID to provision space to. Must be logged in already. (optional when exactly one account is logged in)")
}
