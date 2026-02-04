package space

import (
	"context"
	"fmt"
	"slices"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/didmailto"
)

// spaceAccess is the set of capabilities required by the agent to manage a
// space.
var spaceAccess = []string{
	"assert/*",
	"space/*",
	"blob/*",
	"index/*",
	"store/*",
	"upload/*",
	"access/*",
	"filecoin/*",
	"usage/*",
}

var generateFlags struct {
	name        string
	grantTo     string
	provisionTo string
}

func init() {
	generateCmd.Flags().StringVar(&generateFlags.name, "name", "", "Name for the space (optional)")
	generateCmd.Flags().StringVar(&generateFlags.grantTo, "grant-to", "", "Account DID to grant space access to. Must be logged in already. (optional when exactly one account is logged in)")
	generateCmd.Flags().StringVar(&generateFlags.provisionTo, "provision-to", "", "Account DID to provision space to. Must be logged in already. (optional when exactly one account is logged in)")
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

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}

		c := cmdutil.MustGetClient(cfg.Repo.Dir)
		accounts, err := c.Accounts()
		if err != nil {
			return err
		}

		var provisionAccount did.DID
		var grantAccount did.DID

		// Get provision account
		if generateFlags.provisionTo != "" {
			provisionAccount, err = didmailto.FromInput(generateFlags.provisionTo)
			if err != nil {
				cmd.SilenceUsage = false
				return fmt.Errorf("parsing `--provision-to` account %q: %w", generateFlags.provisionTo, err)
			}
			if !slices.Contains(accounts, provisionAccount) {
				cmd.PrintErrf("Account %s is not logged in yet. Use `guppy login %s` to log in.\n",
					generateFlags.provisionTo, generateFlags.provisionTo)
				return cmdutil.NewHandledCliError(fmt.Errorf("account %s is not logged in", provisionAccount))
			}
		} else {
			switch {
			case len(accounts) == 0:
				cmd.PrintErrf("No accounts are logged in yet. Use `guppy login <account>` to log in.\n")
				return cmdutil.NewHandledCliError(fmt.Errorf("account %s is not logged in", provisionAccount))
			case len(accounts) == 1:
				provisionAccount = accounts[0]
			default:
				var acctsString string
				for _, acct := range accounts {
					acctsString += fmt.Sprintf("- %s\n", acct)
				}
				cmd.PrintErrf("Multiple accounts are logged in.\n%s\nSpecify an account with `--provision-to`.\n",
					acctsString)
				return cmdutil.NewHandledCliError(fmt.Errorf("multiple accounts are logged in"))
			}
		}

		// Get grant account
		if generateFlags.grantTo != "" {
			grantAccount, err = didmailto.FromInput(generateFlags.grantTo)
			if err != nil {
				cmd.SilenceUsage = false
				return fmt.Errorf("parsing `--grant-to` account %q: %w", generateFlags.grantTo, err)
			}
			if !slices.Contains(accounts, grantAccount) {
				cmd.PrintErrf("Account %s is not logged in yet. Use `guppy login %s` to log in.\n", generateFlags.grantTo,
					generateFlags.grantTo)
				return cmdutil.NewHandledCliError(fmt.Errorf("account %s is not logged in", grantAccount))
			}
		} else {
			switch {
			case len(accounts) == 0:
				cmd.PrintErr("No accounts are logged in yet. Use `guppy login <account>` to log in.\n")
				return cmdutil.NewHandledCliError(fmt.Errorf("account %s is not logged in", grantAccount))
			case len(accounts) == 1:
				grantAccount = accounts[0]
			default:
				var acctsString string
				for _, acct := range accounts {
					acctsString += fmt.Sprintf("- %s\n", acct)
				}
				cmd.PrintErrf("Multiple accounts are logged in.\n%s\nSpecify an account with `--grant-to`.\n", acctsString)
				return cmdutil.NewHandledCliError(fmt.Errorf("multiple accounts are logged in"))
			}
		}

		if provisionAccount == did.Undef {
			return fmt.Errorf("no account found to provision space to")
		}
		if grantAccount == did.Undef {
			return fmt.Errorf("no account found to grant space access to")
		}

		cmd.PrintErrf("Provisioning %s to %s...\n\n", space.DID(), provisionAccount)
		_, err = c.ProviderAdd(cmd.Context(), provisionAccount, c.Connection().ID().DID(), space.DID())
		if err != nil {
			return fmt.Errorf("provisioning space: %w", err)
		}

		cmd.PrintErrf("Granting access on %s to %s...\n\n", space.DID(), grantAccount)

		// Build the capabilities to grant
		capabilities := make([]ucan.Capability[ucan.NoCaveats], 0, len(spaceAccess))
		for _, c := range spaceAccess {
			capabilities = append(capabilities, ucan.NewCapability(
				c,
				space.DID().String(),
				ucan.NoCaveats{},
			))
		}

		_, err = grant(cmd.Context(), c, space, grantAccount, capabilities, generateFlags.name)
		if err != nil {
			return fmt.Errorf("granting capabilities: %w", err)
		}

		cmd.PrintErr("Generated space: ")
		// all other output is printed to stderr, only the space did prints to stdout, allowing:
		// export SPACE=$(guppy space generate)
		cmd.Print(space.DID().String())
		cmd.PrintErr("\n\n")

		return nil
	},
}

func grant(ctx context.Context, c *client.Client, spaceSigner principal.Signer, account did.DID, capabilities []ucan.Capability[ucan.NoCaveats], name string) (delegation.Delegation, error) {
	// Create the delegation from space to account
	opts := []delegation.Option{
		delegation.WithNoExpiration(),
	}
	if name != "" {
		opts = append(opts, delegation.WithFacts([]ucan.FactBuilder{
			client.NewSpaceFact(name),
		}))
	}
	delToStore, err := delegation.Delegate(
		spaceSigner,
		account,
		capabilities,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("creating delegation: %w", err)
	}

	delToKeep, err := delegation.Delegate(
		spaceSigner,
		c.Issuer().DID(),
		capabilities,
		delegation.WithNoExpiration(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating delegation: %w", err)
	}

	c.AddProofs(delToStore, delToKeep)

	// Store the delegation via access/delegate
	_, err = c.AccessDelegate(ctx, spaceSigner.DID(), delToStore)
	if err != nil {
		return nil, fmt.Errorf("storing delegation: %w", err)
	}

	return delToStore, nil
}
