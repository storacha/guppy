package space

import (
	"context"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/didmailto"
)

var grantFlags struct {
	capabilities []string
}

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

var grantCmd = &cobra.Command{
	Use:   "grant <space-did> <email-address>",
	Short: "Grant capabilities from a space to an account",
	Long: wordwrap.WrapString(
		"Creates a delegation from a space to an account (email address), granting "+
			"the specified capabilities. The delegation is then stored on the service "+
			"via access/delegate so it can be retrieved by the account holder.",
		80),
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		spaceDIDStr := args[0]
		accountInput := args[1]

		// Parse the space DID
		space, err := did.Parse(spaceDIDStr)
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("invalid space DID: %w", err)
		}

		// Parse the account DID
		account, err := didmailto.FromInput(accountInput)
		if err != nil {
			return fmt.Errorf("invalid email address: %w", err)
		}

		// Get the space key from the client's stored spaces
		c := cmdutil.MustGetClient(*StorePathP)
		var spaceSigner principal.Signer
		for _, s := range c.Spaces() {
			if s.DID() == space {
				spaceSigner = s
				break
			}
		}

		if spaceSigner == nil {
			return fmt.Errorf("space %s not found in local store - use 'guppy space generate' or 'guppy space ls' to see available spaces", spaceDIDStr)
		}

		del, err := grant(cmd.Context(), c, spaceSigner, account)
		if err != nil {
			return fmt.Errorf("granting capabilities: %w", err)
		}

		fmt.Printf("✓ Successfully granted capabilities to %s\n", accountInput)
		fmt.Printf("  Delegation CID: %s\n", del.Link())

		return nil
	},
}

func grant(ctx context.Context, c *client.Client, spaceSigner principal.Signer, account did.DID) (delegation.Delegation, error) {
	// Build the capabilities to grant
	capabilities := make([]ucan.Capability[ucan.NoCaveats], 0, len(grantFlags.capabilities))
	for _, cap := range grantFlags.capabilities {
		capabilities = append(capabilities, ucan.NewCapability(
			cap,
			spaceSigner.DID().String(),
			ucan.NoCaveats{},
		))
	}

	// Create the delegation from space to account
	delToStore, err := delegation.Delegate(
		spaceSigner,
		account,
		capabilities,
		delegation.WithNoExpiration(),
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

func init() {
	grantCmd.Flags().StringSliceVar(&grantFlags.capabilities, "can", spaceAccess,
		"Capabilities to grant (e.g., 'store/add', 'upload/*'). If not specified, grants default space access.")
	SpaceCmd.AddCommand(grantCmd)
}
