package space

import (
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/didmailto"
)

var provisionCmd = &cobra.Command{
	Use:   "provision <space-did> <email-address>",
	Short: "Provision a space with a customer account",
	Long: wordwrap.WrapString(
		"Provisions a space by associating it with a customer account which will "+
			"be billed for the space's usage.",
		80),
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		spaceDIDStr := args[0]
		customerEmail := args[1]

		// Parse the space DID
		spaceDID, err := did.Parse(spaceDIDStr)
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("invalid space DID: %w", err)
		}

		// Convert email to did:mailto
		customerDID, err := didmailto.FromInput(customerEmail)
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("invalid customer email: %w", err)
		}

		c := cmdutil.MustGetClient(*StorePathP)

		fmt.Printf("Provisioning space %s with customer %s...\n", spaceDID, customerEmail)

		_, err = c.ProviderAdd(cmd.Context(), customerDID, c.Connection().ID().DID(), spaceDID)
		if err != nil {
			return fmt.Errorf("provisioning space: %w", err)
		}

		fmt.Printf("Successfully provisioned!\n")

		return nil
	},
}

func init() {
	SpaceCmd.AddCommand(provisionCmd)
}
