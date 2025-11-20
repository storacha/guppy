package space

import (
	"fmt"
	"os"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
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
			return fmt.Errorf("invalid space DID: %w", err)
		}

		// Convert email to did:mailto
		customerDID, err := did.Parse(fmt.Sprintf("did:mailto:%s", customerEmail))
		if err != nil {
			return fmt.Errorf("invalid customer email: %w", err)
		}

		// Get the default provider DID from environment or use default
		providerDIDStr := os.Getenv("STORACHA_SERVICE_DID")
		if providerDIDStr == "" {
			providerDIDStr = "did:web:staging.up.warm.storacha.network"
		}

		providerDID, err := did.Parse(providerDIDStr)
		if err != nil {
			return fmt.Errorf("invalid provider DID: %w", err)
		}

		c := cmdutil.MustGetClient(*StorePathP)

		fmt.Printf("Provisioning space %s with customer %s...\n", spaceDID, customerEmail)

		result, err := c.ProviderAdd(cmd.Context(), customerDID, providerDID, spaceDID)
		if err != nil {
			return fmt.Errorf("provisioning space: %w", err)
		}

		fmt.Printf("Successfully provisioned!\n")
		fmt.Printf("  Consumer: %s\n", result.Consumer)
		fmt.Printf("  Provider: %s\n", result.Provider)

		return nil
	},
}

func init() {
	SpaceCmd.AddCommand(provisionCmd)
}
