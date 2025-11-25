package space

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/internal/cmdutil"
)

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate a new space",
	Long: wordwrap.WrapString(
		"Generates a new Storacha space, and stores it in the local store.",
		80),
	RunE: func(cmd *cobra.Command, args []string) error {
		space, err := signer.Generate()
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("generating signer for space: %w", err)
		}

		c := cmdutil.MustGetClient(*StorePathP)

		err = c.AddSpace(space)
		if err != nil {
			return fmt.Errorf("adding space to client: %w", err)
		}

		accounts := c.Accounts()
		if len(accounts) == 0 {
			return fmt.Errorf("no accounts logged in yet - use 'guppy login' to log in")
		}
		account := accounts[0]

		_, err = c.ProviderAdd(cmd.Context(), account, c.Connection().ID().DID(), space.DID())
		if err != nil {
			return fmt.Errorf("provisioning space: %w", err)
		}

		_, err = grant(cmd.Context(), c, space, account)
		if err != nil {
			return fmt.Errorf("granting capabilities: %w", err)
		}

		// Print JSON output with just the DID (omitting the private key)
		output := map[string]string{
			"id": space.DID().String(),
		}
		jsonBytes, err := json.Marshal(output)
		if err != nil {
			return fmt.Errorf("marshaling output: %w", err)
		}
		fmt.Println(string(jsonBytes))

		return nil
	},
}

func init() {
	SpaceCmd.AddCommand(generateCmd)
}
