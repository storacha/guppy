package space

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/internal/cmdutil"
)

var StorePathP *string

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
