package space

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
)

var infoFlags struct {
	jsonOutput bool
}

var infoCmd = &cobra.Command{
	Use:   "info <space-did>",
	Short: "Get information about a space",
	Long: wordwrap.WrapString(
		"Gets information about a space, including which providers are associated with it. "+
			"This shows the space's provisioning status.",
		80),
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		spaceDIDStr := args[0]

		// Parse the space DID
		spaceDID, err := did.Parse(spaceDIDStr)
		if err != nil {
			return fmt.Errorf("invalid space DID: %w", err)
		}

		c := cmdutil.MustGetClient(*StorePathP)

		result, err := c.SpaceInfo(cmd.Context(), spaceDID)
		if err != nil {
			return fmt.Errorf("getting space info: %w", err)
		}

		if infoFlags.jsonOutput {
			jsonBytes, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return fmt.Errorf("marshaling output: %w", err)
			}
			fmt.Println(string(jsonBytes))
		} else {
			fmt.Printf("Space: %s\n", result.Did)
			if len(result.Providers) > 0 {
				fmt.Printf("Providers:\n")
				for _, provider := range result.Providers {
					fmt.Printf("  - %s\n", provider)
				}
			} else {
				fmt.Printf("No providers (space not provisioned)\n")
			}
		}

		return nil
	},
}

func init() {
	infoCmd.Flags().BoolVar(&infoFlags.jsonOutput, "json", false, "Output in JSON format")
	SpaceCmd.AddCommand(infoCmd)
}
