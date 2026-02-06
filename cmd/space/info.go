package space

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
)

var infoFlags struct {
	jsonOutput bool
}

func init() {
	infoCmd.Flags().BoolVar(&infoFlags.jsonOutput, "json", false, "Output in JSON format")
}

var infoCmd = &cobra.Command{
	Use:   "info <space>",
	Short: "Get information about a space",
	Long: wordwrap.WrapString(
		"Gets information about a space, including which providers are associated with it. "+
			"This shows the space's provisioning status. The space can be specified by DID or by name.",
		80),
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir)

		spaceDID, err := cmdutil.ResolveSpace(c, args[0])
		if err != nil {
			return err
		}

		result, err := c.SpaceInfo(cmd.Context(), spaceDID)
		if err != nil {
			return fmt.Errorf("getting space info: %w", err)
		}

		if infoFlags.jsonOutput {
			jsonBytes, err := json.Marshal(result)
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
