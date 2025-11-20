package space

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	useCmd.Flags().BoolVar(&spaceFlags.json, "json", false, "Output in JSON format")
}

var useCmd = &cobra.Command{
	Use:   "use <name|did>",
	Short: "Set the current space",
	Long:  "Set the specified space as the current active space",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		nameOrDID := args[0]

		space, err := findSpaceByNameOrDID(nameOrDID)
		if err != nil {
			return err
		}

		config, err := loadSpaceConfig()
		if err != nil {
			return err
		}

		config.Current = space.DID

		if err := saveSpaceConfig(config); err != nil {
			return fmt.Errorf("failed to save space config: %w", err)
		}

		if spaceFlags.json {
			data, err := json.MarshalIndent(sanitizeSpace(space), "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}
			fmt.Println(string(data))
		} else {
			fmt.Printf("Set current space to: %s\n", space.Name)
			fmt.Printf("DID: %s\n", space.DID)
		}

		return nil
	},
}
