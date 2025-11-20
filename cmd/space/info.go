package space

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

func init() {
	infoCmd.Flags().BoolVar(&spaceFlags.json, "json", false, "Output in JSON format")
}

var infoCmd = &cobra.Command{
	Use:   "info [name|did]",
	Short: "Show space information",
	Long:  "Display detailed information about a space. If no argument is provided, shows the current space.",
	Args:  cobra.MaximumNArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		var space *Space
		var err error

		if len(args) > 0 {
			space, err = findSpaceByNameOrDID(args[0])
		} else {
			space, err = getCurrentSpace()
		}
		if err != nil {
			return err
		}

		if spaceFlags.json {
			data, err := json.MarshalIndent(sanitizeSpace(space), "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}
			cmd.Println(string(data))
		} else {
			cmd.Printf("Space: %s\n", space.Name)
			cmd.Printf("DID: %s\n", space.DID)
			cmd.Printf("Created: %s\n", space.Created.Format(time.RFC3339))
			cmd.Printf("Registered: %t\n", space.Registered)
			if space.Description != "" {
				cmd.Printf("Description: %s\n", space.Description)
			}

			config, _ := loadSpaceConfig()
			if config != nil && config.Current == space.DID {
				cmd.Printf("Status: Current space\n")
			}
		}

		return nil
	},
}
