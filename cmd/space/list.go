package space

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func init() {
	listCmd.Flags().BoolVar(&spaceFlags.json, "json", false, "Output in JSON format")
}

var listCmd = &cobra.Command{
	Use:     "ls",
	Aliases: []string{"list"},
	Short:   "List all spaces",
	Long:    "List all spaces that have been created locally",

	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := loadSpaceConfig()
		if err != nil {
			return err
		}

		if len(config.Spaces) == 0 {
			cmd.Println("No spaces found. Create one with 'guppy space create <name>'")
			return nil
		}

		if spaceFlags.json {
			safeSpaces := make(map[string]interface{})
			for did, space := range config.Spaces {
				safeSpaces[did] = sanitizeSpace(&space)
			}
			data, err := json.MarshalIndent(safeSpaces, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}
			cmd.Println(string(data))
			return nil
		}

		// Table output
		cmd.Printf("%-20s %-12s %-10s %s\n", "NAME", "REGISTERED", "CURRENT", "DID")
		cmd.Println(strings.Repeat("-", 80))

		for did, space := range config.Spaces {
			current := ""
			if config.Current == did {
				current = "✓"
			}

			registered := "No"
			if space.Registered {
				registered = "Yes"
			}

			cmd.Printf("%-20s %-12s %-10s %s\n", space.Name, registered, current, space.DID)
		}

		return nil
	},
}
