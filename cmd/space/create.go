package space

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
)

func init() {
	createCmd.Flags().StringVar(&spaceFlags.description, "description", "", "Description for the space")
	createCmd.Flags().BoolVar(&spaceFlags.json, "json", false, "Output in JSON format")
}

var createCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new space",
	Long: wordwrap.WrapString(
		"Create a new space with the given name. The space will be generated with "+
			"its own cryptographic identity and saved locally.",
		80),
	Args: cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		spaceName := args[0]

		// Check if space already exists
		if existing, _ := findSpaceByNameOrDID(spaceName); existing != nil {
			return fmt.Errorf("space with name '%s' already exists", spaceName)
		}

		// Generate new space
		space, err := generateSpace()
		if err != nil {
			return fmt.Errorf("failed to generate space: %w", err)
		}

		space.Name = spaceName
		space.Description = spaceFlags.description

		// Save to config
		if err := addSpaceToConfig(space); err != nil {
			return fmt.Errorf("failed to save space: %w", err)
		}

		// Output
		if spaceFlags.json {
			data, err := json.MarshalIndent(sanitizeSpace(space), "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}
			cmd.Println(string(data))
		} else {
			cmd.Printf("Created space: %s\n", spaceName)
			cmd.Printf("DID: %s\n", space.DID)
			cmd.Printf("Set as current space\n")
		}

		return nil
	},
}
