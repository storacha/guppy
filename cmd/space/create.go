package space

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/repo"
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

		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		repo, err := repo.Open(cfg.Repo)
		if err != nil {
			return fmt.Errorf("opening repo: %w", err)
		}

		spaceStore, err := repo.SpaceStore()
		if err != nil {
			return fmt.Errorf("loading space-store: %w", err)
		}

		space, err := spaceStore.CreateSpace(spaceName, spaceFlags.description)
		if err != nil {
			return fmt.Errorf("creating space: %w", err)
		}

		// Output
		if spaceFlags.json {
			data, err := json.MarshalIndent(space.Sanitized(), "", "  ")
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
