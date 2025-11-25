package space

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/repo"
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
		var (
			space repo.Space
			err   error
		)
		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("loading config: %v", err)
		}

		repo, err := repo.Open(cfg.Repo)
		if err != nil {
			return fmt.Errorf("opening repo: %v", err)
		}

		spaceStore, err := repo.SpaceStore()
		if err != nil {
			return fmt.Errorf("opening space store: %v", err)
		}

		if len(args) > 0 {
			space, err = spaceStore.Get(args[0])
			if err != nil {
				return err
			}
		} else {
			space, err = spaceStore.Current()
			if err != nil {
				return err
			}
			// if this is the current space, print so last.
			defer cmd.Println("Status: Current Space")
		}

		if spaceFlags.json {
			data, err := json.MarshalIndent(space.Sanitized(), "", "  ")
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
		}

		return nil
	},
}
