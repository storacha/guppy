package space

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/repo"
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
			return fmt.Errorf("opening space store: %w", err)
		}

		space, err := spaceStore.Get(nameOrDID)
		if err != nil {
			return err
		}

		if err := spaceStore.SetCurrent(space); err != nil {
			return err
		}

		if spaceFlags.json {
			data, err := json.MarshalIndent(space.Sanitized(), "", "  ")
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
