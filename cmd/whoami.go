package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
)

var whoamiCmd = &cobra.Command{
	Use:   "whoami",
	Short: "Print information about the local agent",
	Long:  "Prints information about the local agent.",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("loading config: %v", err)
		}

		c := cmdutil.MustGetClient(cfg.Repo.AgentDataFilePath())
		fmt.Println(c.DID())
		return nil
	},
}
