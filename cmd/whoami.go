package cmd

import (
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/config"
)

var whoamiCmd = &cobra.Command{
	Use:   "whoami",
	Short: "Print information about the local agent",
	Long:  "Prints information about the local agent.",

	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load()
		if err != nil {
			return err
		}
		s, err := agentstore.NewFs(cfg.Repo.Dir)
		if err != nil {
			return err
		}
		principal, err := s.Principal()
		if err != nil {
			return err
		}

		cmd.Println(principal.DID().String())
		return nil
	},
}

func init() {
	rootCmd.AddCommand(whoamiCmd)
}
