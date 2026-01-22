package cmd

import (
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
)

var whoamiCmd = &cobra.Command{
	Use:   "whoami",
	Short: "Print information about the local agent",
	Long:  "Prints information about the local agent.",

	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir)
		cmd.Println(c.DID())
		return nil
	},
}
