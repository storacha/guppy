package cmd

import (
	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/config"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset agent proofs",
	Long: wordwrap.WrapString("Removes all proofs/delegations from the store "+
		"(in other words, log out of all accounts). Preserves the existing agent "+
		"DID.",
		80),

	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load()
		if err != nil {
			return err
		}
		s, err := agentstore.NewFs(cfg.Repo.Dir)
		if err != nil {
			return err
		}
		return s.Reset()
	},
}

func init() {
	rootCmd.AddCommand(resetCmd)
}
