package cmd

import (
	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
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
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir, cfg.Network)
		return c.Reset()
	},
}
