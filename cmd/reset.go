package cmd

import (
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/repo"
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
			return fmt.Errorf("loading config: %w", err)
		}

		repo, err := repo.Open(cfg.Repo)
		if err != nil {
			return fmt.Errorf("opening repo: %w", err)
		}

		c, err := cmdutil.NewClient(cfg.Network, repo)
		if err != nil {
			return fmt.Errorf("creating client: %w", err)
		}
		return c.Reset()
	},
}
