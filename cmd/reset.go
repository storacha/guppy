package cmd

import (
	"github.com/spf13/cobra"
	"github.com/storacha/guppy/internal/cmdutil"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset agent proofs",
	Long: `Removes all proofs/delegations from the store (in other words, log out of all
accounts). Preserves the existing agent DID.`,

	RunE: func(cmd *cobra.Command, args []string) error {
		c := cmdutil.MustGetClient()
		return c.Reset()
	},
}

func init() {
	rootCmd.AddCommand(resetCmd)
}
