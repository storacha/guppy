package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/storacha/guppy/internal/cmdutil"
)

var whoamiCmd = &cobra.Command{
	Use:   "whoami",
	Short: "Print information about the local agent",
	Long:  "Prints information about the local agent.",

	Run: func(cmd *cobra.Command, args []string) {
		c := cmdutil.MustGetClient(storePath)
		fmt.Println(c.DID())
	},
}

func init() {
	rootCmd.AddCommand(whoamiCmd)
}
