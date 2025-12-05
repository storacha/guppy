package account

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "account",
	Short: "Manage accounts",
}

func init() {
	Cmd.AddCommand(listCmd)
}
