package account

import (
	"github.com/spf13/cobra"
)

var StorePathP *string

var Cmd = &cobra.Command{
	Use:   "account",
	Short: "Manage accounts",
}
