package account

import (
	"github.com/spf13/cobra"
)

var StorePathP *string

var AccountCmd = &cobra.Command{
	Use:   "account",
	Short: "Manage accounts",
}
