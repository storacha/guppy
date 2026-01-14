package cmd

import (
	"github.com/storacha/guppy/cmd/account"
)

func init() {
	rootCmd.AddCommand(account.AccountCmd)
}
