package cmd

import (
	"github.com/storacha/guppy/cmd/account"
)

func init() {
	account.StorePathP = &storePath
	rootCmd.AddCommand(account.AccountCmd)
}
