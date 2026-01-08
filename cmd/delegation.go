package cmd

import (
	"github.com/storacha/guppy/cmd/delegation"
)

func init() {
	delegation.StorePathP = &storePath
	rootCmd.AddCommand(delegation.DelegationCmd)
}
