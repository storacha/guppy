package cmd

import (
	"github.com/storacha/guppy/cmd/filecoin"
)

func init() {
	filecoin.StorePathP = &storePath

	rootCmd.AddCommand(filecoin.FilecoinCmd)
}
