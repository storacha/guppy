package cmd

import (
	"github.com/storacha/guppy/cmd/proof"
)

func init() {
	proof.StorePathP = &storePath
	rootCmd.AddCommand(proof.ProofCmd)
}
