package cmd

import (
	"github.com/storacha/guppy/cmd/space"
)

func init() {
	space.StorePathP = &storePath
	rootCmd.AddCommand(space.SpaceCmd)
}
