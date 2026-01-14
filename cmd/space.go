package cmd

import (
	"github.com/storacha/guppy/cmd/space"
)

func init() {
	rootCmd.AddCommand(space.SpaceCmd)
}
