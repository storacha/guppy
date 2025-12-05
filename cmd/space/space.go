package space

import (
	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "space",
	Short: "Manage spaces",
	Long: wordwrap.WrapString(
		"Create and manage Storacha spaces locally. Spaces are isolated storage "+
			"contexts that can be shared with other users via delegations.",
		80),
}

func init() {
	Cmd.AddCommand(
		generateCmd,
		infoCmd,
		listCmd,
		provisionCmd,
	)
}
