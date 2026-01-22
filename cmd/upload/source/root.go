package source

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use: "source",
}

func init() {
	Cmd.AddCommand(
		AddCmd,
		ListCmd,
	)
}
