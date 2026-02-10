package blob

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "blob",
	Short: "Manage blobs",
}

func init() {
	Cmd.AddCommand(
		lsCmd,
	)
}
