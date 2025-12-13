package unixfs

import (
	"github.com/spf13/cobra"
)

var StorePathP *string

var Cmd = &cobra.Command{
	Use:   "unixfs",
	Short: "Interact with UnixFS data",
	Long:  "Commands for inspecting and manipulating UnixFS data structures on the network.",
}
