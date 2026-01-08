package filecoin

import (
	"github.com/spf13/cobra"
)

var StorePathP *string

var FilecoinCmd = &cobra.Command{
	Use:   "filecoin",
	Short: "Interact with Filecoin capabilities",
	Long:  "Commands for interacting with the Filecoin storage pipeline, such as querying piece status.",
}
