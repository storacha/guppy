package proof

import (
	"github.com/spf13/cobra"
)

var StorePathP *string

var Cmd = &cobra.Command{
	Use:   "proof",
	Short: "Manage proofs",
}
