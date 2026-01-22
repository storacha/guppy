package delegation

import "github.com/spf13/cobra"

var StorePathP *string

var Cmd = &cobra.Command{
	Use:   "delegation",
	Short: "Manage delegations",
}
