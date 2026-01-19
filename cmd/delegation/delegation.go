package delegation

import "github.com/spf13/cobra"

var StorePathP *string

var DelegationCmd = &cobra.Command{
	Use:   "delegation",
	Short: "Manage delegations",
}
