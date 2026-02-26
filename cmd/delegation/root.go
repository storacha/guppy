package delegation

import "github.com/spf13/cobra"

var Cmd = &cobra.Command{
	Use:   "delegation",
	Short: "Manage delegations",
}

func init() {
	Cmd.AddCommand(createCmd)
	Cmd.AddCommand(lsCmd)
	Cmd.AddCommand(revokeCmd)
}
