package usage

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "usage",
	Short: "Manage usage reports",
	Long:  "View storage usage reports for your spaces.",
}

func init() {
	Cmd.AddCommand(newReportCmd())
}
