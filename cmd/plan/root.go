package plan

import (
	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "plan",
	Short: "Manage billing plans",
	Long: wordwrap.WrapString(
		"Inspect and manage the billing plan associated with your Storacha account.",
		80),
}

func init() {
	Cmd.AddCommand(
		getCmd,
	)
}
