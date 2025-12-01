package account

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/guppy/internal/cmdutil"
)

var lsFlags struct {
	jsonOutput bool
}

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List logged in accounts",
	Long: wordwrap.WrapString(
		"Lists all Storacha accounts currently logged in.",
		80),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := cmdutil.MustGetClient(*StorePathP)

		accounts := c.Accounts()

		if lsFlags.jsonOutput {
			// Build JSON array of account DIDs
			output := make([]map[string]string, 0, len(accounts))
			for _, account := range accounts {
				output = append(output, map[string]string{
					"id": account.String(),
				})
			}

			jsonBytes, err := json.Marshal(output)
			if err != nil {
				return fmt.Errorf("marshaling output: %w", err)
			}
			fmt.Println(string(jsonBytes))
		} else {
			for _, account := range accounts {
				fmt.Println(account.String())
			}
		}

		return nil
	},
}

func init() {
	lsCmd.Flags().BoolVar(&lsFlags.jsonOutput, "json", false, "Output in JSON format")
	AccountCmd.AddCommand(lsCmd)
}
