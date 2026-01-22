package account

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
)

var listFlags struct {
	jsonOutput bool
}

func init() {
	listCmd.Flags().BoolVar(&listFlags.jsonOutput, "json", false, "Output in JSON format")
}

var listCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List logged in accounts",
	Long: wordwrap.WrapString(
		"Lists all Storacha accounts currently logged in.",
		80),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir)

		accounts, err := c.Accounts()
		if err != nil {
			return err
		}

		if listFlags.jsonOutput {
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
