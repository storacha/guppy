package plan

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/didmailto"
)

var getFlags struct {
	jsonOutput bool
}

func init() {
	getCmd.Flags().BoolVar(&getFlags.jsonOutput, "json", false, "Output in JSON format")
}

var getCmd = &cobra.Command{
	Use:   "get [account-did]",
	Short: "Get the billing plan for an account",
	Long: wordwrap.WrapString(
		"Displays the billing plan the current account is subscribed to. "+
			"If no account DID is provided, it defaults to the currently logged-in account.",
		80),
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir)

		var accountDID did.DID

		if len(args) > 0 {
			accountDID, err = didmailto.FromInput(args[0])
			if err != nil {
				return fmt.Errorf("invalid account DID: %w", err)
			}
		} else {
			accounts, err := c.Accounts()
			if err != nil {
				return fmt.Errorf("listing accounts: %w", err)
			}
			if len(accounts) == 0 {
				return fmt.Errorf("no accounts found. Please login with 'guppy login'")
			}
			accountDID = accounts[0]
		}

		plan, err := c.PlanGet(ctx, accountDID)
		if err != nil {
			if strings.Contains(err.Error(), "billing profile not found") {
				if getFlags.jsonOutput {
					fmt.Println(`{"error": "No billing profile found"}`)
					return nil
				}
				fmt.Printf(" No billing profile found for %s\n", accountDID)
				fmt.Println("   Please visit the console to set up a plan: https://console.storacha.network")
				return nil
			}
			return fmt.Errorf("getting plan: %w", err)
		}

		if getFlags.jsonOutput {
			jsonBytes, err := json.MarshalIndent(plan, "", "  ")
			if err != nil {
				return fmt.Errorf("marshaling output: %w", err)
			}
			fmt.Println(string(jsonBytes))
		} else {
			fmt.Printf("Plan Details for %s:\n", accountDID)
			fmt.Printf("  Product: %s\n", plan.Product)
			fmt.Printf("  Limit:   %s\n", plan.Limit)
			if plan.UpdatedAt != "" {
				fmt.Printf("  Updated: %s\n", plan.UpdatedAt)
			}
		}

		return nil
	},
}
