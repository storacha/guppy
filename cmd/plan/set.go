package plan

import (
	"fmt"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/didmailto"
)

var setCmd = &cobra.Command{
	Use:   "set <product-did>",
	Short: "Set the billing plan for an account",
	Long: wordwrap.WrapString(
		"Sets the billing plan for the account. You must provide the Product DID (e.g., did:web:starter.storacha.network). "+
			"Optionally provide the account DID via --account, otherwise it defaults to the current account.",
		80),
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		productDIDStr := args[0]

		productDID, err := did.Parse(productDIDStr)
		if err != nil {
			return fmt.Errorf("invalid product DID: %w", err)
		}

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir)

		var accountDID did.DID
		if cmd.Flag("account").Changed {
			val, _ := cmd.Flags().GetString("account")
			accountDID, err = didmailto.FromInput(val)
			if err != nil {
				return fmt.Errorf("invalid account DID: %w", err)
			}
		} else {
			accounts, err := c.Accounts()
			if err != nil {
				return err
			}
			if len(accounts) == 0 {
				return fmt.Errorf("no accounts found")
			}
			accountDID = accounts[0]
		}

		fmt.Printf("Setting plan for %s to %s...\n", accountDID, productDID)
		_, err = c.PlanSet(ctx, accountDID, productDID)

		if err != nil {
			if strings.Contains(err.Error(), "billing profile not found") {
				fmt.Println("\n  Update Failed: No Billing Profile Found")
				fmt.Println("   To change plans, you must first set up a payment method.")
				fmt.Println("   Please visit the console: https://console.storacha.network")
				return nil
			}
			return err
		}

		fmt.Println("Success! Plan updated.")
		return nil
	},
}

func init() {
	setCmd.Flags().String("account", "", "The account DID to update")
	Cmd.AddCommand(setCmd)
}
