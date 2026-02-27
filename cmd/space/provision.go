package space

import (
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/didmailto"
)

var provisionCmd = &cobra.Command{
	Use:   "provision <space> <email-address>",
	Short: "Provision a space with a customer account",
	Long: wordwrap.WrapString(
		"Provisions a space by associating it with a customer account which will "+
			"be billed for the space's usage. The space can be specified by DID or "+
			"by name.",
		80),
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		customerEmail := args[1]

		// Convert email to did:mailto
		customerDID, err := didmailto.FromInput(customerEmail)
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("invalid customer email: %w", err)
		}

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir, cfg.Network)

		spaceDID, err := cmdutil.ResolveSpace(c, args[0])
		if err != nil {
			return err
		}

		fmt.Printf("Provisioning space %s with customer %s...\n", spaceDID, customerEmail)

		_, err = c.ProviderAdd(cmd.Context(), customerDID, c.Connection().ID().DID(), spaceDID)
		if err != nil {
			return fmt.Errorf("provisioning space: %w", err)
		}

		fmt.Printf("Successfully provisioned!\n")

		return nil
	},
}
