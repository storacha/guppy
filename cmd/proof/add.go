package proof

import (
	"fmt"
	"os"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/guppy/internal/cmdutil"
)

var addCmd = &cobra.Command{
	Use:   "add <data-or-path>",
	Short: "Add a proof delegated to this agent.",
	Long: wordwrap.WrapString(
		"Parse or decode a proof from the given data or file path. A proof is a delegation for this agent.",
		80),
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		dlg, err := delegation.Parse(args[0])
		if err != nil {
			dlgBytes, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("reading delegation file: %w", err)
			}
			dlg, err = delegation.Extract(dlgBytes)
			if err != nil {
				return fmt.Errorf("extracting delegation: %w", err)
			}
		}

		c := cmdutil.MustGetClient(*StorePathP)

		if dlg.Audience().DID() != c.Issuer().DID() {
			return fmt.Errorf("delegation audience %q does not match agent DID %q", dlg.Audience().DID(), c.Issuer().DID())
		}

		err = c.AddProofs(dlg)
		if err != nil {
			return fmt.Errorf("adding proofs: %w", err)
		}

		return nil
	},
}

func init() {
	ProofCmd.AddCommand(addCmd)
}
