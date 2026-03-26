package delegation

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/spf13/cobra"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
)
 
var revokeCmd = &cobra.Command{
	Use:   "revoke <cid>",
	Short: "Revoke a delegation by CID.",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir)

		targetCid, err := cid.Parse(args[0])
		if err != nil {
			return fmt.Errorf("invalid CID format: %w", err)
		}

		fmt.Printf("Revoking delegation %s...\n", targetCid.String())

		if err := c.Revoke(cmd.Context(), targetCid); err != nil {
			return fmt.Errorf("failed to revoke delegation: %w", err)
		}

		fmt.Println("Success! Delegation has been revoked.")
		return nil
	},
}