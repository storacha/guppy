package proof

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
)

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List proofs of capabilities delegated to this agent.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}

		c := cmdutil.MustGetClient(cfg.Repo.Dir)

		proofs, err := c.Proofs()
		if err != nil {
			return fmt.Errorf("getting proofs: %w", err)
		}

		if len(proofs) == 0 {
			fmt.Println("No proofs found in the local agent store.")
			return nil
		}

		fmt.Printf("Found %d proof(s):\n", len(proofs))

		for _, p := range proofs {
			fmt.Printf("CID:    %s\n", p.Link().String())
			fmt.Printf("Issuer: %s\n", p.Issuer().DID().String())
			fmt.Println("Capabilities:")

			for _, cap := range p.Capabilities() {
				fmt.Printf("  - %v (with: %v)\n", cap.Can(), cap.With())
			}
		}

		return nil
	},
}
