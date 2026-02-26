package delegation

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
)

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List delegations created by this agent for others.",
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

		myDID := c.DID().String()
		count := 0

		for _, p := range proofs {
			issuer := p.Issuer().DID().String()
			audience := p.Audience().DID().String()

			if issuer == myDID && audience != myDID {
				if count == 0 {
					fmt.Println("Delegations created by this agent:")
				}
				count++
				
				fmt.Printf("CID:      %s\n", p.Link().String())
				fmt.Printf("Audience: %s\n", audience)
				fmt.Printf("Capabilities:\n")
				for _, cap := range p.Capabilities() {
					fmt.Printf("  - %s (with: %s)\n", cap.Can(), cap.With())
				}
				fmt.Println()
			}
		}

		if count == 0 {
			fmt.Println("No external delegations created by this agent found.")
		} else {
			fmt.Printf("Total: %d delegation(s)\n", count)
		}

		return nil
	},
}