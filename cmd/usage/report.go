package usage

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
)

func newReportCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "report [space-did]",
		Short: "Show storage usage",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			cfg, err := config.Load[config.Config]()
			if err != nil {
				return err
			}

			c := cmdutil.MustGetClient(cfg.Repo.Dir)

			var spaceDID did.DID

			if len(args) > 0 {
				parsedDID, err := did.Parse(args[0])
				if err != nil {
					return fmt.Errorf("invalid space DID: %w", err)
				}
				spaceDID = parsedDID
			} else {
				spaces, err := c.Spaces()
				if err != nil {
					return fmt.Errorf("listing spaces: %w", err)
				}
				if len(spaces) == 0 {
					return fmt.Errorf("no spaces found")
				}
				spaceDID = spaces[0].DID()
			}

			to := time.Now().UTC()
			from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

			fmt.Printf("Fetching usage for space %s...\n", spaceDID)

			reports, err := c.UsageReport(ctx, spaceDID, from, to)
			if err != nil {
				return err
			}

			fmt.Println("---------------------------------------------------")
			if len(reports) == 0 {
				fmt.Println("No usage data found for this period.")
			} else {
				for providerDID, report := range reports {
					fmt.Printf("Provider:    %s\n", providerDID)
					fmt.Printf("Total Usage: %s\n", humanize.IBytes(report.Size))
					fmt.Println("---------------------------------------------------")
				}
			}

			return nil
		},
	}
}
