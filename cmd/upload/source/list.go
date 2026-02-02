package source

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/preparation"
)

var ListCmd = &cobra.Command{
	Use:     "list <space>",
	Aliases: []string{"ls"},
	Short:   "List sources added to a space",
	Long:    `Lists the sources added to a space. The space can be specified by DID or by name.`,
	Args:    cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		repo, err := preparation.OpenRepo(ctx, cfg.Repo)
		if err != nil {
			return err
		}
		defer repo.Close()

		spaceArg := cmd.Flags().Arg(0)
		if spaceArg == "" {
			cmd.SilenceUsage = false
			return fmt.Errorf("space cannot be empty")
		}

		spaceDID, err := cmdutil.ResolveSpace(cmdutil.MustGetClient(cfg.Repo.Dir), spaceArg)
		if err != nil {
			return err
		}

		sourceIDs, err := repo.ListSpaceSources(ctx, spaceDID)
		if err != nil {
			return err
		}

		fmt.Printf("Sources for space %s:\n", spaceDID)
		for _, sourceID := range sourceIDs {
			source, err := repo.GetSourceByID(ctx, sourceID)
			if err != nil {
				return fmt.Errorf("failed to get source by ID %s: %w", sourceID, err)
			}
			if source.Name() != source.Path() {
				fmt.Printf("- %s: %s\n", source.Name(), source.Path())
			} else {
				fmt.Printf("- %s\n", source.Path())
			}
		}
		if len(sourceIDs) == 0 {
			fmt.Printf("No sources found for space %s. Add a source first with:\n\n$ %s %s <path>\n\n", spaceDID, AddCmd.CommandPath(), spaceDID)
		}

		return nil
	},
}
