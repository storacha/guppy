package upload

import (
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/repo"
)

var sourcesCmd = &cobra.Command{
	Use: "sources",
}

func init() {
	sourcesCmd.AddCommand(sourcesAddCmd, sourcesListCmd)
	sourcesAddCmd.Flags().String("shard-size", "", "Shard size for the space (e.g., 1024, 512B, 100K, 50M, 2G)")
	cobra.CheckErr(viper.BindPFlag("upload.shard_size", sourcesAddCmd.Flags().Lookup("shard-size")))
}

var sourcesAddCmd = &cobra.Command{
	Use:   "add <space> <path>",
	Short: "Add a source to a space",
	Long: wordwrap.WrapString(
		"Adds a source to a space. A source is currently a path on the local "+
			"filesystem, but this may be expanded in the future to include other "+
			"types of data sources. `upload` will upload data from all sources "+
			"associated with a space. Sources are associated with the space locally "+
			"for future local upload commands; no association is made remotely.",
		80),
	Args: cobra.ExactArgs(2),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		space := cmd.Flags().Arg(0)
		if space == "" {
			return fmt.Errorf("space cannot be empty")
		}
		spaceDID, err := did.Parse(space)
		if err != nil {
			return fmt.Errorf("parsing space DID: %w", err)
		}

		path := cmd.Flags().Arg(1)
		if path == "" {
			return fmt.Errorf("path cannot be empty")
		}

		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		repo, err := repo.Open(cfg.Repo)
		if err != nil {
			return fmt.Errorf("opening repo: %w", err)
		}

		c, err := cmdutil.NewClient(cfg.Network, repo)
		if err != nil {
			return fmt.Errorf("creating client: %w", err)
		}

		sqlRepo, err := repo.OpenSQLRepo(ctx)
		if err != nil {
			return err
		}
		defer sqlRepo.Close()

		api := preparation.NewAPI(sqlRepo, c)

		// Parse shard size if provided
		var spaceOptions []model.SpaceOption
		if cfg.Upload.ShardSize != "" {
			shardSize, err := cmdutil.ParseSize(cfg.Upload.ShardSize)
			if err != nil {
				return fmt.Errorf("parsing shard size: %w", err)
			}
			spaceOptions = append(spaceOptions, model.WithShardSize(shardSize))
		}

		_, err = api.FindOrCreateSpace(ctx, spaceDID, spaceDID.String(), spaceOptions...)
		if err != nil {
			return fmt.Errorf("command failed to create space: %w", err)
		}

		source, err := api.CreateSource(ctx, path, path)
		if err != nil {
			return fmt.Errorf("command failed to create source: %w", err)
		}

		err = sqlRepo.AddSourceToSpace(ctx, spaceDID, source.ID())
		if err != nil {
			return fmt.Errorf("command failed to add source to space: %w", err)
		}

		return nil
	},
}

var sourcesListCmd = &cobra.Command{
	Use:     "list <space>",
	Aliases: []string{"ls"},
	Short:   "List sources added to a space",
	Long:    `Lists the sources added to a space.`,
	Args:    cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		space := cmd.Flags().Arg(0)
		if space == "" {
			return fmt.Errorf("space cannot be empty")
		}

		spaceDID, err := did.Parse(space)
		if err != nil {
			return fmt.Errorf("parsing space DID: %w", err)
		}

		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		repo, err := repo.Open(cfg.Repo)
		if err != nil {
			return fmt.Errorf("opening repo: %w", err)
		}

		sqlRepo, err := repo.OpenSQLRepo(ctx)
		if err != nil {
			return err
		}
		defer sqlRepo.Close()

		sourceIDs, err := sqlRepo.ListSpaceSources(ctx, spaceDID)
		if err != nil {
			return err
		}

		cmd.Printf("Sources for space %s:\n", spaceDID)
		for _, sourceID := range sourceIDs {
			source, err := sqlRepo.GetSourceByID(ctx, sourceID)
			if err != nil {
				return fmt.Errorf("failed to get source by ID %s: %w", sourceID, err)
			}
			cmd.Printf("- %s\n", source.Path())
		}
		if len(sourceIDs) == 0 {
			cmd.Printf("No sources found for space %s. Add a source first with:\n\n$ %s %s <path>\n\n", spaceDID,
				sourcesAddCmd.CommandPath(), spaceDID)
		}

		return nil
	},
}
