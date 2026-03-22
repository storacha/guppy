package source

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/spaces/model"
)

var addFlags struct {
	shardSize string
	name      string
}

func init() {
	AddCmd.Flags().StringVar(&addFlags.shardSize, "shard-size", "", "Shard size for the space (e.g., 1024, 512B, 100K, 50M, 2G)")
	AddCmd.Flags().StringVar(&addFlags.name, "name", "", "Name (alias) for the source")
}

var AddCmd = &cobra.Command{
	Use:   "add <space> <path>",
	Short: "Add a source to a space",
	Long: wordwrap.WrapString(
		"Adds a source to a space. A source is currently a path on the local "+
			"filesystem, but this may be expanded in the future to include other "+
			"types of data sources. `upload` will upload data from all sources "+
			"associated with a space. Sources are associated with the space locally "+
			"for future local upload commands; no association is made remotely. "+
			"The space can be specified by DID or by name.",
		80),
	Args: cobra.ExactArgs(2),

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
			return errors.New("space cannot be empty")
		}

		path := cmd.Flags().Arg(1)
		if path == "" {
			cmd.SilenceUsage = false
			return errors.New("path cannot be empty")
		}

		path, err = filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("resolving absolute path: %w", err)
		}

		client := cmdutil.MustGetClient(cfg.Repo.Dir, cfg.Network)
		spaceDID, err := cmdutil.ResolveSpace(client, spaceArg)
		if err != nil {
			return err
		}

		api := preparation.NewAPI(repo, client)

		// Parse shard size if provided
		var spaceOptions []model.SpaceOption
		if addFlags.shardSize != "" {
			shardSize, err := cmdutil.ParseSize(addFlags.shardSize)
			if err != nil {
				return fmt.Errorf("parsing shard size: %w", err)
			}
			spaceOptions = append(spaceOptions, model.WithShardSize(shardSize))
		}

		name := path
		if addFlags.name != "" {
			name = addFlags.name
		}

		_, err = api.FindOrCreateSpace(ctx, spaceDID, spaceDID.String(), spaceOptions...)
		if err != nil {
			return fmt.Errorf("command failed to create space: %w", err)
		}

		source, err := api.CreateSource(ctx, name, path)
		if err != nil {
			return fmt.Errorf("command failed to create source: %w", err)
		}

		err = repo.AddSourceToSpace(ctx, spaceDID, source.ID())
		if err != nil {
			return fmt.Errorf("command failed to add source to space: %w", err)
		}

		return nil
	},
}
