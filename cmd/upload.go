package cmd

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/cmd/internal/upload/ui"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var (
	uploadDbPath     string
	uploadProofPath  string
	uploadSourceName string
	uploadAll        bool
	retry            bool
)

var uploadCmd = &cobra.Command{
	Use:     "upload <space> [source-path-or-name...]",
	Aliases: []string{"up"},
	Short:   "Upload data to a Storacha space",
	Long: wordwrap.WrapString(
		"Uploads data to a Storacha space. By default, this will upload all sources "+
			"added to the space. You can optionally specify one or more source paths "+
			"or names to upload only those specific sources.",
		80),
	Args: cobra.MinimumNArgs(1),

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if uploadDbPath == "" {
			uploadDbPath = filepath.Join(guppyDirPath, "preparation.db")
		}
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		space := args[0]
		if space == "" {
			cmd.SilenceUsage = false
			return errors.New("space cannot be empty")
		}
		spaceDID, err := did.Parse(space)
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("parsing space DID: %w", err)
		}

		requestedSources := args[1:]

		// The command line was valid. Past here, errors do not mean the user needs
		// to see the usage.
		cmd.SilenceUsage = true

		repo, err := makeRepo(ctx)
		if err != nil {
			return err
		}
		// Currently leads to a race condition with the app still running delayed DB
		// queries. We can deal with this issue later, since the process ends at the
		// end of this function anyhow.
		// defer repo.Close()

		api := preparation.NewAPI(repo, cmdutil.MustGetClient(storePath))
		allUploads, err := api.FindOrCreateUploads(ctx, spaceDID)
		if err != nil {
			return fmt.Errorf("command failed to create uploads: %w", err)
		}

		if len(allUploads) == 0 {
			fmt.Printf("No sources found for space. Add a source first with:\n\n$ %s %s <path>\n\n", uploadSourcesAddCmd.CommandPath(), spaceDID)
			return cmdutil.NewHandledCliError(fmt.Errorf("no uploads found for space %s", spaceDID))
		}

		var uploadsToRun []*uploadsmodel.Upload

		if len(requestedSources) == 0 || uploadAll {
			uploadsToRun = allUploads
		} else {
			reqMap := make(map[string]bool)
			for _, p := range requestedSources {
				reqMap[filepath.Clean(p)] = true
				reqMap[p] = true
			}

			for _, u := range allUploads {
				src, err := repo.GetSourceByID(ctx, u.SourceID())
				if err != nil {
					return fmt.Errorf("getting source info: %w", err)
				}

				cleanPath := filepath.Clean(src.Path())
				name := src.Name()

				if reqMap[cleanPath] || reqMap[name] {
					uploadsToRun = append(uploadsToRun, u)
					delete(reqMap, cleanPath)
					delete(reqMap, name)
				}
			}

			if len(reqMap) > 0 {
				for p := range reqMap {
					return fmt.Errorf("source not found in space: %s (did you add it with 'guppy upload sources add'?)", p)
				}
			}
		}

		return ui.RunUploadUI(ctx, repo, api, uploadsToRun, retry)
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)

	uploadCmd.PersistentFlags().StringVar(
		&uploadDbPath,
		"db",
		"",
		"Path to the preparation database file (default: <guppyDir>/preparation.db)",
	)
	uploadCmd.Flags().StringVar(&uploadProofPath, "proof", "", "Path to a UCAN proof file")
	uploadCmd.Flags().BoolVar(&uploadAll, "all", false, "Upload all sources (even if arguments are provided)")
	uploadCmd.Flags().BoolVar(&retry, "retry", false, "Auto-retry failed uploads")
}

var uploadSourceCmd = &cobra.Command{
	Use: "source",
}

func init() {
	uploadCmd.AddCommand(uploadSourceCmd)
}

var uploadSourcesAddShardSize string

var uploadSourcesAddCmd = &cobra.Command{
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

		repo, err := makeRepo(ctx)
		if err != nil {
			return err
		}
		defer repo.Close()

		space := cmd.Flags().Arg(0)
		if space == "" {
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

		spaceDID, err := did.Parse(space)
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("parsing space DID: %w", err)
		}

		api := preparation.NewAPI(repo, cmdutil.MustGetClient(storePath))

		// Parse shard size if provided
		var spaceOptions []model.SpaceOption
		if uploadSourcesAddShardSize != "" {
			shardSize, err := cmdutil.ParseSize(uploadSourcesAddShardSize)
			if err != nil {
				return fmt.Errorf("parsing shard size: %w", err)
			}
			spaceOptions = append(spaceOptions, model.WithShardSize(shardSize))
		}

		name := path
		if uploadSourceName != "" {
			name = uploadSourceName
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

func init() {
	uploadSourceCmd.AddCommand(uploadSourcesAddCmd)
	uploadSourcesAddCmd.Flags().StringVar(&uploadSourcesAddShardSize, "shard-size", "", "Shard size for the space (e.g., 1024, 512B, 100K, 50M, 2G)")
	uploadSourcesAddCmd.Flags().StringVar(&uploadSourceName, "name", "", "Name (alias) for the source")
}

var uploadSourcesListCmd = &cobra.Command{
	Use:     "list <space>",
	Aliases: []string{"ls"},
	Short:   "List sources added to a space",
	Long:    `Lists the sources added to a space.`,
	Args:    cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		repo, err := makeRepo(ctx)
		if err != nil {
			return err
		}
		defer repo.Close()

		space := cmd.Flags().Arg(0)
		if space == "" {
			cmd.SilenceUsage = false
			return errors.New("space cannot be empty")
		}

		spaceDID, err := did.Parse(space)
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("parsing space DID: %w", err)
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
			fmt.Printf("No sources found for space %s. Add a source first with:\n\n$ %s %s <path>\n\n", spaceDID, uploadSourcesAddCmd.CommandPath(), spaceDID)
		}

		return nil
	},
}

func init() {
	uploadSourceCmd.AddCommand(uploadSourcesListCmd)
}

func makeRepo(ctx context.Context) (*sqlrepo.Repo, error) {
	return preparation.OpenRepo(ctx, uploadDbPath)
}
