//go:build wip

package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/cmd/internal/upload/repo"
	"github.com/storacha/guppy/cmd/internal/upload/ui"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
)

var uploadDbPath string

var uploadCmd = &cobra.Command{
	Use:     "upload <space>",
	Aliases: []string{"up"},
	Short:   "WIP - Upload data to the service",
	Args:    cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		space := cmd.Flags().Arg(0)
		if space == "" {
			return errors.New("Space cannot be empty")
		}
		spaceDID, err := did.Parse(space)
		if err != nil {
			return fmt.Errorf("parsing space DID: %w", err)
		}

		// The command line was valid. Past here, errors do not mean the user needs
		// to see the usage.
		cmd.SilenceUsage = true

		repo, _, err := makeRepo(ctx)
		if err != nil {
			return err
		}
		// Currently leads to a race condition with the app still running delayed DB
		// queries. We can deal with this issue later, since the process ends at the
		// end of this function anyhow.
		// defer closeDb()

		api := preparation.NewAPI(repo, cmdutil.MustGetClient())
		uploads, err := api.FindOrCreateUploads(ctx, spaceDID)
		if err != nil {
			return fmt.Errorf("command failed to create uploads: %w", err)
		}

		if len(uploads) == 0 {
			fmt.Printf("No sources found for space. Add a source first with:\n\n$ %s %s <path>\n\n", uploadSourcesAddCmd.CommandPath(), spaceDID)
			cmd.SilenceErrors = true
			return fmt.Errorf("no uploads found for space %s", spaceDID)
		}

		return ui.RunUploadUI(ctx, repo, api, uploads)
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)
	uploadCmd.PersistentFlags().StringVar(&uploadDbPath, "db", "", "Path to the preparation database file (default: ~/.storacha/preparation.db)")
}

var uploadSourcesCmd = &cobra.Command{
	Use: "sources",
}

func init() {
	uploadCmd.AddCommand(uploadSourcesCmd)
}

var uploadSourcesAddShardSize string

var uploadSourcesAddCmd = &cobra.Command{
	Use:   "add <space> <path>",
	Short: "Add a source to a space",
	Long: `Adds a source to a space. A source is currently a path on the local filesystem,
but this may be expanded in the future to include other types of data sources.
` + "`upload`" + ` will upload data from all sources associated with a space.`,
	Args: cobra.ExactArgs(2),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		repo, closeDb, err := makeRepo(ctx)
		if err != nil {
			return err
		}
		defer closeDb()

		space := cmd.Flags().Arg(0)
		if space == "" {
			return errors.New("Space cannot be empty")
		}

		path := cmd.Flags().Arg(1)
		if path == "" {
			return errors.New("Path cannot be empty")
		}

		spaceDID, err := did.Parse(space)
		if err != nil {
			return fmt.Errorf("parsing space DID: %w", err)
		}

		api := preparation.NewAPI(repo, cmdutil.MustGetClient())

		// Parse shard size if provided
		var spaceOptions []model.SpaceOption
		if uploadSourcesAddShardSize != "" {
			shardSize, err := cmdutil.ParseSize(uploadSourcesAddShardSize)
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

		err = repo.AddSourceToSpace(ctx, spaceDID, source.ID())
		if err != nil {
			return fmt.Errorf("command failed to add source to space: %w", err)
		}

		return nil
	},
}

func init() {
	uploadSourcesCmd.AddCommand(uploadSourcesAddCmd)
	uploadSourcesAddCmd.Flags().StringVar(&uploadSourcesAddShardSize, "shardSize", "", "Shard size for the space (e.g., 1024, 512B, 100K, 50M, 2G)")
}

var uploadSourcesListCmd = &cobra.Command{
	Use:     "list <space>",
	Aliases: []string{"ls"},
	Short:   "List sources added to a space",
	Long:    `Lists the sources added to a space.`,
	Args:    cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		repo, closeDb, err := makeRepo(ctx)
		if err != nil {
			return err
		}
		defer closeDb()

		space := cmd.Flags().Arg(0)
		if space == "" {
			return errors.New("Space cannot be empty")
		}

		spaceDID, err := did.Parse(space)
		if err != nil {
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
			fmt.Printf("- %s\n", source.Path())
		}
		if len(sourceIDs) == 0 {
			fmt.Printf("No sources found for space %s. Add a source first with:\n\n$ %s %s <path>\n\n", spaceDID, uploadSourcesAddCmd.CommandPath(), spaceDID)
		}

		return nil
	},
}

func init() {
	uploadSourcesCmd.AddCommand(uploadSourcesListCmd)
}

func makeRepo(ctx context.Context) (*sqlrepo.Repo, func() error, error) {
	dbPath := uploadDbPath
	if dbPath == "" {
		// Use default path: ~/.storacha/preparation.db
		homedir, err := os.UserHomeDir()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get user home directory: %w", err)
		}

		storachaDir := filepath.Join(homedir, ".storacha")

		// Create the directory if it doesn't exist
		if err := os.MkdirAll(storachaDir, 0700); err != nil {
			return nil, nil, fmt.Errorf("failed to create directory %s: %w", storachaDir, err)
		}

		dbPath = filepath.Join(storachaDir, "preparation.db")
	}

	return repo.Make(ctx, dbPath)
}
