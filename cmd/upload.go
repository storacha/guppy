//go:build wip

package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/cmd/internal/upload/repo"
	"github.com/storacha/guppy/cmd/internal/upload/ui"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
)

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
			cmd.SilenceUsage = true
			return fmt.Errorf("no uploads found for space %s", spaceDID)
		}

		return ui.RunUploadUI(ctx, repo, api, uploads)
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)
}

var uploadSourcesCmd = &cobra.Command{
	Use: "sources",
}

func init() {
	uploadCmd.AddCommand(uploadSourcesCmd)
}

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

		_, err = api.FindOrCreateSpace(ctx, spaceDID, spaceDID.String())
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
	return repo.Make(ctx, "guppy.db")
}
