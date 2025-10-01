//go:build wip

package cmd

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/cmd/internal/upload/repo"
	"github.com/storacha/guppy/cmd/internal/upload/ui"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/preparation"
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

		repo, _, err := repo.Make(ctx, "guppy.db")
		if err != nil {
			return err
		}
		// Currently leads to a race condition with the app still running delayed DB
		// queries. We can deal with this issue later, since the process ends at the
		// end of this function anyhow.
		// defer closeDb()

		api := preparation.NewAPI(repo, cmdutil.MustGetClient(), spaceDID)
		uploads, err := api.FindOrCreateUploads(ctx, spaceDID)
		if err != nil {
			return fmt.Errorf("command failed to create uploads: %w", err)
		}

		return ui.RunUploadUI(ctx, repo, api, uploads)
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)
}

var uploadAddSourceCmd = &cobra.Command{
	Use:     "add-space <space> <path>",
	Aliases: []string{"up"},
	Short:   "WIP - Upload data to the service",
	Args:    cobra.ExactArgs(2),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		repo, closeDb, err := repo.Make(ctx, "guppy.db")
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

		api := preparation.NewAPI(repo, cmdutil.MustGetClient(), spaceDID)

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
	uploadCmd.AddCommand(uploadAddSourceCmd)
}
