package upload

import (
	"context"
	"fmt"

	"github.com/storacha/guppy/cmd/internal/upload/repo"
	"github.com/storacha/guppy/cmd/internal/upload/ui"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	_ "modernc.org/sqlite"
)

func AddSource(ctx context.Context, repo *sqlrepo.Repo, space string, path string) error {
	// TK: Move parsing a layer up.
	spaceDID := cmdutil.MustParseDID(space)

	if path == "" {
		return fmt.Errorf("command requires a path argument")
	}

	api := preparation.NewAPI(repo, cmdutil.MustGetClient(), spaceDID)

	_, err := api.FindOrCreateSpace(ctx, spaceDID, "Large Upload Space")
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
}

func Action(ctx context.Context, space string) error {
	spaceDID := cmdutil.MustParseDID(space)

	repo, _, err := repo.Make(ctx, "guppy.db")
	if err != nil {
		return err
	}
	// Currently leads to a race condition with the app still running delayed DB
	// queries. We can deal with this issue later, since the process ends at the
	// end of this function anyhow.
	// defer closeDb()

	api := preparation.NewAPI(repo, cmdutil.MustGetClient(), spaceDID)
	// TK: Move into RunUploadUI?
	uploads, err := api.FindOrCreateUploads(ctx, spaceDID)
	if err != nil {
		return fmt.Errorf("command failed to create uploads: %w", err)
	}

	return ui.RunUploadUI(ctx, repo, api, uploads)
}
