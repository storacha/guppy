//go:build wip

// Subcommands which are not yet ready for production use. Use `-tags=wip` to
// enable them for development and testing.

package main

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"

	"fmt"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/configurations"
	dags "github.com/storacha/guppy/pkg/preparation/dag"
	"github.com/storacha/guppy/pkg/preparation/scans"
	scansmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/storacha/guppy/pkg/preparation/sources"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	"github.com/urfave/cli/v2"
	_ "modernc.org/sqlite"
)

func init() {
	commands = append(commands, &cli.Command{
		Name:   "large-upload",
		Usage:  "WIP - Upload a large amount of data to the service",
		Action: largeUpload,
	})
}

func largeUpload(cCtx *cli.Context) error {
	db, err := sql.Open("sqlite", "guppy.db")
	if err != nil {
		return fmt.Errorf("command failed to open in-memory SQLite database: %w", err)
	}
	defer db.Close()

	_, err = db.ExecContext(cCtx.Context, sqlrepo.Schema)
	if err != nil {
		return fmt.Errorf("command failed to execute schema: %w", err)
	}

	repo := sqlrepo.New(db)

	var uploadsAPI uploads.Uploads

	configurationsAPI := configurations.ConfigurationsAPI{
		Repo: repo,
	}

	sourcesAPI := sources.SourcesAPI{
		Repo: repo,
	}

	scansAPI := scans.Scans{
		Repo:               repo,
		UploadSourceLookup: uploadsAPI.GetSourceIDForUploadID,
		SourceAccessor:     sourcesAPI.AccessByID,
		WalkerFn:           walker.WalkDir,
	}

	dagsAPI := dags.DAGAPI{
		Repo:         repo,
		FileAccessor: scansAPI.OpenFileByID,
	}

	uploadsAPI = uploads.Uploads{
		Repo: repo,
		RunNewScan: func(ctx context.Context, uploadID types.UploadID, fsEntryCb func(id types.FSEntryID, isDirectory bool) error) (types.FSEntryID, error) {
			scan, err := repo.CreateScan(ctx, uploadID)
			if err != nil {
				return uuid.Nil, fmt.Errorf("command failed to create new scan: %w", err)
			}

			err = scansAPI.ExecuteScan(ctx, scan, func(entry scansmodel.FSEntry) error {
				fmt.Println("Processing entry:", entry.Path())
				// Process each file system entry here
				return nil
			})

			if err != nil {
				return uuid.Nil, fmt.Errorf("command failed to execute scan: %w", err)
			}

			if scan.State() != scansmodel.ScanStateCompleted {
				return uuid.Nil, fmt.Errorf("scan did not complete successfully, state: %s, error: %w", scan.State(), scan.Error())
			}

			if !scan.HasRootID() {
				return uuid.Nil, errors.New("completed scan did not have a root ID")
			}

			return scan.RootID(), nil
		},
		UploadDAGScanWorker: dagsAPI.UploadDAGScanWorker,
	}

	configuration, err := configurationsAPI.CreateConfiguration(cCtx.Context, "Large Upload Configuration")
	if err != nil {
		return fmt.Errorf("command failed to create configuration: %w", err)
	}

	_, err = sourcesAPI.CreateSource(cCtx.Context, "Large Upload Source", ".")
	if err != nil {
		return fmt.Errorf("command failed to create source: %w", err)
	}

	uploads, err := uploadsAPI.CreateUploads(cCtx.Context, configuration.ID())

	for _, upload := range uploads {
		uploadsAPI.ExecuteUpload(cCtx.Context, upload)
	}

	return nil
}
