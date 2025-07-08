//go:build wip

// Subcommands which are not yet ready for production use. Use `-tags=wip` to
// enable them for development and testing.

package main

import (
	"database/sql"
	_ "embed"

	"fmt"

	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
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

	api := preparation.NewAPI(repo)

	configuration, err := api.CreateConfiguration(cCtx.Context, "Large Upload Configuration")
	if err != nil {
		return fmt.Errorf("command failed to create configuration: %w", err)
	}

	source, err := api.CreateSource(cCtx.Context, "Large Upload Source", ".")
	if err != nil {
		return fmt.Errorf("command failed to create source: %w", err)
	}
	fmt.Println("Created source:", source.ID())

	err = repo.AddSourceToConfiguration(cCtx.Context, configuration.ID(), source.ID())
	if err != nil {
		return fmt.Errorf("command failed to add source to configuration: %w", err)
	}

	uploads, err := api.CreateUploads(cCtx.Context, configuration.ID())
	if err != nil {
		return fmt.Errorf("command failed to create uploads: %w", err)
	}

	for _, upload := range uploads {
		api.ExecuteUpload(cCtx.Context, upload)
	}

	return nil
}
