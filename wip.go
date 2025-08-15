//go:build wip

// Subcommands which are not yet ready for production use. Use `-tags=wip` to
// enable them for development and testing.

package main

import (
	"database/sql"
	_ "embed"
	"time"

	"fmt"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
	"github.com/urfave/cli/v2"
	_ "modernc.org/sqlite"
)

func init() {
	commands = append(commands, &cli.Command{
		Name:   "large-upload",
		Usage:  "WIP - Upload a large amount of data to the service",
		Action: largeUpload,
	})

	commands = append(commands, &cli.Command{
		Name:   "resume",
		Usage:  "Resume failed uploads",
		Action: resumeUpload,
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

	//Show upload UUIDs
	fmt.Printf("\n📦 Created %d upload(s):\n", len(uploads))
	for _, upload := range uploads {
		fmt.Printf("  Upload ID: %s\n", upload.ID())
	}
	fmt.Println()

	for i, upload := range uploads {
		fmt.Printf("Starting upload %d/%d: %s\n", i+1, len(uploads), upload.ID())

		err := api.ExecuteUpload(cCtx.Context, upload)
		if err != nil {
			fmt.Printf("Upload %s failed: %v\n", upload.ID(), err)
			fmt.Printf("Resume with: guppy resume %s\n", upload.ID())
			continue
		}

		fmt.Printf("✅ Upload %s completed successfully\n", upload.ID())
	}

	//Show resume instructions
	fmt.Println("\n💡 To resume failed uploads later:")
	fmt.Println("  guppy resume           # List and resume uploads")
	fmt.Println("  guppy resume <id>      # Resume specific upload")

	return nil
}

func resumeUpload(cCtx *cli.Context) error {
	db, err := sql.Open("sqlite", "guppy.db")
	if err != nil {
		return fmt.Errorf("command failed to open SQLite database: %w", err)
	}
	defer db.Close()

	_, err = db.ExecContext(cCtx.Context, sqlrepo.Schema)
	if err != nil {
		return fmt.Errorf("command failed to execute schema: %w", err)
	}

	repo := sqlrepo.New(db)
	api := preparation.NewAPI(repo)

	// Get resumable uploads
	uploads, err := repo.GetResumableUploads(cCtx.Context)
	if err != nil {
		return fmt.Errorf("command failed to get resumable uploads: %w", err)
	}

	// Check for upload ID argument
	uploadIDStr := cCtx.Args().First()

	if uploadIDStr != "" {
		// Resume specific upload
		return resumeSpecificUpload(cCtx, api, uploadIDStr)
	}

	if len(uploads) == 0 {
		fmt.Println("No resumable uploads found.")
		return nil
	}

	fmt.Printf("Found %d resumable uploads:\n\n", len(uploads))
	for _, upload := range uploads {
		fmt.Printf("Upload ID: %s\n", upload.ID())
		fmt.Printf("  State: %s\n", upload.State())
		fmt.Printf("  Created: %s\n", upload.CreatedAt().Format("2025-01-02 15:04:05"))
		if upload.Error() != nil {
			fmt.Printf("  Error: %s\n", upload.Error())
		}
		fmt.Println()
	}

	fmt.Println("To resume:")
	fmt.Println("guppy resume <upload-id>")
	fmt.Printf("Example: guppy resume %s\n", uploads[0].ID())

	return nil
}

func resumeSpecificUpload(cCtx *cli.Context, api preparation.API, uploadIDStr string) error {
	uploadID, err := parseUploadID(uploadIDStr)
	if err != nil {
		return fmt.Errorf("invalid upload ID: %w", err)
	}

	upload, err := api.GetUploadByID(cCtx.Context, uploadID)
	if err != nil {
		return fmt.Errorf("command failed to get upload: %w", err)
	}

	if upload == nil {
		return fmt.Errorf("upload with ID %s not found", uploadID)
	}

	if !uploadsmodel.RestartableState(upload.State()) {
		return fmt.Errorf("upload %s is in state '%s' and cannot be resumed", uploadID, upload.State())
	}

	fmt.Printf("Resuming upload %s (state: %s)...\n", uploadID, upload.State())

	err = upload.Restart()
	if err != nil {
		return fmt.Errorf("command failed to restart upload: %w", err)
	}

	err = api.Uploads.Repo.UpdateUpload(cCtx.Context, upload)
	if err != nil {
		return fmt.Errorf("command failed to save restarted upload: %w", err)
	}

	err = api.ExecuteUpload(cCtx.Context, upload)
	if err != nil {
		fmt.Printf("Upload %s failed: %v\n", uploadID, err)
		fmt.Printf("Upload %s failed: %v\n", uploadID, err)
		fmt.Printf("Resume with: guppy resume %s\n", uploadID)
		return fmt.Errorf("command failed to execute upload: %w", err)
	}

	fmt.Printf("Successfully resumed upload %s\n", uploadID)
	return nil
}

func parseUploadID(s string) (id.UploadID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return id.UploadID{}, fmt.Errorf("invalid UUID format: %w", err)
	}
	return id.UploadID(u), nil
}
