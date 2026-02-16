package check

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/preparation"
	prepcheck "github.com/storacha/guppy/pkg/preparation/check"
	"github.com/storacha/guppy/pkg/preparation/dags/nodereader"
	"github.com/storacha/guppy/pkg/preparation/sources"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var checkFlags struct {
	repair bool
}

func init() {
	Cmd.Flags().BoolVar(&checkFlags.repair, "repair", false, "Automatically repair issues found (default: dry-run mode)")
}

var Cmd = &cobra.Command{
	Use:   "check [space] [source-path-or-name]",
	Short: "Check upload integrity and completeness",
	Long: wordwrap.WrapString(
		"Checks uploads for data inconsistencies and incompleteness that may have occurred "+
			"due to interrupted uploads or shutdowns. Performs the following checks:\n\n"+
			"1. Upload Scanned Check - Verifies FS and DAG scans completed\n"+
			"2. File System Integrity Check - Validates FS structure and DAG integrity\n"+
			"3. Node Integrity Check - Ensures all nodes have upload records\n"+
			"4. Node Completeness Check - Verifies all nodes are in shards\n"+
			"5. Shard Completeness Check - Verifies all shards are uploaded and indexed\n"+
			"6. Index Completeness Check - Verifies all indexes are uploaded\n\n"+
			"By default, runs in dry-run mode (reports issues without fixing). "+
			"Use --repair to automatically fix issues where possible.\n\n"+
			"If no arguments are provided, checks all uploads. "+
			"Specify a space to check only uploads for that space. "+
			"Specify both space and source to check a specific upload.",
		80),
	Args: cobra.MaximumNArgs(2),
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

		client := cmdutil.MustGetClient(cfg.Repo.Dir)

		// Determine which uploads to check
		var uploadsToCheck []uploadInfo
		switch len(args) {
		case 0:
			// Check all uploads
			uploadsToCheck, err = getAllUploads(ctx, repo)
			if err != nil {
				return fmt.Errorf("getting all uploads: %w", err)
			}
			if len(uploadsToCheck) == 0 {
				fmt.Println("No uploads found.")
				return nil
			}
			fmt.Printf("Checking %d upload(s)...\n", len(uploadsToCheck))

		case 1:
			// Check uploads for specified space
			spaceArg := args[0]
			spaceDID, err := cmdutil.ResolveSpace(client, spaceArg)
			if err != nil {
				return err
			}
			uploadsToCheck, err = getUploadsForSpace(ctx, repo, spaceDID)
			if err != nil {
				return fmt.Errorf("getting uploads for space %s: %w", spaceDID, err)
			}
			if len(uploadsToCheck) == 0 {
				fmt.Printf("No uploads found for space %s.\n", spaceDID)
				return nil
			}
			fmt.Printf("Checking %d upload(s) for space %s...\n", len(uploadsToCheck), spaceDID)

		case 2:
			// Check specific upload for space and source
			spaceArg := args[0]
			sourceArg := args[1]
			spaceDID, err := cmdutil.ResolveSpace(client, spaceArg)
			if err != nil {
				return err
			}
			upload, err := getUploadForSpaceAndSource(ctx, repo, spaceDID, sourceArg)
			if err != nil {
				return fmt.Errorf("getting upload for space %s and source %s: %w", spaceDID, sourceArg, err)
			}
			uploadsToCheck = []uploadInfo{upload}
			fmt.Printf("Checking upload for space %s, source %s...\n", spaceDID, sourceArg)
		}

		// Initialize checker with proper access to the original files on disk
		// Create sources API with default local FS accessor
		sourcesAPI := sources.API{
			Repo: repo,
			GetLocalFSForPathFn: func(path string) (fs.FS, error) {
				absPath, err := filepath.Abs(path)
				if err != nil {
					return nil, fmt.Errorf("resolving absolute path: %w", err)
				}
				info, err := os.Stat(absPath)
				if err != nil {
					return nil, fmt.Errorf("statting path: %w", err)
				}
				// os.DirFS() only accepts directories, so fall back to a sub-FS for files
				if info.IsDir() {
					return os.DirFS(absPath), nil
				}
				dir := filepath.Dir(absPath)
				base := filepath.Base(absPath)
				fsys, err := fs.Sub(os.DirFS(dir), base)
				if err != nil {
					return nil, fmt.Errorf("getting sub fs: %w", err)
				}
				return fsys, nil
			},
		}

		// Create node reader opener with the sources API
		nodeReaderOpener, err := nodereader.NewNodeReaderOpener(
			repo.LinksForCID,
			func(ctx context.Context, sourceID id.SourceID, path string) (fs.File, error) {
				source, err := repo.GetSourceByID(ctx, sourceID)
				if err != nil {
					return nil, fmt.Errorf("failed to get source by ID %s: %w", sourceID, err)
				}

				fsys, err := sourcesAPI.Access(source)
				if err != nil {
					return nil, fmt.Errorf("failed to access source %s: %w", sourceID, err)
				}

				f, err := fsys.Open(path)
				if err != nil {
					return nil, fmt.Errorf("failed to open file %s in source %s: %w", path, sourceID, err)
				}
				return f, nil
			},
			true, // checkReads - validate data integrity
		)
		if err != nil {
			return fmt.Errorf("creating node reader opener: %w", err)
		}

		// Create checker with initialized OpenNodeReader
		checker := &prepcheck.Checker{
			Repo:           repo,
			OpenNodeReader: nodeReaderOpener.OpenNodeReader,
		}

		var opts []prepcheck.Option
		if checkFlags.repair {
			opts = append(opts, prepcheck.WithRepairs())
		}

		// Run checks
		allPassed := true
		totalRepairs := 0
		for i, uploadInfo := range uploadsToCheck {
			if len(uploadsToCheck) > 1 {
				fmt.Printf("\n[%d/%d] ", i+1, len(uploadsToCheck))
			}
			fmt.Printf("Upload: %s\n", uploadInfo.displayName)

			report, err := checker.CheckUpload(ctx, uploadInfo.uploadID, opts...)
			if err != nil {
				return fmt.Errorf("checking upload %s: %w", uploadInfo.uploadID, err)
			}

			printReport(report)

			if !report.OverallPass {
				allPassed = false
			}
			totalRepairs += report.RepairsApplied
		}

		// Summary
		if len(uploadsToCheck) > 1 {
			fmt.Println("\n" + strings.Repeat("=", 60))
			fmt.Printf("Summary: Checked %d upload(s)\n", len(uploadsToCheck))
			if allPassed {
				fmt.Println("✓ All uploads passed all checks")
			} else {
				fmt.Println("✗ Some uploads have issues")
			}
			if totalRepairs > 0 {
				fmt.Printf("Applied %d repair(s)\n", totalRepairs)
			}
		}

		return nil
	},
}

// uploadInfo holds information about an upload for display purposes.
type uploadInfo struct {
	uploadID    id.UploadID
	displayName string // e.g., "space_name / source_path"
}

// getAllUploads retrieves all uploads across all spaces.
func getAllUploads(ctx context.Context, repo preparation.Repo) ([]uploadInfo, error) {
	// List all spaces
	spaces, err := repo.ListSpaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing spaces: %w", err)
	}

	var allUploads []uploadInfo
	for _, space := range spaces {
		uploads, err := getUploadsForSpace(ctx, repo, space.DID())
		if err != nil {
			return nil, fmt.Errorf("getting uploads for space %s: %w", space.DID(), err)
		}
		allUploads = append(allUploads, uploads...)
	}

	return allUploads, nil
}

// getUploadsForSpace retrieves all uploads for a specific space.
// Follows the same pattern as cmd/upload/root.go
func getUploadsForSpace(ctx context.Context, repo preparation.Repo, spaceDID did.DID) ([]uploadInfo, error) {
	// Get source IDs for this space
	sourceIDs, err := repo.ListSpaceSources(ctx, spaceDID)
	if err != nil {
		return nil, fmt.Errorf("listing space sources: %w", err)
	}

	if len(sourceIDs) == 0 {
		return []uploadInfo{}, nil
	}

	// Get or create uploads for these sources
	uploads, err := repo.FindOrCreateUploads(ctx, spaceDID, sourceIDs)
	if err != nil {
		return nil, fmt.Errorf("finding uploads: %w", err)
	}

	// Build uploadInfo with display names
	result := make([]uploadInfo, 0, len(uploads))
	for _, upload := range uploads {
		source, err := repo.GetSourceByID(ctx, upload.SourceID())
		if err != nil {
			return nil, fmt.Errorf("getting source %s: %w", upload.SourceID(), err)
		}

		// Create a friendly display name
		spaceName := spaceDID.String()
		if len(spaceName) > 20 {
			spaceName = spaceName[:20] + "..."
		}

		result = append(result, uploadInfo{
			uploadID:    upload.ID(),
			displayName: fmt.Sprintf("%s / %s", spaceName, source.Path()),
		})
	}

	return result, nil
}

// getUploadForSpaceAndSource finds a specific upload by matching source name or path.
// The sourceArg can be either a source name or a source path.
// Follows the same pattern as cmd/upload/root.go
func getUploadForSpaceAndSource(ctx context.Context, repo preparation.Repo, spaceDID did.DID, sourceArg string) (uploadInfo, error) {
	// Get source IDs for this space
	sourceIDs, err := repo.ListSpaceSources(ctx, spaceDID)
	if err != nil {
		return uploadInfo{}, fmt.Errorf("listing space sources: %w", err)
	}

	if len(sourceIDs) == 0 {
		return uploadInfo{}, fmt.Errorf("no sources found for space %s", spaceDID)
	}

	// Get uploads for these sources
	uploads, err := repo.FindOrCreateUploads(ctx, spaceDID, sourceIDs)
	if err != nil {
		return uploadInfo{}, fmt.Errorf("finding uploads: %w", err)
	}

	// Try to match by name or path (same logic as cmd/upload/root.go)
	cleanSourceArg := filepath.Clean(sourceArg)

	for _, upload := range uploads {
		source, err := repo.GetSourceByID(ctx, upload.SourceID())
		if err != nil {
			continue
		}

		cleanPath := filepath.Clean(source.Path())
		if cleanPath == cleanSourceArg || source.Name() == sourceArg {
			spaceName := spaceDID.String()
			if len(spaceName) > 20 {
				spaceName = spaceName[:20] + "..."
			}

			return uploadInfo{
				uploadID:    upload.ID(),
				displayName: fmt.Sprintf("%s / %s", spaceName, source.Path()),
			}, nil
		}
	}

	return uploadInfo{}, fmt.Errorf("source %q not found in space %s", sourceArg, spaceDID)
}

func printReport(report *prepcheck.CheckReport) {
	fmt.Println(strings.Repeat("-", 60))

	for _, checkResult := range report.Checks {
		status := "✓ PASS"
		if !checkResult.Passed {
			status = "✗ FAIL"
		}
		fmt.Printf("%s: %s\n", checkResult.Name, status)

		if len(checkResult.Issues) > 0 {
			fmt.Println("  Issues:")
			for _, issue := range checkResult.Issues {
				symbol := "⚠"
				if issue.Type == prepcheck.IssueTypeError {
					symbol = "✗"
				}
				fmt.Printf("    %s %s\n", symbol, issue.Description)
				if issue.Details != "" {
					fmt.Printf("      %s\n", issue.Details)
				}
			}
		}

		if len(checkResult.Repairs) > 0 {
			fmt.Println("  Repairs:")
			for _, repair := range checkResult.Repairs {
				status := "✓ applied"
				if !repair.Applied {
					status = fmt.Sprintf("✗ failed: %v", repair.Error)
				}
				fmt.Printf("    [%s] %s\n", status, repair.Description)
			}
		}
	}

	fmt.Println(strings.Repeat("-", 60))
	if report.OverallPass {
		fmt.Println("✓ All checks passed")
	} else {
		failedCount := 0
		for _, checkResult := range report.Checks {
			if !checkResult.Passed {
				failedCount++
			}
		}
		fmt.Printf("✗ %d check(s) failed\n", failedCount)
	}

	if report.RepairsApplied > 0 {
		fmt.Printf("Applied %d repair(s)\n", report.RepairsApplied)
	}
}
