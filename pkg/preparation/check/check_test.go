package check

import (
	"io/fs"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	stestutil "github.com/storacha/go-libstoracha/testutil"
	"github.com/stretchr/testify/require"

	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/internal/testutil"
	scansmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// TestCheckUpload tests the full CheckUpload orchestration (integration tests)
func TestCheckUpload(t *testing.T) {
	tests := []struct {
		name               string
		setupUpload        func(*testing.T, *sqlrepo.Repo) id.UploadID
		withRepairs        bool
		wantOverallPass    bool
		wantRepairsApplied int
		checkResults       func(*testing.T, *CheckReport)
	}{
		{
			name: "clean upload - upload scanned check passes",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithFileAndDAGScan("test.file", true).
					Build()

				return upload.ID()
			},
			wantOverallPass:    false, // Will fail because nodes aren't actually in shards
			wantRepairsApplied: 0,
			checkResults: func(t *testing.T, report *CheckReport) {
				require.Len(t, report.Checks, 6, "should run all 6 checks")
				// At least the first two checks should pass
				require.True(t, report.Checks[0].Passed, "UploadScanned should pass")
				require.True(t, report.Checks[1].Passed, "FileSystemIntegrity should pass")
			},
		},
		{
			name: "missing DAGScans - repairs create them",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).WithRootFSEntry().Build()
				// Create file without DAGScan
				// file, _, err := repo.FindOrCreateFile(t.Context(), "file1.txt", time.Now(), 0644, 100, []byte("checksum"), source.ID(), upload.SpaceDID())
				// require.NoError(t, err)
				// err = upload.SetRootFSEntryID(file.ID())
				// require.NoError(t, err)
				// err = repo.UpdateUpload(t.Context(), upload)
				// require.NoError(t, err)
				return upload.ID()
			},
			withRepairs:        true,
			wantOverallPass:    false,
			wantRepairsApplied: 1,
			checkResults: func(t *testing.T, report *CheckReport) {
				// Find FileSystemIntegrity check
				var fsCheck *CheckResult
				for i := range report.Checks {
					if report.Checks[i].Name == "File System Integrity Check" {
						fsCheck = &report.Checks[i]
						break
					}
				}
				require.NotNil(t, fsCheck, "FileSystemIntegrity check should exist")
				require.False(t, fsCheck.Passed)
				require.NotEmpty(t, fsCheck.Issues)
				require.Len(t, fsCheck.Repairs, 1)
				require.True(t, fsCheck.Repairs[0].Applied)
			},
		},
		{
			name: "invalid root_fs_entry_id and root_cid - repairs clear them",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithInvalidRootFSEntry().
					WithInvalidRootCID().
					Build()
				return upload.ID()
			},
			withRepairs:        true,
			wantOverallPass:    false,
			wantRepairsApplied: 2,
			checkResults: func(t *testing.T, report *CheckReport) {
				// UploadScanned check should fail and repair
				uploadScannedCheck := report.Checks[0]
				require.Equal(t, "Upload Scanned Check", uploadScannedCheck.Name)
				require.False(t, uploadScannedCheck.Passed)
				require.Len(t, uploadScannedCheck.Repairs, 2)
				require.True(t, uploadScannedCheck.Repairs[0].Applied)
				require.True(t, uploadScannedCheck.Repairs[1].Applied)
			},
		},
		{
			name: "node integrity - all nodes have uploads",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithFileAndDAGScan("test.file", true).
					Build()

				return upload.ID()
			},
			withRepairs:        true,
			wantOverallPass:    false, // Will fail at NodeCompleteness (not in shards)
			wantRepairsApplied: 0,
			checkResults: func(t *testing.T, report *CheckReport) {
				// NodeIntegrity should pass since FindOrCreateRawNode creates the record
				var nodeIntegrityCheck *CheckResult
				for i := range report.Checks {
					if report.Checks[i].Name == "Node Integrity Check" {
						nodeIntegrityCheck = &report.Checks[i]
						break
					}
				}
				require.NotNil(t, nodeIntegrityCheck)
				require.True(t, nodeIntegrityCheck.Passed)
			},
		},
		{
			name: "dry-run mode - issues detected but no repairs",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithRootFSEntry().
					WithInvalidRootCID().
					Build()
				return upload.ID()
			},
			withRepairs:        false, // dry-run
			wantOverallPass:    false,
			wantRepairsApplied: 0,
			checkResults: func(t *testing.T, report *CheckReport) {
				uploadScannedCheck := report.Checks[0]
				require.False(t, uploadScannedCheck.Passed)
				require.NotEmpty(t, uploadScannedCheck.Issues)
				require.Empty(t, uploadScannedCheck.Repairs, "no repairs in dry-run mode")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testdb.CreateTestDB(t)
			repo := stestutil.Must(sqlrepo.New(db))(t)

			uploadID := tt.setupUpload(t, repo)

			checker := &Checker{Repo: repo}
			var opts []Option
			if tt.withRepairs {
				opts = append(opts, WithRepairs())
			}

			report, err := checker.CheckUpload(t.Context(), uploadID, opts...)
			require.NoError(t, err)

			require.Equal(t, uploadID, report.UploadID)
			require.Equal(t, tt.wantOverallPass, report.OverallPass, "OverallPass mismatch")
			require.Equal(t, tt.wantRepairsApplied, report.RepairsApplied, "RepairsApplied mismatch")

			if tt.checkResults != nil {
				tt.checkResults(t, report)
			}
		})
	}
}

// TestCheckUploadScanned tests the checkUploadScanned function
func TestCheckUploadScanned(t *testing.T) {
	tests := []struct {
		name            string
		setupUpload     func(*testing.T, *sqlrepo.Repo) id.UploadID
		withRepairs     bool
		wantPassed      bool
		wantIssueCount  int
		wantRepairCount int
		validateIssues  func(*testing.T, []Issue)
		validateRepairs func(*testing.T, []Repair)
		validateDB      func(*testing.T, *sqlrepo.Repo, id.UploadID)
	}{
		{
			name: "both root_fs_entry_id and root_cid valid - passes",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithFileAndDAGScan("test.file", true).
					Build()
				return upload.ID()
			},
			wantPassed:      true,
			wantIssueCount:  0,
			wantRepairCount: 0,
		},
		{
			name: "both NULL - errors",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).Build()
				return upload.ID()
			},
			wantPassed:      false,
			wantIssueCount:  2, // Both root_fs_entry_id and root_cid are NULL
			wantRepairCount: 0,
			validateIssues: func(t *testing.T, issues []Issue) {
				require.Equal(t, IssueTypeError, issues[0].Type)
				require.Contains(t, issues[0].Description, "Filesystem scan")
				require.Equal(t, IssueTypeError, issues[1].Type)
				require.Contains(t, issues[1].Description, "DAG scan")
			},
		},
		{
			name: "root_fs_entry_id points to non-existent - repair",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithInvalidRootFSEntry().
					WithRootCID().
					Build()
				return upload.ID()
			},
			withRepairs:     true,
			wantPassed:      false,
			wantIssueCount:  1,
			wantRepairCount: 1,
			validateRepairs: func(t *testing.T, repairs []Repair) {
				require.True(t, repairs[0].Applied)
				require.Contains(t, repairs[0].Description, "Set root_fs_entry_id to NULL")
			},
			validateDB: func(t *testing.T, repo *sqlrepo.Repo, uploadID id.UploadID) {
				upload, err := repo.GetUploadByID(t.Context(), uploadID)
				require.NoError(t, err)
				require.Equal(t, id.Nil, upload.RootFSEntryID())
			},
		},
		{
			name: "root_cid points to non-existent - repair",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithRootFSEntry().
					WithInvalidRootCID().
					Build()
				return upload.ID()
			},
			withRepairs:     true,
			wantPassed:      false,
			wantIssueCount:  1,
			wantRepairCount: 1,
			validateRepairs: func(t *testing.T, repairs []Repair) {
				require.True(t, repairs[0].Applied)
				require.Contains(t, repairs[0].Description, "Set root_cid to NULL")
			},
			validateDB: func(t *testing.T, repo *sqlrepo.Repo, uploadID id.UploadID) {
				upload, err := repo.GetUploadByID(t.Context(), uploadID)
				require.NoError(t, err)
				require.Equal(t, cid.Undef, upload.RootCID())
			},
		},
		{
			name: "both invalid - multiple repairs",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithInvalidRootFSEntry().
					WithInvalidRootCID().
					Build()
				return upload.ID()
			},
			withRepairs:     true,
			wantPassed:      false,
			wantIssueCount:  2,
			wantRepairCount: 2,
			validateRepairs: func(t *testing.T, repairs []Repair) {
				require.True(t, repairs[0].Applied)
				require.True(t, repairs[1].Applied)
			},
		},
		{
			name: "repair in dry-run mode - not applied",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithRootFSEntry().
					WithInvalidRootCID().
					Build()
				return upload.ID()
			},
			withRepairs:     false, // dry-run
			wantPassed:      false,
			wantIssueCount:  1,
			wantRepairCount: 0, // No repairs in dry-run
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testdb.CreateTestDB(t)
			repo := stestutil.Must(sqlrepo.New(db))(t)

			uploadID := tt.setupUpload(t, repo)

			checker := &Checker{Repo: repo}
			var opts []Option
			if tt.withRepairs {
				opts = append(opts, WithRepairs())
			}

			report, err := checker.CheckUpload(t.Context(), uploadID, opts...)
			require.NoError(t, err)

			// Get the Upload Scanned Check result (first check)
			require.NotEmpty(t, report.Checks)
			result := report.Checks[0]
			require.Equal(t, "Upload Scanned Check", result.Name)

			require.Equal(t, tt.wantPassed, result.Passed)
			require.Len(t, result.Issues, tt.wantIssueCount)
			require.Len(t, result.Repairs, tt.wantRepairCount)

			if tt.validateIssues != nil {
				tt.validateIssues(t, result.Issues)
			}
			if tt.validateRepairs != nil {
				tt.validateRepairs(t, result.Repairs)
			}
			if tt.validateDB != nil {
				tt.validateDB(t, repo, uploadID)
			}
		})
	}
}

// TestCheckFileSystemIntegrity tests filesystem and DAG validation
func TestCheckFileSystemIntegrity(t *testing.T) {
	tests := []struct {
		name            string
		setupUpload     func(*testing.T, *sqlrepo.Repo) id.UploadID
		withRepairs     bool
		wantPassed      bool
		wantIssueCount  int
		wantRepairCount int
	}{
		{
			name: "single file with valid DAGScan - passes",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithFileAndDAGScan("test.file", true).
					Build()

				return upload.ID()
			},
			wantPassed:      true,
			wantIssueCount:  0,
			wantRepairCount: 0,
		},
		{
			name: "file missing DAGScan - repair creates it",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithRootFSEntry().
					Build()

				return upload.ID()
			},
			withRepairs:     true,
			wantPassed:      false,
			wantIssueCount:  1,
			wantRepairCount: 1,
		},
		{
			name: "file with invalid DAG - missing node - repair clears CID",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, source := testutil.NewUploadBuilder(t, repo).Build()
				// Create file with DAGScan pointing to non-existent node
				file, _, err := repo.FindOrCreateFile(t.Context(), "file1.txt", time.Now(), 0644, 100, []byte("checksum"), source.ID(), upload.SpaceDID())
				require.NoError(t, err)

				dagScan, err := repo.CreateDAGScan(t.Context(), file.ID(), false, upload.ID(), upload.SpaceDID())
				require.NoError(t, err)

				// Complete with non-existent CID
				fakeCID := stestutil.RandomCID(t).(cidlink.Link).Cid
				err = dagScan.Complete(fakeCID)
				require.NoError(t, err)
				err = repo.UpdateDAGScan(t.Context(), dagScan)
				require.NoError(t, err)

				err = upload.SetRootFSEntryID(file.ID())
				require.NoError(t, err)
				err = repo.UpdateUpload(t.Context(), upload)
				require.NoError(t, err)

				return upload.ID()
			},
			withRepairs:     true,
			wantPassed:      false,
			wantIssueCount:  1,
			wantRepairCount: 2, // Both DAGScan CID and upload.root_cid should be cleared
		},
		{
			name: "dry-run mode - detects issues but no repairs",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, _ := testutil.NewUploadBuilder(t, repo).
					WithRootFSEntry().
					Build()

				return upload.ID()
			},
			withRepairs:     false, // dry-run
			wantPassed:      false,
			wantIssueCount:  1,
			wantRepairCount: 0,
		},
		{
			name: "directory with child having missing DAGScan - repair creates it",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, source := testutil.NewUploadBuilder(t, repo).Build()

				// Create directory
				dir, _, err := repo.FindOrCreateDirectory(t.Context(), "mydir", time.Now(), fs.ModeDir|0755, []byte("dirchecksum"), source.ID(), upload.SpaceDID())
				require.NoError(t, err)

				// Create child file WITHOUT DAGScan
				file, _, err := repo.FindOrCreateFile(t.Context(), "mydir/file.txt", time.Now(), 0644, 100, []byte("checksum"), source.ID(), upload.SpaceDID())
				require.NoError(t, err)
				err = repo.CreateDirectoryChildren(t.Context(), dir, []scansmodel.FSEntry{file})
				require.NoError(t, err)

				// Create directory DAGScan (but not for the child - will remain incomplete)
				_, err = repo.CreateDAGScan(t.Context(), dir.ID(), true, upload.ID(), upload.SpaceDID())
				require.NoError(t, err)

				err = upload.SetRootFSEntryID(dir.ID())
				require.NoError(t, err)
				err = repo.UpdateUpload(t.Context(), upload)
				require.NoError(t, err)

				return upload.ID()
			},
			withRepairs:     true,
			wantPassed:      false,
			wantIssueCount:  1,
			wantRepairCount: 1, // Create DAGScan for child file
		},
		{
			name: "directory with child with invalid DAG - marks directory invalid",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, source := testutil.NewUploadBuilder(t, repo).Build()

				// Create directory
				dir, _, err := repo.FindOrCreateDirectory(t.Context(), "mydir", time.Now(), fs.ModeDir|0755, []byte("dirchecksum"), source.ID(), upload.SpaceDID())
				require.NoError(t, err)

				// Create child file with invalid DAG (pointing to non-existent node)
				file, _, err := repo.FindOrCreateFile(t.Context(), "mydir/file.txt", time.Now(), 0644, 100, []byte("checksum"), source.ID(), upload.SpaceDID())
				require.NoError(t, err)
				err = repo.CreateDirectoryChildren(t.Context(), dir, []scansmodel.FSEntry{file})
				require.NoError(t, err)

				fileDagScan, err := repo.CreateDAGScan(t.Context(), file.ID(), false, upload.ID(), upload.SpaceDID())
				require.NoError(t, err)
				fakeCID := stestutil.RandomCID(t).(cidlink.Link).Cid
				err = fileDagScan.Complete(fakeCID)
				require.NoError(t, err)
				err = repo.UpdateDAGScan(t.Context(), fileDagScan)
				require.NoError(t, err)

				// Create directory DAGScan (but not for the child - will remain incomplete)
				_, err = repo.CreateDAGScan(t.Context(), dir.ID(), true, upload.ID(), upload.SpaceDID())
				require.NoError(t, err)

				err = upload.SetRootFSEntryID(dir.ID())
				require.NoError(t, err)
				err = repo.UpdateUpload(t.Context(), upload)
				require.NoError(t, err)

				return upload.ID()
			},
			withRepairs:     true,
			wantPassed:      false,
			wantIssueCount:  1,
			wantRepairCount: 1, // Clear child DAGScan CID (child error detected, but directory scan not yet complete so not cleared)
		},
		{
			name: "nested directories - error in grandchild propagates up",
			setupUpload: func(t *testing.T, repo *sqlrepo.Repo) id.UploadID {
				upload, source := testutil.NewUploadBuilder(t, repo).Build()

				// Create root directory
				rootDir, _, err := repo.FindOrCreateDirectory(t.Context(), "root", time.Now(), fs.ModeDir|0755, []byte("rootchecksum"), source.ID(), upload.SpaceDID())
				require.NoError(t, err)

				// Create subdirectory
				subDir, _, err := repo.FindOrCreateDirectory(t.Context(), "root/subdir", time.Now(), fs.ModeDir|0755, []byte("subchecksum"), source.ID(), upload.SpaceDID())
				require.NoError(t, err)
				err = repo.CreateDirectoryChildren(t.Context(), rootDir, []scansmodel.FSEntry{subDir})
				require.NoError(t, err)

				// Create grandchild file with invalid DAG
				grandchildFile, _, err := repo.FindOrCreateFile(t.Context(), "root/subdir/file.txt", time.Now(), 0644, 100, []byte("checksum"), source.ID(), upload.SpaceDID())
				require.NoError(t, err)
				err = repo.CreateDirectoryChildren(t.Context(), subDir, []scansmodel.FSEntry{grandchildFile})
				require.NoError(t, err)

				// Create DAGScan for grandchild with invalid CID
				grandchildDagScan, err := repo.CreateDAGScan(t.Context(), grandchildFile.ID(), false, upload.ID(), upload.SpaceDID())
				require.NoError(t, err)
				fakeCID := stestutil.RandomCID(t).(cidlink.Link).Cid
				err = grandchildDagScan.Complete(fakeCID)
				require.NoError(t, err)
				err = repo.UpdateDAGScan(t.Context(), grandchildDagScan)
				require.NoError(t, err)

				// Create DAGScans for subdirectory (will be marked invalid due to child error)
				_, err = repo.CreateDAGScan(t.Context(), subDir.ID(), true, upload.ID(), upload.SpaceDID())
				require.NoError(t, err)

				// Create DAGScan for root directory (will be marked invalid due to child error)
				_, err = repo.CreateDAGScan(t.Context(), rootDir.ID(), true, upload.ID(), upload.SpaceDID())
				require.NoError(t, err)

				err = upload.SetRootFSEntryID(rootDir.ID())
				require.NoError(t, err)
				err = repo.UpdateUpload(t.Context(), upload)
				require.NoError(t, err)

				return upload.ID()
			},
			withRepairs:     true,
			wantPassed:      false,
			wantIssueCount:  1,
			wantRepairCount: 1, // Clear grandchild DAGScan CID (error detected, but directory scans incomplete)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testdb.CreateTestDB(t)
			repo := stestutil.Must(sqlrepo.New(db))(t)

			uploadID := tt.setupUpload(t, repo)

			checker := &Checker{Repo: repo}
			cfg := &config{applyRepairs: tt.withRepairs}
			result, err := checker.checkFileSystemIntegrity(t.Context(), uploadID, cfg)
			require.NoError(t, err)

			require.Equal(t, "File System Integrity Check", result.Name)
			require.Equal(t, tt.wantPassed, result.Passed, "FileSystemIntegrity check passed mismatch")
			require.Len(t, result.Issues, tt.wantIssueCount, "Issue count mismatch")
			require.Len(t, result.Repairs, tt.wantRepairCount, "Repair count mismatch")
		})
	}
}

func TestCheckNodeIntegrity(t *testing.T) {
	db := testdb.CreateTestDB(t)
	repo := stestutil.Must(sqlrepo.New(db))(t)

	upload, _ := testutil.NewUploadBuilder(t, repo).
		WithRootFSEntry().
		WithRootCID().
		Build()

	checker := &Checker{Repo: repo}
	cfg := &config{applyRepairs: true}
	result, err := checker.checkNodeIntegrity(t.Context(), upload.ID(), cfg)
	require.NoError(t, err)

	require.Equal(t, "Node Integrity Check", result.Name)
	require.True(t, result.Passed)
	require.Empty(t, result.Issues)
}

func TestCheckNodeCompleteness(t *testing.T) {
	db := testdb.CreateTestDB(t)
	repo := stestutil.Must(sqlrepo.New(db))(t)

	upload, _ := testutil.NewUploadBuilder(t, repo).
		WithRootFSEntry().
		WithRootCID().
		Build()

	checker := &Checker{Repo: repo}
	cfg := &config{applyRepairs: true}
	result, err := checker.checkNodeCompleteness(t.Context(), upload.ID(), cfg)
	require.NoError(t, err)

	require.Equal(t, "Node Completeness Check", result.Name)
	require.False(t, result.Passed, "should fail because nodes aren't in shards")
	require.NotEmpty(t, result.Issues)
}

func TestShardCompleteness(t *testing.T) {
	db := testdb.CreateTestDB(t)
	repo := stestutil.Must(sqlrepo.New(db))(t)

	upload, _ := testutil.NewUploadBuilder(t, repo).
		WithRootFSEntry().
		WithRootCID().
		WithIncompleteShards(2).
		Build()

	checker := &Checker{Repo: repo}
	cfg := &config{applyRepairs: true}
	result, err := checker.checkShardCompleteness(t.Context(), upload.ID(), cfg)
	require.NoError(t, err)

	require.Equal(t, "Shard Completeness Check", result.Name)
	require.False(t, result.Passed, "should fail with incomplete shards")
	require.NotEmpty(t, result.Issues)
}

func TestIndexCompleteness(t *testing.T) {
	db := testdb.CreateTestDB(t)
	repo := stestutil.Must(sqlrepo.New(db))(t)

	upload, _ := testutil.NewUploadBuilder(t, repo).
		WithRootFSEntry().
		WithRootCID().
		WithIncompleteIndexes(1).
		Build()

	checker := &Checker{Repo: repo}
	cfg := &config{applyRepairs: true}
	result, err := checker.checkIndexCompleteness(t.Context(), upload.ID(), cfg)
	require.NoError(t, err)

	require.Equal(t, "Index Completeness Check", result.Name)
	require.False(t, result.Passed, "should fail with incomplete indexes")
	require.NotEmpty(t, result.Issues)
}
