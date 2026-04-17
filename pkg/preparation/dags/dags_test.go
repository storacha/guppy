package dags_test

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/guppy/pkg/preparation/dags"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

// TestExecuteDagScansForUpload_TerminatesWhenBadChildBlocksParent is a
// regression test for a livelock in [dags.API.ExecuteDagScansForUpload]. An
// unreadable file leaves its parent directory perpetually "awaiting children":
// previously, the deferred directory scan was counted as progress on every
// pass, so the outer loop never returned and the upload pipeline appeared
// stuck (see stack.txt). With the fix, only completed scans count as progress,
// so a pass with nothing but bad files and their (blocked) ancestors exits
// with [types.BadFSEntriesError], which the upload worker can then hand to
// [uploads.API.handleBadFSEntries].
func TestExecuteDagScansForUpload_TerminatesWhenBadChildBlocksParent(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
	spaceDID := testutil.RandomDID(t)
	sourceID := id.New()
	uploadID := id.New()
	modTime := time.Now().UTC().Truncate(time.Second)

	parentDir, _, err := repo.FindOrCreateDirectory(t.Context(), "parent", modTime, fs.ModeDir|0755, []byte("parent-cksum"), sourceID, spaceDID)
	require.NoError(t, err)

	badFile, _, err := repo.FindOrCreateFile(t.Context(), "parent/bad.txt", modTime, 0644, 100, []byte("bad-cksum"), sourceID, spaceDID)
	require.NoError(t, err)

	require.NoError(t, repo.CreateDirectoryChildren(t.Context(), parentDir, []scanmodel.FSEntry{badFile}))

	_, err = repo.CreateDAGScan(t.Context(), badFile.ID(), false, uploadID, spaceDID)
	require.NoError(t, err)
	_, err = repo.CreateDAGScan(t.Context(), parentDir.ID(), true, uploadID, spaceDID)
	require.NoError(t, err)

	errFileUnavailable := errors.New("simulated unreadable file (e.g. broken symlink)")
	api := dags.API{
		Repo: repo,
		FileAccessor: func(ctx context.Context, fsEntryID id.FSEntryID) (fs.File, id.SourceID, string, error) {
			return nil, id.Nil, "", errFileUnavailable
		},
	}

	// A bounded deadline is the backstop: if the livelock regresses, the
	// function never returns and this test fails with context.DeadlineExceeded
	// instead of hanging the whole suite.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	nodeCalls := 0
	nodeCB := func(node dagmodel.Node, data []byte) error {
		nodeCalls++
		return nil
	}

	err = api.ExecuteDagScansForUpload(ctx, uploadID, nodeCB)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "ExecuteDagScansForUpload hung — the bad-child livelock has regressed")

	var badEntriesErr types.BadFSEntriesError
	require.ErrorAs(t, err, &badEntriesErr)
	require.Len(t, badEntriesErr.Errs(), 1)
	require.Equal(t, badFile.ID(), badEntriesErr.Errs()[0].FsEntryID())
	require.ErrorIs(t, badEntriesErr.Errs()[0], errFileUnavailable)
	require.Zero(t, nodeCalls, "no nodes should be produced when the only file is unreadable")
}
