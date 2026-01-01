package scans_test

import (
	"context"
	"fmt"
	"io/fs"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/scans"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type repoErrOnUpdateUpload struct {
	scans.Repo
}

func (m repoErrOnUpdateUpload) UpdateUpload(ctx context.Context, upload *uploadmodel.Upload) error {
	// Simulate an error when updating the upload
	return fmt.Errorf("error updating upload with ID %s", upload.ID())
}

var _ scans.Repo = (*repoErrOnUpdateUpload)(nil)

func newUploadAndScansAPI(t *testing.T) (*uploadmodel.Upload, scans.API) {
	sourceID := id.New()
	spaceDID := testutil.RandomDID(t)

	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)
	uploads, err := repo.FindOrCreateUploads(t.Context(), spaceDID, []id.SourceID{sourceID})
	require.NoError(t, err)
	require.Len(t, uploads, 1)
	upload := uploads[0]

	scansAPI := scans.API{
		Repo: repo,
		SourceAccessor: func(ctx context.Context, sourceID id.SourceID) (fs.FS, error) {
			memFS := afero.NewMemMapFs()
			err := memFS.Chtimes(".", time.Now(), time.Now())
			require.NoError(t, err)
			return afero.NewIOFS(memFS), nil
		},
		WalkerFn: walker.WalkDir,
	}

	return upload, scansAPI
}

func TestExecuteScan(t *testing.T) {
	t.Run("with a successful scan", func(t *testing.T) {
		memFS := afero.NewMemMapFs()
		memFS.MkdirAll("dir1/dir2", 0755)
		afero.WriteFile(memFS, "a", []byte("file a"), 0644)
		afero.WriteFile(memFS, "dir1/b", []byte("file b"), 0644)
		afero.WriteFile(memFS, "dir1/c", []byte("file c"), 0644)
		afero.WriteFile(memFS, "dir1/dir2/d", []byte("file d"), 0644)

		// Set the last modified time for the files; Afero's in-memory FS doesn't do
		// that automatically on creation, we expect it to be present.
		for _, path := range []string{".", "a", "dir1", "dir1/b", "dir1/c", "dir1/dir2", "dir1/dir2/d"} {
			err := memFS.Chtimes(path, time.Now(), time.Now())
			require.NoError(t, err)
		}

		upload, scansAPI := newUploadAndScansAPI(t)
		scansAPI.SourceAccessor = func(ctx context.Context, sourceID id.SourceID) (fs.FS, error) {
			// Use the in-memory filesystem for testing
			return afero.NewIOFS(memFS), nil
		}

		var rootEntry model.FSEntry
		scansAPI.WalkerFn = func(fsys fs.FS, root string, visitor walker.FSVisitor) (model.FSEntry, error) {
			var err error
			rootEntry, err = walker.WalkDir(fsys, root, visitor)
			assert.NoError(t, err)
			return rootEntry, err
		}

		err := scansAPI.ExecuteScan(t.Context(), upload.ID(), func(entry model.FSEntry) error { return nil })
		require.NoError(t, err)

		// Reload the upload
		upload, err = scansAPI.Repo.GetUploadByID(t.Context(), upload.ID())
		require.NoError(t, err)

		require.Equal(t, rootEntry.ID(), upload.RootFSEntryID())
	})

	t.Run("with an error updating the scan", func(t *testing.T) {
		upload, scansAPI := newUploadAndScansAPI(t)
		scansAPI.Repo = repoErrOnUpdateUpload{Repo: scansAPI.Repo}

		err := scansAPI.ExecuteScan(t.Context(), upload.ID(), func(entry model.FSEntry) error { return nil })

		require.ErrorContains(t, err, "updating upload: error updating upload with ID")
	})

	t.Run("with an error accessing the source", func(t *testing.T) {
		upload, scansAPI := newUploadAndScansAPI(t)
		scansAPI.SourceAccessor = func(ctx context.Context, sourceID id.SourceID) (fs.FS, error) {
			return nil, fmt.Errorf("couldn't access source for source ID %s", sourceID)
		}

		err := scansAPI.ExecuteScan(t.Context(), upload.ID(), func(entry model.FSEntry) error {
			return nil
		})

		require.ErrorContains(t, err, "accessing source: couldn't access source for source ID")
	})

	t.Run("with an error walking the source", func(t *testing.T) {
		upload, scansAPI := newUploadAndScansAPI(t)
		scansAPI.WalkerFn = func(fsys fs.FS, root string, visitor walker.FSVisitor) (model.FSEntry, error) {
			return nil, fmt.Errorf("error walking the source at root %s", root)
		}

		err := scansAPI.ExecuteScan(t.Context(), upload.ID(), func(entry model.FSEntry) error {
			return nil
		})

		require.ErrorContains(t, err, "recursively creating directories: error walking the source at root")
	})

	t.Run("when the context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		upload, scansAPI := newUploadAndScansAPI(t)
		scansAPI.WalkerFn = func(fsys fs.FS, root string, visitor walker.FSVisitor) (model.FSEntry, error) {
			cancel() // Cancel the context to simulate a cancelation
			return nil, ctx.Err()
		}

		err := scansAPI.ExecuteScan(ctx, upload.ID(), func(entry model.FSEntry) error {
			return nil
		})

		require.ErrorIs(t, err, ctx.Err())
	})
}
