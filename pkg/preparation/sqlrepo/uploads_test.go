package sqlrepo_test

import (
	"testing"

	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/stretchr/testify/require"
)

func TestCreateUploads(t *testing.T) {
	repo := sqlrepo.New(testdb.CreateTestDB(t))
	did := testutil.RandomDID(t)
	space, err := repo.FindOrCreateSpace(t.Context(), did, "space name")
	require.NoError(t, err)
	source1, err := repo.CreateSource(t.Context(), "source1 name", "source1/path")
	require.NoError(t, err)
	source2, err := repo.CreateSource(t.Context(), "source2 name", "source2/path")
	require.NoError(t, err)
	sourceIDs := []id.SourceID{source1.ID(), source2.ID()}

	uploads, err := repo.FindOrCreateUploads(t.Context(), space.DID(), sourceIDs)
	require.NoError(t, err)

	for i, upload := range uploads {
		readUpload, err := repo.GetUploadByID(t.Context(), upload.ID())
		require.NoError(t, err)
		require.Equal(t, upload, readUpload)

		require.Equal(t, space.DID(), upload.SpaceDID())
		require.Equal(t, sourceIDs[i], upload.SourceID())
		require.NotEmpty(t, upload.CreatedAt())
		require.Empty(t, upload.RootFSEntryID())
	}

	///

	source3, err := repo.CreateSource(t.Context(), "source3 name", "source3/path")
	require.NoError(t, err)
	sourceIDs = []id.SourceID{source1.ID(), source3.ID(), source2.ID()}

	uploadsAgain, err := repo.FindOrCreateUploads(t.Context(), space.DID(), sourceIDs)
	require.NoError(t, err)

	require.Equal(t, uploads[0], uploadsAgain[0])
	require.Equal(t, uploads[1], uploadsAgain[2])

	for i, upload := range uploadsAgain {
		readUpload, err := repo.GetUploadByID(t.Context(), upload.ID())
		require.NoError(t, err)
		require.Equal(t, upload, readUpload)

		require.Equal(t, space.DID(), upload.SpaceDID())
		require.Equal(t, sourceIDs[i], upload.SourceID())
		require.NotEmpty(t, upload.CreatedAt())
		require.Empty(t, upload.RootFSEntryID())
	}
}
