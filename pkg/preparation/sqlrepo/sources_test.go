package sqlrepo_test

import (
	"path/filepath"
	"testing"

	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/stretchr/testify/require"
)

func TestCreateSource(t *testing.T) {
	repo := sqlrepo.New(testdb.CreateTestDB(t))

	source, err := repo.CreateSource(t.Context(), "source name", "source/path")
	require.NoError(t, err)

	readSourceByID, err := repo.GetSourceByID(t.Context(), source.ID())

	require.NoError(t, err)
	require.Equal(t, source, readSourceByID)

	readSourceByName, err := repo.GetSourceByName(t.Context(), "source name")

	require.NoError(t, err)
	require.Equal(t, source, readSourceByName)
}

func TestCreateSourceDuplicatePath(t *testing.T) {
	repo := sqlrepo.New(testdb.CreateTestDB(t))

	path := "source/path"
	absPath, err := filepath.Abs(path)
	require.NoError(t, err)

	_, err = repo.CreateSource(t.Context(), "source name", path)
	require.NoError(t, err)

	_, err = repo.CreateSource(t.Context(), "second source", path)
	require.ErrorContains(t, err, "source with path")
	require.ErrorContains(t, err, absPath)
}
