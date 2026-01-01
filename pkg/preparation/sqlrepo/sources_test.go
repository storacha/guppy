package sqlrepo_test

import (
	"path/filepath"
	"testing"

	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/guppy/pkg/preparation/internal/testdb"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/stretchr/testify/require"
)

func TestCreateSource(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

	source, err := repo.CreateSource(t.Context(), "source name", "source/path")
	require.NoError(t, err)

	readSourceByID, err := repo.GetSourceByID(t.Context(), source.ID())

	require.NoError(t, err)
	require.Equal(t, source, readSourceByID)

	readSourceByName, err := repo.GetSourceByName(t.Context(), "source name")

	require.NoError(t, err)
	require.Equal(t, source, readSourceByName)
}

func TestCreateSourceReturnsExisting(t *testing.T) {
	repo := testutil.Must(sqlrepo.New(testdb.CreateTestDB(t)))(t)

	source, err := repo.CreateSource(t.Context(), "source name", "source/path")
	require.NoError(t, err)

	absPath, err := filepath.Abs("source/path")
	require.NoError(t, err)
	require.Equal(t, absPath, source.Path())

	duplicate, err := repo.CreateSource(t.Context(), "another name", "./source/path")
	require.NoError(t, err)

	require.Equal(t, source, duplicate)
}
