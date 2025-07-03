package sqlrepo_test

import (
	"testing"

	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/stretchr/testify/require"
)

func TestCreateSource(t *testing.T) {
	repo := sqlrepo.New(testutil.CreateTestDB(t))

	source, err := repo.CreateSource(t.Context(), "source name", "source/path")
	require.NoError(t, err)

	readSourceByID, err := repo.GetSourceByID(t.Context(), source.ID())

	require.NoError(t, err)
	require.Equal(t, source, readSourceByID)

	readSourceByName, err := repo.GetSourceByName(t.Context(), "source name")

	require.NoError(t, err)
	require.Equal(t, source, readSourceByName)
}
