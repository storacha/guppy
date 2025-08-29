package testutil

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// CreateTestDB creates a temporary SQLite database for testing. It returns the
// database connection, a cleanup function, and any error encountered.
func CreateTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	require.NoError(t, err, "failed to open in-memory SQLite database")

	t.Cleanup(func() {
		db.Close()
	})

	_, err = db.ExecContext(t.Context(), sqlrepo.Schema)
	require.NoError(t, err, "failed to execute schema")

	// Disable foreign key checks to simplify test.
	_, err = db.ExecContext(t.Context(), "PRAGMA foreign_keys = OFF;")
	require.NoError(t, err, "failed to disable foreign keys")

	return db
}

func RandomCID(t *testing.T) cid.Cid {
	t.Helper()

	bytes := make([]byte, 10)
	_, err := crand.Read(bytes)
	require.NoError(t, err)

	hash, err := multihash.Sum(bytes, multihash.SHA2_256, -1)
	return cid.NewCidV1(cid.Raw, hash)
}

type MockClient struct {
	T                        *testing.T
	SpaceBlobAddInvocations  []spaceBlobAddInvocation
	SpaceIndexAddInvocations []spaceIndexAddInvocation
}

type spaceBlobAddInvocation struct {
	Space     did.DID
	BlobAdded []byte
}

type spaceIndexAddInvocation struct {
	Space     did.DID
	IndexLink ipld.Link
}

var _ storacha.Client = (*MockClient)(nil)

func (m *MockClient) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (multihash.Multihash, delegation.Delegation, error) {
	contentBytes, err := io.ReadAll(content)
	require.NoError(m.T, err, "reading content for SpaceBlobAdd")

	m.SpaceBlobAddInvocations = append(m.SpaceBlobAddInvocations, spaceBlobAddInvocation{
		Space:     space,
		BlobAdded: contentBytes,
	})

	digest, err := multihash.Sum(contentBytes, multihash.SHA2_256, -1)
	return digest, nil, nil
}

func (m *MockClient) SpaceIndexAdd(ctx context.Context, indexLink ipld.Link, space did.DID) error {
	m.SpaceIndexAddInvocations = append(m.SpaceIndexAddInvocations, spaceIndexAddInvocation{
		Space:     space,
		IndexLink: indexLink,
	})

	return nil
}
