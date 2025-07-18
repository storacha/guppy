package sqlrepo_test

import (
	crand "crypto/rand"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func randomCID(t *testing.T) cid.Cid {
	t.Helper()

	bytes := make([]byte, 10)
	_, err := crand.Read(bytes)
	require.NoError(t, err)

	hash, err := multihash.Sum(bytes, multihash.SHA2_256, -1)
	return cid.NewCidV1(cid.Raw, hash)
}
