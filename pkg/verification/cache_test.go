package verification_test

import (
	"net/url"
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/pkg/verification"
	"github.com/stretchr/testify/require"
)

func TestIndexCache(t *testing.T) {
	t.Run("returns false when looking up a slice that was never added", func(t *testing.T) {
		cache := verification.NewIndexCache()

		unknownSlice := testutil.RandomMultihash(t)
		_, found := cache.IndexForSlice(unknownSlice)

		require.False(t, found, "should not find an index for unknown slice")
	})

	t.Run("returns the index that contains a given slice", func(t *testing.T) {
		cache := verification.NewIndexCache()

		rootCID := testutil.RandomCID(t).(cidlink.Link).Cid
		index, sliceDigest := randomIndex(t)

		cache.Add(rootCID, index)

		foundIndex, found := cache.IndexForSlice(sliceDigest)

		require.True(t, found, "should find index for known slice")
		require.Equal(t, index, foundIndex, "found index should match the one that was added")
	})

	t.Run("handles multiple indexes with different slices", func(t *testing.T) {
		cache := verification.NewIndexCache()

		root1 := testutil.RandomCID(t).(cidlink.Link).Cid
		index1, slice1 := randomIndex(t)

		root2 := testutil.RandomCID(t).(cidlink.Link).Cid
		index2, slice2 := randomIndex(t)

		cache.Add(root1, index1)
		cache.Add(root2, index2)

		foundIndex1, found1 := cache.IndexForSlice(slice1)
		require.True(t, found1)
		require.Equal(t, index1, foundIndex1)

		foundIndex2, found2 := cache.IndexForSlice(slice2)
		require.True(t, found2)
		require.Equal(t, index2, foundIndex2)
	})
}

func TestLocationCache(t *testing.T) {
	t.Run("returns empty slice when looking up an unknown shard", func(t *testing.T) {
		cache := verification.NewLocationCache()

		unknownShard := testutil.RandomMultihash(t)
		locations := cache.LocationsForShard(unknownShard)

		require.Empty(t, locations, "should return no locations for unknown shard")
	})

	t.Run("stores and retrieves a location commitment for a shard", func(t *testing.T) {
		cache := verification.NewLocationCache()

		// Create a location commitment
		issuer := testutil.Must(ed25519signer.Generate())(t)
		audience := testutil.Must(ed25519signer.Generate())(t)
		space := testutil.Must(ed25519signer.Generate())(t)

		shardDigest := testutil.RandomMultihash(t)
		commitment := createLocationCommitment(t, issuer, audience, space.DID(), shardDigest)

		cache.Add(commitment)

		// Should be able to find the location by shard digest
		locations := cache.LocationsForShard(shardDigest)

		require.Len(t, locations, 1, "should have one location for the shard")
		require.Equal(t, commitment.Link(), locations[0].Commitment.Link())
		require.Equal(t, space.DID(), locations[0].Caveats.Space)
	})

	t.Run("stores multiple locations for the same shard from different providers", func(t *testing.T) {
		cache := verification.NewLocationCache()

		// Two different storage providers (issuers) for the same shard
		issuer1 := testutil.Must(ed25519signer.Generate())(t)
		issuer2 := testutil.Must(ed25519signer.Generate())(t)
		audience := testutil.Must(ed25519signer.Generate())(t)
		space := testutil.Must(ed25519signer.Generate())(t)

		shardDigest := testutil.RandomMultihash(t)

		// Different storage providers make different commitments for the same shard
		commitment1 := createLocationCommitment(t, issuer1, audience, space.DID(), shardDigest)
		commitment2 := createLocationCommitment(t, issuer2, audience, space.DID(), shardDigest)

		cache.Add(commitment1)
		cache.Add(commitment2)

		locations := cache.LocationsForShard(shardDigest)

		require.Len(t, locations, 2, "should have two locations for the shard")
	})

	t.Run("deduplicates identical commitments for the same shard", func(t *testing.T) {
		cache := verification.NewLocationCache()

		issuer := testutil.Must(ed25519signer.Generate())(t)
		audience := testutil.Must(ed25519signer.Generate())(t)
		space := testutil.Must(ed25519signer.Generate())(t)

		shardDigest := testutil.RandomMultihash(t)

		// Adding the same commitment twice should not create duplicates
		commitment := createLocationCommitment(t, issuer, audience, space.DID(), shardDigest)

		cache.Add(commitment)
		cache.Add(commitment) // Add the same commitment again

		locations := cache.LocationsForShard(shardDigest)

		require.Len(t, locations, 1, "should have only one location (deduped)")
	})

	t.Run("keeps locations for different shards separate", func(t *testing.T) {
		cache := verification.NewLocationCache()

		issuer := testutil.Must(ed25519signer.Generate())(t)
		audience := testutil.Must(ed25519signer.Generate())(t)
		space := testutil.Must(ed25519signer.Generate())(t)

		shard1 := testutil.RandomMultihash(t)
		shard2 := testutil.RandomMultihash(t)

		commitment1 := createLocationCommitment(t, issuer, audience, space.DID(), shard1)
		commitment2 := createLocationCommitment(t, issuer, audience, space.DID(), shard2)

		cache.Add(commitment1)
		cache.Add(commitment2)

		locations1 := cache.LocationsForShard(shard1)
		locations2 := cache.LocationsForShard(shard2)

		require.Len(t, locations1, 1, "shard1 should have one location")
		require.Len(t, locations2, 1, "shard2 should have one location")
		require.NotEqual(t, locations1[0].Commitment.Link(), locations2[0].Commitment.Link())
	})
}

// randomIndex generates a ShardedDagIndex with a single slice and returns it,
// along with the digest of that slice.
func randomIndex(t *testing.T) (blobindex.ShardedDagIndex, multihash.Multihash) {
	t.Helper()
	_, index := testutil.RandomShardedDagIndexView(t, 1)
	// Extract one slice from the index to return
	for _, slices := range index.Shards().Iterator() {
		for slice := range slices.Iterator() {
			return index, slice
		}
	}
	t.Fatal("index has no slices")
	return nil, nil
}

func createLocationCommitment(t *testing.T, issuer, audience principal.Signer, space did.DID, shardDigest multihash.Multihash) delegation.Delegation {
	t.Helper()

	storageURL, err := url.Parse("https://storage.example.com/blob/test")
	require.NoError(t, err)

	caveats := assertcap.LocationCaveats{
		Space:   space,
		Content: captypes.FromHash(shardDigest),
		Range: &assertcap.Range{
			Offset: 0,
			Length: ptr(uint64(1000)),
		},
		Location: []url.URL{*storageURL},
	}

	dlg := testutil.Must(assertcap.Location.Delegate(
		issuer,
		audience,
		issuer.DID().String(),
		caveats,
	))(t)

	return dlg
}
