package locator_test

import (
	"context"
	"fmt"
	"io"
	"iter"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld/block"
	"github.com/storacha/go-ucanto/core/ipld/hash/sha256"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/validator"
	"github.com/storacha/guppy/pkg/client/locator"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocator(t *testing.T) {
	t.Run("locates block from indexer", func(t *testing.T) {
		blockHash := testutil.RandomMultihash(t)
		rootLink := testutil.RandomCID(t)
		shardHash := testutil.RandomMultihash(t)

		space := testutil.RandomSigner(t)
		provider1 := testutil.RandomSigner(t)
		provider2 := testutil.RandomSigner(t)

		claim1, err := assertcap.Location.Delegate(
			provider1,
			provider1.DID(),
			provider1.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(shardHash),
				Location: ctestutil.Urls(
					"https://storage1.example.com/block/abc123",
					"https://storage2.example.com/block/abc123",
				),
			},
		)
		require.NoError(t, err)

		claim2, err := assertcap.Location.Delegate(
			provider2,
			provider2.DID(),
			provider2.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(shardHash),
				Location: ctestutil.Urls(
					"https://storage3.example.com/block/abc123",
				),
			},
		)
		require.NoError(t, err)

		index := blobindex.NewShardedDagIndexView(rootLink, -1)
		index.SetSlice(shardHash, blockHash, blobindex.Position{
			Offset: 10,
			Length: 2048,
		})

		// This could be any kind of delegation; we just need to see that it gets
		// sent to the indexer, whatever it is.
		authDelegation := testutil.RandomLocationDelegation(t)

		mockIndexer := newMockIndexerClient(func(digests []mh.Multihash) ([]delegation.Delegation, []blobindex.ShardedDagIndexView, error) {
			assert.ElementsMatch(t, []mh.Multihash{blockHash}, digests)
			return []delegation.Delegation{claim1, claim2}, []blobindex.ShardedDagIndexView{index}, nil
		})
		locator := locator.NewIndexLocator(mockIndexer, func(space did.DID) (delegation.Delegation, error) {
			return authDelegation, nil
		})

		locations, err := locator.Locate(t.Context(), space.DID(), blockHash)
		require.NoError(t, err)

		require.Len(t, locations, 2)
		require.ElementsMatch(t, ctestutil.Urls(
			"https://storage1.example.com/block/abc123",
			"https://storage2.example.com/block/abc123",
		), locations[0].Commitment.Nb().Location)
		require.Equal(t, blobindex.Position{
			Offset: 10,
			Length: 2048,
		}, locations[0].Position)
		require.ElementsMatch(t, ctestutil.Urls(
			"https://storage3.example.com/block/abc123",
		), locations[1].Commitment.Nb().Location)
		require.Equal(t, blobindex.Position{
			Offset: 10,
			Length: 2048,
		}, locations[1].Position)

		require.Len(t, mockIndexer.Queries, 1)
		require.Equal(t, types.Query{
			Hashes:      []mh.Multihash{blockHash},
			Delegations: []delegation.Delegation{authDelegation},
			Match: types.Match{
				Subject: []did.DID{space.DID()},
			},
		}, mockIndexer.Queries[0])

		_, err = locator.Locate(t.Context(), space.DID(), blockHash)
		require.NoError(t, err)
		require.Len(t, mockIndexer.Queries, 1)
	})

	t.Run("caches unrequested blocks", func(t *testing.T) {
		// Create two different block hashes that will be in the same shard
		block1Hash := testutil.RandomMultihash(t)
		block2Hash := testutil.RandomMultihash(t)
		rootLink := testutil.RandomCID(t)
		shardHash := testutil.RandomMultihash(t)

		space := testutil.RandomSigner(t)
		provider := testutil.RandomSigner(t)

		// Create a claim for the shard
		claim, err := assertcap.Location.Delegate(
			provider,
			provider.DID(),
			provider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(shardHash),
				Location: ctestutil.Urls(
					"https://storage.example.com/shard/xyz",
				),
			},
		)
		require.NoError(t, err)

		// Create an index that contains BOTH blocks in the same shard
		index := blobindex.NewShardedDagIndexView(rootLink, -1)
		index.SetSlice(shardHash, block1Hash, blobindex.Position{
			Offset: 0,
			Length: 1024,
		})
		index.SetSlice(shardHash, block2Hash, blobindex.Position{
			Offset: 1024,
			Length: 2048,
		})

		requiredDelegations := []delegation.Delegation{
			testutil.RandomLocationDelegation(t),
		}

		mockIndexer := newMockIndexerClient(func(digests []mh.Multihash) ([]delegation.Delegation, []blobindex.ShardedDagIndexView, error) {
			assert.ElementsMatch(t, []mh.Multihash{block1Hash}, digests)
			return []delegation.Delegation{claim}, []blobindex.ShardedDagIndexView{index}, nil
		})
		locator := locator.NewIndexLocator(mockIndexer, func(space did.DID) (delegation.Delegation, error) {
			return requiredDelegations[0], nil
		})

		// First, locate block1
		locations1, err := locator.Locate(t.Context(), space.DID(), block1Hash)
		require.NoError(t, err)
		require.Len(t, locations1, 1)
		require.Equal(t, blobindex.Position{
			Offset: 0,
			Length: 1024,
		}, locations1[0].Position)
		require.ElementsMatch(t, ctestutil.Urls(
			"https://storage.example.com/shard/xyz",
		), locations1[0].Commitment.Nb().Location)

		// Verify that we made one query
		require.Len(t, mockIndexer.Queries, 1)

		// Now locate block2, which should be served from cache
		locations2, err := locator.Locate(t.Context(), space.DID(), block2Hash)
		require.NoError(t, err)
		require.Len(t, locations2, 1)
		require.Equal(t, blobindex.Position{
			Offset: 1024,
			Length: 2048,
		}, locations2[0].Position)
		require.ElementsMatch(t, ctestutil.Urls(
			"https://storage.example.com/shard/xyz",
		), locations2[0].Commitment.Nb().Location)

		// Verify that we STILL have only one query - block2 came from cache
		require.Len(t, mockIndexer.Queries, 1, "Second block should be served from cache without making a new query")
	})

	t.Run("location-only query for block in cached index", func(t *testing.T) {
		// Create two different block hashes that will be in different shards
		block1Hash := testutil.RandomMultihash(t)
		block2Hash := testutil.RandomMultihash(t)
		rootLink := testutil.RandomCID(t)
		shard1Hash := testutil.RandomMultihash(t)
		shard2Hash := testutil.RandomMultihash(t)

		space := testutil.RandomSigner(t)
		provider := testutil.RandomSigner(t)

		// Create a claim for shard1 only (not shard2)
		claim1, err := assertcap.Location.Delegate(
			provider,
			provider.DID(),
			provider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(shard1Hash),
				Location: ctestutil.Urls(
					"https://storage.example.com/shard1",
				),
			},
		)
		require.NoError(t, err)

		// Create an index that contains BOTH blocks in different shards
		index := blobindex.NewShardedDagIndexView(rootLink, -1)
		index.SetSlice(shard1Hash, block1Hash, blobindex.Position{
			Offset: 0,
			Length: 1024,
		})
		index.SetSlice(shard2Hash, block2Hash, blobindex.Position{
			Offset: 0,
			Length: 2048,
		})

		requiredDelegations := []delegation.Delegation{
			testutil.RandomLocationDelegation(t),
		}

		// Create a mock indexer that responds differently based on query type
		mockIndexer := newLocationQueryMockIndexer(claim1, index)
		locator := locator.NewIndexLocator(mockIndexer, func(space did.DID) (delegation.Delegation, error) {
			return requiredDelegations[0], nil
		})

		// First, locate block1 - should return index and claim for shard1
		locations1, err := locator.Locate(t.Context(), space.DID(), block1Hash)
		require.NoError(t, err)
		require.Len(t, locations1, 1)
		require.Equal(t, blobindex.Position{
			Offset: 0,
			Length: 1024,
		}, locations1[0].Position)
		require.ElementsMatch(t, ctestutil.Urls(
			"https://storage.example.com/shard1",
		), locations1[0].Commitment.Nb().Location)

		// Verify that we made one query
		require.Len(t, mockIndexer.Queries, 1)
		require.Equal(t, types.QueryTypeStandard, mockIndexer.Queries[0].Type)

		// Now locate block2 - should make a location-only query since we have the index
		// But the mock won't return a location claim for shard2, so we should get an error
		_, err = locator.Locate(t.Context(), space.DID(), block2Hash)
		require.Error(t, err)
		require.Contains(t, err.Error(), digestutil.Format(block2Hash))
		require.Contains(t, err.Error(), "no locations found")

		// Verify that we made a second query, but it was location-only, and was
		// targeted at shard2
		require.Len(t, mockIndexer.Queries, 2, "Should have made two queries")
		require.Equal(t, shard2Hash, mockIndexer.Queries[1].Hashes[0],
			"Second query should be for shard2 hash")
		require.Equal(t, types.QueryTypeLocation, mockIndexer.Queries[1].Type,
			"Second query should be location-only since index is cached")
	})

	t.Run("cache is space-scoped", func(t *testing.T) {
		// Create a block hash
		blockHash := testutil.RandomMultihash(t)
		rootLink := testutil.RandomCID(t)
		shardHash := testutil.RandomMultihash(t)

		// Create TWO different spaces
		space1 := testutil.RandomSigner(t)
		space2 := testutil.RandomSigner(t)
		provider := testutil.RandomSigner(t)

		// Create a claim for space1
		claim1, err := assertcap.Location.Delegate(
			provider,
			provider.DID(),
			provider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space1.DID(),
				Content: captypes.FromHash(shardHash),
				Location: ctestutil.Urls(
					"https://storage1.example.com/space1/block",
				),
			},
		)
		require.NoError(t, err)

		// Create a claim for space2
		claim2, err := assertcap.Location.Delegate(
			provider,
			provider.DID(),
			provider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space2.DID(),
				Content: captypes.FromHash(shardHash),
				Location: ctestutil.Urls(
					"https://storage2.example.com/space2/block",
				),
			},
		)
		require.NoError(t, err)

		// Create an index with the block
		index := blobindex.NewShardedDagIndexView(rootLink, -1)
		index.SetSlice(shardHash, blockHash, blobindex.Position{
			Offset: 0,
			Length: 1024,
		})

		requiredDelegations := []delegation.Delegation{
			testutil.RandomLocationDelegation(t),
		}

		// Create a mock indexer that returns the appropriate claim based on the query
		mockIndexer := &spaceScopedMockIndexer{
			space1Claim: claim1,
			space2Claim: claim2,
			index:       index,
		}
		session := locator.NewIndexLocator(mockIndexer, func(space did.DID) (delegation.Delegation, error) {
			return requiredDelegations[0], nil
		})

		// Locate the block for space1
		locations1, err := session.Locate(t.Context(), space1.DID(), blockHash)
		require.NoError(t, err)
		require.Len(t, locations1, 1)
		require.ElementsMatch(t, ctestutil.Urls(
			"https://storage1.example.com/space1/block",
		), locations1[0].Commitment.Nb().Location)

		// Locate the block for space2 - should get different location
		locations2, err := session.Locate(t.Context(), space2.DID(), blockHash)
		require.NoError(t, err)
		require.Len(t, locations2, 1)
		require.ElementsMatch(t, ctestutil.Urls(
			"https://storage2.example.com/space2/block",
		), locations2[0].Commitment.Nb().Location)

		// Verify both queries were made (cache didn't incorrectly return space1's result for space2)
		require.Equal(t, 2, mockIndexer.queryCount, "Cache should be space-scoped, requiring separate queries for each space")
	})
}

func TestLocateMany(t *testing.T) {
	t.Run("locates multiple digests at once across two shards", func(t *testing.T) {
		// Create three different block hashes
		block1Hash := testutil.RandomMultihash(t)
		block2Hash := testutil.RandomMultihash(t)
		block3Hash := testutil.RandomMultihash(t)
		rootLink := testutil.RandomCID(t)
		shard1Hash := testutil.RandomMultihash(t)
		shard2Hash := testutil.RandomMultihash(t)

		space := testutil.RandomSigner(t)
		provider := testutil.RandomSigner(t)

		// Create a claim for shard1
		claim1, err := assertcap.Location.Delegate(
			provider,
			provider.DID(),
			provider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(shard1Hash),
				Location: ctestutil.Urls(
					"https://storage.example.com/shard1/abc",
				),
			},
		)
		require.NoError(t, err)

		// Create a claim for shard2
		claim2, err := assertcap.Location.Delegate(
			provider,
			provider.DID(),
			provider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(shard2Hash),
				Location: ctestutil.Urls(
					"https://storage.example.com/shard2/def",
				),
			},
		)
		require.NoError(t, err)

		// Create an index with blocks 1 and 2 in shard1, and block 3 in shard2
		index := blobindex.NewShardedDagIndexView(rootLink, -1)
		index.SetSlice(shard1Hash, block1Hash, blobindex.Position{
			Offset: 0,
			Length: 1024,
		})
		index.SetSlice(shard1Hash, block2Hash, blobindex.Position{
			Offset: 1024,
			Length: 2048,
		})
		index.SetSlice(shard2Hash, block3Hash, blobindex.Position{
			Offset: 0,
			Length: 512,
		})

		authDelegation := testutil.RandomLocationDelegation(t)

		mockIndexer := newMockIndexerClient(func(digests []mh.Multihash) ([]delegation.Delegation, []blobindex.ShardedDagIndexView, error) {
			assert.ElementsMatch(t, []mh.Multihash{block1Hash, block2Hash, block3Hash}, digests)
			return []delegation.Delegation{claim1, claim2}, []blobindex.ShardedDagIndexView{index}, nil
		})
		locator := locator.NewIndexLocator(mockIndexer, func(space did.DID) (delegation.Delegation, error) {
			return authDelegation, nil
		})

		// Locate all three blocks at once
		locations, err := locator.LocateMany(t.Context(), space.DID(), []mh.Multihash{block1Hash, block2Hash, block3Hash})
		require.NoError(t, err)

		// Verify we got locations for all three blocks
		require.True(t, locations.Has(block1Hash), "should have location for block1")
		require.True(t, locations.Has(block2Hash), "should have location for block2")
		require.True(t, locations.Has(block3Hash), "should have location for block3")

		// Verify the positions and URLs are correct
		block1Locations := locations.Get(block1Hash)
		require.Len(t, block1Locations, 1)
		require.Equal(t, blobindex.Position{Offset: 0, Length: 1024}, block1Locations[0].Position)
		require.ElementsMatch(t, ctestutil.Urls("https://storage.example.com/shard1/abc"), block1Locations[0].Commitment.Nb().Location)

		block2Locations := locations.Get(block2Hash)
		require.Len(t, block2Locations, 1)
		require.Equal(t, blobindex.Position{Offset: 1024, Length: 2048}, block2Locations[0].Position)
		require.ElementsMatch(t, ctestutil.Urls("https://storage.example.com/shard1/abc"), block2Locations[0].Commitment.Nb().Location)

		block3Locations := locations.Get(block3Hash)
		require.Len(t, block3Locations, 1)
		require.Equal(t, blobindex.Position{Offset: 0, Length: 512}, block3Locations[0].Position)
		require.ElementsMatch(t, ctestutil.Urls("https://storage.example.com/shard2/def"), block3Locations[0].Commitment.Nb().Location)

		// Verify only one query was made
		require.Len(t, mockIndexer.Queries, 1)
		require.ElementsMatch(t, []mh.Multihash{block1Hash, block2Hash, block3Hash}, mockIndexer.Queries[0].Hashes)
	})

	t.Run("returns partial results when some blocks not found", func(t *testing.T) {
		// Create two block hashes, but only one will be in the index
		block1Hash := testutil.RandomMultihash(t)
		block2Hash := testutil.RandomMultihash(t)
		rootLink := testutil.RandomCID(t)
		shardHash := testutil.RandomMultihash(t)

		space := testutil.RandomSigner(t)
		provider := testutil.RandomSigner(t)

		claim, err := assertcap.Location.Delegate(
			provider,
			provider.DID(),
			provider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(shardHash),
				Location: ctestutil.Urls(
					"https://storage.example.com/shard/abc",
				),
			},
		)
		require.NoError(t, err)

		// Create an index with only block1
		index := blobindex.NewShardedDagIndexView(rootLink, -1)
		index.SetSlice(shardHash, block1Hash, blobindex.Position{
			Offset: 0,
			Length: 1024,
		})

		authDelegation := testutil.RandomLocationDelegation(t)

		mockIndexer := newMockIndexerClient(func(digests []mh.Multihash) ([]delegation.Delegation, []blobindex.ShardedDagIndexView, error) {
			assert.ElementsMatch(t, []mh.Multihash{block1Hash, block2Hash}, digests)
			return []delegation.Delegation{claim}, []blobindex.ShardedDagIndexView{index}, nil
		})
		locator := locator.NewIndexLocator(mockIndexer, func(space did.DID) (delegation.Delegation, error) {
			return authDelegation, nil
		})

		// Locate both blocks
		locations, err := locator.LocateMany(t.Context(), space.DID(), []mh.Multihash{block1Hash, block2Hash})
		require.NoError(t, err)

		// Verify we got location for block1 but not block2
		require.True(t, locations.Has(block1Hash), "should have location for block1")
		require.False(t, locations.Has(block2Hash), "should not have location for block2")

		block1Locations := locations.Get(block1Hash)
		require.Len(t, block1Locations, 1)
		require.Equal(t, blobindex.Position{Offset: 0, Length: 1024}, block1Locations[0].Position)
	})

	t.Run("batches queries for uncached blocks only", func(t *testing.T) {
		// Create three block hashes
		block1Hash := testutil.RandomMultihash(t)
		block2Hash := testutil.RandomMultihash(t)
		block3Hash := testutil.RandomMultihash(t)
		rootLink := testutil.RandomCID(t)
		shard1Hash := testutil.RandomMultihash(t)
		shard2Hash := testutil.RandomMultihash(t)

		space := testutil.RandomSigner(t)
		provider := testutil.RandomSigner(t)

		// Create a claim for shard1
		claim1, err := assertcap.Location.Delegate(
			provider,
			provider.DID(),
			provider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(shard1Hash),
				Location: ctestutil.Urls(
					"https://storage.example.com/shard1/abc",
				),
			},
		)
		require.NoError(t, err)

		// Create a claim for shard2
		claim2, err := assertcap.Location.Delegate(
			provider,
			provider.DID(),
			provider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(shard2Hash),
				Location: ctestutil.Urls(
					"https://storage.example.com/shard2/def",
				),
			},
		)
		require.NoError(t, err)

		// Create an index with blocks 1 and 2 in shard1, and block 3 in shard2
		index := blobindex.NewShardedDagIndexView(rootLink, -1)
		index.SetSlice(shard1Hash, block1Hash, blobindex.Position{
			Offset: 0,
			Length: 1024,
		})
		index.SetSlice(shard1Hash, block2Hash, blobindex.Position{
			Offset: 1024,
			Length: 2048,
		})
		index.SetSlice(shard2Hash, block3Hash, blobindex.Position{
			Offset: 0,
			Length: 512,
		})

		authDelegation := testutil.RandomLocationDelegation(t)

		mockIndexer := newMockIndexerClient(func(digests []mh.Multihash) ([]delegation.Delegation, []blobindex.ShardedDagIndexView, error) {
			assert.ElementsMatch(t, []mh.Multihash{block1Hash}, digests)
			return []delegation.Delegation{claim1, claim2}, []blobindex.ShardedDagIndexView{index}, nil
		})
		locator := locator.NewIndexLocator(mockIndexer, func(space did.DID) (delegation.Delegation, error) {
			return authDelegation, nil
		})

		// First, locate block1 to populate the cache
		_, err = locator.Locate(t.Context(), space.DID(), block1Hash)
		require.NoError(t, err)
		require.Len(t, mockIndexer.Queries, 1)

		// Now locate all three blocks - all should come from cache
		locations, err := locator.LocateMany(t.Context(), space.DID(), []mh.Multihash{block1Hash, block2Hash, block3Hash})
		require.NoError(t, err)

		// All three blocks should have locations (all from cache due to index)
		require.True(t, locations.Has(block1Hash))
		require.True(t, locations.Has(block2Hash))
		require.True(t, locations.Has(block3Hash))

		// Verify the URLs are correct for each shard
		require.ElementsMatch(t, ctestutil.Urls("https://storage.example.com/shard1/abc"), locations.Get(block1Hash)[0].Commitment.Nb().Location)
		require.ElementsMatch(t, ctestutil.Urls("https://storage.example.com/shard1/abc"), locations.Get(block2Hash)[0].Commitment.Nb().Location)
		require.ElementsMatch(t, ctestutil.Urls("https://storage.example.com/shard2/def"), locations.Get(block3Hash)[0].Commitment.Nb().Location)

		// Should still be only one query since all blocks were cached from first query
		require.Len(t, mockIndexer.Queries, 1, "All blocks should be served from cache")
	})
}

// spaceScopedMockIndexer returns different claims based on the space DID in the query
type spaceScopedMockIndexer struct {
	space1Claim delegation.Delegation
	space2Claim delegation.Delegation
	index       blobindex.ShardedDagIndexView
	queryCount  int
}

var _ locator.IndexerClient = (*spaceScopedMockIndexer)(nil)

func (m *spaceScopedMockIndexer) QueryClaims(ctx context.Context, query types.Query) (types.QueryResult, error) {
	m.queryCount++

	// Determine which claim to return based on the space DID in the query
	var claim delegation.Delegation
	if len(query.Match.Subject) > 0 {
		// Extract space DID from space1's claim
		match1, _ := assertcap.Location.Match(validator.NewSource(
			m.space1Claim.Capabilities()[0],
			m.space1Claim,
		))
		space1DID := match1.Value().Nb().Space

		if query.Match.Subject[0] == space1DID {
			claim = m.space1Claim
		} else {
			claim = m.space2Claim
		}
	}

	// Build index block
	indexReader, err := blobindex.Archive(m.index)
	if err != nil {
		return nil, fmt.Errorf("archiving index: %w", err)
	}
	indexBytes, err := io.ReadAll(indexReader)
	if err != nil {
		return nil, fmt.Errorf("reading index bytes: %w", err)
	}
	hash, err := mh.Sum(indexBytes, sha256.Code, -1)
	if err != nil {
		return nil, fmt.Errorf("hashing index bytes: %w", err)
	}
	indexBlock := block.NewBlock(
		cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Car), hash)},
		indexBytes,
	)

	return &mockQueryResult{
		claims:      []delegation.Delegation{claim},
		indexBlocks: []block.Block{indexBlock},
	}, nil
}

func newMockIndexerClient(claimsAndIndexesFn func([]mh.Multihash) ([]delegation.Delegation, []blobindex.ShardedDagIndexView, error)) *mockIndexerClient {
	return &mockIndexerClient{
		claimsAndIndexesFn: claimsAndIndexesFn,
	}
}

type mockIndexerClient struct {
	claimsAndIndexesFn func([]mh.Multihash) ([]delegation.Delegation, []blobindex.ShardedDagIndexView, error)

	Queries []types.Query
}

var _ locator.IndexerClient = (*mockIndexerClient)(nil)

func (m *mockIndexerClient) QueryClaims(ctx context.Context, query types.Query) (types.QueryResult, error) {
	m.Queries = append(m.Queries, query)

	claims, indexes, err := m.claimsAndIndexesFn(query.Hashes)
	if err != nil {
		return nil, err
	}

	var indexBlocks []block.Block
	for _, index := range indexes {
		indexReader, err := blobindex.Archive(index)
		if err != nil {
			return nil, fmt.Errorf("archiving index: %w", err)
		}
		indexBytes, err := io.ReadAll(indexReader)
		if err != nil {
			return nil, fmt.Errorf("reading index bytes: %w", err)
		}
		hash, err := mh.Sum(indexBytes, sha256.Code, -1)
		if err != nil {
			return nil, fmt.Errorf("hashing index bytes: %w", err)
		}
		indexBlock := block.NewBlock(
			cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Car), hash)},
			indexBytes,
		)
		indexBlocks = append(indexBlocks, indexBlock)
	}

	return &mockQueryResult{
		claims:      claims,
		indexBlocks: indexBlocks,
	}, nil
}

// locationQueryMockIndexer returns indexes on standard queries, but only claims on location queries
type locationQueryMockIndexer struct {
	claim delegation.Delegation
	index blobindex.ShardedDagIndexView

	Queries []types.Query
}

var _ locator.IndexerClient = (*locationQueryMockIndexer)(nil)

func newLocationQueryMockIndexer(claim delegation.Delegation, index blobindex.ShardedDagIndexView) *locationQueryMockIndexer {
	return &locationQueryMockIndexer{
		claim: claim,
		index: index,
	}
}

func (m *locationQueryMockIndexer) QueryClaims(ctx context.Context, query types.Query) (types.QueryResult, error) {
	m.Queries = append(m.Queries, query)

	var claims []delegation.Delegation
	var indexBlocks []block.Block

	// For location-only queries, only return claims (no indexes)
	if query.Type == types.QueryTypeLocation {
		// Return empty result for location queries (simulating no location for shard2)
		return &mockQueryResult{
			claims:      nil,
			indexBlocks: nil,
		}, nil
	}

	// For standard queries, return both index and claims
	claims = []delegation.Delegation{m.claim}

	indexReader, err := blobindex.Archive(m.index)
	if err != nil {
		return nil, fmt.Errorf("archiving index: %w", err)
	}
	indexBytes, err := io.ReadAll(indexReader)
	if err != nil {
		return nil, fmt.Errorf("reading index bytes: %w", err)
	}
	hash, err := mh.Sum(indexBytes, sha256.Code, -1)
	if err != nil {
		return nil, fmt.Errorf("hashing index bytes: %w", err)
	}
	indexBlock := block.NewBlock(
		cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Car), hash)},
		indexBytes,
	)
	indexBlocks = append(indexBlocks, indexBlock)

	return &mockQueryResult{
		claims:      claims,
		indexBlocks: indexBlocks,
	}, nil
}

type mockQueryResult struct {
	claims      []delegation.Delegation
	indexBlocks []block.Block
}

var _ types.QueryResult = (*mockQueryResult)(nil)

func (m *mockQueryResult) Root() block.Block {
	return nil
}

func (m *mockQueryResult) Blocks() iter.Seq2[block.Block, error] {
	return func(yield func(block.Block, error) bool) {
		for _, claim := range m.claims {
			for block, err := range claim.Blocks() {
				if !yield(block, err) {
					return
				}
			}
		}

		for _, indexBlock := range m.indexBlocks {
			if !yield(indexBlock, nil) {
				return
			}
		}
	}
}

func (m *mockQueryResult) Claims() []ipld.Link {
	var claimsLinks []ipld.Link
	for _, claim := range m.claims {
		claimsLinks = append(claimsLinks, claim.Link())
	}

	return claimsLinks
}

func (m *mockQueryResult) Indexes() []ipld.Link {
	var indexLinks []ipld.Link
	for _, index := range m.indexBlocks {
		indexLinks = append(indexLinks, index.Link())
	}

	return indexLinks
}
