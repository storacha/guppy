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
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld/block"
	"github.com/storacha/go-ucanto/core/ipld/hash/sha256"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client/locator"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/storacha/indexing-service/pkg/types"
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

		// These could be any kind of delegations; we just need to see that they get
		// sent to the indexer, whatever they are.
		requiredDelegations := []delegation.Delegation{
			testutil.RandomLocationDelegation(t),
			testutil.RandomEqualsDelegation(t),
		}

		mockIndexer := newMockIndexerClient([]delegation.Delegation{claim1, claim2}, []blobindex.ShardedDagIndexView{index})
		locator := locator.NewIndexLocator(mockIndexer, requiredDelegations)

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
			Hashes:      []multihash.Multihash{blockHash},
			Delegations: requiredDelegations,
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

		mockIndexer := newMockIndexerClient([]delegation.Delegation{claim}, []blobindex.ShardedDagIndexView{index})
		locator := locator.NewIndexLocator(mockIndexer, requiredDelegations)

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
		locator := locator.NewIndexLocator(mockIndexer, requiredDelegations)

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
		require.Contains(t, err.Error(), block2Hash.String())
		require.Contains(t, err.Error(), "no locations found")

		// Verify that we made a second query, but it was location-only
		require.Len(t, mockIndexer.Queries, 2, "Should have made two queries")
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
		session := locator.NewIndexLocator(mockIndexer, requiredDelegations)

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

// source is a helper type for matching capabilities
type source struct {
	capability ucan.Capability[any]
	delegation delegation.Delegation
}

func (s source) Capability() ucan.Capability[any] {
	return s.capability
}

func (s source) Delegation() delegation.Delegation {
	return s.delegation
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
		match1, _ := assertcap.Location.Match(source{
			capability: m.space1Claim.Capabilities()[0],
			delegation: m.space1Claim,
		})
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
	hash, err := multihash.Sum(indexBytes, sha256.Code, -1)
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

func newMockIndexerClient(claims []delegation.Delegation, indexes []blobindex.ShardedDagIndexView) *mockIndexerClient {
	return &mockIndexerClient{
		claims:  claims,
		indexes: indexes,
	}
}

type mockIndexerClient struct {
	claims  []delegation.Delegation
	indexes []blobindex.ShardedDagIndexView

	Queries []types.Query
}

var _ locator.IndexerClient = (*mockIndexerClient)(nil)

func (m *mockIndexerClient) QueryClaims(ctx context.Context, query types.Query) (types.QueryResult, error) {
	m.Queries = append(m.Queries, query)

	var indexBlocks []block.Block
	for _, index := range m.indexes {
		indexReader, err := blobindex.Archive(index)
		if err != nil {
			return nil, fmt.Errorf("archiving index: %w", err)
		}
		indexBytes, err := io.ReadAll(indexReader)
		if err != nil {
			return nil, fmt.Errorf("reading index bytes: %w", err)
		}
		hash, err := multihash.Sum(indexBytes, sha256.Code, -1)
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
		claims:      m.claims,
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
	hash, err := multihash.Sum(indexBytes, sha256.Code, -1)
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
