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
	"github.com/storacha/guppy/pkg/client/locator"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestLocator(t *testing.T) {
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
	session := locator.NewIndexLocator(mockIndexer, requiredDelegations)

	location, err := session.Locate(t.Context(), blockHash)
	require.NoError(t, err)

	require.Len(t, location.Commitments, 2)
	require.ElementsMatch(t, ctestutil.Urls(
		"https://storage1.example.com/block/abc123",
		"https://storage2.example.com/block/abc123",
	), location.Commitments[0].Nb().Location)
	require.ElementsMatch(t, ctestutil.Urls(
		"https://storage3.example.com/block/abc123",
	), location.Commitments[1].Nb().Location)

	require.Equal(t, blobindex.Position{
		Offset: 10,
		Length: 2048,
	}, location.Position)

	require.Len(t, mockIndexer.Queries, 1)
	require.Equal(t, types.Query{
		Hashes:      []multihash.Multihash{blockHash},
		Delegations: requiredDelegations,
	}, mockIndexer.Queries[0])

	location, err = session.Locate(t.Context(), blockHash)
	require.NoError(t, err)
	require.Len(t, mockIndexer.Queries, 1)
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
