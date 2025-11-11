package indexersession

import (
	"bytes"
	"context"
	"fmt"
	"net/url"

	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/indexing-service/pkg/types"
)

func New(indexer IndexerClient) *IndexerSession {
	return &IndexerSession{
		indexer: indexer,
	}
}

type IndexerSession struct {
	indexer IndexerClient
}

type IndexerClient interface {
	QueryClaims(ctx context.Context, query types.Query) (types.QueryResult, error)
}

type NotFoundError struct {
	Hash mh.Multihash
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("no locations found for block %s", e.Hash.String())
}

type Location struct {
	Urls     []url.URL
	Position blobindex.Position
}

// TK: Should take a space?
func (s *IndexerSession) Locate(ctx context.Context, hash mh.Multihash) (Location, error) {
	result, err := s.indexer.QueryClaims(ctx, types.Query{
		Hashes: []mh.Multihash{hash},
	})
	if err != nil {
		return Location{}, fmt.Errorf("querying claims for %s: %w", hash.String(), err)
	}

	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(result.Blocks()))
	if err != nil {
		return Location{}, err
	}

	urls := blobindex.NewMultihashMap[[]url.URL](-1)
	positions := blobindex.NewMultihashMap[blobindex.Position](-1)
	shards := blobindex.NewMultihashMap[mh.Multihash](-1)

	for _, link := range result.Claims() {
		d, err := delegation.NewDelegationView(link, bs)
		if err != nil {
			return Location{}, err
		}

		match, err := assert.Location.Match(source{
			capability: d.Capabilities()[0],
			delegation: d,
		})
		if err != nil {
			continue
		}

		cap := match.Value()
		claimHash := cap.Nb().Content.Hash()

		var knownLocations []url.URL
		if urls.Has(claimHash) {
			knownLocations = urls.Get(claimHash)
		}
		urls.Set(claimHash, append(knownLocations, cap.Nb().Location...))
	}

	for _, link := range result.Indexes() {
		indexBlock, ok, err := bs.Get(link)
		if err != nil {
			return Location{}, fmt.Errorf("getting index block: %w", err)
		}
		if !ok {
			return Location{}, fmt.Errorf("index block not found: %s", link.String())
		}
		index, err := blobindex.Extract(bytes.NewReader(indexBlock.Bytes()))
		if err != nil {
			return Location{}, fmt.Errorf("extracting index: %w", err)
		}

		for shardHash, shardMap := range index.Shards().Iterator() {
			for sliceHash, position := range shardMap.Iterator() {
				positions.Set(sliceHash, position)
				shards.Set(sliceHash, shardHash)
			}
		}
	}

	if positions.Has(hash) && shards.Has(hash) && urls.Has(shards.Get(hash)) {
		return Location{
			Urls:     urls.Get(shards.Get(hash)),
			Position: positions.Get(hash),
		}, nil
	}

	return Location{}, &NotFoundError{Hash: hash}
}

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
