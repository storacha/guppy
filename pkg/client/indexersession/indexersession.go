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
		indexer:   indexer,
		urls:      blobindex.NewMultihashMap[[]url.URL](-1),
		positions: blobindex.NewMultihashMap[blobindex.Position](-1),
		shards:    blobindex.NewMultihashMap[mh.Multihash](-1),
	}
}

type IndexerSession struct {
	indexer   IndexerClient
	urls      blobindex.MultihashMap[[]url.URL]
	positions blobindex.MultihashMap[blobindex.Position]
	shards    blobindex.MultihashMap[mh.Multihash]
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
	location := s.getCached(hash)

	if location.Urls == nil {
		if err := s.query(ctx, hash); err != nil {
			return Location{}, err
		}
		location = s.getCached(hash)
	}

	if location.Urls == nil {
		return Location{}, &NotFoundError{Hash: hash}
	}

	return location, nil
}

func (s *IndexerSession) getCached(hash mh.Multihash) Location {
	if s.positions.Has(hash) && s.shards.Has(hash) && s.urls.Has(s.shards.Get(hash)) {
		return Location{
			Urls:     s.urls.Get(s.shards.Get(hash)),
			Position: s.positions.Get(hash),
		}
	}

	return Location{}
}

func (s *IndexerSession) query(ctx context.Context, hash mh.Multihash) error {
	result, err := s.indexer.QueryClaims(ctx, types.Query{
		Hashes: []mh.Multihash{hash},
	})
	if err != nil {
		return fmt.Errorf("querying claims for %s: %w", hash.String(), err)
	}

	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(result.Blocks()))
	if err != nil {
		return err
	}

	for _, link := range result.Claims() {
		d, err := delegation.NewDelegationView(link, bs)
		if err != nil {
			return err
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
		if s.urls.Has(claimHash) {
			knownLocations = s.urls.Get(claimHash)
		}
		s.urls.Set(claimHash, append(knownLocations, cap.Nb().Location...))
	}

	for _, link := range result.Indexes() {
		indexBlock, ok, err := bs.Get(link)
		if err != nil {
			return fmt.Errorf("getting index block: %w", err)
		}
		if !ok {
			return fmt.Errorf("index block not found: %s", link.String())
		}
		index, err := blobindex.Extract(bytes.NewReader(indexBlock.Bytes()))
		if err != nil {
			return fmt.Errorf("extracting index: %w", err)
		}

		for shardHash, shardMap := range index.Shards().Iterator() {
			for sliceHash, position := range shardMap.Iterator() {
				s.positions.Set(sliceHash, position)
				s.shards.Set(sliceHash, shardHash)
			}
		}
	}

	return nil
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
