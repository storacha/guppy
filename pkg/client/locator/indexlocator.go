package locator

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	indexclient "github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/indexing-service/pkg/types"
)

func NewIndexLocator(indexer IndexerClient, delegations []delegation.Delegation) *indexLocator {
	return &indexLocator{
		indexer:     indexer,
		delegations: delegations,
		commitments: make(map[string][]ucan.Capability[assert.LocationCaveats]),
		positions:   make(map[string]blobindex.Position),
		shards:      make(map[string]mh.Multihash),
	}
}

type indexLocator struct {
	indexer     IndexerClient
	delegations []delegation.Delegation
	commitments map[string][]ucan.Capability[assert.LocationCaveats]
	positions   map[string]blobindex.Position
	shards      map[string]mh.Multihash
}

func cacheKey(spaceDID did.DID, hash mh.Multihash) string {
	return spaceDID.String() + ":" + hash.String()
}

var _ Locator = (*indexLocator)(nil)

type IndexerClient interface {
	QueryClaims(ctx context.Context, query types.Query) (types.QueryResult, error)
}

type NotFoundError struct {
	Hash mh.Multihash
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("no locations found for block %s", e.Hash.String())
}

func (s *indexLocator) Locate(ctx context.Context, spaceDID did.DID, hash mh.Multihash) (Location, error) {
	location := s.getCached(spaceDID, hash)

	if location.Commitments == nil {
		if err := s.query(ctx, spaceDID, hash); err != nil {
			return Location{}, err
		}
		location = s.getCached(spaceDID, hash)
	}

	if location.Commitments == nil {
		return Location{}, &NotFoundError{Hash: hash}
	}

	return location, nil
}

func (s *indexLocator) getCached(spaceDID did.DID, hash mh.Multihash) Location {
	hashKey := cacheKey(spaceDID, hash)
	shard, hasShardInfo := s.shards[hashKey]
	if !hasShardInfo {
		return Location{}
	}

	position, hasPosition := s.positions[hashKey]
	if !hasPosition {
		return Location{}
	}

	shardKey := cacheKey(spaceDID, shard)
	commitments, hasCommitments := s.commitments[shardKey]
	if !hasCommitments {
		return Location{}
	}

	return Location{
		Shard:       shard,
		Commitments: commitments,
		Position:    position,
	}
}

func (s *indexLocator) query(ctx context.Context, spaceDID did.DID, hash mh.Multihash) error {
	result, err := s.indexer.QueryClaims(ctx, types.Query{
		Hashes:      []mh.Multihash{hash},
		Delegations: s.delegations,
		Match: types.Match{
			Subject: []did.DID{spaceDID},
		},
	})
	if err != nil {
		var failedResponseErr indexclient.ErrFailedResponse
		if ok := errors.As(err, &failedResponseErr); ok {
			return fmt.Errorf("indexer responded with status %d: %s", failedResponseErr.StatusCode, failedResponseErr.Body)
		}
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

		claimKey := cacheKey(spaceDID, claimHash)
		knownCommitments := s.commitments[claimKey]
		s.commitments[claimKey] = append(knownCommitments, cap)
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
				sliceKey := cacheKey(spaceDID, sliceHash)
				s.positions[sliceKey] = position
				s.shards[sliceKey] = shardHash
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
