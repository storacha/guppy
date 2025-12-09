package locator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-libstoracha/digestutil"
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
		inclusions:  make(map[string][]inclusion),
	}
}

type indexLocator struct {
	indexer     IndexerClient
	delegations []delegation.Delegation
	mu          sync.RWMutex
	commitments map[string][]ucan.Capability[assert.LocationCaveats]
	inclusions  map[string][]inclusion
}

type inclusion struct {
	shard    mh.Multihash
	position blobindex.Position
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
	return fmt.Sprintf("no locations found for block: %s", digestutil.Format(e.Hash))
}

func (s *indexLocator) Locate(ctx context.Context, spaceDID did.DID, hash mh.Multihash) ([]Location, error) {
	locations, inclusions := s.getCached(spaceDID, hash)

	if len(locations) == 0 {
		if len(inclusions) == 0 {
			if err := s.query(ctx, spaceDID, hash, false); err != nil {
				return nil, err
			}
			locations, _ = s.getCached(spaceDID, hash)
		} else {
			// If we have an inclusion but no locations, query the indexer for the
			// location of the shard. Repeat for each inclusion until we find a
			// location for the block.
			for _, inclusion := range inclusions {
				if err := s.query(ctx, spaceDID, inclusion.shard, true); err != nil {
					return nil, err
				}
				locations, _ = s.getCached(spaceDID, hash)
				if len(locations) > 0 {
					break
				}
			}
		}
	}

	// If we still have no locations then we failed to locate the block.
	if len(locations) == 0 {
		return nil, &NotFoundError{Hash: hash}
	}

	return locations, nil
}

// getCached retrieves cached locations for the given spaceDID and hash. It
// returns the locations and a boolean indicating whether we know of at least
// one shard which includes the block. If the second return value is false, the
// caller should perform a full query to the indexer to attempt to discover new
// locations. If true, if the locations slice is empty, the caller can perform a
// location-only query to fetch the remaining location information (or to
// confirm that there are no locations).
func (s *indexLocator) getCached(spaceDID did.DID, hash mh.Multihash) ([]Location, []inclusion) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	inclusions, hasInclusions := s.inclusions[cacheKey(spaceDID, hash)]
	if !hasInclusions {
		return nil, nil
	}

	var locations []Location
	for _, inclusion := range inclusions {
		shardCommitments, hasCommitments := s.commitments[cacheKey(spaceDID, inclusion.shard)]
		if hasCommitments {
			for _, commitment := range shardCommitments {
				locations = append(locations, Location{
					Commitment: commitment,
					Digest:     hash,
					Position:   inclusion.position,
				})
			}
		}
	}

	return locations, inclusions
}

func (s *indexLocator) query(ctx context.Context, spaceDID did.DID, hash mh.Multihash, omitIndexes bool) error {
	var queryType types.QueryType
	if omitIndexes {
		queryType = types.QueryTypeLocation
	} else {
		queryType = types.QueryTypeStandard
	}

	result, err := s.indexer.QueryClaims(ctx, types.Query{
		Hashes:      []mh.Multihash{hash},
		Delegations: s.delegations,
		Match: types.Match{
			Subject: []did.DID{spaceDID},
		},
		Type: queryType,
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

	s.mu.Lock()
	defer s.mu.Unlock()

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
		s.commitments[claimKey] = append(s.commitments[claimKey], cap)
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
				s.inclusions[cacheKey(spaceDID, sliceHash)] = append(s.inclusions[cacheKey(spaceDID, sliceHash)], inclusion{
					shard:    shardHash,
					position: position,
				})
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
