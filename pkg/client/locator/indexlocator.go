package locator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
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

// AuthorizeRetrievalFunc creates a content retrieval authorization for the
// provided space(s).
//
// It should create a short lived delegation that allows the indexing service
// to retrieve indexes from the space(s).
type AuthorizeRetrievalFunc func(spaces []did.DID) (delegation.Delegation, error)

func NewIndexLocator(indexer IndexerClient, authorizeRetrieval AuthorizeRetrievalFunc) *indexLocator {
	return &indexLocator{
		indexer:            indexer,
		authorizeRetrieval: authorizeRetrieval,
		commitments:        make(map[string][]ucan.Capability[assert.LocationCaveats]),
		inclusions:         make(map[string][]inclusion),
	}
}

type indexLocator struct {
	indexer            IndexerClient
	authorizeRetrieval AuthorizeRetrievalFunc
	mu                 sync.RWMutex
	commitments        map[string][]ucan.Capability[assert.LocationCaveats]
	inclusions         map[string][]inclusion
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

func (s *indexLocator) Locate(ctx context.Context, spaces []did.DID, hash mh.Multihash) ([]Location, error) {
	locations, inclusions := s.getCached(spaces, hash)

	if len(locations) == 0 {
		if len(inclusions) == 0 {
			if err := s.query(ctx, spaces, hash, false); err != nil {
				return nil, err
			}
			locations, _ = s.getCached(spaces, hash)
		} else {
			// If we have an inclusion but no locations, query the indexer for the
			// location of the shard. Repeat for each inclusion until we find a
			// location for the block.
			for _, inclusion := range inclusions {
				if err := s.query(ctx, spaces, inclusion.shard, true); err != nil {
					return nil, err
				}
				locations, _ = s.getCached(spaces, hash)
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

// getCached retrieves cached locations for the given spaces and hash. It
// returns the locations and a boolean indicating whether we know of at least
// one shard which includes the block. If the second return value is false, the
// caller should perform a full query to the indexer to attempt to discover new
// locations. If true, if the locations slice is empty, the caller can perform a
// location-only query to fetch the remaining location information (or to
// confirm that there are no locations).
func (s *indexLocator) getCached(spaces []did.DID, hash mh.Multihash) ([]Location, []inclusion) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var locations []Location
	var inclusions []inclusion
	for _, spaceDID := range spaces {
		incs, hasInclusions := s.inclusions[cacheKey(spaceDID, hash)]
		if !hasInclusions {
			continue
		}
		inclusions = append(inclusions, incs...)

		for _, inclusion := range incs {
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
	}

	return locations, inclusions
}

func (s *indexLocator) query(ctx context.Context, spaces []did.DID, hash mh.Multihash, omitIndexes bool) error {
	var queryType types.QueryType
	if omitIndexes {
		queryType = types.QueryTypeLocation
	} else {
		queryType = types.QueryTypeStandard
	}

	auth, err := s.authorizeRetrieval(spaces)
	if err != nil {
		return fmt.Errorf("authorizing retrieval for spaces %v: %w", spaces, err)
	}

	result, err := s.indexer.QueryClaims(ctx, types.Query{
		Hashes:      []mh.Multihash{hash},
		Delegations: []delegation.Delegation{auth},
		Match: types.Match{
			Subject: spaces,
		},
		Type: queryType,
	})
	if err != nil {
		var failedResponseErr indexclient.ErrFailedResponse
		if ok := errors.As(err, &failedResponseErr); ok {
			return fmt.Errorf("indexer responded with status %d: %s", failedResponseErr.StatusCode, failedResponseErr.Body)
		}
		return fmt.Errorf("querying claims for %s: %w", digestutil.Format(hash), err)
	}

	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(result.Blocks()))
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// record the space each blob belongs to
	blobSpace := blobindex.NewMultihashMap[did.DID](-1)

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

		claimKey := cacheKey(cap.Nb().Space, claimHash)
		s.commitments[claimKey] = append(s.commitments[claimKey], cap)
		blobSpace.Set(claimHash, cap.Nb().Space)
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
		indexCid, err := cid.Parse(link.String())
		if err != nil {
			return fmt.Errorf("parsing index CID: %w", err)
		}

		for shardHash, shardMap := range index.Shards().Iterator() {
			spaceDID := blobSpace.Get(shardHash)
			if spaceDID == did.Undef {
				if len(spaces) == 1 {
					spaceDID = spaces[0]
				} else {
					// if we don't know the space for this shard from our query result,
					// and we are locating across multiple spaces then assume it is the
					// same space as the index (it should be).
					spaceDID = blobSpace.Get(indexCid.Hash())
					// we expect the indexer to return a location claim for the index though
					if spaceDID == did.Undef {
						return fmt.Errorf("missing location claim for index %q in query results for hash: %s", indexCid, digestutil.Format(hash))
					}
				}
			}
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
