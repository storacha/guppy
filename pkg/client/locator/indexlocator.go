package locator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
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

var log = logging.Logger("client/dagservice")

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

// Locate retrieves locations for the data identified by the given digest, in
// the given space.
func (s *indexLocator) Locate(ctx context.Context, spaces []did.DID, digest mh.Multihash) ([]Location, error) {
	log.Debugw("Locating block", "digest", digestutil.Format(digest), "spaces", spaces)
	locations, err := s.LocateMany(ctx, spaces, []mh.Multihash{digest})
	if err != nil {
		return nil, err
	}

	if locations.Has(digest) {
		return locations.Get(digest), nil
	}

	return nil, NotFoundError{Hash: digest}
}

func (s *indexLocator) LocateMany(ctx context.Context, spaces []did.DID, digests []mh.Multihash) (blobindex.MultihashMap[[]Location], error) {
	log.Debugw("Locating blocks", "count", len(digests), "spaces", spaces)
	locations := blobindex.NewMultihashMap[[]Location](len(digests))
	needInclusions := make([]mh.Multihash, 0, len(digests))
	needLocations := make([]mh.Multihash, 0, len(digests))

	for _, digest := range digests {
		cachedLocations, cachedInclusions := s.getCached(spaces, digest)

		if len(cachedLocations) == 0 {
			if len(cachedInclusions) == 0 {
				needInclusions = append(needInclusions, digest)
			} else {
				// If we have an inclusion but no locations, query the indexer for the
				// location of the shard.
				for _, inclusion := range cachedInclusions {
					needLocations = append(needLocations, inclusion.shard)
				}
			}
		}
	}

	if err := s.query(ctx, spaces, needInclusions, false); err != nil {
		return nil, err
	}

	if err := s.query(ctx, spaces, needLocations, true); err != nil {
		return nil, err
	}

	for _, digest := range digests {
		foundLocations, _ := s.getCached(spaces, digest)
		if len(foundLocations) > 0 {
			locations.Set(digest, foundLocations)
		}
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
func (s *indexLocator) getCached(spaces []did.DID, digest mh.Multihash) ([]Location, []inclusion) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var locations []Location
	var inclusions []inclusion
	for _, spaceDID := range spaces {
		incs, hasInclusions := s.inclusions[cacheKey(spaceDID, digest)]
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
						Position:   inclusion.position,
					})
				}
			}
		}
	}

	return locations, inclusions
}

func (s *indexLocator) query(ctx context.Context, spaces []did.DID, digests []mh.Multihash, omitIndexes bool) error {
	if len(digests) == 0 {
		return nil
	}

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
		Hashes:      digests,
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
		hashStrings := make([]string, len(digests))
		for i, h := range digests {
			hashStrings[i] = digestutil.Format(h)
		}
		return fmt.Errorf("querying claims for [%s]: %w", strings.Join(hashStrings, ", "), err)
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
		claimDigest := cap.Nb().Content.Hash()

		claimKey := cacheKey(cap.Nb().Space, claimDigest)
		s.commitments[claimKey] = append(s.commitments[claimKey], cap)
		blobSpace.Set(claimDigest, cap.Nb().Space)
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

		for shardDigest, shardMap := range index.Shards().Iterator() {
			spaceDID := blobSpace.Get(shardDigest)
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
						digestStrs := make([]string, 0, len(digests))
						for _, d := range digests {
							digestStrs = append(digestStrs, digestutil.Format(d))
						}
						return fmt.Errorf("missing location claim for index %q in query results for: %s", indexCid, strings.Join(digestStrs, ", "))
					}
				}
			}
			for sliceDigest, position := range shardMap.Iterator() {
				key := cacheKey(spaceDID, sliceDigest)
				s.inclusions[key] = append(s.inclusions[key], inclusion{
					shard:    shardDigest,
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
