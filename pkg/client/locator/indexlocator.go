package locator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var (
	log    = logging.Logger("client/locator")
	tracer = otel.Tracer("client/locator")
	meter  = otel.Meter("client/locator")

	locateDuration, _ = meter.Float64Histogram(
		"locate.duration",
		metric.WithDescription("Duration of Locate operations"),
		metric.WithUnit("s"),
	)

	locateCacheResult, _ = meter.Int64Counter(
		"locate.cache",
		metric.WithDescription("Cache hits and misses for Locate operations"),
	)
)

// AuthorizeRetrievalFunc creates a content retrieval authorization for the
// provided space.
//
// It should create a short lived delegation that allows the indexing service
// to retrieve indexes from the space.
type AuthorizeRetrievalFunc func(space did.DID) (delegation.Delegation, error)

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

// Locate retrieves locations for the data identified by the given digest, in
// the given space.
func (s *indexLocator) Locate(ctx context.Context, spaceDID did.DID, digest mh.Multihash) ([]Location, error) {
	log.Infof("Locating block %s in space %s", digestutil.Format(digest), spaceDID.String())

	locations, err := s.LocateMany(ctx, spaceDID, []mh.Multihash{digest})
	if err != nil {
		return nil, err
	}

	if locations.Has(digest) {
		return locations.Get(digest), nil
	}

	return nil, NotFoundError{Hash: digest}
}

func (s *indexLocator) LocateMany(ctx context.Context, spaceDID did.DID, digests []mh.Multihash) (retMap blobindex.MultihashMap[[]Location], retErr error) {
	log.Infof("Locating %d blocks in space %s", len(digests), spaceDID.String())

	ctx, span := tracer.Start(ctx, "locate-many", trace.WithAttributes(
		attribute.String("space", spaceDID.String()),
		attribute.Int("count", len(digests)),
	))
	startTime := time.Now()
	defer func() {
		status := "success"
		if retErr != nil {
			status = "error"
			span.SetStatus(codes.Error, retErr.Error())
			span.RecordError(retErr)
		}
		locateDuration.Record(ctx, time.Since(startTime).Seconds(),
			metric.WithAttributes(attribute.String("status", status)),
		)
		span.End()
	}()

	locations := blobindex.NewMultihashMap[[]Location](len(digests))
	needInclusions := make([]mh.Multihash, 0, len(digests))
	needLocations := make([]mh.Multihash, 0, len(digests))

	for _, digest := range digests {
		cachedLocations, cachedInclusions := s.getCached(spaceDID, digest)

		if len(cachedLocations) > 0 {
			locateCacheResult.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "hit")))
		} else if len(cachedInclusions) > 0 {
			locateCacheResult.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "partial")))
			// If we have an inclusion but no locations, query the indexer for the
			// location of the shard.
			for _, inclusion := range cachedInclusions {
				needLocations = append(needLocations, inclusion.shard)
			}
		} else {
			locateCacheResult.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "miss")))
			needInclusions = append(needInclusions, digest)
		}
	}

	span.SetAttributes(
		attribute.Int("count-missing-inclusions", len(needInclusions)),
		attribute.Int("count-missing-locations", len(needLocations)),
	)

	if err := s.query(ctx, spaceDID, needInclusions, false); err != nil {
		return nil, err
	}

	if err := s.query(ctx, spaceDID, needLocations, true); err != nil {
		return nil, err
	}

	for _, digest := range digests {
		foundLocations, _ := s.getCached(spaceDID, digest)
		if len(foundLocations) > 0 {
			locations.Set(digest, foundLocations)
		}
	}

	return locations, nil
}

// getCached retrieves cached locations for the given spaceDID and digest. It
// returns the locations and a boolean indicating whether we know of at least
// one shard which includes the block. If the second return value is false, the
// caller should perform a full query to the indexer to attempt to discover new
// locations. If true, if the locations slice is empty, the caller can perform a
// location-only query to fetch the remaining location information (or to
// confirm that there are no locations).
func (s *indexLocator) getCached(spaceDID did.DID, digest mh.Multihash) ([]Location, []inclusion) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	inclusions, hasInclusions := s.inclusions[cacheKey(spaceDID, digest)]
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
					Position:   inclusion.position,
				})
			}
		}
	}

	return locations, inclusions
}

func (s *indexLocator) query(ctx context.Context, spaceDID did.DID, digests []mh.Multihash, omitIndexes bool) (retErr error) {
	if len(digests) == 0 {
		return nil
	}

	var queryType types.QueryType
	if omitIndexes {
		queryType = types.QueryTypeLocation
	} else {
		queryType = types.QueryTypeStandard
	}

	ctx, span := tracer.Start(ctx, "locate-query", trace.WithAttributes(
		attribute.String("space", spaceDID.String()),
		attribute.Int("digest-count", len(digests)),
		attribute.String("query-type", queryType.String()),
	))
	defer func() {
		if retErr != nil {
			span.SetStatus(codes.Error, retErr.Error())
			span.RecordError(retErr)
		}
		span.End()
	}()

	auth, err := s.authorizeRetrieval(spaceDID)
	if err != nil {
		return fmt.Errorf("authorizing retrieval for space %s: %w", spaceDID, err)
	}

	result, err := s.indexer.QueryClaims(ctx, types.Query{
		Hashes:      digests,
		Delegations: []delegation.Delegation{auth},
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

		claimKey := cacheKey(spaceDID, claimDigest)
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

		for shardDigest, shardMap := range index.Shards().Iterator() {
			for sliceDigest, position := range shardMap.Iterator() {
				s.inclusions[cacheKey(spaceDID, sliceDigest)] = append(s.inclusions[cacheKey(spaceDID, sliceDigest)], inclusion{
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
