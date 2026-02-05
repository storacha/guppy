package verification

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cenkalti/backoff/v5"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/indexing-service/pkg/types"
)

// IndexingServiceClient is the interface for querying the indexing service.
type IndexingServiceClient interface {
	QueryClaims(ctx context.Context, query types.Query) (types.QueryResult, error)
}

type Indexer struct {
	client        IndexingServiceClient
	authorize     AuthorizeIndexerRetrievalFunc
	indexCache    *IndexCache
	locationCache *LocationCache
}

// NewIndexer creates a new Indexer with the given indexing service client and
// authorization function. Note: the authorize function can be nil, in which
// case no authorization will be sent for indexer queries.
func NewIndexer(client IndexingServiceClient, authorize AuthorizeIndexerRetrievalFunc) *Indexer {
	return &Indexer{
		client:        client,
		authorize:     authorize,
		indexCache:    NewIndexCache(),
		locationCache: NewLocationCache(),
	}
}

// do a query and cache the results
func (i *Indexer) doQuery(ctx context.Context, digest multihash.Multihash) error {
	result, err := backoff.Retry(ctx, func() (types.QueryResult, error) {
		q := types.Query{Hashes: []multihash.Multihash{digest}}
		if i.authorize != nil {
			auth, err := i.authorize()
			if err != nil {
				return nil, fmt.Errorf("authorizing indexer retrieval: %w", err)
			}

			spaces := []did.DID{}
			for _, c := range auth.Capabilities() {
				if c.Can() == contentcap.RetrieveAbility {
					s, err := did.Parse(c.With())
					if err != nil {
						return nil, fmt.Errorf("parsing space DID from authorized capability: %w", err)
					}
					spaces = append(spaces, s)
				}
			}

			q.Match = types.Match{Subject: spaces}
			q.Delegations = []delegation.Delegation{auth}
		}

		return i.client.QueryClaims(ctx, q)
	}, backoff.WithMaxTries(3))
	if err != nil {
		return fmt.Errorf("querying indexer for slice %s: %w", digestutil.Format(digest), err)
	}

	bs, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(result.Blocks()))
	if err != nil {
		return fmt.Errorf("creating blockstore from query result: %w", err)
	}

	for _, root := range result.Indexes() {
		b, ok, err := bs.Get(root)
		if err != nil {
			log.Warnw("getting index block", "root", root, "error", err)
			continue
		}
		if !ok {
			log.Warnf("index block %s not found in result blocks", root)
			continue
		}
		index, err := blobindex.Extract(bytes.NewReader(b.Bytes()))
		if err != nil {
			log.Warnw("extracting blob index", "error", err)
			continue
		}
		i.indexCache.Add(toCID(root), index)
	}

	for _, root := range result.Claims() {
		dlg, err := delegation.NewDelegationView(root, bs)
		if err != nil {
			log.Warnw("extracting claim", "error", err)
			continue
		}
		if len(dlg.Capabilities()) < 1 || dlg.Capabilities()[0].Can() != assert.LocationAbility {
			continue
		}
		i.locationCache.Add(dlg)
	}

	return nil
}

// FindShard finds the shard and its position for the given slice.
func (i *Indexer) FindShard(ctx context.Context, slice multihash.Multihash) (multihash.Multihash, blobindex.Position, error) {
	index, ok := i.indexCache.IndexForSlice(slice)
	if !ok {
		err := i.doQuery(ctx, slice)
		if err != nil {
			return nil, blobindex.Position{}, err
		}
		idx, ok := i.indexCache.IndexForSlice(slice)
		if !ok {
			return nil, blobindex.Position{}, fmt.Errorf("index for slice not found: %s", digestutil.Format(slice))
		}
		index = idx
	}

	for shard, slices := range index.Shards().Iterator() {
		for s, pos := range slices.Iterator() {
			if bytes.Equal(s, slice) {
				return shard, pos, nil
			}
		}
	}
	return nil, blobindex.Position{}, fmt.Errorf("slice not found in index: %s", digestutil.Format(slice))
}

// FindLocations finds at least 1 location for the given shard or returns an
// error.
func (i *Indexer) FindLocations(ctx context.Context, shard multihash.Multihash) ([]Location, error) {
	locations := i.locationCache.LocationsForShard(shard)
	if len(locations) == 0 {
		err := i.doQuery(ctx, shard)
		if err != nil {
			return nil, err
		}
		locations = i.locationCache.LocationsForShard(shard)
		if len(locations) == 0 {
			return nil, fmt.Errorf("location for shard %q not found", digestutil.Format(shard))
		}
	}
	return locations, nil
}
