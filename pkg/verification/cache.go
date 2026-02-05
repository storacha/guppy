package verification

import (
	"maps"
	"slices"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/bytemap"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/validator"
)

type IndexCache struct {
	indexes    map[cid.Cid]blobindex.ShardedDagIndex
	sliceIndex bytemap.ByteMap[multihash.Multihash, cid.Cid]
	mutex      sync.RWMutex
}

func NewIndexCache() *IndexCache {
	return &IndexCache{
		indexes:    map[cid.Cid]blobindex.ShardedDagIndex{},
		sliceIndex: bytemap.NewByteMap[multihash.Multihash, cid.Cid](0),
	}
}

func (c *IndexCache) Add(root cid.Cid, index blobindex.ShardedDagIndex) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.indexes[root] = index
	for _, slices := range index.Shards().Iterator() {
		for slice := range slices.Iterator() {
			c.sliceIndex.Set(slice, root)
		}
	}
}

func (c *IndexCache) IndexForSlice(slice multihash.Multihash) (blobindex.ShardedDagIndex, bool) {
	root := c.sliceIndex.Get(slice)
	idx, ok := c.indexes[root]
	return idx, ok
}

type Location struct {
	Commitment delegation.Delegation
	// Caveats are the decoded caveats from the commitment delegation.
	Caveats assert.LocationCaveats
}

type LocationCache struct {
	// shard digest -> commitment CID -> location info
	locations bytemap.ByteMap[multihash.Multihash, map[cid.Cid]Location]
	mutex     sync.RWMutex
}

func NewLocationCache() *LocationCache {
	return &LocationCache{
		locations: bytemap.NewByteMap[multihash.Multihash, map[cid.Cid]Location](0),
	}
}

func (c *LocationCache) Add(commitment delegation.Delegation) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	match, err := assert.Location.Match(validator.NewSource(commitment.Capabilities()[0], commitment))
	if err != nil {
		log.Warnw("adding location to cache", "error", err)
		return
	}

	root := toCID(commitment.Link())
	nb := match.Value().Nb()

	shardLocations := c.locations.Get(nb.Content.Hash())
	if shardLocations == nil {
		shardLocations = map[cid.Cid]Location{}
		c.locations.Set(nb.Content.Hash(), shardLocations)
	}
	shardLocations[root] = Location{Commitment: commitment, Caveats: nb}
}

func (c *LocationCache) LocationsForShard(shard multihash.Multihash) []Location {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return slices.Collect(maps.Values(c.locations.Get(shard)))
}
