package verification_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/testutil"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/pkg/verification"
	"github.com/stretchr/testify/require"
)

func TestStatBlocks(t *testing.T) {
	t.Run("retrieves and verifies two raw blocks", func(t *testing.T) {
		ctx := t.Context()

		block1Data := []byte("hello world")
		block1Hash, err := multihash.Sum(block1Data, multihash.SHA2_256, -1)
		require.NoError(t, err)
		block1CID := cid.NewCidV1(cid.Raw, block1Hash)

		block2Data := []byte("goodbye world")
		block2Hash, err := multihash.Sum(block2Data, multihash.SHA2_256, -1)
		require.NoError(t, err)
		block2CID := cid.NewCidV1(cid.Raw, block2Hash)

		shard1Hash := testutil.RandomMultihash(t)
		shard2Hash := testutil.RandomMultihash(t)
		position1 := blobindex.Position{Offset: 0, Length: uint64(len(block1Data))}
		position2 := blobindex.Position{Offset: 0, Length: uint64(len(block2Data))}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Return different data based on Range header to distinguish blocks
			rangeHeader := r.Header.Get("Range")
			if rangeHeader == fmt.Sprintf("bytes=0-%d", len(block1Data)-1) {
				w.Write(block1Data)
			} else {
				w.Write(block2Data)
			}
		}))
		defer server.Close()

		serverURL, _ := url.Parse(server.URL)
		location1 := createTestLocation(t, shard1Hash, *serverURL)
		location2 := createTestLocation(t, shard2Hash, *serverURL)

		stub := &stubShardFinder{
			shards: map[string]shardInfo{
				string(block1Hash): {shard: shard1Hash, position: position1},
				string(block2Hash): {shard: shard2Hash, position: position2},
			},
			locations: map[string][]verification.Location{
				string(shard1Hash): {location1},
				string(shard2Hash): {location2},
			},
		}

		var stats []verification.BlockStat
		for stat, err := range verification.StatBlocks(ctx, nil, nil, stub, []cid.Cid{block1CID, block2CID}) {
			require.NoError(t, err)
			stats = append(stats, stat)
		}

		require.Len(t, stats, 2)

		require.Equal(t, uint64(cid.Raw), stats[0].Codec)
		require.Equal(t, uint64(len(block1Data)), stats[0].Size)
		require.Equal(t, block1Hash, multihash.Multihash(stats[0].Digest))
		require.Empty(t, stats[0].Links, "raw blocks have no links")

		require.Equal(t, uint64(cid.Raw), stats[1].Codec)
		require.Equal(t, uint64(len(block2Data)), stats[1].Size)
		require.Equal(t, block2Hash, multihash.Multihash(stats[1].Digest))
		require.Empty(t, stats[1].Links, "raw blocks have no links")
	})

	t.Run("fails integrity check when data does not match hash", func(t *testing.T) {
		ctx := t.Context()

		// First block: valid
		block1Data := []byte("valid block")
		block1Hash, err := multihash.Sum(block1Data, multihash.SHA2_256, -1)
		require.NoError(t, err)
		block1CID := cid.NewCidV1(cid.Raw, block1Hash)

		// Second block: will be tampered
		block2Data := []byte("original data")
		block2Hash, err := multihash.Sum(block2Data, multihash.SHA2_256, -1)
		require.NoError(t, err)
		block2CID := cid.NewCidV1(cid.Raw, block2Hash)

		tamperedData := []byte("tampered data")

		shard1Hash := testutil.RandomMultihash(t)
		shard2Hash := testutil.RandomMultihash(t)
		position1 := blobindex.Position{Offset: 0, Length: uint64(len(block1Data))}
		position2 := blobindex.Position{Offset: 0, Length: uint64(len(block2Data))}

		server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write(block1Data)
		}))
		defer server1.Close()

		server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write(tamperedData)
		}))
		defer server2.Close()

		server1URL, _ := url.Parse(server1.URL)
		server2URL, _ := url.Parse(server2.URL)
		location1 := createTestLocation(t, shard1Hash, *server1URL)
		location2 := createTestLocation(t, shard2Hash, *server2URL)

		stub := &stubShardFinder{
			shards: map[string]shardInfo{
				string(block1Hash): {shard: shard1Hash, position: position1},
				string(block2Hash): {shard: shard2Hash, position: position2},
			},
			locations: map[string][]verification.Location{
				string(shard1Hash): {location1},
				string(shard2Hash): {location2},
			},
		}

		var stats []verification.BlockStat
		var gotError error
		for stat, err := range verification.StatBlocks(ctx, nil, nil, stub, []cid.Cid{block1CID, block2CID}) {
			if err != nil {
				gotError = err
				break
			}
			stats = append(stats, stat)
		}

		require.Len(t, stats, 1, "first block should succeed")
		require.Equal(t, block1Hash, multihash.Multihash(stats[0].Digest))

		require.Error(t, gotError)
		require.Contains(t, gotError.Error(), "integrity check failed")
	})

	t.Run("returns error when shard not found", func(t *testing.T) {
		ctx := t.Context()

		// First block: valid
		block1Data := []byte("valid block")
		block1Hash, err := multihash.Sum(block1Data, multihash.SHA2_256, -1)
		require.NoError(t, err)
		block1CID := cid.NewCidV1(cid.Raw, block1Hash)

		// Second block: shard not found
		block2Hash := testutil.RandomMultihash(t)
		block2CID := cid.NewCidV1(cid.Raw, block2Hash)

		shard1Hash := testutil.RandomMultihash(t)
		position1 := blobindex.Position{Offset: 0, Length: uint64(len(block1Data))}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write(block1Data)
		}))
		defer server.Close()

		serverURL, _ := url.Parse(server.URL)
		location1 := createTestLocation(t, shard1Hash, *serverURL)

		stub := &stubShardFinder{
			shards: map[string]shardInfo{
				string(block1Hash): {shard: shard1Hash, position: position1},
				// block2Hash has no shard entry
			},
			locations: map[string][]verification.Location{
				string(shard1Hash): {location1},
			},
		}

		var stats []verification.BlockStat
		var gotError error
		for stat, err := range verification.StatBlocks(ctx, nil, nil, stub, []cid.Cid{block1CID, block2CID}) {
			if err != nil {
				gotError = err
				break
			}
			stats = append(stats, stat)
		}

		require.Len(t, stats, 1, "first block should succeed")
		require.Equal(t, block1Hash, multihash.Multihash(stats[0].Digest))

		require.Error(t, gotError)
		require.Contains(t, gotError.Error(), "finding shard")
	})

	t.Run("returns error when no locations found", func(t *testing.T) {
		ctx := t.Context()

		// First block: valid
		block1Data := []byte("valid block")
		block1Hash, err := multihash.Sum(block1Data, multihash.SHA2_256, -1)
		require.NoError(t, err)
		block1CID := cid.NewCidV1(cid.Raw, block1Hash)

		// Second block: no locations
		block2Data := []byte("no location block")
		block2Hash, err := multihash.Sum(block2Data, multihash.SHA2_256, -1)
		require.NoError(t, err)
		block2CID := cid.NewCidV1(cid.Raw, block2Hash)

		shard1Hash := testutil.RandomMultihash(t)
		shard2Hash := testutil.RandomMultihash(t)
		position1 := blobindex.Position{Offset: 0, Length: uint64(len(block1Data))}
		position2 := blobindex.Position{Offset: 0, Length: uint64(len(block2Data))}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write(block1Data)
		}))
		defer server.Close()

		serverURL, _ := url.Parse(server.URL)
		location1 := createTestLocation(t, shard1Hash, *serverURL)

		stub := &stubShardFinder{
			shards: map[string]shardInfo{
				string(block1Hash): {shard: shard1Hash, position: position1},
				string(block2Hash): {shard: shard2Hash, position: position2},
			},
			locations: map[string][]verification.Location{
				string(shard1Hash): {location1},
				// shard2Hash has no location entry
			},
		}

		var stats []verification.BlockStat
		var gotError error
		for stat, err := range verification.StatBlocks(ctx, nil, nil, stub, []cid.Cid{block1CID, block2CID}) {
			if err != nil {
				gotError = err
				break
			}
			stats = append(stats, stat)
		}

		require.Len(t, stats, 1, "first block should succeed")
		require.Equal(t, block1Hash, multihash.Multihash(stats[0].Digest))

		require.Error(t, gotError)
		require.Contains(t, gotError.Error(), "finding location")
	})
}

func TestVerifyDAGRetrieval(t *testing.T) {
	t.Run("retrieves a single block DAG", func(t *testing.T) {
		ctx := t.Context()

		blockData := []byte("single block dag")
		blockHash, _ := multihash.Sum(blockData, multihash.SHA2_256, -1)
		rootCID := cid.NewCidV1(cid.Raw, blockHash)

		shardHash := testutil.RandomMultihash(t)
		position := blobindex.Position{Offset: 0, Length: uint64(len(blockData))}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write(blockData)
		}))
		defer server.Close()

		serverURL, _ := url.Parse(server.URL)
		location := createTestLocation(t, shardHash, *serverURL)

		stub := &stubShardFinder{
			shards: map[string]shardInfo{
				string(blockHash): {shard: shardHash, position: position},
			},
			locations: map[string][]verification.Location{
				string(shardHash): {location},
			},
		}

		var blocks []verification.VerifiedBlock
		for block, err := range verification.VerifyDAGRetrieval(ctx, nil, nil, stub, rootCID) {
			require.NoError(t, err)
			blocks = append(blocks, block)
		}

		require.Len(t, blocks, 1)
		require.Equal(t, blockHash, multihash.Multihash(blocks[0].Stat.Digest))
	})

	t.Run("stops on first error", func(t *testing.T) {
		ctx := t.Context()

		blockHash := testutil.RandomMultihash(t)
		rootCID := cid.NewCidV1(cid.Raw, blockHash)

		stub := &stubShardFinder{
			shards:    map[string]shardInfo{},
			locations: map[string][]verification.Location{},
		}

		var gotError error
		for _, err := range verification.VerifyDAGRetrieval(ctx, nil, nil, stub, rootCID) {
			if err != nil {
				gotError = err
				break
			}
		}

		require.ErrorContains(t, gotError, "finding shard for")
	})
}

type shardInfo struct {
	shard    multihash.Multihash
	position blobindex.Position
}

type stubShardFinder struct {
	shards    map[string]shardInfo
	locations map[string][]verification.Location
}

func (s *stubShardFinder) FindShard(ctx context.Context, slice multihash.Multihash) (multihash.Multihash, blobindex.Position, error) {
	info, ok := s.shards[string(slice)]
	if !ok {
		return nil, blobindex.Position{}, fmt.Errorf("shard not found for slice")
	}
	return info.shard, info.position, nil
}

func (s *stubShardFinder) FindLocations(ctx context.Context, shard multihash.Multihash) ([]verification.Location, error) {
	locs, ok := s.locations[string(shard)]
	if !ok || len(locs) == 0 {
		return nil, fmt.Errorf("no locations found for shard")
	}
	return locs, nil
}

func createTestLocation(t *testing.T, shardHash multihash.Multihash, storageURL url.URL) verification.Location {
	t.Helper()

	provider := testutil.Must(ed25519signer.Generate())(t)
	space := testutil.Must(ed25519signer.Generate())(t)

	caveats := assertcap.LocationCaveats{
		Space:   space.DID(),
		Content: captypes.FromHash(shardHash),
		Range: &assertcap.Range{
			Offset: 0,
			Length: ptr(uint64(1000)),
		},
		Location: []url.URL{storageURL},
	}

	dlg := testutil.Must(assertcap.Location.Delegate(
		provider,
		provider,
		provider.DID().String(),
		caveats,
	))(t)

	// Use the LocationCache to properly parse the delegation into a Location
	cache := verification.NewLocationCache()
	cache.Add(dlg)

	locations := cache.LocationsForShard(shardHash)
	require.Len(t, locations, 1)
	return locations[0]
}

func ptr[T any](v T) *T {
	return &v
}
