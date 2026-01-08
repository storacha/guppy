package dagservice_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestDAGService(t *testing.T) {
	space := testutil.RandomDID(t)

	testCases := []struct {
		name      string
		codec     uint64
		blockData []byte
	}{
		{
			name:  "DAG-PB node",
			codec: cid.DagProtobuf,
			blockData: func() []byte {
				// Create a simple DAG-PB node
				node := dag.NodeWithData(testutil.RandomBytes(t, 64))
				data, err := node.Marshal()
				require.NoError(t, err)
				return data
			}(),
		},
		{
			name:      "raw node",
			codec:     cid.Raw,
			blockData: testutil.RandomBytes(t, 256),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// The block data should be embedded in a larger shard
			shardData := testutil.RandomBytes(t, 1024)
			offset := uint64(512)
			length := uint64(len(tc.blockData))
			copy(shardData[offset:offset+length], tc.blockData)

			// Create a CID from the block data
			hash, err := mh.Sum(tc.blockData, mh.SHA2_256, -1)
			require.NoError(t, err)
			blockCID := cid.NewCidV1(tc.codec, hash)

			location := locator.Location{
				Commitment: ucan.NewCapability(
					assertcap.Location.Can(),
					space.String(),
					assertcap.LocationCaveats{
						Space:   space.DID(),
						Content: captypes.FromHash(hash),
						Location: ctestutil.Urls(
							"https://storage1.example.com/block/abc123",
							"https://storage2.example.com/block/abc123",
						),
					},
				),
				Position: blobindex.Position{
					Offset: offset,
					Length: length,
				},
			}

			lctr := newStubLocator()
			lctr.locations.Set(blockCID.Hash(), []locator.Location{location})

			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
				location.Commitment: shardData,
			})
			require.NoError(t, err)

			ds := dagservice.NewDAGService(
				lctr,
				retriever,
				space,
			)

			node, err := ds.Get(t.Context(), blockCID)
			require.NoError(t, err)
			require.Equal(t, blockCID, node.Cid())
		})
	}
}

func newStubLocator() stubLocator {
	return stubLocator{
		locations: blobindex.NewMultihashMap[[]locator.Location](-1),
	}
}

type stubLocator struct {
	locations blobindex.MultihashMap[[]locator.Location]
}

var _ locator.Locator = stubLocator{}

func (m stubLocator) Locate(ctx context.Context, space did.DID, hash mh.Multihash) ([]locator.Location, error) {
	if m.locations.Has(hash) {
		return m.locations.Get(hash), nil
	}
	return nil, nil
}

func newMockRetriever(responses map[ucan.Capability[assertcap.LocationCaveats]][]byte) (*mockRetriever, error) {
	data := make(map[string][]byte)
	for commitment, resp := range responses {
		key, err := commitmentKey(commitment)
		if err != nil {
			return nil, err
		}
		data[key] = resp
	}
	return &mockRetriever{data: data}, nil
}

type mockRetriever struct {
	data     map[string][]byte
	requests [][]locator.Location
	mu       sync.Mutex
}

var _ dagservice.Retriever = (*mockRetriever)(nil)

func commitmentKey(commitment ucan.Capability[assertcap.LocationCaveats]) (string, error) {
	json, err := commitment.MarshalJSON()
	return string(json), err
}

func (r *mockRetriever) Retrieve(ctx context.Context, locations []locator.Location) (io.ReadCloser, error) {
	location := locations[0]

	key, err := commitmentKey(location.Commitment)
	if err != nil {
		return nil, err
	}
	if data, ok := r.data[key]; ok {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.requests = append(r.requests, locations)
		return io.NopCloser(bytes.NewReader(data[location.Position.Offset : location.Position.Offset+location.Position.Length])), nil
	}
	return nil, fmt.Errorf("no data for location %s", key)
}
