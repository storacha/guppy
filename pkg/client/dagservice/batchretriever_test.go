package dagservice_test

import (
	"net/url"
	"testing"

	"github.com/storacha/go-libstoracha/blobindex"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	stestutil "github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	"github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

// // TK: This already exists

// type mockRetriever struct {
// 	responses map[string][]byte
// }

// func (mr *mockRetriever) Retrieve(ctx context.Context, space did.DID, locations []locator.Location, retrievalOpts ...rclient.Option) ([]byte, error) {
// 	return nil, nil
// }

// func (mr *mockRetriever) addResponse(space did.DID, location locator.Location, data []byte) {
// 	if mr.responses == nil {
// 		mr.responses = make(map[string][]byte)
// 	}

// 	key := fmt.Sprintf("%s-%s", space.String(), location.Commitment.Nb())

// 	mr.responses[key] = data
// }

func TestBatchRetriever(t *testing.T) {
	t.Run("retrieves an individual slice", func(t *testing.T) {
		spaceDID := stestutil.RandomDID(t)
		storageProvider := stestutil.RandomDID(t)

		serverURL, err := url.Parse("https://storage1.example.com/blob/foo")
		require.NoError(t, err)

		commitment := ucan.NewCapability(
			assertcap.Location.Can(),
			storageProvider.String(),
			assertcap.LocationCaveats{
				Space:   spaceDID,
				Content: captypes.FromHash(stestutil.RandomMultihash(t)),
				Range: &assertcap.Range{
					Offset: 0,
					Length: testutil.Ptr(uint64(36)),
				},
				Location: []url.URL{*serverURL},
			},
		)

		underlyingRetriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
			commitment: []byte("here is some slice data, and more slice data"),
		})
		require.NoError(t, err)
		batchRetriever := dagservice.NewBatchRetriever(t.Context(), underlyingRetriever)

		retrievedBytes, err := batchRetriever.Retrieve(t.Context(), []locator.Location{{
			Commitment: commitment,
			Position: blobindex.Position{
				Offset: 8,
				Length: 15,
			},
		}})
		require.NoError(t, err)

		require.Equal(t, []byte("slice data"), retrievedBytes)
	})

	t.Run("retrieves multiple contiguous slices at once", func(t *testing.T) {
		spaceDID := stestutil.RandomDID(t)
		storageProvider := stestutil.RandomDID(t)

		serverURL, err := url.Parse("https://storage1.example.com/blob/foo")
		require.NoError(t, err)

		commitment := ucan.NewCapability(
			assertcap.Location.Can(),
			storageProvider.String(),
			assertcap.LocationCaveats{
				Space:   spaceDID,
				Content: captypes.FromHash(stestutil.RandomMultihash(t)),
				Range: &assertcap.Range{
					Offset: 0,
					Length: testutil.Ptr(uint64(36)),
				},
				Location: []url.URL{*serverURL},
			},
		)

		underlyingRetriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
			commitment: []byte("here is some slice data, and more slice data"),
		})
		require.NoError(t, err)
		batchRetriever := dagservice.NewBatchRetriever(t.Context(), underlyingRetriever)

		retrievedBytesChs := []chan []byte{make(chan []byte, 1), make(chan []byte, 1)}

		for i, position := range []blobindex.Position{
			{Offset: 8, Length: 15},
			{Offset: 29, Length: 15},
		} {
			go func(position blobindex.Position) {
				b, err := batchRetriever.Retrieve(t.Context(), []locator.Location{{
					Commitment: commitment,
					Position:   position,
				}})
				require.NoError(t, err)
				retrievedBytesChs[i] <- b
				close(retrievedBytesChs[i])
			}(position)
		}

		require.Equal(t, []byte("some slice data"), <-retrievedBytesChs[0])
		require.Equal(t, []byte("more slice data"), <-retrievedBytesChs[1])
		require.Equal(t, 1, len(underlyingRetriever.requests))
	})
}
