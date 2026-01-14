package dagservice_test

import (
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	stestutil "github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestStorachaExchange(t *testing.T) {
	t.Run("locates and retrieves a block", func(t *testing.T) {
		space := stestutil.RandomDID(t)
		shardData := []byte("Hello, world! This is test data for retrieval.")
		shardHash, err := mh.Sum(shardData, mh.SHA2_256, -1)
		require.NoError(t, err)

		location := locator.Location{
			Commitment: ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(shardHash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/block/abc123",
						"https://storage2.example.com/block/abc123",
					),
				},
			),

			// "world"
			Position: blobindex.Position{
				Offset: 7,
				Length: 5,
			},
		}

		blockData := shardData[7 : 7+5] // "world"
		blockHash, err := mh.Sum(blockData, mh.SHA2_256, -1)
		require.NoError(t, err)
		blockCID := cid.NewCidV1(cid.Raw, blockHash)

		lctr := newStubLocator()
		lctr.locations.Set(blockCID.Hash(), []locator.Location{location})
		retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
			location.Commitment: shardData,
		})
		require.NoError(t, err)

		exchange := dagservice.NewExchange(lctr, retriever, space)
		blk, err := exchange.GetBlock(t.Context(), blockCID)
		require.NoError(t, err)
		require.Equal(t, blockCID, blk.Cid())
		require.Equal(t, blockData, blk.RawData())
	})

	t.Run("validates block hash", func(t *testing.T) {
		space := stestutil.RandomDID(t)
		wrongData := []byte("This is the !WRONG! data returned by server")
		correctData := []byte("This is the correct data.")
		correctHash, err := mh.Sum(correctData, mh.SHA2_256, -1)
		require.NoError(t, err)

		location := locator.Location{
			Commitment: ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(correctHash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/block/abc123",
						"https://storage2.example.com/block/abc123",
					),
				},
			),

			// "correct"/"!WRONG!"
			Position: blobindex.Position{
				Offset: 12,
				Length: 7,
			},
		}

		blockData := correctData[12 : 12+7] // "correct"
		blockHash, err := mh.Sum(blockData, mh.SHA2_256, -1)
		require.NoError(t, err)
		blockCID := cid.NewCidV1(cid.Raw, blockHash)

		lctr := newStubLocator()
		lctr.locations.Set(blockCID.Hash(), []locator.Location{location})
		retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
			location.Commitment: wrongData,
		})
		require.NoError(t, err)

		exchange := dagservice.NewExchange(lctr, retriever, space)
		_, err = exchange.GetBlock(t.Context(), blockCID)
		require.ErrorContains(t, err, "content hash mismatch")
	})
}
