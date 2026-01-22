package dagservice_test

import (
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	stestutil "github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestStorachaExchange(t *testing.T) {
	t.Run("GetBlock()", func(t *testing.T) {
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

			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space})
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

			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space})
			_, err = exchange.GetBlock(t.Context(), blockCID)
			require.ErrorContains(t, err, "content hash mismatch")
		})
	})

	t.Run("GetBlocks()", func(t *testing.T) {
		t.Run("retrieves multiple blocks", func(t *testing.T) {
			space := stestutil.RandomDID(t)
			shardData := []byte("Hello, world! This is test data for retrieval.")
			shardHash, err := mh.Sum(shardData, mh.SHA2_256, -1)
			require.NoError(t, err)

			// Block 1: "Hello" (offset 0, length 5)
			block1Data := shardData[0:5]
			block1Hash, err := mh.Sum(block1Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block1CID := cid.NewCidV1(cid.Raw, block1Hash)

			// Block 2: "world" (offset 7, length 5)
			block2Data := shardData[7:12]
			block2Hash, err := mh.Sum(block2Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block2CID := cid.NewCidV1(cid.Raw, block2Hash)

			shardCommitment := ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(shardHash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/shard",
					),
				},
			)

			location1 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 0, Length: 5},
			}
			location2 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 7, Length: 5},
			}

			lctr := newStubLocator()
			lctr.locations.Set(block1CID.Hash(), []locator.Location{location1})
			lctr.locations.Set(block2CID.Hash(), []locator.Location{location2})

			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
				shardCommitment: shardData,
			})
			require.NoError(t, err)

			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space})
			blocksCh, err := exchange.GetBlocks(t.Context(), []cid.Cid{block1CID, block2CID})
			require.NoError(t, err)

			blocks := make(map[string][]byte)
			for blk := range blocksCh {
				blocks[blk.Cid().String()] = blk.RawData()
			}

			require.Len(t, blocks, 2)
			require.Equal(t, block1Data, blocks[block1CID.String()])
			require.Equal(t, block2Data, blocks[block2CID.String()])
		})

		t.Run("coalesces contiguous blocks into single request", func(t *testing.T) {
			space := stestutil.RandomDID(t)
			shardData := []byte("AAAAABBBBBCCCCC")
			shardHash, err := mh.Sum(shardData, mh.SHA2_256, -1)
			require.NoError(t, err)

			// Block 1: "AAAAA" (offset 0, length 5)
			block1Data := shardData[0:5]
			block1Hash, err := mh.Sum(block1Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block1CID := cid.NewCidV1(cid.Raw, block1Hash)

			// Block 2: "BBBBB" (offset 5, length 5) - contiguous with block 1
			block2Data := shardData[5:10]
			block2Hash, err := mh.Sum(block2Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block2CID := cid.NewCidV1(cid.Raw, block2Hash)

			// Block 3: "CCCCC" (offset 10, length 5) - contiguous with block 2
			block3Data := shardData[10:15]
			block3Hash, err := mh.Sum(block3Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block3CID := cid.NewCidV1(cid.Raw, block3Hash)

			shardCommitment := ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(shardHash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/shard",
					),
				},
			)

			location1 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 0, Length: 5},
			}
			location2 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 5, Length: 5},
			}
			location3 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 10, Length: 5},
			}

			lctr := newStubLocator()
			lctr.locations.Set(block1CID.Hash(), []locator.Location{location1})
			lctr.locations.Set(block2CID.Hash(), []locator.Location{location2})
			lctr.locations.Set(block3CID.Hash(), []locator.Location{location3})

			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
				shardCommitment: shardData,
			})
			require.NoError(t, err)

			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space})
			blocksCh, err := exchange.GetBlocks(t.Context(), []cid.Cid{block1CID, block2CID, block3CID})
			require.NoError(t, err)

			blocks := make(map[string][]byte)
			for blk := range blocksCh {
				blocks[blk.Cid().String()] = blk.RawData()
			}

			require.Len(t, blocks, 3)
			require.Equal(t, block1Data, blocks[block1CID.String()])
			require.Equal(t, block2Data, blocks[block2CID.String()])
			require.Equal(t, block3Data, blocks[block3CID.String()])

			// Should have made only 1 request for all 3 contiguous blocks
			require.Len(t, retriever.requests, 1)
			// The coalesced request should span all blocks
			require.Equal(t, uint64(0), retriever.requests[0].Position.Offset)
			require.Equal(t, uint64(15), retriever.requests[0].Position.Length)
		})

		t.Run("does not coalesce non-contiguous blocks when maxGap is 0", func(t *testing.T) {
			space := stestutil.RandomDID(t)
			shardData := []byte("AAAAAXXXXXBBBBB")
			shardHash, err := mh.Sum(shardData, mh.SHA2_256, -1)
			require.NoError(t, err)

			// Block 1: "AAAAA" (offset 0, length 5)
			block1Data := shardData[0:5]
			block1Hash, err := mh.Sum(block1Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block1CID := cid.NewCidV1(cid.Raw, block1Hash)

			// Block 2: "BBBBB" (offset 10, length 5) - NOT contiguous with block 1
			block2Data := shardData[10:15]
			block2Hash, err := mh.Sum(block2Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block2CID := cid.NewCidV1(cid.Raw, block2Hash)

			shardCommitment := ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(shardHash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/shard",
					),
				},
			)

			location1 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 0, Length: 5},
			}
			location2 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 10, Length: 5},
			}

			lctr := newStubLocator()
			lctr.locations.Set(block1CID.Hash(), []locator.Location{location1})
			lctr.locations.Set(block2CID.Hash(), []locator.Location{location2})

			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
				shardCommitment: shardData,
			})
			require.NoError(t, err)

			// With maxGap=0, blocks must be exactly contiguous
			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space}, dagservice.WithMaxGap(0))
			blocksCh, err := exchange.GetBlocks(t.Context(), []cid.Cid{block1CID, block2CID})
			require.NoError(t, err)

			blocks := make(map[string][]byte)
			for blk := range blocksCh {
				blocks[blk.Cid().String()] = blk.RawData()
			}

			require.Len(t, blocks, 2)
			require.Equal(t, block1Data, blocks[block1CID.String()])
			require.Equal(t, block2Data, blocks[block2CID.String()])

			// Should have made 2 separate requests
			require.Len(t, retriever.requests, 2)
		})

		t.Run("does not coalesce blocks from different shards", func(t *testing.T) {
			space := stestutil.RandomDID(t)

			shard1Data := []byte("AAAAABBBBB")
			shard1Hash, err := mh.Sum(shard1Data, mh.SHA2_256, -1)
			require.NoError(t, err)

			shard2Data := []byte("CCCCCDDDD")
			shard2Hash, err := mh.Sum(shard2Data, mh.SHA2_256, -1)
			require.NoError(t, err)

			// Block 1: "AAAAA" from shard 1
			block1Data := shard1Data[0:5]
			block1Hash, err := mh.Sum(block1Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block1CID := cid.NewCidV1(cid.Raw, block1Hash)

			// Block 2: "CCCCC" from shard 2
			block2Data := shard2Data[0:5]
			block2Hash, err := mh.Sum(block2Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block2CID := cid.NewCidV1(cid.Raw, block2Hash)

			shard1Commitment := ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(shard1Hash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/shard1",
					),
				},
			)

			shard2Commitment := ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(shard2Hash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/shard2",
					),
				},
			)

			location1 := locator.Location{
				Commitment: shard1Commitment,
				Position:   blobindex.Position{Offset: 0, Length: 5},
			}
			location2 := locator.Location{
				Commitment: shard2Commitment,
				Position:   blobindex.Position{Offset: 0, Length: 5},
			}

			lctr := newStubLocator()
			lctr.locations.Set(block1CID.Hash(), []locator.Location{location1})
			lctr.locations.Set(block2CID.Hash(), []locator.Location{location2})

			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
				shard1Commitment: shard1Data,
				shard2Commitment: shard2Data,
			})
			require.NoError(t, err)

			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space})
			blocksCh, err := exchange.GetBlocks(t.Context(), []cid.Cid{block1CID, block2CID})
			require.NoError(t, err)

			blocks := make(map[string][]byte)
			for blk := range blocksCh {
				blocks[blk.Cid().String()] = blk.RawData()
			}

			require.Len(t, blocks, 2)
			require.Equal(t, block1Data, blocks[block1CID.String()])
			require.Equal(t, block2Data, blocks[block2CID.String()])

			// Should have made 2 separate requests (different shards)
			require.Len(t, retriever.requests, 2)
		})

		t.Run("returns error when block has no locations", func(t *testing.T) {
			space := stestutil.RandomDID(t)

			blockData := []byte("missing block")
			blockHash, err := mh.Sum(blockData, mh.SHA2_256, -1)
			require.NoError(t, err)
			blockCID := cid.NewCidV1(cid.Raw, blockHash)

			lctr := newStubLocator()
			// Don't set any locations for the block

			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{})
			require.NoError(t, err)

			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space})
			_, err = exchange.GetBlocks(t.Context(), []cid.Cid{blockCID})
			require.ErrorContains(t, err, "no locations found")
		})

		t.Run("retrieves single block", func(t *testing.T) {
			space := stestutil.RandomDID(t)
			shardData := []byte("Hello, world!")
			shardHash, err := mh.Sum(shardData, mh.SHA2_256, -1)
			require.NoError(t, err)

			blockData := shardData[0:5] // "Hello"
			blockHash, err := mh.Sum(blockData, mh.SHA2_256, -1)
			require.NoError(t, err)
			blockCID := cid.NewCidV1(cid.Raw, blockHash)

			shardCommitment := ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(shardHash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/shard",
					),
				},
			)

			location := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 0, Length: 5},
			}

			lctr := newStubLocator()
			lctr.locations.Set(blockCID.Hash(), []locator.Location{location})

			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
				shardCommitment: shardData,
			})
			require.NoError(t, err)

			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space})
			blocksCh, err := exchange.GetBlocks(t.Context(), []cid.Cid{blockCID})
			require.NoError(t, err)

			var receivedBlocks [][]byte
			for blk := range blocksCh {
				receivedBlocks = append(receivedBlocks, blk.RawData())
			}

			require.Len(t, receivedBlocks, 1)
			require.Equal(t, blockData, receivedBlocks[0])
		})

		t.Run("handles empty cid list", func(t *testing.T) {
			space := stestutil.RandomDID(t)

			lctr := newStubLocator()
			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{})
			require.NoError(t, err)

			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space})
			blocksCh, err := exchange.GetBlocks(t.Context(), []cid.Cid{})
			require.NoError(t, err)

			var receivedBlocks [][]byte
			for blk := range blocksCh {
				receivedBlocks = append(receivedBlocks, blk.RawData())
			}

			require.Len(t, receivedBlocks, 0)
		})

		t.Run("coalesces blocks within maxGap", func(t *testing.T) {
			space := stestutil.RandomDID(t)
			// Layout: "AAAAAXXXXXBBBBB" where XXXXX is a 5-byte gap
			shardData := []byte("AAAAAXXXXXBBBBB")
			shardHash, err := mh.Sum(shardData, mh.SHA2_256, -1)
			require.NoError(t, err)

			// Block 1: "AAAAA" (offset 0, length 5)
			block1Data := shardData[0:5]
			block1Hash, err := mh.Sum(block1Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block1CID := cid.NewCidV1(cid.Raw, block1Hash)

			// Block 2: "BBBBB" (offset 10, length 5) - 5 bytes gap from block 1
			block2Data := shardData[10:15]
			block2Hash, err := mh.Sum(block2Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block2CID := cid.NewCidV1(cid.Raw, block2Hash)

			shardCommitment := ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(shardHash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/shard",
					),
				},
			)

			location1 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 0, Length: 5},
			}
			location2 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 10, Length: 5},
			}

			lctr := newStubLocator()
			lctr.locations.Set(block1CID.Hash(), []locator.Location{location1})
			lctr.locations.Set(block2CID.Hash(), []locator.Location{location2})

			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
				shardCommitment: shardData,
			})
			require.NoError(t, err)

			// maxGap of 5 should coalesce the blocks (gap is exactly 5 bytes)
			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space}, dagservice.WithMaxGap(5))
			blocksCh, err := exchange.GetBlocks(t.Context(), []cid.Cid{block1CID, block2CID})
			require.NoError(t, err)

			blocks := make(map[string][]byte)
			for blk := range blocksCh {
				blocks[blk.Cid().String()] = blk.RawData()
			}

			require.Len(t, blocks, 2)
			require.Equal(t, block1Data, blocks[block1CID.String()])
			require.Equal(t, block2Data, blocks[block2CID.String()])

			// Should have made only 1 request due to maxGap
			require.Len(t, retriever.requests, 1)
			// The coalesced request should span from offset 0 to 15
			require.Equal(t, uint64(0), retriever.requests[0].Position.Offset)
			require.Equal(t, uint64(15), retriever.requests[0].Position.Length)
		})

		t.Run("does not coalesce blocks beyond maxGap", func(t *testing.T) {
			space := stestutil.RandomDID(t)
			// Layout: "AAAAAXXXXXBBBBB" where XXXXX is a 5-byte gap
			shardData := []byte("AAAAAXXXXXBBBBB")
			shardHash, err := mh.Sum(shardData, mh.SHA2_256, -1)
			require.NoError(t, err)

			// Block 1: "AAAAA" (offset 0, length 5)
			block1Data := shardData[0:5]
			block1Hash, err := mh.Sum(block1Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block1CID := cid.NewCidV1(cid.Raw, block1Hash)

			// Block 2: "BBBBB" (offset 10, length 5) - 5 bytes gap from block 1
			block2Data := shardData[10:15]
			block2Hash, err := mh.Sum(block2Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block2CID := cid.NewCidV1(cid.Raw, block2Hash)

			shardCommitment := ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(shardHash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/shard",
					),
				},
			)

			location1 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 0, Length: 5},
			}
			location2 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 10, Length: 5},
			}

			lctr := newStubLocator()
			lctr.locations.Set(block1CID.Hash(), []locator.Location{location1})
			lctr.locations.Set(block2CID.Hash(), []locator.Location{location2})

			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
				shardCommitment: shardData,
			})
			require.NoError(t, err)

			// maxGap of 4 should NOT coalesce the blocks (gap is 5 bytes)
			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space}, dagservice.WithMaxGap(4))
			blocksCh, err := exchange.GetBlocks(t.Context(), []cid.Cid{block1CID, block2CID})
			require.NoError(t, err)

			blocks := make(map[string][]byte)
			for blk := range blocksCh {
				blocks[blk.Cid().String()] = blk.RawData()
			}

			require.Len(t, blocks, 2)
			require.Equal(t, block1Data, blocks[block1CID.String()])
			require.Equal(t, block2Data, blocks[block2CID.String()])

			// Should have made 2 separate requests
			require.Len(t, retriever.requests, 2)
		})

		t.Run("coalesces multiple blocks with gaps", func(t *testing.T) {
			space := stestutil.RandomDID(t)
			// Layout: "AAAAA__BBBBB__CCCCC" (2-byte gaps)
			shardData := []byte("AAAAA..BBBBB..CCCCC")
			shardHash, err := mh.Sum(shardData, mh.SHA2_256, -1)
			require.NoError(t, err)

			// Block 1: "AAAAA" (offset 0, length 5)
			block1Data := shardData[0:5]
			block1Hash, err := mh.Sum(block1Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block1CID := cid.NewCidV1(cid.Raw, block1Hash)

			// Block 2: "BBBBB" (offset 7, length 5) - 2 bytes gap
			block2Data := shardData[7:12]
			block2Hash, err := mh.Sum(block2Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block2CID := cid.NewCidV1(cid.Raw, block2Hash)

			// Block 3: "CCCCC" (offset 14, length 5) - 2 bytes gap
			block3Data := shardData[14:19]
			block3Hash, err := mh.Sum(block3Data, mh.SHA2_256, -1)
			require.NoError(t, err)
			block3CID := cid.NewCidV1(cid.Raw, block3Hash)

			shardCommitment := ucan.NewCapability(
				assertcap.Location.Can(),
				space.String(),
				assertcap.LocationCaveats{
					Space:   space.DID(),
					Content: captypes.FromHash(shardHash),
					Location: ctestutil.Urls(
						"https://storage1.example.com/shard",
					),
				},
			)

			location1 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 0, Length: 5},
			}
			location2 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 7, Length: 5},
			}
			location3 := locator.Location{
				Commitment: shardCommitment,
				Position:   blobindex.Position{Offset: 14, Length: 5},
			}

			lctr := newStubLocator()
			lctr.locations.Set(block1CID.Hash(), []locator.Location{location1})
			lctr.locations.Set(block2CID.Hash(), []locator.Location{location2})
			lctr.locations.Set(block3CID.Hash(), []locator.Location{location3})

			retriever, err := newMockRetriever(map[ucan.Capability[assertcap.LocationCaveats]][]byte{
				shardCommitment: shardData,
			})
			require.NoError(t, err)

			// maxGap of 2 should coalesce all 3 blocks
			exchange := dagservice.NewExchange(lctr, retriever, []did.DID{space}, dagservice.WithMaxGap(2))
			blocksCh, err := exchange.GetBlocks(t.Context(), []cid.Cid{block1CID, block2CID, block3CID})
			require.NoError(t, err)

			blocks := make(map[string][]byte)
			for blk := range blocksCh {
				blocks[blk.Cid().String()] = blk.RawData()
			}

			require.Len(t, blocks, 3)
			require.Equal(t, block1Data, blocks[block1CID.String()])
			require.Equal(t, block2Data, blocks[block2CID.String()])
			require.Equal(t, block3Data, blocks[block3CID.String()])

			// Should have made only 1 request
			require.Len(t, retriever.requests, 1)
			// The coalesced request should span from offset 0 to 19
			require.Equal(t, uint64(0), retriever.requests[0].Position.Offset)
			require.Equal(t, uint64(19), retriever.requests[0].Position.Length)
		})
	})
}
