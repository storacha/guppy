package dagservice

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"

	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client/locator"
)

var log = logging.Logger("client/dagservice")

type storachaExchange struct {
	locator   locator.Locator
	retriever Retriever
	space     did.DID
	shards    blobindex.MultihashMap[[]byte]
}

var _ exchange.Interface = (*storachaExchange)(nil)

func NewExchange(locator locator.Locator, retriever Retriever, space did.DID) exchange.Interface {
	return &storachaExchange{
		locator:   locator,
		retriever: retriever,
		space:     space,
		shards:    blobindex.NewMultihashMap[[]byte](-1),
	}
}

func (se *storachaExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	locations, err := se.locator.Locate(ctx, se.space, c.Hash())
	if err != nil {
		return nil, fmt.Errorf("locating block %s: %w", c, err)
	}

	// Randomly pick one of the available locations
	location := locations[rand.Intn(len(locations))]

	blockReader, err := se.retriever.Retrieve(ctx, location)
	if err != nil {
		return nil, fmt.Errorf("retrieving block %s: %w", c, err)
	}
	defer blockReader.Close()

	blockBytes, err := io.ReadAll(blockReader)
	if err != nil {
		return nil, fmt.Errorf("reading block %s data: %w", c.String(), err)
	}

	block, err := makeBlock(blockBytes, c)
	if err != nil {
		return nil, fmt.Errorf("creating block %s (data length: %d): %w", c.String(), len(blockBytes), err)
	}

	return block, nil
}

// makeBlock creates a block from the given data and CID, verifying that the
// data matches the expected hash in the CID.
func makeBlock(data []byte, c cid.Cid) (blocks.Block, error) {
	expectedDigest := c.Hash()
	decHash, err := mh.Decode(expectedDigest)
	if err != nil {
		return nil, fmt.Errorf("decoding content multihash %s: %w", expectedDigest, err)
	}

	receivedDigest, err := mh.Sum(data, decHash.Code, -1)
	if err != nil {
		return nil, fmt.Errorf("hashing content %s: %w", expectedDigest, err)
	}

	if !bytes.Equal(expectedDigest, receivedDigest) {
		return nil, fmt.Errorf("content hash mismatch for content %s; got %s", digestutil.Format(expectedDigest), digestutil.Format(receivedDigest))
	}

	block, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, fmt.Errorf("creating block %s (data length: %d): %w", c.String(), len(data), err)
	}

	return block, nil
}

type coalescedLocation struct {
	location locator.Location
	slices   []slice
}

func (cl coalescedLocation) isEmpty() bool {
	return cl.location == (locator.Location{})
}

type slice struct {
	cid      cid.Cid
	position blobindex.Position
}

func sameShard(a, b locator.Location) bool {
	return bytes.Equal(a.Commitment.Nb().Content.Hash(), b.Commitment.Nb().Content.Hash())
}

func contiguous(a, b locator.Location) bool {
	if !sameShard(a, b) {
		return false
	}

	return a.Position.Offset+a.Position.Length == b.Position.Offset
}

// GetBlocks will attempt to fetch multiple contiguous blocks in a single
// request where possible. Note that the contiguous blocks must be provided in
// `cids` in order, or they will not be coalesced.
func (se *storachaExchange) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	out := make(chan blocks.Block)

	digests := make([]mh.Multihash, 0, len(cids))
	for _, c := range cids {
		digests = append(digests, c.Hash())
	}

	locations, err := se.locator.LocateMany(ctx, se.space, digests)
	if err != nil {
		return nil, fmt.Errorf("locating blocks: %w", err)
	}

	var coalescedLocations []coalescedLocation
	var currentLocation coalescedLocation
	for _, cid := range cids {
		locs := locations.Get(cid.Hash())
		if len(locs) == 0 {
			return nil, fmt.Errorf("no locations found for block %s", cid.String())
		}

		// If we don't have a current location, make one
		if currentLocation.isEmpty() {
			currentLocation = coalescedLocation{
				location: locs[rand.Intn(len(locs))],
				slices: []slice{
					{
						cid: cid,
						position: blobindex.Position{
							Offset: locs[0].Position.Offset,
							Length: locs[0].Position.Length,
						},
					},
				},
			}
		} else {
			// See if any of the locations for this block are contiguous with the
			// current location.
			found := false
			if !currentLocation.isEmpty() {
				for _, loc := range locs {
					if contiguous(currentLocation.location, loc) {
						currentLocation.location.Position.Length += loc.Position.Length
						currentLocation.slices = append(currentLocation.slices, slice{
							cid: cid,
							position: blobindex.Position{
								Offset: loc.Position.Offset,
								Length: loc.Position.Length,
							},
						})
						found = true
						break
					}
				}
			}

			// If none of the locations for this block are contiguous with the
			// current location, emit, then make a new one.
			if !found {
				coalescedLocations = append(coalescedLocations, currentLocation)
				currentLocation = coalescedLocation{
					location: locs[rand.Intn(len(locs))],
					slices: []slice{
						{
							cid: cid,
							position: blobindex.Position{
								Offset: locs[0].Position.Offset,
								Length: locs[0].Position.Length,
							},
						},
					},
				}
			}
		}
	}

	if !currentLocation.isEmpty() {
		coalescedLocations = append(coalescedLocations, currentLocation)
	}

	var wg sync.WaitGroup
	for _, cloc := range coalescedLocations {
		wg.Add(1)
		go func(cloc coalescedLocation) {
			defer wg.Done()
			blockReader, err := se.retriever.Retrieve(ctx, cloc.location)
			if err != nil {
				log.Errorf("retrieving blocks starting at offset %d: %v", cloc.location.Position.Offset, err)
				return
			}
			defer blockReader.Close()

			readBytes, err := io.ReadAll(blockReader)
			if err != nil {
				log.Errorf("reading blocks starting at offset %d data: %v", cloc.location.Position.Offset, err)
				return
			}

			for _, slice := range cloc.slices {
				offset := slice.position.Offset - cloc.location.Position.Offset
				blk, err := makeBlock(readBytes[offset:offset+slice.position.Length], slice.cid)
				if err != nil {
					log.Errorf("creating block %s at offset %d: %v", slice.cid.String(), slice.position.Offset, err)
					return
				}

				out <- blk
			}
		}(cloc)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out, nil
}

func (se *storachaExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	return nil
}
func (se *storachaExchange) Close() error {
	return nil
}
