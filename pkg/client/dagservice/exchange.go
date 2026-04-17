package dagservice

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
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

// DefaultMaxGap is the default maximum gap (in bytes) between blocks that can
// still be coalesced into a single request. This value accommodates the typical
// overhead between adjacent blocks in a CAR file, which consists of a varint
// length prefix (1-10 bytes) and a CID (34-37 bytes for CIDv0/CIDv1 with
// SHA2-256).
const DefaultMaxGap = 64

type storachaExchange struct {
	locator   locator.Locator
	retriever Retriever
	spaces    []did.DID
	shards    blobindex.MultihashMap[[]byte]
	maxGap    uint64
}

var _ exchange.Interface = (*storachaExchange)(nil)

// ExchangeOption configures a storachaExchange.
type ExchangeOption func(*storachaExchange)

// WithMaxGap sets the maximum gap (in bytes) between blocks that can still be
// coalesced into a single request. A maxGap of 0 means blocks must be exactly
// contiguous to be coalesced. The default is [DefaultMaxGap].
func WithMaxGap(maxGap uint64) ExchangeOption {
	return func(se *storachaExchange) {
		se.maxGap = maxGap
	}
}

// NewExchange creates a new exchange for retrieving blocks from Storacha.
func NewExchange(locator locator.Locator, retriever Retriever, spaces []did.DID, opts ...ExchangeOption) exchange.Interface {
	se := &storachaExchange{
		locator:   locator,
		retriever: retriever,
		spaces:    spaces,
		shards:    blobindex.NewMultihashMap[[]byte](-1),
		maxGap:    DefaultMaxGap,
	}
	for _, opt := range opts {
		opt(se)
	}
	return se
}

func (se *storachaExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	log.Debugw("Getting block", "cid", c, "spaces", se.spaces)
	locations, err := se.locator.Locate(ctx, se.spaces, c.Hash())
	if err != nil {
		return nil, fmt.Errorf("locating block %s: %w", c, err)
	}

	var reqErr error
	for _, location := range locations {
		blockReader, err := se.retriever.Retrieve(ctx, location)
		if err != nil {
			reqErr = fmt.Errorf("retrieving block %s: %w", c, err)
			continue
		}

		blockBytes, err := io.ReadAll(blockReader)
		blockReader.Close()
		if err != nil {
			reqErr = fmt.Errorf("reading block %s data: %w", c.String(), err)
			continue
		}

		block, err := makeBlock(blockBytes, c)
		if err != nil {
			reqErr = fmt.Errorf("creating block %s (data length: %d): %w", c.String(), len(blockBytes), err)
			continue
		}
		return block, nil
	}

	return nil, reqErr
}

// makeBlock creates a block from the given data and CID, verifying that the
// data matches the expected hash in the CID.
func makeBlock(data []byte, c cid.Cid) (blocks.Block, error) {
	expectedDigest := c.Hash()
	decHash, err := mh.Decode(expectedDigest)
	if err != nil {
		return nil, fmt.Errorf("decoding content multihash %s: %w", expectedDigest, err)
	}

	actualDigest, err := mh.Sum(data, decHash.Code, -1)
	if err != nil {
		return nil, fmt.Errorf("hashing content %s: %w", expectedDigest, err)
	}

	if !bytes.Equal(expectedDigest, actualDigest) {
		return nil, fmt.Errorf("content hash mismatch for content %s; got %s", digestutil.Format(expectedDigest), digestutil.Format(actualDigest))
	}

	block, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, fmt.Errorf("creating block %s (data length: %d): %w", c.String(), len(data), err)
	}

	return block, nil
}

type coalescedLocation struct {
	locations []locator.Location
	slices    []slice
}

func (cl coalescedLocation) isEmpty() bool {
	return len(cl.locations) == 0
}

type slice struct {
	cid      cid.Cid
	position blobindex.Position
}

func sameShard(a, b locator.Location) bool {
	return bytes.Equal(a.Commitment.Nb().Content.Hash(), b.Commitment.Nb().Content.Hash())
}

// withinGap checks if location b is within maxGap bytes after the end of
// location a, in the same shard. A maxGap of 0 means they must be exactly
// contiguous.
func withinGap(a, b locator.Location, maxGap uint64) bool {
	if !sameShard(a, b) {
		return false
	}

	endOfA := a.Position.Offset + a.Position.Length

	if b.Position.Offset >= endOfA && b.Position.Offset <= endOfA+maxGap {
		return true
	} else {
		log.Debugf("Locations not within gap: a ends at %d, b starts at %d; gap is %d, maxGap is %d", endOfA, b.Position.Offset, b.Position.Offset-endOfA, maxGap)
		return false
	}
}

// GetBlocks will attempt to fetch multiple contiguous blocks in a single
// request where possible.
func (se *storachaExchange) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	log.Debugw("Getting blocks", "count", len(cids), "spaces", se.spaces)
	out := make(chan blocks.Block)

	digests := make([]mh.Multihash, 0, len(cids))
	for _, c := range cids {
		digests = append(digests, c.Hash())
	}

	locations, err := se.locator.LocateMany(ctx, se.spaces, digests)
	if err != nil {
		return nil, fmt.Errorf("locating blocks: %w", err)
	}

	// Sort the CIDs by the offset of their first location. This is a best effort
	// to ensure that contiguous blocks are adjacent in the list. It can fail if a
	// block is available in multiple shards, and thus at multiple offsets; the
	// offset we sort by may not be the one that allows coalescing with the
	// running batch of blocks when we get to it. But the most common case is that
	// a block is found in a single shard (which may be replicated to multiple
	// locations), so the first location's offset is a reasonable heuristic.
	slices.SortFunc(cids, func(cidA, cidB cid.Cid) int {
		locsA := locations.Get(cidA.Hash())
		locsB := locations.Get(cidB.Hash())

		if len(locsA) == 0 || len(locsB) == 0 {
			return 0
		}

		return cmp.Compare(locsA[0].Position.Offset, locsB[0].Position.Offset)
	})

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
				locations: locs,
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
			// See if any of the locations for this block are within the max gap of
			// the current location.
			found := false
			for _, loc := range locs {
				// We check against the first location of the current batch (which defines the shard)
				if withinGap(currentLocation.locations[0], loc, se.maxGap) {
					// Extend the batch range based on the first location's coordinate system
					currentLocation.slices = append(currentLocation.slices, slice{
						cid: cid,
						position: blobindex.Position{
							Offset: loc.Position.Offset,
							Length: loc.Position.Length,
						},
					})
					// Update batch length to cover all slices
					firstLoc := currentLocation.locations[0]
					newEnd := loc.Position.Offset + loc.Position.Length
					for i := range currentLocation.locations {
						currentLocation.locations[i].Position.Length = newEnd - firstLoc.Position.Offset
					}
					found = true
					break
				}
			}

			// If none of the locations for this block are contiguous with the
			// current location, emit, then make a new one.
			if !found {
				coalescedLocations = append(coalescedLocations, currentLocation)
				currentLocation = coalescedLocation{
					locations: locs,
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
		log.Infof("Fetching %d coalesced blocks at offset %d with length %d from %d locations", len(cloc.slices), cloc.locations[0].Position.Offset, cloc.locations[0].Position.Length, len(cloc.locations))
		wg.Add(1)
		go func(cloc coalescedLocation) {
			defer wg.Done()

			var reqErr error
			for _, loc := range cloc.locations {
				blockReader, err := se.retriever.Retrieve(ctx, loc)
				if err != nil {
					reqErr = fmt.Errorf("retrieving blocks starting at offset %d from location %s: %v", loc.Position.Offset, loc.Commitment.With(), err)
					continue
				}

				// Slices appear in order, and cannot overlap, so we can slice up the data
				// as we read it.
				pos := loc.Position.Offset
				success := true
				for _, slc := range cloc.slices {
					// Skip to slice offset
					if slc.position.Offset > pos {
						skipped, err := io.CopyN(io.Discard, blockReader, int64(slc.position.Offset-pos))
						pos += uint64(skipped)
						if err != nil {
							reqErr = fmt.Errorf("skipping to block %s at %d-%d: expected to skip %d bytes, skipped %d: %v", slc.cid.String(), slc.position.Offset, slc.position.Offset+slc.position.Length, slc.position.Offset-pos, skipped, err)
							success = false
							break
						}
					}

					// Read slice data
					sliceBytes := make([]byte, slc.position.Length)
					read, err := io.ReadFull(blockReader, sliceBytes)
					pos += uint64(read)
					if err != nil {
						reqErr = fmt.Errorf("reading block %s at %d-%d: expected %d bytes, got %d: %v", slc.cid.String(), slc.position.Offset, slc.position.Offset+slc.position.Length, slc.position.Length, read, err)
						success = false
						break
					}

					// Create block
					blk, err := makeBlock(sliceBytes, slc.cid)
					if err != nil {
						reqErr = fmt.Errorf("creating block %s at %d-%d: %v", slc.cid.String(), slc.position.Offset, slc.position.Offset+slc.position.Length, err)
						success = false
						break
					}

					select {
					case out <- blk:
					case <-ctx.Done():
						blockReader.Close()
						return
					}
				}
				blockReader.Close()

				if success {
					return
				}
				// If we failed mid-batch, we continue to the next location
			}

			if reqErr != nil {
				log.Errorf("failed to fetch batch: %v", reqErr)
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
