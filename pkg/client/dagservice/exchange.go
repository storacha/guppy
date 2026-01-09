package dagservice

import (
	"bytes"
	"context"
	"fmt"
	"io"

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

	blockReader, err := se.retriever.Retrieve(ctx, locations)
	if err != nil {
		return nil, fmt.Errorf("retrieving block %s: %w", c, err)
	}
	defer blockReader.Close()

	blockBytes, err := io.ReadAll(blockReader)
	if err != nil {
		return nil, fmt.Errorf("reading block %s data: %w", c.String(), err)
	}

	expectedDigest := c.Hash()
	decHash, err := mh.Decode(expectedDigest)
	if err != nil {
		return nil, fmt.Errorf("decoding content multihash %s: %w", expectedDigest, err)
	}

	receivedDigest, err := mh.Sum(blockBytes, decHash.Code, -1)
	if err != nil {
		return nil, fmt.Errorf("hashing content %s: %w", expectedDigest, err)
	}

	if !bytes.Equal(expectedDigest, receivedDigest) {
		return nil, fmt.Errorf("content hash mismatch for content %s; got %s", digestutil.Format(expectedDigest), digestutil.Format(receivedDigest))
	}

	// Create a block from the data and CID
	// This will verify that the data hashes to the expected CID
	block, err := blocks.NewBlockWithCid(blockBytes, c)
	if err != nil {
		return nil, fmt.Errorf("creating block %s (data length: %d): %w", c.String(), len(blockBytes), err)
	}

	return block, nil
}

func (se *storachaExchange) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	out := make(chan blocks.Block)

	// Just fetch each block sequentially for now
	go func() {
		defer close(out)
		for _, c := range cids {
			blk, err := se.GetBlock(ctx, c)
			if err != nil {
				log.Errorf("error getting block %s: %v", c, err)
				return
			}
			out <- blk
		}
	}()

	return out, nil
}

func (se *storachaExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	return nil
}
func (se *storachaExchange) Close() error {
	return nil
}
