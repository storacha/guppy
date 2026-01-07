package dagservice

import (
	"context"
	"fmt"

	"github.com/ipfs/boxo/exchange"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-libstoracha/blobindex"
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

	blockBytes, err := se.retriever.Retrieve(ctx, se.space, locations)
	if err != nil {
		return nil, fmt.Errorf("retrieving block %s: %w", c, err)
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
