package dagservice

import (
	"context"
	"fmt"
	"math/rand"

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

	// Randomly pick one of the available locations
	location := locations[rand.Intn(len(locations))]

	shardBytes, err := se.retriever.Retrieve(ctx, se.space, location.Commitment)
	if err != nil {
		return nil, fmt.Errorf("retrieving block %s: %w", c, err)
	}

	// Cache the shard
	se.shards.Set(location.Commitment.Nb().Content.Hash(), shardBytes)

	blockBytes := shardBytes[location.Position.Offset : location.Position.Offset+location.Position.Length]

	// Create a block from the data and CID
	// This will verify that the data hashes to the expected CID
	block, err := blocks.NewBlockWithCid(blockBytes, c)
	if err != nil {
		return nil, fmt.Errorf("creating block %s (data length: %d): %w", c.String(), len(shardBytes), err)
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
