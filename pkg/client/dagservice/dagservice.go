package dagservice

import (
	"context"
	"errors"
	"fmt"
	"math/rand"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipldfmt "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multicodec"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	rclient "github.com/storacha/go-ucanto/client/retrieval"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/locator"

	// Register codecs
	_ "github.com/ipld/go-codec-dagpb"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
)

type Retriever interface {
	Retrieve(ctx context.Context, space did.DID, locationCommitment ucan.Capability[assert.LocationCaveats], retrievalOpts ...rclient.Option) ([]byte, error)
}

var _ Retriever = (*client.Client)(nil)

type dagService struct {
	locator   locator.Locator
	retriever Retriever
	space     did.DID
	shards    blobindex.MultihashMap[[]byte]
}

var _ ipldfmt.DAGService = (*dagService)(nil)

func NewDAGService(locator locator.Locator, retriever Retriever, space did.DID) *dagService {
	return &dagService{
		locator:   locator,
		retriever: retriever,
		space:     space,
		shards:    blobindex.NewMultihashMap[[]byte](-1),
	}
}

func (d *dagService) Get(ctx context.Context, c cid.Cid) (ipldfmt.Node, error) {
	locations, err := d.locator.Locate(ctx, d.space, c.Hash())
	if err != nil {
		return nil, fmt.Errorf("locating block %s: %w", c, err)
	}

	// Randomly pick one of the available locations
	location := locations[rand.Intn(len(locations))]

	shardBytes, err := d.retriever.Retrieve(ctx, d.space, location.Commitment)
	if err != nil {
		return nil, fmt.Errorf("retrieving block %s: %w", c, err)
	}

	// Cache the shard
	d.shards.Set(location.Commitment.Nb().Content.Hash(), shardBytes)

	blockBytes := shardBytes[location.Position.Offset : location.Position.Offset+location.Position.Length]

	// Create a block from the data and CID
	// This will verify that the data hashes to the expected CID
	block, err := blocks.NewBlockWithCid(blockBytes, c)
	if err != nil {
		return nil, fmt.Errorf("creating block %s (data length: %d): %w", c.String(), len(shardBytes), err)
	}

	// Use codec-specific decoders to get the proper concrete types
	// (e.g., ProtoNode for DAG-PB which UnixFS requires)
	codec := c.Prefix().Codec
	switch codec {
	case cid.DagProtobuf:
		// DAG-PB must use DecodeProtobufBlock to get a ProtoNode
		node, err := dag.DecodeProtobufBlock(block)
		if err != nil {
			return nil, fmt.Errorf("decoding DAG-PB block %s: %w", c, err)
		}
		return node, nil
	case cid.Raw:
		// Raw blocks decode to RawNode
		node, err := dag.DecodeRawBlock(block)
		if err != nil {
			return nil, fmt.Errorf("decoding raw block %s: %w", c, err)
		}
		return node, nil
	default:
		return nil, fmt.Errorf("unsupported codec %s (0x%x) for block %s", multicodec.Code(codec), codec, c)
	}
}

func (d *dagService) GetMany(context.Context, []cid.Cid) <-chan *ipldfmt.NodeOption {
	panic("not implemented")
}

func (d *dagService) Add(context.Context, ipldfmt.Node) error {
	return errors.ErrUnsupported
}

func (d *dagService) AddMany(context.Context, []ipldfmt.Node) error {
	return errors.ErrUnsupported
}

func (d *dagService) Remove(ctx context.Context, cid cid.Cid) error {
	return errors.ErrUnsupported
}

func (d *dagService) RemoveMany(context.Context, []cid.Cid) error {
	return errors.ErrUnsupported
}
