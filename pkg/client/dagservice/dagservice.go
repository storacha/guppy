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

	// Register codecs
	_ "github.com/ipld/go-codec-dagpb"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"

	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/locator"
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
	return d.getNode(ctx, c)
}

func (d *dagService) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipldfmt.NodeOption {
	out := make(chan *ipldfmt.NodeOption, len(cids))

	go func() {
		defer close(out)

		for _, c := range cids {
			select {
			case <-ctx.Done():
				out <- &ipldfmt.NodeOption{Err: ctx.Err()}
				return
			default:
			}

			node, err := d.getNode(ctx, c)

			select {
			case out <- &ipldfmt.NodeOption{Node: node, Err: err}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
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

func (d *dagService) getNode(ctx context.Context, c cid.Cid) (ipldfmt.Node, error) {
	locations, err := d.locator.Locate(ctx, d.space, c.Hash())
	if err != nil {
		return nil, fmt.Errorf("locating block %s: %w", c, err)
	}
	if len(locations) == 0 {
		return nil, ipldfmt.ErrNotFound{Cid: c}
	}

	location := d.selectLocation(locations)

	shardBytes, err := d.getShard(ctx, location)
	if err != nil {
		return nil, fmt.Errorf("retrieving block %s: %w", c, err)
	}

	return nodeFromShard(c, location, shardBytes)
}

func (d *dagService) selectLocation(locations []locator.Location) locator.Location {
	for _, l := range locations {
		if d.shards.Has(l.Commitment.Nb().Content.Hash()) {
			return l
		}
	}

	return locations[rand.Intn(len(locations))]
}

func (d *dagService) getShard(ctx context.Context, location locator.Location) ([]byte, error) {
	shardHash := location.Commitment.Nb().Content.Hash()
	if d.shards.Has(shardHash) {
		return d.shards.Get(shardHash), nil
	}

	shardBytes, err := d.retriever.Retrieve(ctx, d.space, location.Commitment)
	if err != nil {
		return nil, err
	}

	d.shards.Set(shardHash, shardBytes)
	return shardBytes, nil
}

func nodeFromShard(c cid.Cid, location locator.Location, shardBytes []byte) (ipldfmt.Node, error) {
	end := location.Position.Offset + location.Position.Length
	if end > uint64(len(shardBytes)) {
		return nil, fmt.Errorf("block %s position %d+%d exceeds shard length %d", c, location.Position.Offset, location.Position.Length, len(shardBytes))
	}

	blockBytes := shardBytes[location.Position.Offset:end]

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
