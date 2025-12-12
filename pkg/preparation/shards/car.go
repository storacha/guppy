package shards

import (
	"bytes"
	"context"
	"fmt"
	"io"

	cbor "github.com/ipfs/go-ipld-cbor"
	ipldcar "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-varint"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
)

// A CAR header with zero roots.
var noRootsHeader []byte
var noRootsDigestState []byte
var noRootsPieceCIDState []byte

func init() {
	var err error
	noRootsHeaderWithoutLength, err := cbor.Encode(
		ipldcar.CarHeader{
			Roots:   nil,
			Version: 1,
		},
	)
	if err != nil {
		panic(fmt.Sprintf("failed to encode CAR header: %v", err))
	}

	var buf bytes.Buffer
	err = util.LdWrite(&buf, noRootsHeaderWithoutLength)
	if err != nil {
		panic(fmt.Sprintf("failed to length-delimit CAR header: %v", err))
	}

	noRootsHeader = buf.Bytes()
	hasher := newShardHashState()
	defer hasher.reset()
	_, err = hasher.Write(noRootsHeader)
	if err != nil {
		panic(fmt.Sprintf("failed to hash CAR header: %v", err))
	}
	noRootsDigestState, noRootsPieceCIDState, err = hasher.marshal()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal hash states: %v", err))
	}
}

type CAREncoder struct {
}

var _ ShardEncoder = (*CAREncoder)(nil)

// NewCAREncoder creates a new shard encoder that outputs CAR files.
func NewCAREncoder() *CAREncoder {
	return &CAREncoder{}
}

func (c *CAREncoder) WriteHeader(ctx context.Context, w io.Writer) error {
	_, err := w.Write(noRootsHeader)
	if err != nil {
		return fmt.Errorf("writing CAR header: %w", err)
	}
	return nil
}

func (c *CAREncoder) WriteNode(ctx context.Context, node model.Node, data []byte, w io.Writer) error {
	_, err := w.Write(lengthVarint(uint64(node.CID().ByteLen()) + node.Size()))
	if err != nil {
		return fmt.Errorf("writing varint for %s: %w", node.CID(), err)
	}
	_, err = w.Write(node.CID().Bytes())
	if err != nil {
		return fmt.Errorf("writing CID bytes for %s: %w", node.CID(), err)
	}
	_, err = w.Write(data)
	if err != nil {
		return fmt.Errorf("writing data for %s: %w", node.CID(), err)
	}
	return nil
}

func (c *CAREncoder) HeaderEncodingLength() uint64 {
	return uint64(len(noRootsHeader))
}

func (c CAREncoder) NodeEncodingLength(node model.Node) uint64 {
	cid := node.CID()
	blockSize := node.Size()
	pllen := uint64(len(cidlink.Link{Cid: cid}.Binary())) + blockSize
	vilen := uint64(varint.UvarintSize(uint64(pllen)))
	return pllen + vilen
}

func (c *CAREncoder) HeaderDigestState() []byte {
	return noRootsDigestState
}

func (c *CAREncoder) HeaderPieceCIDState() []byte {
	return noRootsPieceCIDState
}
