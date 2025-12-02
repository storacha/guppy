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
}

type CAREncoder struct {
	nodeReader NodeDataGetter
}

var _ ShardEncoder = (*CAREncoder)(nil)

// NewCAREncoder creates a new shard encoder that outputs CAR files.
func NewCAREncoder(nodeReader NodeDataGetter) *CAREncoder {
	return &CAREncoder{nodeReader}
}

func (c *CAREncoder) Encode(ctx context.Context, nodes []model.Node, w io.Writer) error {
	_, err := w.Write(noRootsHeader)
	if err != nil {
		return fmt.Errorf("writing CAR header: %w", err)
	}

	for _, n := range nodes {
		_, err := w.Write(lengthVarint(uint64(n.CID().ByteLen()) + n.Size()))
		if err != nil {
			return fmt.Errorf("writing varint for %s: %w", n.CID(), err)
		}
		_, err = w.Write(n.CID().Bytes())
		if err != nil {
			return fmt.Errorf("writing CID bytes for %s: %w", n.CID(), err)
		}
		data, err := c.nodeReader.GetData(ctx, n)
		if err != nil {
			return fmt.Errorf("getting data for %s: %w", n.CID(), err)
		}
		_, err = w.Write(data)
		if err != nil {
			return fmt.Errorf("writing data for %s: %w", n.CID(), err)
		}
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
