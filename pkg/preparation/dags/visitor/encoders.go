package visitor

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	gocid "github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type nodeEncoderChooser struct {
	originalChooser func(datamodel.LinkPrototype) (codec.Encoder, error)
	visitUnixFSNode func(cid cid.Cid, size uint64, ufsData []byte, pbLinks []dagpb.PBLink, data []byte) error
	visitRawNode    func(cid cid.Cid, size uint64, data []byte) error
}

func (ec nodeEncoderChooser) EncoderChooser(lp datamodel.LinkPrototype) (codec.Encoder, error) {
	originalEncode, err := ec.originalChooser(lp)
	if err != nil {
		return nil, err
	}

	return func(node datamodel.Node, w io.Writer) error {
		cid, data, err := encode(originalEncode, lp.(cidlink.LinkPrototype).Codec, node, w)
		if err != nil {
			return fmt.Errorf("encoding node: %w", err)
		}

		switch lp.(cidlink.LinkPrototype).Codec {
		case gocid.DagProtobuf:
			if err != nil {
				return fmt.Errorf("encoding node: %w", err)
			}
			pbNode, ok := node.(dagpb.PBNode)
			if !ok {
				return fmt.Errorf("failed to cast node to PBNode")
			}
			ufsData := pbNode.FieldData().Must().Bytes()
			links := make([]dagpb.PBLink, 0, pbNode.FieldLinks().Length())
			iter := pbNode.FieldLinks().Iterator()
			for !iter.Done() {
				_, link := iter.Next()
				links = append(links, link)
			}

			if err := ec.visitUnixFSNode(cid, uint64(len(data)), ufsData, links, data); err != nil {
				return fmt.Errorf("visiting unixfs node: %w", err)
			}

		case gocid.Raw:
			if err := ec.visitRawNode(cid, uint64(len(data)), data); err != nil {
				return fmt.Errorf("visiting raw node: %w", err)
			}

		default:
			return fmt.Errorf("unsupported codec %d", lp.(cidlink.LinkPrototype).Codec)
		}
		return nil
	}, nil
}
