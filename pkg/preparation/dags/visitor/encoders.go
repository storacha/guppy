package visitor

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	gocid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type nodeEncoderChooser struct {
	originalChooser func(datamodel.LinkPrototype) (codec.Encoder, error)
	visitUnixFSNode func(datamodelNode datamodel.Node, cid cid.Cid, data []byte) error
	visitRawNode    func(datamodelNode datamodel.Node, cid cid.Cid, data []byte) error
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
			if err := ec.visitUnixFSNode(node, cid, data); err != nil {
				return fmt.Errorf("visiting unixfs node: %w", err)
			}

		case gocid.Raw:
			if err := ec.visitRawNode(node, cid, data); err != nil {
				return fmt.Errorf("visiting raw node: %w", err)
			}

		default:
			return fmt.Errorf("unsupported codec %d", lp.(cidlink.LinkPrototype).Codec)
		}
		return nil
	}, nil
}
