package visitor

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type VisitNodeFn func(datamodelNode datamodel.Node, cid cid.Cid, data []byte) error

type nodeEncoderChooser struct {
	originalChooser func(datamodel.LinkPrototype) (codec.Encoder, error)

	// visitFns maps codecs to visit functions
	visitFns map[uint64]VisitNodeFn
}

func (ec nodeEncoderChooser) EncoderChooser(lp datamodel.LinkPrototype) (codec.Encoder, error) {
	originalEncode, err := ec.originalChooser(lp)
	if err != nil {
		return nil, err
	}

	codec := lp.(cidlink.LinkPrototype).Codec
	visit, ok := ec.visitFns[codec]
	if !ok {
		return nil, fmt.Errorf("no visit function for codec %d", codec)
	}

	return func(node datamodel.Node, w io.Writer) error {
		cid, data, err := encode(originalEncode, codec, node, w)
		if err != nil {
			return fmt.Errorf("encoding node: %w", err)
		}

		if err := visit(node, cid, data); err != nil {
			return fmt.Errorf("visiting node: %w", err)
		}

		return nil
	}, nil
}
