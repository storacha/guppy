package visitor

import (
	"fmt"

	"github.com/ipfs/go-cid"
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

	switch lp.(cidlink.LinkPrototype).Codec {
	case cid.DagProtobuf:
		return unixFSEncoder{
			originalEncode:  originalEncode,
			visitUnixFSNode: ec.visitUnixFSNode,
		}.Encode, nil

	case cid.Raw:
		return rawEncoder{
			originalEncode: originalEncode,
			visitRawNode:   ec.visitRawNode,
		}.Encode, nil
	default:
		return nil, fmt.Errorf("unsupported codec %d", lp.(cidlink.LinkPrototype).Codec)
	}
}
