package visitor

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type unixFSOrRawVisitorEncoderChooser struct {
	unixFSNodeVisitorEncoderChooser
	visitRawNode func(cid cid.Cid, size uint64, data []byte) error
}

func (ec unixFSOrRawVisitorEncoderChooser) EncoderChooser(lp datamodel.LinkPrototype) (codec.Encoder, error) {
	if lp.(cidlink.LinkPrototype).Codec == cid.DagProtobuf {
		return ec.unixFSNodeVisitorEncoderChooser.EncoderChooser(lp)
	}

	originalEncode, err := ec.originalChooser(lp)
	if err != nil {
		return nil, err
	}

	return rawNodeVisitorEncoder{
		visitRawNode:   ec.visitRawNode,
		originalEncode: originalEncode,
	}.Encode, nil
}
