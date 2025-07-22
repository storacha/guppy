package visitor

import (
	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type unixFSOrRawVisitorEncoderChooser struct {
	visitUnixFSNode func(cid cid.Cid, size uint64, ufsData []byte, pbLinks []dagpb.PBLink, data []byte) error
	visitRawNode    func(cid cid.Cid, size uint64, data []byte) error
	originalChooser func(datamodel.LinkPrototype) (codec.Encoder, error)
}

func (ec unixFSOrRawVisitorEncoderChooser) EncoderChooser(lp datamodel.LinkPrototype) (codec.Encoder, error) {
	originalEncode, err := ec.originalChooser(lp)
	if err != nil {
		return nil, err
	}
	if lp.(cidlink.LinkPrototype).Codec == cid.DagProtobuf {
		return unixFSNodeVisitorEncoder{
			visitUnixFSNode: ec.visitUnixFSNode,
			originalEncode:  originalEncode,
		}.Encode, nil
	}
	return rawNodeVisitorEncoder{
		visitRawNode:   ec.visitRawNode,
		originalEncode: originalEncode,
	}.Encode, nil
}
