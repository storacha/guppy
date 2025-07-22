package visitor

import (
	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type unixFSNodeVisitorEncoderChooser struct {
	originalChooser func(datamodel.LinkPrototype) (codec.Encoder, error)
	visitUnixFSNode func(cid cid.Cid, size uint64, ufsData []byte, pbLinks []dagpb.PBLink, data []byte) error
}

func (ec unixFSNodeVisitorEncoderChooser) EncoderChooser(lp datamodel.LinkPrototype) (codec.Encoder, error) {
	originalEncode, err := ec.originalChooser(lp)
	if err != nil {
		return nil, err
	}
	return unixFSNodeVisitorEncoder{
		visitUnixFSNode: ec.visitUnixFSNode,
		originalEncode:  originalEncode,
	}.Encode, nil
}
