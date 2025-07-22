package visitor

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type unixFSOrRawVisitorEncoderChooser struct {
	v               UnixFSFileNodeVisitor
	originalChooser func(datamodel.LinkPrototype) (codec.Encoder, error)
}

func (ec unixFSOrRawVisitorEncoderChooser) EncoderChooser(lp datamodel.LinkPrototype) (codec.Encoder, error) {
	originalEncode, err := ec.originalChooser(lp)
	if err != nil {
		return nil, err
	}
	if lp.(cidlink.LinkPrototype).Codec == cid.DagProtobuf {
		return unixFSNodeVisitorEncoder{
			v:              ec.v.UnixFSDirectoryNodeVisitor,
			originalEncode: originalEncode,
		}.Encode, nil
	}
	return rawNodeVisitorEncoder{
		v:              ec.v,
		originalEncode: originalEncode,
	}.Encode, nil
}
