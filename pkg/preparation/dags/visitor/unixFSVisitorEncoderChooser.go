package visitor

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type unixFSVisitorEncoderChooser struct {
	v        UnixFSVisitor
	original func(datamodel.LinkPrototype) (codec.Encoder, error)
}

func (v unixFSVisitorEncoderChooser) EncoderChooser(lp datamodel.LinkPrototype) (codec.Encoder, error) {
	original, err := v.original(lp)
	if err != nil {
		return nil, err
	}
	if lp.(cidlink.LinkPrototype).Codec == cid.DagProtobuf {
		return unixFSNodeVisitorEncoder{
			v:        v.v.UnixFSNodeVisitor,
			original: original,
		}.Encode, nil
	}
	return rawVisitorEncoder{
		v:        v.v,
		original: original,
	}.Encode, nil
}
