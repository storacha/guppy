package visitor

import (
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type unixFSNodeVisitorEncoderChooser struct {
	v        UnixFSNodeVisitor
	original func(datamodel.LinkPrototype) (codec.Encoder, error)
}

func (v unixFSNodeVisitorEncoderChooser) EncoderChooser(lp datamodel.LinkPrototype) (codec.Encoder, error) {
	original, err := v.original(lp)
	if err != nil {
		return nil, err
	}
	return unixFSNodeVisitorEncoder{
		v:        v.v,
		original: original,
	}.Encode, nil
}
