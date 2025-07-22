package visitor

import (
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type unixFSNodeVisitorEncoderChooser struct {
	v               UnixFSDirectoryNodeVisitor
	originalChooser func(datamodel.LinkPrototype) (codec.Encoder, error)
}

func (ec unixFSNodeVisitorEncoderChooser) EncoderChooser(lp datamodel.LinkPrototype) (codec.Encoder, error) {
	originalEncode, err := ec.originalChooser(lp)
	if err != nil {
		return nil, err
	}
	return unixFSNodeVisitorEncoder{
		v:              ec.v,
		originalEncode: originalEncode,
	}.Encode, nil
}
