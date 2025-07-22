package visitor

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type rawVisitorEncoder struct {
	v        UnixFSVisitor
	original codec.Encoder
}

func (v rawVisitorEncoder) Encode(node datamodel.Node, w io.Writer) error {
	// Implement the encoding logic here
	cid, data, err := encode(v.original, cid.Raw, node, w)
	if err != nil {
		return fmt.Errorf("encoding node: %w", err)
	}
	return v.v.VisitRawNode(cid, uint64(len(data)), data)
}
