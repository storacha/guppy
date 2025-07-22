package visitor

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type rawNodeVisitorEncoder struct {
	v              UnixFSFileNodeVisitor
	originalEncode codec.Encoder
}

func (e rawNodeVisitorEncoder) Encode(node datamodel.Node, w io.Writer) error {
	// Implement the encoding logic here
	cid, data, err := encode(e.originalEncode, cid.Raw, node, w)
	if err != nil {
		return fmt.Errorf("encoding node: %w", err)
	}
	return e.v.visitRawNode(cid, uint64(len(data)), data)
}
