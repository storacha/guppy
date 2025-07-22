package visitor

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
)

type unixFSNodeVisitorEncoder struct {
	v              UnixFSDirectoryNodeVisitor
	originalEncode codec.Encoder
}

// Encode encodes the node as a UnixFS node and visits it using the
// UnixFSNodeVisitor. The node must be a PBNode, containing UnixFS data.
func (e unixFSNodeVisitorEncoder) Encode(node datamodel.Node, w io.Writer) error {
	cid, data, err := encode(e.originalEncode, cid.DagProtobuf, node, w)
	if err != nil {
		return fmt.Errorf("encoding node: %w", err)
	}
	pbNode, ok := node.(dagpb.PBNode)
	if !ok {
		return fmt.Errorf("failed to cast node to PBNode")
	}
	ufsData := pbNode.FieldData().Must().Bytes()
	links := make([]dagpb.PBLink, 0, pbNode.FieldLinks().Length())
	iter := pbNode.FieldLinks().Iterator()
	for !iter.Done() {
		_, link := iter.Next()
		links = append(links, link)
	}
	if err := e.v.visitUnixFSNode(cid, uint64(len(data)), ufsData, links, data); err != nil {
		return fmt.Errorf("visiting unixfs node: %w", err)
	}
	return nil
}
