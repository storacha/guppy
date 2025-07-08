package model

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/types"
)

// LinkParams holds the parameters for creating a new Link.
type LinkParams struct {
	Name  string
	TSize uint64
	Hash  cid.Cid
}

// Link represents a DAG protobuf link inside a Node
type Link struct {
	name   string
	tSize  uint64
	hash   cid.Cid
	parent cid.Cid
	order  uint64
}

// Name returns the name of the link
func (l *Link) Name() string {
	return l.name
}

// TSize returns the total size of the target node
func (l *Link) TSize() uint64 {
	return l.tSize
}

// Hash returns the CID of the link
func (l *Link) Hash() cid.Cid {
	return l.hash
}

// Parent returns the parent CID of the link
func (l *Link) Parent() cid.Cid {
	return l.parent
}

// Order returns the order of the link, which is used to determine the order of links in a Node
func (l *Link) Order() uint64 {
	return l.order
}

func validateLink(link *Link) error {
	if link.hash == cid.Undef {
		return types.ErrEmpty{Field: "link hash"}
	}
	if link.parent == cid.Undef {
		return types.ErrEmpty{Field: "link parent"}
	}
	if link.parent.Type() != cid.DagProtobuf {
		return fmt.Errorf("invalid CID type: expected DagProtobuf, got %x", link.parent.Type())
	}
	return nil
}

// NewLink creates a new Link instance with the provided name, tSize, hash, parent CID, and order.
func NewLink(params LinkParams, parent cid.Cid, order uint64) (*Link, error) {
	link := &Link{
		name:   params.Name,
		tSize:  params.TSize,
		hash:   params.Hash,
		parent: parent,
		order:  order,
	}
	if err := validateLink(link); err != nil {
		return nil, err
	}
	return link, nil
}

// LinkScanner is a function type for scanning a Link from the database
type LinkScanner func(name *string, tSize *uint64, hash *cid.Cid, parent *cid.Cid, order *uint64) error

// ReadLinkFromDatabase reads a Link from the database using the provided scanner function.
func ReadLinkFromDatabase(scanner LinkScanner) (*Link, error) {
	var link Link

	if err := scanner(&link.name, &link.tSize, &link.hash, &link.parent, &link.order); err != nil {
		return nil, err
	}
	if err := validateLink(&link); err != nil {
		return nil, err
	}
	return &link, nil
}

// Node represents a generic interface content addressed block in a DAG.
type Node interface {
	CID() cid.Cid
	Size() uint64
	isNode()
}

type node struct {
	cid  cid.Cid
	size uint64
}

// CID returns the CID of the Node
func (n *node) CID() cid.Cid {
	return n.cid
}

func (n *node) Size() uint64 {
	return n.size
}

func validateNode(node *node) error {
	if node.cid == cid.Undef {
		return types.ErrEmpty{Field: "node CID"}
	}
	return nil
}

// UnixFSNode represents a DAG protobuf node that contains UnixFS data.
// It is immutable and self contained - there is no dependency on the file system entity that created it.
// It can be reused across different uploads
// The raw serialized data for the node can always be assembled from the UFSData and associated links
type UnixFSNode struct {
	node
	ufsdata []byte
}

// UFSData returns unixfs data portion of the dag protobuf node
func (n *UnixFSNode) UFSData() []byte {
	return n.ufsdata
}

func (n *UnixFSNode) isNode() {}

func validateUnixFSNode(node *UnixFSNode) error {
	if err := validateNode(&node.node); err != nil {
		return err
	}
	if node.cid.Type() != cid.DagProtobuf {
		return fmt.Errorf("invalid CID type: expected DagProtobuf, got %x", node.cid.Type())
	}
	if len(node.ufsdata) == 0 {
		return types.ErrEmpty{Field: "ufsdata"}
	}
	return nil
}

// NewUnixFSNode creates a new UnixFSNode instance with the provided CID, Size, and UFS data.
func NewUnixFSNode(cid cid.Cid, size uint64, ufsdata []byte) (*UnixFSNode, error) {
	node := &UnixFSNode{
		node: node{
			cid:  cid,
			size: size,
		},
		ufsdata: ufsdata,
	}
	if err := validateUnixFSNode(node); err != nil {
		return nil, err
	}
	return node, nil
}

// RawNode represents a block of data in a file
// The underlying data is not serialized in the node, but rather stored in a file system
// If the file sytem changes and the cid changes, the node is no longer valid
type RawNode struct {
	node
	path     string
	sourceID types.SourceID
	offset   uint64
}

// Path returns the path of the raw node in the source
func (n *RawNode) Path() string {
	return n.path
}

// SourceID returns the source ID for the raw node
func (n *RawNode) SourceID() types.SourceID {
	return n.sourceID
}

// Offset returns the offset of the data block in the file
func (n *RawNode) Offset() uint64 {
	return n.offset
}
func (n *RawNode) isNode() {}

func validateRawNode(node *RawNode) error {
	if err := validateNode(&node.node); err != nil {
		return err
	}
	if node.cid.Type() != cid.Raw {
		return fmt.Errorf("invalid CID type: expected Raw, got %x", node.cid.Type())
	}
	if node.sourceID == uuid.Nil {
		return types.ErrEmpty{Field: "sourceID"}
	}
	return nil
}

// NewRawNode creates a new RawNode instance with the provided CID, Size, path, source ID, and offset.
func NewRawNode(cid cid.Cid, size uint64, path string, sourceID types.SourceID, offset uint64) (*RawNode, error) {
	node := &RawNode{
		node: node{
			cid:  cid,
			size: size,
		},
		path:     path,
		sourceID: sourceID,
		offset:   offset,
	}
	if err := validateRawNode(node); err != nil {
		return nil, err
	}
	return node, nil
}

// NodeWriter is a function type for writing a Node to the database.
type NodeWriter func(cid cid.Cid, size uint64, ufsdata []byte, path string, sourceID types.SourceID, offset uint64) error

// WriteNodeToDatabase writes a Node to the database using the provided writer function.
func WriteNodeToDatabase(writer NodeWriter, node Node) error {
	switch n := node.(type) {
	case *UnixFSNode:
		return writer(n.cid, n.size, n.ufsdata, "", uuid.Nil, 0)
	case *RawNode:
		return writer(n.cid, n.size, nil, n.path, n.sourceID, n.offset)
	default:
		return fmt.Errorf("unsupported node type: %T", node)
	}
}

// NodeScanner is a function type for scanning a Node from the database.
type NodeScanner func(cid *cid.Cid, size *uint64, ufsdata *[]byte, path *string, sourceID *types.SourceID, offset *uint64) error

// ReadNodeFromDatabase reads a Node from the database using the provided scanner function.
func ReadNodeFromDatabase(scanner NodeScanner) (Node, error) {
	var node node
	var ufsdata []byte
	var path string
	var sourceID types.SourceID
	var offset uint64
	if err := scanner(&node.cid, &node.size, &ufsdata, &path, &sourceID, &offset); err != nil {
		return nil, err
	}
	switch node.cid.Type() {
	case cid.DagProtobuf:
		unixFSNode := &UnixFSNode{
			node:    node,
			ufsdata: ufsdata,
		}
		if err := validateUnixFSNode(unixFSNode); err != nil {
			return nil, err
		}
		return unixFSNode, nil
	case cid.Raw:
		rawNode := &RawNode{
			node:     node,
			path:     path,
			sourceID: sourceID,
			offset:   offset,
		}
		if err := validateRawNode(rawNode); err != nil {
			return nil, err
		}
		return rawNode, nil
	default:
		return nil, fmt.Errorf("unsupported CID type: %x", node.cid.Type())
	}
}
