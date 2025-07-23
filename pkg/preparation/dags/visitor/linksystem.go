package visitor

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
)

// This file is certainly a bit of a hack-- the intent is to expose an ipld
// LinkSystem that visits nodes upon storage. The LinkSystem doesn't easily do
// this, because it wants to break up encoding, hashing, and storage into
// separate steps and only let you configure each step separately. Here though,
// we kind of need to do something custom for the entire thing. The only
// available solution was to take the encoding step and do everything there --
// encoding, hashing, and visiting steps -- then eat the cost of IPLD's
// separate hashing + storage steps by making them as close as possible to
// no-ops. This is a bit of a hack, but it enables us to use the default code
// in https://github.com/ipfs/go-unixfsnode/tree/main/data/builder which keeps
// a bunch of confusing complexity out of this codebase.

func encode(encoder codec.Encoder, codec uint64, node datamodel.Node, w io.Writer) (cid.Cid, []byte, error) {
	var buf bytes.Buffer
	hasher := sha256.New()
	mw := io.MultiWriter(&buf, w, hasher)
	if err := encoder(node, mw); err != nil {
		return cid.Undef, nil, err
	}
	data := buf.Bytes()
	hash := hasher.Sum(nil)
	mh, err := multihash.Encode(hash, multihash.SHA2_256)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("failed to encode multihash: %w", err)
	}
	cid := cid.NewCidV1(codec, mh)
	return cid, data, nil
}

func identityHasherChooser(lp datamodel.LinkPrototype) (hash.Hash, error) {
	return multihash.GetHasher(multihash.IDENTITY)
}

func noopStorage(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
	return io.Discard, func(l datamodel.Link) error {
		// This is a no-op for writing links, as we handle link creation in VisitUnixFSNode.
		return nil
	}, nil
}

func simpleLinkSystem() *linking.LinkSystem {
	ls := cidlink.DefaultLinkSystem()

	// uses identity hasher to avoid extra hash computation, since we will do that in the encode step
	ls.HasherChooser = identityHasherChooser
	// no op storage system
	ls.StorageWriteOpener = noopStorage

	return &ls
}

// LinkSystem returns a LinkSystem that visits UnixFS nodes using UnixFSNodeVisitor when links are stored.
func (v UnixFSDirectoryNodeVisitor) LinkSystem() *linking.LinkSystem {
	ls := simpleLinkSystem()

	// use the visitor encoder chooser to handle encoding
	ls.EncoderChooser = unixFSOrRawVisitorEncoderChooser{
		originalChooser: ls.EncoderChooser,
		visitUnixFSNode: v.visitUnixFSNode,
		visitRawNode: func(cid cid.Cid, size uint64, data []byte) error {
			return fmt.Errorf("raw nodes are not supported in UnixFSDirectoryNodeVisitor")
		},
	}.EncoderChooser
	return ls
}

// LinkSystem returns a LinkSystem that visits raw nodes or UnixFS nodes using UnixFSVisitor when links are stored.
func (v UnixFSFileNodeVisitor) LinkSystem() *linking.LinkSystem {
	ls := simpleLinkSystem()

	// use the visitor encoder chooser to handle encoding
	ls.EncoderChooser = unixFSOrRawVisitorEncoderChooser{
		originalChooser: ls.EncoderChooser,
		visitUnixFSNode: v.visitUnixFSNode,
		visitRawNode:    v.visitRawNode,
	}.EncoderChooser
	return ls
}
