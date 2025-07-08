package visitor

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
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

type unixFSNodeVisitorEncoderChooser struct {
	v        UnixFSNodeVisitor
	original func(datamodel.LinkPrototype) (codec.Encoder, error)
}

type unixFSNodeVisitorEncoder struct {
	v        UnixFSNodeVisitor
	original codec.Encoder
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

func (v unixFSNodeVisitorEncoder) Encode(node datamodel.Node, w io.Writer) error {
	cid, data, err := encode(v.original, cid.DagProtobuf, node, w)
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
	if err := v.v.VisitUnixFSNode(cid, uint64(len(data)), ufsData, links, data); err != nil {
		return fmt.Errorf("visiting unixfs node: %w", err)
	}
	return nil
}

// LinkSystem returns a LinkSystem that visits UnixFS nodes using UnixFSNodeVisitor when links are stored.
func (v UnixFSNodeVisitor) LinkSystem() *linking.LinkSystem {
	ls := cidlink.DefaultLinkSystem()

	// uses identity hasher to avoid extra hash computation, since we will do that in the encode step
	ls.HasherChooser = identityHasherChooser
	// no op storage system
	ls.StorageWriteOpener = noopStorage
	// use the visitor encoder chooser to handle encoding
	ls.EncoderChooser = unixFSNodeVisitorEncoderChooser{
		v:        v,
		original: ls.EncoderChooser,
	}.EncoderChooser

	return &ls
}

type unixFSVisitorEncoderChooser struct {
	v        UnixFSVisitor
	original func(datamodel.LinkPrototype) (codec.Encoder, error)
}

type rawVisitorEncoder struct {
	v        UnixFSVisitor
	original codec.Encoder
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

func (v rawVisitorEncoder) Encode(node datamodel.Node, w io.Writer) error {
	// Implement the encoding logic here
	cid, data, err := encode(v.original, cid.Raw, node, w)
	if err != nil {
		return fmt.Errorf("encoding node: %w", err)
	}
	return v.v.VisitRawNode(cid, uint64(len(data)), data)
}

// LinkSystem returns a LinkSystem that visits raw nodes or UnixFS nodes using UnixFSVisitor when links are stored.
func (v UnixFSVisitor) LinkSystem() *linking.LinkSystem {
	ls := v.UnixFSNodeVisitor.LinkSystem()
	ls.EncoderChooser = unixFSVisitorEncoderChooser{
		v:        v,
		original: ls.EncoderChooser,
	}.EncoderChooser
	return ls
}
