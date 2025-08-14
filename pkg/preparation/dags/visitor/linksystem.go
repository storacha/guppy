package visitor

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
)

type VisitNodeFunc func(datamodelNode datamodel.Node, cid cid.Cid, data []byte) error

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

func noopStorage(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
	return io.Discard, func(l datamodel.Link) error {
		// This is a no-op for writing links, as we handle link creation in VisitUnixFSNode.
		return nil
	}, nil
}

func (v UnixFSDirectoryNodeVisitor) LinkSystem() *linking.LinkSystem {
	return linkSystemWithVisitFns(map[uint64]VisitNodeFunc{
		cid.DagProtobuf: v.visitUnixFSNode,
	})
}

func (v UnixFSFileNodeVisitor) LinkSystem() *linking.LinkSystem {
	return linkSystemWithVisitFns(map[uint64]VisitNodeFunc{
		cid.DagProtobuf: v.visitUnixFSNode,
		cid.Raw:         v.visitRawNode,
	})
}

// linkSystemWithVisitFns returns a [LinkSystem] which skips the storage step
// entirely, and instead visits nodes using the provided visit functions during
// the encode step. The visit functions will be called with the [datamodel.Node], the
// [cid.Cid] of the node, and the raw data that was encoded with the encoder the
// [cidlink.DefaultLinkSystem] would use.
func linkSystemWithVisitFns(visitFns map[uint64]VisitNodeFunc) *linking.LinkSystem {
	ls := cidlink.DefaultLinkSystem()
	originalChooser := ls.EncoderChooser

	// no op storage system
	ls.StorageWriteOpener = noopStorage

	ls.EncoderChooser = func(lp datamodel.LinkPrototype) (codec.Encoder, error) {
		originalEncode, err := originalChooser(lp)
		if err != nil {
			return nil, err
		}

		codec := lp.(cidlink.LinkPrototype).Codec
		visit, ok := visitFns[codec]
		if !ok {
			return nil, fmt.Errorf("no visit function for codec %d", codec)
		}

		return func(node datamodel.Node, w io.Writer) error {
			cid, data, err := encode(originalEncode, codec, node, w)
			if err != nil {
				return fmt.Errorf("encoding node: %w", err)
			}

			if err := visit(node, cid, data); err != nil {
				return fmt.Errorf("visiting node: %w", err)
			}

			return nil
		}, nil
	}
	return &ls
}
