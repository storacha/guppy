package file

import (
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/guppy/pkg/preparation/dag/unixfs"

	// raw needed for opening as bytes
	_ "github.com/ipld/go-ipld-prime/codec/raw"
)

// UnixFSVisitor is an interface for visiting file nodes during the DAG building process.
type UnixFSVisitor interface {
	VisitRawNode(cid cid.Cid, size uint64, offset uint64, data []byte) error
	VisitUnixFSNode(cid cid.Cid, size uint64, ufsData []byte, links []dagpb.PBLink, data []byte) error
}

type fileShardMeta struct {
	link       datamodel.Link
	byteSize   uint64
	storedSize uint64
}

type fileShards []fileShardMeta

func (fs fileShards) totalByteSize() uint64 {
	var total uint64
	for _, f := range fs {
		total += f.byteSize
	}
	return total
}

func (fs fileShards) totalStoredSize() uint64 {
	var total uint64
	for _, f := range fs {
		total += f.storedSize
	}
	return total
}

func (fs fileShards) byteSizes() []uint64 {
	sizes := make([]uint64, len(fs))
	for i, f := range fs {
		sizes[i] = f.byteSize
	}
	return sizes
}

// BuildUnixFSFile creates a dag of ipld Nodes representing file data,
// visiting each node with the provided visitor.
// This code is adapted from https://github.com/ipfs/go-unixfsnode/blob/main/data/builder/file.go
// but is tailored to the dag walking process we need for database storage
func BuildUnixFSFile(r io.Reader, blockSize uint64, linksPerNode uint64, visitor UnixFSVisitor) (cid.Cid, error) {
	c := unixfs.NewChunker(r, blockSize)
	var prev fileShards
	depth := 1
	for {
		next, err := fileTreeRecursive(depth, prev, c, linksPerNode, visitor)
		if err != nil {
			return cid.Undef, err
		}
		// end of file, no more data to process
		if prev != nil && prev[0].link == next.link {
			if next.link == nil {
				// no links created, this is an empty file
				err = visitor.VisitRawNode(unixfs.EmptyFileCID, 0, 0, nil)
				if err != nil {
					return cid.Undef, fmt.Errorf("failed to visit raw node for empty file: %w", err)
				}
				return unixfs.EmptyFileCID, nil // empty file case
			}
			return next.link.(cidlink.Link).Cid, nil
		}

		prev = []fileShardMeta{next}
		depth++
	}
}

// fileTreeRecursive packs a file into chunks recursively, returning a root for
// this level of recursion, the number of file bytes consumed for this level of
// recursion and and the number of bytes used to store this level of recursion.
func fileTreeRecursive(
	depth int,
	children fileShards,
	c unixfs.Chunker,
	linksPerNode uint64,
	visitor UnixFSVisitor,
) (fileShardMeta, error) {
	if depth == 1 {
		// file leaf, next chunk, encode as raw bytes, store and retuen
		if len(children) > 0 {
			return fileShardMeta{}, fmt.Errorf("leaf nodes cannot have children")
		}
		offset := c.Offset()
		leafCid, leaf, err := c.NextBytes()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fileShardMeta{}, nil
			}
			return fileShardMeta{}, err
		}
		err = visitor.VisitRawNode(leafCid, uint64(len(leaf)), offset, leaf)
		if err != nil {
			return fileShardMeta{}, fmt.Errorf("failed to visit raw node: %w", err)
		}

		return fileShardMeta{link: cidlink.Link{Cid: leafCid}, byteSize: uint64(len(leaf)), storedSize: uint64(len(leaf))}, nil
	}

	// depth > 1

	if children == nil {
		children = make(fileShards, 0)
	}

	// fill up the links for this level, if we need to go beyond
	// DefaultLinksPerBlock we'll end up back here making a parallel tree
	for uint64(len(children)) < linksPerNode {
		// descend down toward the leaves
		next, err := fileTreeRecursive(depth-1, nil, c, linksPerNode, visitor)
		if err != nil {
			return fileShardMeta{}, err
		} else if next.link == nil { // eof
			break
		}
		children = append(children, next)
	}

	if len(children) == 0 {
		// empty case
		return fileShardMeta{}, nil
	} else if len(children) == 1 {
		// degenerate case
		return children[0], nil
	}

	// make the unixfs node
	ufsNode, err := builder.BuildUnixFS(func(b *builder.Builder) {
		builder.FileSize(b, children.totalByteSize())
		builder.BlockSizes(b, children.byteSizes())
	})
	if err != nil {
		return fileShardMeta{}, err
	}
	ufsData := data.EncodeUnixFSData(ufsNode)
	links, err := toLinks(children)
	if err != nil {
		return fileShardMeta{}, err
	}
	nd, err := unixfs.BuildNode(ufsData, links)
	if err != nil {
		return fileShardMeta{}, fmt.Errorf("failed to build node: %w", err)
	}
	cid, data, err := unixfs.WritePBNode(nd)
	if err != nil {
		return fileShardMeta{}, fmt.Errorf("failed to write PBNode: %w", err)
	}
	err = visitor.VisitUnixFSNode(cid, uint64(len(data)), ufsData, links, data)
	if err != nil {
		return fileShardMeta{}, fmt.Errorf("failed to visit unixfs node: %w", err)
	}
	return fileShardMeta{
		link:       cidlink.Link{Cid: cid},
		byteSize:   children.totalByteSize(),
		storedSize: children.totalStoredSize() + uint64(len(data)),
	}, nil
}

func toLinks(children fileShards) ([]dagpb.PBLink, error) {
	links := make([]dagpb.PBLink, 0, len(children))
	for _, c := range children {
		link, err := builder.BuildUnixFSDirectoryEntry("", int64(c.storedSize), c.link)
		if err != nil {
			return nil, fmt.Errorf("failed to build unixfs directory entry: %w", err)
		}
		links = append(links, link)
	}
	return links, nil
}
