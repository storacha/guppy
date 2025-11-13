package dagfs

import (
	"context"
	"fmt"
	"io"
	"io/fs"

	"github.com/ipfs/boxo/ipld/unixfs"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	ipldfmt "github.com/ipfs/go-ipld-format"
)

type dir struct {
	ctx        context.Context
	uioDir     uio.Directory
	name       string
	dagService ipldfmt.DAGService

	// Memoized from UnixFS operations
	links   []*ipldfmt.Link
	fsNode  *unixfs.FSNode
	dagNode ipldfmt.Node

	// State for ReadDir
	offset int
}

var _ fs.ReadDirFile = (*dir)(nil)

func (d *dir) Stat() (fs.FileInfo, error) {
	if d.fsNode == nil {
		node, err := d.uioDir.GetNode()
		if err != nil {
			return nil, err
		}
		d.dagNode = node
		fsNode, err := unixfs.ExtractFSNode(node)
		if err != nil {
			return nil, err
		}
		d.fsNode = fsNode
	}

	return &dirEntryFileInfo{
		name:   d.name,
		fsNode: d.fsNode,
	}, nil
}

func (d *dir) Read([]byte) (int, error) {
	return 0, fs.ErrInvalid
}

func (d *dir) Close() error {
	// No resources to close
	return nil
}

func (d *dir) ReadDir(n int) ([]fs.DirEntry, error) {
	if d.links == nil {
		links, err := d.uioDir.Links(d.ctx)
		if err != nil {
			return nil, err
		}
		d.links = links
		d.offset = 0
	}

	if d.offset >= len(d.links) {
		if n <= 0 {
			return nil, nil
		}
		return nil, io.EOF
	}

	var entries []fs.DirEntry
	remaining := len(d.links) - d.offset

	count := remaining
	if n > 0 && n < remaining {
		count = n
	}

	for i := 0; i < count; i++ {
		link := d.links[d.offset+i]
		node, err := link.GetNode(d.ctx, d.dagService)
		if err != nil {
			return nil, fmt.Errorf("failed to get node for link %s: %w", link.Name, err)
		}
		fsNode, err := unixfs.ExtractFSNode(node)
		if err != nil {
			return nil, fmt.Errorf("failed to extract FSNode for link %s: %w", link.Name, err)
		}
		entries = append(entries, &dirEntryFileInfo{
			name:   link.Name,
			fsNode: fsNode,
		})
	}

	d.offset += count

	if n > 0 && d.offset >= len(d.links) {
		return entries, io.EOF
	}

	return entries, nil
}
