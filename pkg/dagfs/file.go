package dagfs

import (
	"io/fs"

	"github.com/ipfs/boxo/ipld/unixfs"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
)

type file struct {
	name      string
	fsNode    *unixfs.FSNode
	dagReader uio.DagReader
}

var _ fs.File = (*file)(nil)

func (f *file) Stat() (fs.FileInfo, error) {
	return &dirEntryFileInfo{
		name:   f.name,
		fsNode: f.fsNode,
	}, nil
}

func (f *file) Read(p []byte) (int, error) {
	return f.dagReader.Read(p)
}

func (f *file) Close() error {
	return f.dagReader.Close()
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	return f.dagReader.Seek(offset, whence)
}
