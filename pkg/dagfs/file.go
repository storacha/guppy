package dagfs

import (
	"io/fs"

	"github.com/ipfs/boxo/ipld/unixfs"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
)

type ufsFile struct {
	name   string
	fsNode *unixfs.FSNode
	uio.DagReader
}

var _ fs.File = (*ufsFile)(nil)

func (uf *ufsFile) Stat() (fs.FileInfo, error) {
	return &ufsDirEntryFileInfo{
		name:   uf.name,
		fsNode: uf.fsNode,
	}, nil
}

type rawFile struct {
	name string
	uio.DagReader
}

var _ fs.File = (*rawFile)(nil)

func (rf *rawFile) Stat() (fs.FileInfo, error) {
	return &rawDirEntryFileInfo{
		name: rf.name,
		size: rf.DagReader.Size(),
	}, nil
}
