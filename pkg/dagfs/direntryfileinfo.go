package dagfs

import (
	"io/fs"
	"time"

	"github.com/ipfs/boxo/ipld/unixfs"
)

type dirEntryFileInfo struct {
	name   string
	fsNode *unixfs.FSNode
}

var _ fs.DirEntry = (*dirEntryFileInfo)(nil)
var _ fs.FileInfo = (*dirEntryFileInfo)(nil)

func (de *dirEntryFileInfo) Name() string {
	return de.name
}

func (de *dirEntryFileInfo) IsDir() bool {
	return de.fsNode.IsDir()
}

func (de *dirEntryFileInfo) Type() fs.FileMode {
	if de.IsDir() {
		return fs.ModeDir
	}
	return 0
}

func (de *dirEntryFileInfo) Size() int64 {
	return int64(de.fsNode.FileSize())
}

func (de *dirEntryFileInfo) Mode() fs.FileMode {
	return de.fsNode.Mode()
}

func (de *dirEntryFileInfo) ModTime() time.Time {
	return de.fsNode.ModTime()
}

func (de *dirEntryFileInfo) Sys() interface{} {
	return nil
}

func (de *dirEntryFileInfo) Info() (fs.FileInfo, error) {
	return de, nil
}
