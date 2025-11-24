package dagfs

import (
	"io/fs"
	"time"

	"github.com/ipfs/boxo/ipld/unixfs"
)

type ufsDirEntryFileInfo struct {
	name   string
	fsNode *unixfs.FSNode
}

var _ fs.DirEntry = (*ufsDirEntryFileInfo)(nil)
var _ fs.FileInfo = (*ufsDirEntryFileInfo)(nil)

func (ude *ufsDirEntryFileInfo) Name() string {
	return ude.name
}

func (ude *ufsDirEntryFileInfo) IsDir() bool {
	return ude.fsNode.IsDir()
}

func (ude *ufsDirEntryFileInfo) Type() fs.FileMode {
	if ude.IsDir() {
		return fs.ModeDir
	}
	return 0
}

func (ude *ufsDirEntryFileInfo) Size() int64 {
	return int64(ude.fsNode.FileSize())
}

func (ude *ufsDirEntryFileInfo) Mode() fs.FileMode {
	m := ude.fsNode.Mode()
	// ude.fsNode.Mode() looks like it tries to include the dir mode, but it
	// doesn't appear to succeed. So, add it here.
	if ude.IsDir() {
		m |= fs.ModeDir
	}
	return m
}

func (ude *ufsDirEntryFileInfo) ModTime() time.Time {
	return ude.fsNode.ModTime()
}

func (ude *ufsDirEntryFileInfo) Sys() interface{} {
	return nil
}

func (ude *ufsDirEntryFileInfo) Info() (fs.FileInfo, error) {
	return ude, nil
}

type rawDirEntryFileInfo struct {
	name string
	// For raw nodes without UnixFS metadata
	size uint64
}

var _ fs.DirEntry = (*rawDirEntryFileInfo)(nil)
var _ fs.FileInfo = (*rawDirEntryFileInfo)(nil)

func (rde *rawDirEntryFileInfo) Name() string {
	return rde.name
}

func (rde *rawDirEntryFileInfo) IsDir() bool {
	// Raw nodes are always files
	return false
}

func (rde *rawDirEntryFileInfo) Type() fs.FileMode {
	// Raw nodes are always files
	return 0
}

func (rde *rawDirEntryFileInfo) Size() int64 {
	return int64(rde.size)
}

func (rde *rawDirEntryFileInfo) Mode() fs.FileMode {
	// Default mode for raw nodes (regular file with 0644 permissions)
	return 0644
}

func (rde *rawDirEntryFileInfo) ModTime() time.Time {
	// Raw nodes don't have modification time
	return time.Time{}
}

func (rde *rawDirEntryFileInfo) Sys() interface{} {
	return nil
}

func (rde *rawDirEntryFileInfo) Info() (fs.FileInfo, error) {
	return rde, nil
}
