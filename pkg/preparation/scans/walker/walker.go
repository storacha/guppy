package walker

import (
	"fmt"
	"io/fs"
	"path"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
)

var log = logging.Logger("preparation/scans/walker")

// FSVisitor is an interface that defines methods for visiting files and directories
// during a file system walk.
type FSVisitor interface {
	VisitFile(path string, dirEntry fs.DirEntry) (*model.File, error)
	VisitDirectory(path string, dirEntry fs.DirEntry, children []model.FSEntry) (*model.Directory, error)
}

// WalkDir walks the file system rooted at root, calling the visitor for each file and directory, and returns the root directory entry.
func WalkDir(fsys fs.FS, root string, visitor FSVisitor) (model.FSEntry, error) {
	if !fs.ValidPath(root) {
		return nil, fmt.Errorf("invalid path: %s", root)
	}
	info, err := fs.Stat(fsys, root)
	if err != nil {
		return nil, fmt.Errorf("statting root: %w", err)
	}
	return walkDir(fsys, root, fs.FileInfoToDirEntry(info), visitor)
}

// walkDir recursively descends the file system, calling the visitor for each file and directory.
func walkDir(fsys fs.FS, name string, d fs.DirEntry, visitor FSVisitor) (model.FSEntry, error) {
	log.Debugf("Walking %s", name)
	if !d.IsDir() {
		return visitor.VisitFile(name, d)
	}

	dirEntries, err := fs.ReadDir(fsys, name)
	if err != nil {
		return nil, fmt.Errorf("reading directory %s: %w", name, err)
	}

	children := make([]model.FSEntry, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		entryName := path.Join(name, dirEntry.Name())
		child, err := walkDir(fsys, entryName, dirEntry, visitor)
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}
	return visitor.VisitDirectory(name, d, children)
}
