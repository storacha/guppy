package preparation
import (
	"io/fs"
	"os"
)
// singleFileFS adapts a single file to the fs.FS interface.
// The file is exposed as the root entry "." of the filesystem.
type singleFileFS struct {
	path string
}

func (s singleFileFS) Open(name string) (fs.File, error) {
	if name == "." {
		return os.Open(s.path)
	}
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}
