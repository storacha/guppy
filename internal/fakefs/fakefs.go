// fakefs provides a fake, pseudo-random filesystem implementation of [fs.FS].
// It is useful for testing code that works with [fs.FS] without needing to set
// up actual files on disk, such as the upload process.
package fakefs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"path"
	"strings"
	"time"

	"github.com/wordgen/wordlists/eff"
)

/////////////////////////////////////////////////////////////////
// FS
/////////////////////////////////////////////////////////////////

type FS struct {
	r Rand
}

var _ fs.FS = FS{}

// New creates a new fake filesystem with the given seed. Different seeds
// will produce different filesystems. The same seed will always produce the
// same filesystem.
func New(seed uint64) fs.FS {
	return FS{r: NewRand(seed)}
}

func (fsys FS) Open(name string) (fs.File, error) {
	name = path.Clean(name)

	if name == "." || name == "/" {
		return &Dir{
			File: File{
				r:    fsys.r.NextString("."),
				name: ".",
			},
			depth: 0,
		}, nil
	}

	segments := strings.Split(name, "/")
	file, err := fsys.Open("")
	if err != nil {
		return nil, err
	}
	for _, segment := range segments {
		dir, ok := file.(*Dir)
		if !ok {
			return nil, &fs.PathError{
				Op:   "open",
				Path: name,
				Err:  errors.New("not a directory"),
			}
		}

		var foundFile fs.File
		for {
			files, err := dir.readDirFiles(1)
			if err != nil {
				return nil, err
			}
			if len(files) == 0 {
				break
			}
			info, err := files[0].Stat()
			if err != nil {
				return nil, err
			}
			if info.Name() == segment {
				foundFile = files[0]
				break
			}
		}

		if foundFile == nil {
			return nil, &fs.PathError{
				Op:   "open",
				Path: name,
				Err:  fs.ErrNotExist,
			}
		}
		file = foundFile
	}

	return file, nil
}

/////////////////////////////////////////////////////////////////
// File
/////////////////////////////////////////////////////////////////

type File struct {
	r         Rand
	name      string
	bytesRead int
}

var _ fs.File = (*File)(nil)
var _ io.Seeker = (*File)(nil)

func (f *File) Stat() (fs.FileInfo, error) {
	return FileInfo{
		r:    f.r.NextString("Stat"),
		name: f.name,
	}, nil
}

func (f *File) Read(b []byte) (int, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}

	rng := rand.New(rand.NewPCG(f.r.NextString("Read").Uint64(), 0))

	// Advance the RNG state to where we're reading from
	for range f.bytesRead {
		rng.Uint64()
	}

	n := min(len(b), int(info.Size())-f.bytesRead)
	for i := range n / 8 {
		binary.LittleEndian.PutUint64(b[i*8:], rng.Uint64())
	}
	var finalBytes [8]byte
	binary.LittleEndian.PutUint64(finalBytes[:], rng.Uint64())
	copy(b[(n/8)*8:], finalBytes[:n%8])

	if n < len(b) {
		return n, io.EOF
	}

	f.bytesRead += n
	return n, nil
}

func (f *File) Close() error {
	return nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = int64(f.bytesRead) + offset
	case io.SeekEnd:
		newOffset = info.Size() + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("invalid offset: %d", newOffset)
	}
	if newOffset > info.Size() {
		newOffset = info.Size()
	}

	f.bytesRead = int(newOffset)
	return newOffset, nil
}

func (f *File) String() string {
	return fmt.Sprintf("%T(%s)", f, f.name)
}

/////////////////////////////////////////////////////////////////
// File
/////////////////////////////////////////////////////////////////

type FileInfo struct {
	r    Rand
	name string
}

var _ fs.FileInfo = FileInfo{}

func (fi FileInfo) Name() string {
	return fi.name
}

func (fi FileInfo) Size() int64 {
	return (1 << 5) + int64(fi.r.NextString("Size").Uint64()%(1<<22))
}

func (fi FileInfo) Mode() fs.FileMode {
	return fs.FileMode(fi.r.NextString("Mode").Uint64()>>32) & fs.ModePerm
}

func (fi FileInfo) ModTime() time.Time {
	return time.Unix(int64(fi.r.NextString("ModTime").Uint64()%1_000_000_0000), 0)
}

func (fi FileInfo) IsDir() bool {
	return false
}

func (fi FileInfo) Sys() any {
	return nil
}

func (fi FileInfo) Type() fs.FileMode {
	panic("not implemented")
}

type DirFileInfo struct {
	FileInfo
}

func (dfi DirFileInfo) IsDir() bool {
	return true
}

func (dfi DirFileInfo) Mode() fs.FileMode {
	return dfi.FileInfo.Mode() | fs.ModeDir
}

/////////////////////////////////////////////////////////////////
// Dir
/////////////////////////////////////////////////////////////////

type Dir struct {
	File
	readEntries int
	depth       int
}

var _ fs.ReadDirFile = (*Dir)(nil)

func (d *Dir) Read([]byte) (int, error) {
	return 0, fmt.Errorf("cannot read from directory")
}

func (d *Dir) Stat() (fs.FileInfo, error) {
	fi, err := d.File.Stat()
	return DirFileInfo{FileInfo: fi.(FileInfo)}, err
}

func (d *Dir) ReadDir(n int) ([]fs.DirEntry, error) {
	files, err := d.readDirFiles(n)
	if err != nil {
		return nil, err
	}
	entries := make([]fs.DirEntry, len(files))
	for i, file := range files {
		fileInfo, err := file.Stat()
		if err != nil {
			return nil, err
		}
		entries[i] = fs.FileInfoToDirEntry(fileInfo)
	}
	return entries, nil
}

func (d *Dir) readDirFiles(n int) ([]fs.File, error) {
	r := d.r.NextString("readDirFiles")
	countToReturn := d.remainingEntries()
	if n > 0 {
		countToReturn = min(n, countToReturn)
	}
	files := make([]fs.File, 0, countToReturn)
	for i := range countToReturn {
		// Make sure the name is unique in the directory
		name := Pick(r.NextString("Children-Name"), eff.Large, d.readEntries+i)
		fr := r.Next(uint64(d.readEntries + i))

		var fsfile fs.File
		file := File{
			r:    fr,
			name: name,
		}
		isDir := fr.NextString("IsDir").Uint64()%10 > uint64(d.depth+5)
		if isDir {
			fsfile = &Dir{
				File:  file,
				depth: d.depth + 1,
			}
		} else {
			fsfile = &file
		}

		files = append(files, fsfile)
	}
	d.readEntries += countToReturn
	return files, nil
}

func (d *Dir) numEntries() int {
	return 5 + int(d.r.Uint64()%20)
}

func (d *Dir) remainingEntries() int {
	return d.numEntries() - d.readEntries
}
