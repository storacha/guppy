package model

import (
	"fmt"
	"io/fs"
	"time"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/types"
)

type FSEntryID = uuid.UUID

type FSEntry interface {
	ID() FSEntryID
	// Path is the path within the datasource
	Path() string
	LastModified() time.Time
	Mode() fs.FileMode
	// Checksum is a way to uniquely identify the file or directory.
	// For files, it's a hash of path, modified, mode, and size
	// For directories, it's a hash of path and modified, plus the concatenation of the checksums of all its children.
	Checksum() []byte
	SourceID() types.SourceID
	isFsEntry()
}

type fsEntry struct {
	id           FSEntryID
	path         string
	lastModified time.Time
	mode         fs.FileMode
	checksum     []byte         // checksum is the hash of the
	sourceID     types.SourceID // sourceID is the ID of the source this entry belongs to
}

func (f *fsEntry) ID() FSEntryID {
	return f.id
}

func (f *fsEntry) Path() string {
	return f.path
}

func (f *fsEntry) LastModified() time.Time {
	return f.lastModified
}

func (f *fsEntry) Mode() fs.FileMode {
	return f.mode
}

func (f *fsEntry) Checksum() []byte {
	return f.checksum
}
func (f *fsEntry) SourceID() types.SourceID {
	return f.sourceID
}

type File struct {
	fsEntry
	size uint64 // size is the size of the file in bytes
}

func (f *File) isFsEntry() {}
func (f *File) Size() uint64 {
	return f.size
}

type Directory struct {
	fsEntry
}

func (d *Directory) isFsEntry() {}

func validateFsEntry(f *fsEntry) error {
	if f.id == uuid.Nil {
		return types.ErrEmpty{"id"}
	}
	if f.path == "" {
		return types.ErrEmpty{"path"}
	}
	if f.lastModified.IsZero() {
		return types.ErrEmpty{"lastModified"}
	}
	if f.checksum == nil {
		return types.ErrEmpty{"checksum"}
	}
	if f.sourceID == uuid.Nil {
		return types.ErrEmpty{"sourceID"}
	}
	return nil
}

func NewFile(path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID types.SourceID) (*File, error) {
	file := &File{
		fsEntry: fsEntry{
			id:           uuid.New(),
			path:         path,
			lastModified: lastModified,
			mode:         mode,
			checksum:     checksum,
			sourceID:     sourceID,
		},
		size: size,
	}
	if err := validateFsEntry(&file.fsEntry); err != nil {
		return nil, err
	}
	return file, nil
}

func NewDirectory(path string, lastModified time.Time, mode fs.FileMode, checksum []byte, sourceID types.SourceID) (*Directory, error) {
	directory := &Directory{
		fsEntry: fsEntry{
			id:           uuid.New(),
			path:         path,
			lastModified: lastModified,
			mode:         mode,
			checksum:     checksum,
			sourceID:     sourceID,
		},
	}
	if err := validateFsEntry(&directory.fsEntry); err != nil {
		return nil, err
	}
	return directory, nil
}

type FSEntryWriter func(id FSEntryID, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID types.SourceID) error

func WriteFSEntryToDatabase(entry FSEntry, writer FSEntryWriter) error {
	size := uint64(0)
	if file, ok := entry.(*File); ok {
		size = file.Size()
	}
	return writer(entry.ID(), entry.Path(), entry.LastModified(), entry.Mode(), size, entry.Checksum(), entry.SourceID())
}

type FSEntryScanner func(id *FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *types.SourceID) error

func ReadFSEntryFromDatabase(scanner FSEntryScanner) (FSEntry, error) {
	fsEntry := &fsEntry{}
	size := uint64(0) // size is only used for files
	err := scanner(&fsEntry.id, &fsEntry.path, &fsEntry.lastModified, &fsEntry.mode, &size, &fsEntry.checksum, &fsEntry.sourceID)
	if err != nil {
		return nil, fmt.Errorf("reading file from database: %w", err)
	}
	if err := validateFsEntry(fsEntry); err != nil {
		return nil, err
	}
	if fsEntry.mode.IsDir() {
		return &Directory{fsEntry: *fsEntry}, nil
	}
	return &File{fsEntry: *fsEntry, size: size}, nil
}
