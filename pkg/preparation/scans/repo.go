package scans

import (
	"context"
	"io/fs"
	"time"

	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types"
)

type Repo interface {
	CreateScan(ctx context.Context, uploadID types.UploadID) (*model.Scan, error)
	FindOrCreateFile(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID types.SourceID) (*model.File, bool, error)
	FindOrCreateDirectory(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, checksum []byte, sourceID types.SourceID) (*model.Directory, bool, error)
	CreateDirectoryChildren(ctx context.Context, parent *model.Directory, children []model.FSEntry) error
	DirectoryChildren(ctx context.Context, dir *model.Directory) ([]model.FSEntry, error)
	UpdateScan(ctx context.Context, scan *model.Scan) error
	GetFileByID(ctx context.Context, fileID types.FSEntryID) (*model.File, error)
}
