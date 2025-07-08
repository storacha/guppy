package scans

import (
	"context"
	"io/fs"

	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/types/timestamp"
)

type Repo interface {
	CreateScan(ctx context.Context, uploadID id.UploadID) (*model.Scan, error)
	FindOrCreateFile(ctx context.Context, path string, lastModified timestamp.Timestamp, mode fs.FileMode, size uint64, checksum []byte, sourceID id.SourceID) (*model.File, bool, error)
	FindOrCreateDirectory(ctx context.Context, path string, lastModified timestamp.Timestamp, mode fs.FileMode, checksum []byte, sourceID id.SourceID) (*model.Directory, bool, error)
	CreateDirectoryChildren(ctx context.Context, parent *model.Directory, children []model.FSEntry) error
	DirectoryChildren(ctx context.Context, dir *model.Directory) ([]model.FSEntry, error)
	UpdateScan(ctx context.Context, scan *model.Scan) error
	GetFileByID(ctx context.Context, fileID id.FSEntryID) (*model.File, error)
}
