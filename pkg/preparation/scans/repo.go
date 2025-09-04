package scans

import (
	"context"
	"io/fs"
	"time"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

type Repo interface {
	FindOrCreateFile(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (*model.File, bool, error)
	GetUploadByID(ctx context.Context, uploadID id.UploadID) (*uploadmodel.Upload, error)
	UpdateUpload(ctx context.Context, upload *uploadmodel.Upload) error
	FindOrCreateDirectory(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (*model.Directory, bool, error)
	CreateDirectoryChildren(ctx context.Context, parent *model.Directory, children []model.FSEntry) error
	DirectoryChildren(ctx context.Context, dir *model.Directory) ([]model.FSEntry, error)
	GetFileByID(ctx context.Context, fileID id.FSEntryID) (*model.File, error)
}
