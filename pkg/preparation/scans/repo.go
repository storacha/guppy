package scans

import (
	"context"
	"io/fs"
	"time"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

type Repo interface {
	CreateScan(ctx context.Context, uploadID id.UploadID) (*model.Scan, error)
	FindOrCreateFile(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (*model.File, bool, error)
	FindOrCreateDirectory(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, checksum []byte, sourceID id.SourceID, spaceDID did.DID) (*model.Directory, bool, error)
	ScansForUploadByStatus(ctx context.Context, uploadID id.UploadID, states ...model.ScanState) ([]*model.Scan, error)
	CreateDirectoryChildren(ctx context.Context, parent *model.Directory, children []model.FSEntry) error
	DirectoryChildren(ctx context.Context, dir *model.Directory) ([]model.FSEntry, error)
	UpdateScan(ctx context.Context, scan *model.Scan) error
	GetFileByID(ctx context.Context, fileID id.FSEntryID) (*model.File, error)
}
