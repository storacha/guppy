package uploads

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
)

type Repo interface {
	// GetUploadByID retrieves an upload by its unique ID.
	GetUploadByID(ctx context.Context, uploadID id.UploadID) (*model.Upload, error)
	// GetSourceIDForUploadID retrieves the source ID associated with a given upload ID.
	GetSourceIDForUploadID(ctx context.Context, uploadID id.UploadID) (id.SourceID, error)
	// CreateUploads creates uploads for a given configuration
	CreateUploads(ctx context.Context, configurationID id.ConfigurationID, sourceIDs []id.SourceID) ([]*model.Upload, error)
	// UpdateUpload updates the state of an upload in the repository.
	UpdateUpload(ctx context.Context, upload *model.Upload) error
	// CIDForFSEntry retrieves the CID for a file system entry by its ID.
	CIDForFSEntry(ctx context.Context, fsEntryID id.FSEntryID) (cid.Cid, error)
	// CreateDAGScanForFSEntry creates a new DAG scan for a file system entry.
	CreateDAGScan(ctx context.Context, fsEntryID id.FSEntryID, isDirectory bool, uploadID id.UploadID) error
	// ListConfigurationSources lists all configuration sources for the given configuration ID.
	ListConfigurationSources(ctx context.Context, configID id.ConfigurationID) ([]id.SourceID, error)
}
