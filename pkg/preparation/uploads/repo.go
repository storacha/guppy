package uploads

import (
	"context"

	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
)

type Repo interface {
	// GetUploadByID retrieves an upload by its unique ID.
	GetUploadByID(ctx context.Context, uploadID types.UploadID) (*model.Upload, error)
	// GetSourceIDForUploadID retrieves the source ID associated with a given upload ID.
	GetSourceIDForUploadID(ctx context.Context, uploadID types.UploadID) (types.SourceID, error)
	// CreateUploads creates uploads for a given configuration
	CreateUploads(ctx context.Context, configurationID types.ConfigurationID, sourceIDs []types.SourceID) ([]*model.Upload, error)
	// UpdateUpload updates the state of an upload in the repository.
	UpdateUpload(ctx context.Context, upload *model.Upload) error
}
