package uploads

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

type Repo interface {
	// GetUploadByID retrieves an upload by its unique ID.
	GetUploadByID(ctx context.Context, uploadID id.UploadID) (*uploadmodel.Upload, error)
	// GetSourceIDForUploadID retrieves the source ID associated with a given upload ID.
	GetSourceIDForUploadID(ctx context.Context, uploadID id.UploadID) (id.SourceID, error)
	// CreateUploads creates uploads for a given configuration
	CreateUploads(ctx context.Context, configurationID id.ConfigurationID, sourceIDs []id.SourceID) ([]*uploadmodel.Upload, error)
	// UpdateUpload updates the state of an upload in the repository.
	UpdateUpload(ctx context.Context, upload *uploadmodel.Upload) error
	// CIDForFSEntry retrieves the CID for a file system entry by its ID.
	CIDForFSEntry(ctx context.Context, fsEntryID id.FSEntryID) (cid.Cid, error)
	// CreateDAGScanForFSEntry creates a new DAG scan for a file system entry.
	CreateDAGScan(ctx context.Context, fsEntryID id.FSEntryID, isDirectory bool, uploadID id.UploadID) (dagmodel.DAGScan, error)
	// ListConfigurationSources lists all configuration sources for the given configuration ID.
	ListConfigurationSources(ctx context.Context, configID id.ConfigurationID) ([]id.SourceID, error)
}

// IncompleteDagScanError is returned by CIDForFSEntry when the DAG scan for the file system entry is not completed.
type IncompleteDagScanError struct {
	DagScan dagmodel.DAGScan
}

func (e IncompleteDagScanError) Unwrap() error {
	return e.DagScan.Error()
}

func (e IncompleteDagScanError) Error() string {
	if err := e.Unwrap(); err != nil {
		return fmt.Sprintf("DAG scan for fs entry %s is not completed: %s", e.DagScan.FsEntryID(), err)
	}
	return fmt.Sprintf("DAG scan for fs_entry_id %s is not completed, current state: %s", e.DagScan.FsEntryID(), e.DagScan.State())
}
