package uploads

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	shardmodel "github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

type Repo interface {
	// GetUploadByID retrieves an upload by its unique ID.
	GetUploadByID(ctx context.Context, uploadID id.UploadID) (*uploadmodel.Upload, error)
	// FindOrCreateUploads ensures uploads exist for a given space
	FindOrCreateUploads(ctx context.Context, spaceDID did.DID, sourceIDs []id.SourceID) ([]*uploadmodel.Upload, error)
	// UpdateUpload updates the state of an upload in the repository.
	UpdateUpload(ctx context.Context, upload *uploadmodel.Upload) error
	// CIDForFSEntry retrieves the CID for a file system entry by its ID.
	CIDForFSEntry(ctx context.Context, fsEntryID id.FSEntryID) (cid.Cid, error)
	// CreateDAGScan creates a new DAG scan for a file system entry.
	CreateDAGScan(ctx context.Context, fsEntryID id.FSEntryID, isDirectory bool, uploadID id.UploadID, spaceDID did.DID) (dagmodel.DAGScan, error)
	// ListSpaceSources lists all space sources for the given space DID.
	ListSpaceSources(ctx context.Context, spaceDID did.DID) ([]id.SourceID, error)
	GetShardByID(ctx context.Context, shardID id.ShardID) (*shardmodel.Shard, error)
	UpdateShard(ctx context.Context, shard *shardmodel.Shard) error
}
