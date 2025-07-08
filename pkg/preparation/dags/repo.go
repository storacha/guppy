package dags

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types"
)

// Repo defines the interface for interacting with DAG scans, nodes, and links in the repository.
type Repo interface {
	UpdateDAGScan(ctx context.Context, dagScan model.DAGScan) error
	FindOrCreateRawNode(ctx context.Context, cid cid.Cid, size uint64, path string, sourceID types.SourceID, offset uint64) (*model.RawNode, bool, error)
	FindOrCreateUnixFSNode(ctx context.Context, cid cid.Cid, size uint64, ufsdata []byte) (*model.UnixFSNode, bool, error)
	CreateLinks(ctx context.Context, parent cid.Cid, links []model.LinkParams) error
	GetChildScans(ctx context.Context, directoryScans *model.DirectoryDAGScan) ([]model.DAGScan, error)
	DAGScansForUploadByStatus(ctx context.Context, uploadID types.UploadID, states ...model.DAGScanState) ([]model.DAGScan, error)
	DirectoryLinks(ctx context.Context, dirScan *model.DirectoryDAGScan) ([]model.LinkParams, error)
}
