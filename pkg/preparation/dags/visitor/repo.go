package visitor

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Repo defines the interface for a repository that manages file system entries during a scan
type Repo interface {
	FindOrCreateRawNode(ctx context.Context, cid cid.Cid, size uint64, spaceDID did.DID, path string, sourceID id.SourceID, offset uint64) (*model.RawNode, bool, error)
	FindOrCreateUnixFSNode(ctx context.Context, cid cid.Cid, size uint64, spaceDID did.DID, ufsdata []byte) (*model.UnixFSNode, bool, error)
	CreateLinks(ctx context.Context, parent cid.Cid, spaceDID did.DID, links []model.LinkParams) error
}
