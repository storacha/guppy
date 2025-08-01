package dags

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/dags/visitor"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
)

const BlockSize = 1 << 20         // 1 MiB
const DefaultLinksPerBlock = 1024 // Default number of links per block for UnixFS

var log = logging.Logger("preparation/dags")

func init() {
	// Set the default links per block for UnixFS.
	// annoying this is not more easily configurable, but this is the only way to set it globally.
	builder.DefaultLinksPerBlock = DefaultLinksPerBlock
}

// API provides methods to interact with the DAG scans in the repository.
type API struct {
	Repo         Repo
	FileAccessor FileAccessorFunc
}

// FileAccessorFunc is a function type that retrieves a file for a given fsEntryID.
type FileAccessorFunc func(ctx context.Context, fsEntryID id.FSEntryID) (fs.File, id.SourceID, string, error)

// RestartDagScansForUpload restarts all canceled or running DAG scans for the given upload ID.
func (a API) RestartDagScansForUpload(ctx context.Context, uploadID id.UploadID) error {
	// restart all canceled/running dag scans
	restartableDagScans, err := a.Repo.DAGScansForUploadByStatus(ctx, uploadID, model.DAGScanStateCanceled, model.DAGScanStateRunning)
	if err != nil {
		return fmt.Errorf("getting restartable dag scans for upload %s: %w", uploadID, err)
	}
	for _, dagScan := range restartableDagScans {
		err := dagScan.Restart()
		if err != nil {
			return fmt.Errorf("restarting dag scan %s: %w", dagScan.FsEntryID(), err)
		}
		if err := a.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
			return fmt.Errorf("updating restarted dag scan %s: %w", dagScan.FsEntryID(), err)
		}
	}
	return nil
}

var _ uploads.RestartDagScansForUploadFunc = API{}.RestartDagScansForUpload

// RunDagScansForUpload runs all pending and awaiting children DAG scans for the given upload, until there are no more scans to process.
func (a API) RunDagScansForUpload(ctx context.Context, uploadID id.UploadID, nodeCB func(node model.Node, data []byte) error) error {
	for {
		dagScans, err := a.Repo.DAGScansForUploadByStatus(ctx, uploadID, model.DAGScanStatePending, model.DAGScanStateAwaitingChildren)
		if err != nil {
			return fmt.Errorf("getting dag scans for upload %s: %w", uploadID, err)
		}
		log.Debugf("Found %d pending or awaiting children dag scans for upload %s", len(dagScans), uploadID)
		if len(dagScans) == 0 {
			return nil // No pending or awaiting children scans found, exit the loop
		}
		executions := 0
		for _, dagScan := range dagScans {
			switch dagScan.State() {
			case model.DAGScanStatePending:
				log.Debugf("Executing dag scan %s in state %s", dagScan.FsEntryID(), dagScan.State())
				if err := a.ExecuteDAGScan(ctx, dagScan, nodeCB); err != nil {
					return fmt.Errorf("executing dag scan %s: %w", dagScan.FsEntryID(), err)
				}
				executions++
			case model.DAGScanStateAwaitingChildren:
				log.Debugf("Handling awaiting children for dag scan %s in state %s", dagScan.FsEntryID(), dagScan.State())
				if err := a.HandleAwaitingChildren(ctx, dagScan); err != nil {
					return fmt.Errorf("handling awaiting children for dag scan %s: %w", dagScan.FsEntryID(), err)
				}
				// if the scan is now in a state where it can be executed, execute it
				if dagScan.State() == model.DAGScanStatePending {
					if err := a.ExecuteDAGScan(ctx, dagScan, nodeCB); err != nil {
						return fmt.Errorf("executing dag scan %s after handling awaiting children: %w", dagScan.FsEntryID(), err)
					}
					executions++
				}
			default:
				return fmt.Errorf("unexpected dag scan state %s for scan %s", dagScan.State(), dagScan.FsEntryID())
			}
		}
		if executions == 0 {
			return nil // No scans executed, only awaiting children handled and no pending scans left
		}
	}
}

var _ uploads.RunDagScansForUploadFunc = API{}.RunDagScansForUpload

// ExecuteDAGScan executes a dag scan on the given fs entry, creating a unix fs dag for the given file or directory.
func (a API) ExecuteDAGScan(ctx context.Context, dagScan model.DAGScan, nodeCB func(node model.Node, data []byte) error) error {
	err := dagScan.Start()
	if err != nil {
		log.Debug("Failed to start dag scan:", err)
		return fmt.Errorf("starting dag scan: %w", err)
	}
	if err := a.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
		log.Debugf("Failed to update dag scan %s: %v", dagScan.FsEntryID(), err)
		return fmt.Errorf("updating dag scan: %w", err)
	}
	cid, err := a.executeDAGScan(ctx, dagScan, nodeCB)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			if err := dagScan.Cancel(); err != nil {
				return fmt.Errorf("canceling dag scan: %w", err)
			}
		} else {
			if err := dagScan.Fail(err.Error()); err != nil {
				return fmt.Errorf("failing dag scan: %w", err)
			}
		}
	} else {
		log.Debugf("Completing DAG scan for %s with CID:", dagScan.FsEntryID(), cid)
		if err := dagScan.Complete(cid); err != nil {
			return fmt.Errorf("completing dag scan: %w", err)
		}
	}
	// Update the scan in the repository after completion or failure.
	log.Debugf("Updating dag scan %s after execution", dagScan.FsEntryID())
	if err := a.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
		return fmt.Errorf("updating dag scan after fail: %w", err)
	}
	return nil
}

func (a API) executeDAGScan(ctx context.Context, dagScan model.DAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	switch ds := dagScan.(type) {
	case *model.FileDAGScan:
		return a.executeFileDAGScan(ctx, ds, nodeCB)
	case *model.DirectoryDAGScan:
		return a.executeDirectoryDAGScan(ctx, ds, nodeCB)
	default:
		return cid.Undef, fmt.Errorf("unrecognized DAG scan type: %T", dagScan)
	}
}

func (a API) executeFileDAGScan(ctx context.Context, dagScan *model.FileDAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	log.Debugf("Executing file DAG scan for fsEntryID %s", dagScan.FsEntryID())
	f, sourceID, path, err := a.FileAccessor(ctx, dagScan.FsEntryID())
	if err != nil {
		return cid.Undef, fmt.Errorf("accessing file for DAG scan: %w", err)
	}
	defer f.Close()
	reader := visitor.ReaderPositionFromReader(f)
	visitor := visitor.NewUnixFSFileNodeVisitor(ctx, a.Repo, sourceID, path, reader, nodeCB)
	log.Debugf("Building UnixFS file with source ID %s and path %s", sourceID, path)
	l, _, err := builder.BuildUnixFSFile(reader, fmt.Sprintf("size-%d", BlockSize), visitor.LinkSystem())
	if err != nil {
		return cid.Undef, fmt.Errorf("building UnixFS file: %w", err)
	}
	log.Debugf("Built UnixFS file with CID: %s", l.(cidlink.Link).Cid)
	return l.(cidlink.Link).Cid, nil
}

func (a API) executeDirectoryDAGScan(ctx context.Context, dagScan *model.DirectoryDAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	log.Debugf("Executing directory DAG scan for fsEntryID %s", dagScan.FsEntryID())
	childLinks, err := a.Repo.DirectoryLinks(ctx, dagScan)
	if err != nil {
		return cid.Undef, fmt.Errorf("getting directory links for DAG scan: %w", err)
	}
	log.Debugf("Found %d child links for directory scan %s", len(childLinks), dagScan.FsEntryID())
	visitor := visitor.NewUnixFSDirectoryNodeVisitor(ctx, a.Repo, nodeCB)
	pbLinks, err := toLinks(childLinks)
	if err != nil {
		return cid.Undef, fmt.Errorf("converting links to PBLinks: %w", err)
	}
	log.Debugf("Building UnixFS directory with %d links", len(pbLinks))
	l, _, err := builder.BuildUnixFSDirectory(pbLinks, visitor.LinkSystem())
	log.Debugf("Built UnixFS directory with CID: %s", l.(cidlink.Link).Cid)
	return l.(cidlink.Link).Cid, err
}

// HandleAwaitingChildren checks if all child scans of a directory scan are completed and marks the parent scan pending if so.
func (a API) HandleAwaitingChildren(ctx context.Context, dagScan model.DAGScan) error {
	if dagScan.State() != model.DAGScanStateAwaitingChildren {
		return fmt.Errorf("DAG scan is not in awaiting children state: %s", dagScan.State())
	}
	switch ds := dagScan.(type) {
	case *model.DirectoryDAGScan:
		childScans, err := a.Repo.GetChildScans(ctx, ds)
		if err != nil {
			return fmt.Errorf("getting child scans: %w", err)
		}
		completeScans := make([]model.DAGScan, 0, len(childScans))
		for _, childScan := range childScans {
			if childScan.State() == model.DAGScanStateCompleted {
				completeScans = append(completeScans, childScan)
			}
			if childScan.State() == model.DAGScanStateFailed {
				if err := dagScan.Fail(fmt.Sprintf("child scan failed: %s", childScan.Error())); err != nil {
					return fmt.Errorf("marking scan as failed: %w", err)
				}
				if err := a.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
					return fmt.Errorf("updating scan after failure: %w", err)
				}
				return nil
			}
		}
		if len(completeScans) == len(childScans) {
			if err := ds.ChildrenCompleted(); err != nil {
				return fmt.Errorf("marking children as completed: %w", err)
			}
			if err := a.Repo.UpdateDAGScan(ctx, ds); err != nil {
				return fmt.Errorf("updating directory scan after children completed: %w", err)
			}
			return nil // All children completed successfully, mark the scan as pending
		}
		return nil // Still awaiting children, nothing to do
	case *model.FileDAGScan:
		return fmt.Errorf("DAG scan is not a directory scan: %T", ds)
	default:
		return fmt.Errorf("unrecognized DAG scan type: %T", dagScan)
	}
}

func toLinks(linkParams []model.LinkParams) ([]dagpb.PBLink, error) {
	links := make([]dagpb.PBLink, 0, len(linkParams))
	for _, c := range linkParams {
		link, err := builder.BuildUnixFSDirectoryEntry(c.Name, int64(c.TSize), cidlink.Link{Cid: c.Hash})
		if err != nil {
			return nil, fmt.Errorf("failed to build unixfs directory entry: %w", err)
		}
		links = append(links, link)
	}
	return links, nil
}
