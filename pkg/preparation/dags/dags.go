package dags

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/dags/visitor"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

const BlockSize = 1 << 20         // 1 MiB
const DefaultLinksPerBlock = 1024 // Default number of links per block for UnixFS

func init() {
	// Set the default links per block for UnixFS.
	// annoying this is not more easily configurable, but this is the only way to set it globally.
	builder.DefaultLinksPerBlock = DefaultLinksPerBlock
}

// API provides methods to interact with the DAG scans in the repository.
type API struct {
	Repo         Repo
	FileAccessor FileAccessorFn
}

// FileAccessorFn is a function type that retrieves a file for a given fsEntryID.
type FileAccessorFn func(ctx context.Context, fsEntryID id.FSEntryID) (fs.File, id.SourceID, string, error)

// UploadDAGScanWorker processes DAG scans for an upload until the context is canceled or the work channel is closed.
func (a API) UploadDAGScanWorker(ctx context.Context, work <-chan struct{}, uploadID id.UploadID, nodeCB func(node model.Node, data []byte) error) error {
	err := a.RestartScansForUpload(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("restarting scans for upload %s: %w", uploadID, err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // Exit if the context is canceled
		case _, ok := <-work:
			if !ok {
				return nil // Channel closed, exit the loop
			}
			// Run all pending and awaiting children DAG scans for the given upload.
			if err := a.RunDagScansForUpload(ctx, uploadID, nodeCB); err != nil {
				return fmt.Errorf("running dag scans for upload %s: %w", uploadID, err)
			}
		}
	}
}

// RestartScansForUpload restarts all canceled or running DAG scans for the given upload ID.
func (a API) RestartScansForUpload(ctx context.Context, uploadID id.UploadID) error {
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

// RunDagScansForUpload runs all pending and awaiting children DAG scans for the given upload, until there are no more scans to process.
func (a API) RunDagScansForUpload(ctx context.Context, uploadID id.UploadID, nodeCB func(node model.Node, data []byte) error) error {
	for {
		dagScans, err := a.Repo.DAGScansForUploadByStatus(ctx, uploadID, model.DAGScanStatePending, model.DAGScanStateAwaitingChildren)
		if err != nil {
			return fmt.Errorf("getting dag scans for upload %s: %w", uploadID, err)
		}
		if len(dagScans) == 0 {
			return nil // No pending or awaiting children scans found, exit the loop
		}
		executions := 0
		for _, dagScan := range dagScans {
			switch dagScan.State() {
			case model.DAGScanStatePending:
				if err := a.ExecuteDAGScan(ctx, dagScan, nodeCB); err != nil {
					return fmt.Errorf("executing dag scan %s: %w", dagScan.FsEntryID(), err)
				}
				executions++
			case model.DAGScanStateAwaitingChildren:
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

// ExecuteDAGScan executes a dag scan on the given fs entry, creating a unix fs dag for the given file or directory.
func (a API) ExecuteDAGScan(ctx context.Context, dagScan model.DAGScan, nodeCB func(node model.Node, data []byte) error) error {
	err := dagScan.Start()
	if err != nil {
		return fmt.Errorf("starting scan: %w", err)
	}
	if err := a.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
		return fmt.Errorf("updating scan: %w", err)
	}
	cid, err := a.executeDAGScan(ctx, dagScan, nodeCB)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			if err := dagScan.Cancel(); err != nil {
				return fmt.Errorf("canceling scan: %w", err)
			}
		} else {
			if err := dagScan.Fail(err.Error()); err != nil {
				return fmt.Errorf("failing scan: %w", err)
			}
		}
	} else {
		if err := dagScan.Complete(cid); err != nil {
			return fmt.Errorf("completing scan: %w", err)
		}
	}
	// Update the scan in the repository after completion or failure.
	if err := a.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
		return fmt.Errorf("updating scan after fail: %w", err)
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
	f, sourceID, path, err := a.FileAccessor(ctx, dagScan.FsEntryID())
	if err != nil {
		return cid.Undef, fmt.Errorf("accessing file for DAG scan: %w", err)
	}
	defer f.Close()
	reader := visitor.ReaderPositionFromReader(f)
	visitor := visitor.NewUnixFSVisitor(ctx, a.Repo, sourceID, path, reader, nodeCB)
	l, _, err := builder.BuildUnixFSFile(reader, fmt.Sprintf("size-%d", BlockSize), visitor.LinkSystem())
	return l.(cidlink.Link).Cid, err
}

func (a API) executeDirectoryDAGScan(ctx context.Context, dagScan *model.DirectoryDAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	childLinks, err := a.Repo.DirectoryLinks(ctx, dagScan)
	if err != nil {
		return cid.Undef, fmt.Errorf("getting directory links for DAG scan: %w", err)
	}
	visitor := visitor.NewUnixFSNodeVisitor(ctx, a.Repo, nodeCB)
	pbLinks, err := toLinks(childLinks)
	if err != nil {
		return cid.Undef, fmt.Errorf("converting links to PBLinks: %w", err)
	}
	l, _, err := builder.BuildUnixFSDirectory(pbLinks, visitor.LinkSystem())
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
				if err := dagScan.Fail("child scan failed"); err != nil {
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
