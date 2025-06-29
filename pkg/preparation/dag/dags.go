package dags

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/dag/directory"
	"github.com/storacha/guppy/pkg/preparation/dag/file"
	"github.com/storacha/guppy/pkg/preparation/dag/model"
	"github.com/storacha/guppy/pkg/preparation/dag/visitor"
	"github.com/storacha/guppy/pkg/preparation/types"
)

// UnixFSParams defines the parameters for UnixFS DAG scans.
type UnixFSParams interface {
	BlockSize() uint64
	LinksPerNode() uint64
	UseHAMTDirectorySize() uint64
}

// DAGAPI provides methods to interact with the DAG scans in the repository.
type DAGAPI struct {
	Repo         Repo
	FileAccessor FileAccessorFn
	UnixFSParams UnixFSParamsFn
}

// FileAccessorFn is a function type that retrieves a file for a given fsEntryID.
type FileAccessorFn func(ctx context.Context, fsEntryID types.FSEntryID) (fs.File, types.SourceID, string, error)

// UnixFSParamsFn is a function type that retrieves UnixFS parameters for a given upload ID.
type UnixFSParamsFn func(ctx context.Context, uploadID types.UploadID) UnixFSParams

// UploadDAGScanWorker processes DAG scans for an upload until the context is canceled or the work channel is closed.
func (d DAGAPI) UploadDAGScanWorker(ctx context.Context, work <-chan struct{}, uploadID types.UploadID, onScanTerminated func(types.FSEntryID, error) error, nodeCB func(node model.Node, data []byte) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // Exit if the context is canceled
		case _, ok := <-work:
			if !ok {
				return nil // Channel closed, exit the loop
			}
			// Run all pending and awaiting children DAG scans for the given upload.
			if err := d.RunDagScansForUpload(ctx, uploadID, onScanTerminated, nodeCB); err != nil {
				return fmt.Errorf("running dag scans for upload %s: %w", uploadID, err)
			}
		}
	}
}

// RunDagScansForUpload runs all pending and awaiting children DAG scans for the given upload, until there are no more scans to process.
func (d DAGAPI) RunDagScansForUpload(ctx context.Context, uploadID types.UploadID, onScanTerminated func(types.FSEntryID, error) error, nodeCB func(node model.Node, data []byte) error) error {
	for {
		dagScans, err := d.Repo.DAGScansForUploadByStatus(ctx, uploadID, model.DAGScanStatePending, model.DAGScanStateAwaitingChildren)
		if err != nil {
			return fmt.Errorf("getting dag scans for upload %s: %w", uploadID, err)
		}
		if len(dagScans) == 0 {
			return nil // No pending or awaiting children scans found, exit the loop
		}
		for _, dagScan := range dagScans {
			switch dagScan.State() {
			case model.DAGScanStatePending:
				if err := d.ExecuteDAGScan(ctx, dagScan, onScanTerminated, nodeCB); err != nil {
					return fmt.Errorf("executing dag scan %s: %w", dagScan.FsEntryID(), err)
				}
			case model.DAGScanStateAwaitingChildren:
				if err := d.HandleAwaitingChildren(ctx, dagScan, onScanTerminated); err != nil {
					return fmt.Errorf("handling awaiting children for dag scan %s: %w", dagScan.FsEntryID(), err)
				}
			default:
				return fmt.Errorf("unexpected dag scan state %s for scan %s", dagScan.State(), dagScan.FsEntryID())
			}
		}
	}
}

// ExecuteDAGScan executes a dag scan on the given fs entry, creating a unix fs dag for the given file or directory.
func (d DAGAPI) ExecuteDAGScan(ctx context.Context, dagScan model.DAGScan, onScanTerminated func(types.FSEntryID, error) error, nodeCB func(node model.Node, data []byte) error) error {
	err := dagScan.Start()
	if err != nil {
		return fmt.Errorf("starting scan: %w", err)
	}
	if err := d.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
		return fmt.Errorf("updating scan: %w", err)
	}
	cid, err := d.executeDAGScan(ctx, dagScan, nodeCB)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			if err := dagScan.Cancel(); err != nil {
				return fmt.Errorf("cancelling scan: %w", err)
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
	if err := d.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
		return fmt.Errorf("updating scan after fail: %w", err)
	}
	if err := onScanTerminated(dagScan.FsEntryID(), dagScan.Error()); err != nil {
		return fmt.Errorf("on scan terminated callback: %w", err)
	}
	return nil
}

func (d DAGAPI) executeDAGScan(ctx context.Context, dagScan model.DAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	switch ds := dagScan.(type) {
	case *model.FileDAGScan:
		return d.executeFileDAGScan(ctx, ds, nodeCB)
	case *model.DirectoryDAGScan:
		return d.executeDirectoryDAGScan(ctx, ds, nodeCB)
	default:
		return cid.Undef, fmt.Errorf("unrecognized DAG scan type: %T", dagScan)
	}
}

func (d DAGAPI) executeFileDAGScan(ctx context.Context, dagScan *model.FileDAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	f, sourceID, path, err := d.FileAccessor(ctx, dagScan.FsEntryID())
	if err != nil {
		return cid.Undef, fmt.Errorf("accessing file for DAG scan: %w", err)
	}
	defer f.Close()
	unixFSParams := d.UnixFSParams(ctx, dagScan.UploadID())
	visitor := visitor.NewUnixFSVisitor(ctx, d.Repo, sourceID, path, nodeCB)
	return file.BuildUnixFSFile(f, unixFSParams.BlockSize(), unixFSParams.LinksPerNode(), visitor)
}

func (d DAGAPI) executeDirectoryDAGScan(ctx context.Context, dagScan *model.DirectoryDAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	childLinks, err := d.Repo.DirectoryLinks(ctx, dagScan)
	if err != nil {
		return cid.Undef, fmt.Errorf("getting directory links for DAG scan: %w", err)
	}
	visitor := visitor.NewUnixFSNodeVisitor(ctx, d.Repo, nodeCB)
	return directory.BuildUnixFSDirectory(childLinks, d.UnixFSParams(ctx, dagScan.UploadID()).UseHAMTDirectorySize(), visitor)
}

// HandleAwaitingChildren checks if all child scans of a directory scan are completed and marks the parent scan pending if so.
func (d DAGAPI) HandleAwaitingChildren(ctx context.Context, dagScan model.DAGScan, onScanTerminated func(types.FSEntryID, error) error) error {
	if dagScan.State() != model.DAGScanStateAwaitingChildren {
		return fmt.Errorf("DAG scan is not in awaiting children state: %s", dagScan.State())
	}
	switch ds := dagScan.(type) {
	case *model.DirectoryDAGScan:
		childScans, err := d.Repo.GetChildScans(ctx, ds)
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
				if err := d.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
					return fmt.Errorf("updating scan after failure: %w", err)
				}
				if err := onScanTerminated(dagScan.FsEntryID(), dagScan.Error()); err != nil {
					return fmt.Errorf("on scan terminated callback for scan: %w", err)
				}
				return nil
			}
		}
		if len(completeScans) == len(childScans) {
			if err := ds.ChildrenCompleted(); err != nil {
				return fmt.Errorf("marking children as completed: %w", err)
			}
			if err := d.Repo.UpdateDAGScan(ctx, ds); err != nil {
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
