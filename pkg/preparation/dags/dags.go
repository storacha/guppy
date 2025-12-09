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
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/dags/visitor"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const BlockSize = 1 << 20         // 1 MiB
const DefaultLinksPerBlock = 1024 // Default number of links per block for UnixFS

var (
	log    = logging.Logger("preparation/dags")
	tracer = otel.Tracer("preparation/dags")
)

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

var _ uploads.ExecuteDagScansForUploadFunc = API{}.ExecuteDagScansForUpload
var _ uploads.RemoveBadNodesFunc = API{}.RemoveBadNodes

// ExecuteDagScansForUpload runs all pending and awaiting children DAG scans for the given upload, until there are no more scans to process.
func (a API) ExecuteDagScansForUpload(ctx context.Context, uploadID id.UploadID, scanCB func(scan *model.DAGScan) error, nodeCB func(node model.Node, data []byte) error) error {
	var badFsEntryErrs []types.BadFSEntryError
	for {
		ctx, span := tracer.Start(ctx, "dag-scans-batch", trace.WithAttributes(
			attribute.String("upload.id", uploadID.String()),
		))
		defer span.End() // In case of early return

		dagScans, err := a.Repo.IncompleteDAGScansForUpload(ctx, uploadID)
		if err != nil {
			return fmt.Errorf("getting dag scans for upload %s: %w", uploadID, err)
		}
		log.Debugf("Found %d pending or awaiting children dag scans for upload %s", len(dagScans), uploadID)
		if len(dagScans) == 0 {
			return nil // No pending or awaiting children scans found, exit the loop
		}
		executions := 0
		for _, dagScan := range dagScans {
			if dagScan.CID().Defined() {
				return fmt.Errorf("tried to execute completed dag scan %s (cid %s)", dagScan.FsEntryID(), dagScan.CID())
			}

			log.Debugf("Executing dag scan %s", dagScan.FsEntryID())
			if err := a.executeDAGScan(ctx, dagScan, nodeCB); err != nil {
				var errBadFSEntry types.BadFSEntryError
				if errors.As(err, &errBadFSEntry) {
					badFsEntryErrs = append(badFsEntryErrs, errBadFSEntry)
					continue // Continue with the next dag scan
				}
				return fmt.Errorf("executing dag scan %s: %w", dagScan.FsEntryID(), err)
			}

			executions++
			span.SetAttributes(attribute.Int("executed", executions))
		}

		span.End()

		if executions == 0 {
			if len(badFsEntryErrs) > 0 {
				return types.NewBadFSEntriesError(badFsEntryErrs)
			}
			return nil // No scans executed, only awaiting children handled and no pending scans left
		}
	}
}

// executeDAGScan executes a dag scan on the given fs entry, creating a unix fs dag for the given file or directory.
func (a API) executeDAGScan(ctx context.Context, dagScan model.DAGScan, nodeCB func(node model.Node, data []byte) error) error {
	var err error
	var cid cid.Cid
	switch ds := dagScan.(type) {
	case *model.FileDAGScan:
		cid, err = a.executeFileDAGScan(ctx, ds, nodeCB)
	case *model.DirectoryDAGScan:
		cid, err = a.executeDirectoryDAGScan(ctx, ds, nodeCB)
	default:
		return fmt.Errorf("unrecognized DAG scan type: %T", dagScan)
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			return fmt.Errorf("executing dag scan: %w", err)
		}

		return types.NewBadFSEntryError(dagScan.FsEntryID(), err)
	}

	log.Debugf("Completing DAG scan for %s with CID:", dagScan.FsEntryID(), cid)
	if err := dagScan.Complete(cid); err != nil {
		return fmt.Errorf("completing dag scan: %w", err)
	}

	// Update the scan in the repository after completion or failure.
	log.Debugf("Updating dag scan %s after execution", dagScan.FsEntryID())
	if err := a.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
		return fmt.Errorf("updating dag scan after fail: %w", err)
	}
	return nil
}

func (a API) executeFileDAGScan(ctx context.Context, dagScan *model.FileDAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	log.Debugf("Executing file DAG scan for fsEntryID %s", dagScan.FsEntryID())
	f, sourceID, path, err := a.FileAccessor(ctx, dagScan.FsEntryID())
	if err != nil {
		return cid.Undef, fmt.Errorf("accessing file for DAG scan: %w", err)
	}
	defer f.Close()
	reader := visitor.ReaderPositionFromReader(f)
	visitor := visitor.NewUnixFSFileNodeVisitor(ctx, a.Repo, sourceID, path, reader, dagScan.SpaceDID(), nodeCB)
	log.Debugf("Building UnixFS file with source ID %s and path %s", sourceID, path)
	l, _, err := builder.BuildUnixFSFile(reader, fmt.Sprintf("size-%d", BlockSize), visitor.LinkSystem())
	if err != nil {
		return cid.Undef, fmt.Errorf("building UnixFS file: %w", err)
	}
	log.Debugf("Built UnixFS file with CID: %s", l.(cidlink.Link).Cid)
	return l.(cidlink.Link).Cid, nil
}

func (a API) executeDirectoryDAGScan(ctx context.Context, dagScan *model.DirectoryDAGScan, nodeCB func(node model.Node, data []byte) error) (cid.Cid, error) {
	hasIncompleteChildren, err := a.Repo.HasIncompleteChildren(ctx, dagScan)
	if err != nil {
		return cid.Undef, fmt.Errorf("looking for incomplete children: %w", err)
	}
	if hasIncompleteChildren {
		log.Debugf("Directory DAG scan %s has incomplete children, skipping on this pass", dagScan.FsEntryID())
		return cid.Undef, nil
	}

	log.Debugf("Executing directory DAG scan for fsEntryID %s", dagScan.FsEntryID())
	childLinks, err := a.Repo.DirectoryLinks(ctx, dagScan)
	if err != nil {
		return cid.Undef, fmt.Errorf("getting directory links for DAG scan: %w", err)
	}
	log.Debugf("Found %d child links for directory scan %s", len(childLinks), dagScan.FsEntryID())
	visitor := visitor.NewUnixFSDirectoryNodeVisitor(ctx, a.Repo, dagScan.SpaceDID(), nodeCB)
	pbLinks, err := toLinks(childLinks)
	if err != nil {
		return cid.Undef, fmt.Errorf("converting links to PBLinks: %w", err)
	}
	log.Debugf("Building UnixFS directory with %d links", len(pbLinks))
	l, _, err := builder.BuildUnixFSDirectory(pbLinks, visitor.LinkSystem())
	if err != nil {
		return cid.Undef, fmt.Errorf("building UnixFS directory: %w", err)
	}
	log.Debugf("Built UnixFS directory with CID: %s", l.(cidlink.Link).Cid)
	return l.(cidlink.Link).Cid, nil
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

func (a API) RemoveBadNodes(ctx context.Context, spaceDID did.DID, nodeCIDs []cid.Cid) error {
	return a.Repo.DeleteNodes(ctx, spaceDID, nodeCIDs)
}
