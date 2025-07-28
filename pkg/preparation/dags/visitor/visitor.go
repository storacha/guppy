package visitor

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var log = logging.Logger("preparation/dags/visitor")

// NodeCallback is a function type that is called for each node created during the scan.
type NodeCallback func(node model.Node, data []byte) error

// A UnixFSDirectoryNodeVisitor provides a link system for
// [builder.BuildUnixFSDirectory] which visits produced nodes with [cb] as
// they're encoded.
type UnixFSDirectoryNodeVisitor struct {
	repo Repo
	ctx  context.Context
	cb   NodeCallback
}

// NewUnixFSDirectoryNodeVisitor creates a new [UnixFSDirectoryNodeVisitor].
func NewUnixFSDirectoryNodeVisitor(ctx context.Context, repo Repo, cb NodeCallback) UnixFSDirectoryNodeVisitor {
	return UnixFSDirectoryNodeVisitor{
		repo: repo,
		ctx:  ctx,
		cb:   cb,
	}
}

// visitUnixFSNode is called for each UnixFS node found during the scan.
func (v UnixFSDirectoryNodeVisitor) visitUnixFSNode(datamodelNode datamodel.Node, cid cid.Cid, data []byte) error {
	size := uint64(len(data))
	pbNode, ok := datamodelNode.(dagpb.PBNode)
	if !ok {
		return fmt.Errorf("failed to cast node to PBNode")
	}
	ufsData := pbNode.FieldData().Must().Bytes()
	pbLinks := make([]dagpb.PBLink, 0, pbNode.FieldLinks().Length())
	iter := pbNode.FieldLinks().Iterator()
	for !iter.Done() {
		_, pbLink := iter.Next()
		pbLinks = append(pbLinks, pbLink)
	}

	node, created, err := v.repo.FindOrCreateUnixFSNode(v.ctx, cid, size, ufsData)
	if err != nil {
		return fmt.Errorf("creating unixfs node: %w", err)
	}
	if created {
		if len(pbLinks) > 0 {
			links := make([]model.LinkParams, len(pbLinks))
			for i, link := range pbLinks {
				links[i] = model.LinkParams{
					Hash:  link.FieldHash().Link().(cidlink.Link).Cid,
					Name:  link.FieldName().Must().String(),
					TSize: uint64(link.FieldTsize().Must().Int()),
				}
			}
			if err := v.repo.CreateLinks(v.ctx, cid, links); err != nil {
				return fmt.Errorf("creating links for unixfs node %s: %w", cid, err)
			}
		}
		if v.cb != nil {
			if err := v.cb(node, data); err != nil {
				return fmt.Errorf("on node callback: %w", err)
			}
		}
	}
	return nil
}

// A UnixFSFileNodeVisitor provides a link system for
// [builder.BuildUnixFSFile] which visits produced nodes with [cb] as
// they're encoded.
type UnixFSFileNodeVisitor struct {
	UnixFSDirectoryNodeVisitor
	sourceID       id.SourceID
	path           string // path is the root path of the scan
	readerPosition ReaderPosition
}

func NewUnixFSFileNodeVisitor(ctx context.Context, repo Repo, sourceID id.SourceID, path string, readerPosition ReaderPosition, cb NodeCallback) UnixFSFileNodeVisitor {
	return UnixFSFileNodeVisitor{
		UnixFSDirectoryNodeVisitor: NewUnixFSDirectoryNodeVisitor(ctx, repo, cb),
		sourceID:                   sourceID,
		path:                       path,
		readerPosition:             readerPosition,
	}
}

// visitRawNode is called for each raw node found during the scan.
func (v UnixFSFileNodeVisitor) visitRawNode(datamodelNode datamodel.Node, cid cid.Cid, data []byte) error {
	log.Debugf("Visiting raw node with CID: %s", cid)
	size := uint64(len(data))

	// this raw block has already been read, so we subtract its size to get the beginning offset
	offset := v.readerPosition.Offset() - size
	node, created, err := v.repo.FindOrCreateRawNode(v.ctx, cid, size, v.path, v.sourceID, offset)
	if err != nil {
		return fmt.Errorf("creating raw node: %w", err)
	}
	if created && v.cb != nil {
		if err := v.cb(node, data); err != nil {
			return fmt.Errorf("on node callback: %w", err)
		}
	}
	return nil
}
