package visitor

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Repo defines the interface for a repository that manages file system entries during a scan
type Repo interface {
	FindOrCreateRawNode(ctx context.Context, cid cid.Cid, size uint64, path string, sourceID id.SourceID, offset uint64) (*model.RawNode, bool, error)
	FindOrCreateUnixFSNode(ctx context.Context, cid cid.Cid, size uint64, ufsdata []byte) (*model.UnixFSNode, bool, error)
	CreateLinks(ctx context.Context, parent cid.Cid, links []model.LinkParams) error
}

// NodeCallback is a function type that is called for each node created during the scan.
type NodeCallback func(node model.Node, data []byte) error

// UnixFSNodeVisitor is a struct that implements the directory.UnixFSNodeVisitor interface.
type UnixFSNodeVisitor struct {
	repo Repo
	ctx  context.Context
	cb   NodeCallback
}

// NewUnixFSNodeVisitor creates a new UnixFSNodeVisitor with the provided context, repository, source ID, path, and callback function.
func NewUnixFSNodeVisitor(ctx context.Context, repo Repo, cb NodeCallback) UnixFSNodeVisitor {
	return UnixFSNodeVisitor{
		repo: repo,
		ctx:  ctx,
		cb:   cb,
	}
}

// VisitUnixFSNode is called for each UnixFS node found during the scan.
func (v UnixFSNodeVisitor) VisitUnixFSNode(cid cid.Cid, size uint64, ufsData []byte, pbLinks []dagpb.PBLink, data []byte) error {
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

// UnixFSVisitor is a struct that implements the file.UnixFSVisitor interface.
type UnixFSVisitor struct {
	UnixFSNodeVisitor
	sourceID       id.SourceID
	path           string // path is the root path of the scan
	readerPosition ReaderPosition
}

func NewUnixFSVisitor(ctx context.Context, repo Repo, sourceID id.SourceID, path string, readerPosition ReaderPosition, cb NodeCallback) UnixFSVisitor {
	return UnixFSVisitor{
		UnixFSNodeVisitor: NewUnixFSNodeVisitor(ctx, repo, cb),
		sourceID:          sourceID,
		path:              path,
		readerPosition:    readerPosition,
	}
}

// VisitRawNode is called for each raw node found during the scan.
func (v UnixFSVisitor) VisitRawNode(cid cid.Cid, size uint64, data []byte) error {
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

type ReaderPosition interface {
	io.Reader
	Offset() uint64
}

// ReaderPositionFromReader creates a ReaderPosition from an io.Reader.
func ReaderPositionFromReader(r io.Reader) ReaderPosition {
	return &readerPosition{
		reader: r,
		offset: 0,
	}
}

type readerPosition struct {
	reader io.Reader
	offset uint64
}

func (frp *readerPosition) Read(p []byte) (n int, err error) {
	n, err = frp.reader.Read(p)
	if n > 0 {
		frp.offset += uint64(n)
	}
	return n, err
}

func (frp *readerPosition) Offset() uint64 {
	return frp.offset
}
