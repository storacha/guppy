package dags

import (
	"context"
	"io"
	"io/fs"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

const CACHE_SIZE = 256

type NodeReader struct {
	repo       Repo
	checkReads bool
	dataCache  *lru.Cache[cid.Cid, []byte]
	fileOpener FileOpenerFn
}

type FileOpenerFn func(ctx context.Context, sourceID id.SourceID, path string) (fs.File, error)

func NewNodeReader(repo Repo, fileOpener FileOpenerFn, checkReads bool) (*NodeReader, error) {
	cache, err := lru.New[cid.Cid, []byte](CACHE_SIZE)
	if err != nil {
		return nil, err
	}

	return &NodeReader{
		repo:       repo,
		checkReads: checkReads,
		dataCache:  cache,
		fileOpener: fileOpener,
	}, nil
}

func (nr *NodeReader) AddToCache(node model.Node, data []byte) {
	if node == nil || data == nil {
		return
	}
	nr.dataCache.ContainsOrAdd(node.CID(), data)
}

func (nr *NodeReader) GetData(ctx context.Context, node model.Node) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "get-node-data", trace.WithAttributes(
		attribute.String("node.cid", node.CID().String()),
		attribute.Int("node.size", int(node.Size())),
	))
	defer span.End()

	if data, ok := nr.dataCache.Get(node.CID()); ok {
		span.SetAttributes(attribute.Bool("cache.hit", true))
		return data, nil
	}

	start := time.Now()
	data, err := nr.getData(ctx, node)
	span.SetAttributes(attribute.Int64("fetch.duration_ms", time.Since(start).Milliseconds()))
	if err != nil {
		return nil, err
	}

	if nr.checkReads {
		if len(data) != int(node.Size()) {
			return nil, fs.ErrInvalid
		}
		foundCID, err := node.CID().Prefix().Sum(data)
		if err != nil {
			return nil, err
		}
		if !foundCID.Equals(node.CID()) {
			return nil, fs.ErrInvalid
		}
	}

	span.SetAttributes(attribute.Bool("cache.hit", false))
	nr.dataCache.Add(node.CID(), data)
	return data, nil
}

func (nr *NodeReader) getData(ctx context.Context, node model.Node) ([]byte, error) {
	switch n := node.(type) {
	case *model.RawNode:
		return nr.getRawNodeData(ctx, n)
	case *model.UnixFSNode:
		return nr.getUnixFSNodeData(ctx, n)
	default:
		return nil, fs.ErrInvalid
	}
}

func (nr *NodeReader) getRawNodeData(ctx context.Context, node *model.RawNode) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "get-raw-node", trace.WithAttributes(
		attribute.String("node.cid", node.CID().String()),
		attribute.Int("node.size", int(node.Size())),
	))
	defer span.End()

	file, err := nr.fileOpener(ctx, node.SourceID(), node.Path())
	if err != nil {
		return nil, err
	}
	defer file.Close()
	seeker, ok := file.(io.ReadSeeker)
	if !ok {
		return nil, fs.ErrInvalid
	}
	if _, err := seeker.Seek(int64(node.Offset()), io.SeekStart); err != nil {
		return nil, err
	}
	data := make([]byte, node.Size())
	if _, err := io.ReadFull(seeker, data); err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.Int64("read.bytes", int64(len(data))))
	return data, nil
}

func (nr *NodeReader) getUnixFSNodeData(ctx context.Context, node *model.UnixFSNode) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "get-unixfs-node", trace.WithAttributes(
		attribute.String("node.cid", node.CID().String()),
		attribute.Int("node.size", int(node.Size())),
	))
	defer span.End()

	if node.UFSData() == nil {
		return nil, fs.ErrInvalid
	}
	links, err := nr.repo.LinksForCID(ctx, node.CID(), node.SpaceDID())
	if err != nil {
		return nil, err
	}

	pbNode, err := buildNode(node.UFSData(), links)
	if err != nil {
		return nil, err
	}
	return ipld.Encode(pbNode, dagpb.Encode)
}

func buildNode(ufsData []byte, links []*model.Link) (datamodel.Node, error) {
	pbb := dagpb.Type.PBNode.NewBuilder()
	pbm, err := pbb.BeginMap(2)
	if err != nil {
		return nil, err
	}
	if err = pbm.AssembleKey().AssignString("Data"); err != nil {
		return nil, err
	}
	if err = pbm.AssembleValue().AssignBytes(ufsData); err != nil {
		return nil, err
	}
	if err = pbm.AssembleKey().AssignString("Links"); err != nil {
		return nil, err
	}
	lnks, err := pbm.AssembleValue().BeginList(int64(len(links)))
	if err != nil {
		return nil, err
	}
	for _, link := range links {
		pbLink, err := builder.BuildUnixFSDirectoryEntry(link.Name(), int64(link.TSize()), cidlink.Link{Cid: link.Hash()})
		if err != nil {
			return nil, err
		}
		if err := lnks.AssembleValue().AssignNode(pbLink); err != nil {
			return nil, err
		}
	}
	if err := lnks.Finish(); err != nil {
		return nil, err
	}
	if err := pbm.Finish(); err != nil {
		return nil, err
	}
	return pbb.Build(), nil
}
