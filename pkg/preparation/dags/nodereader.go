package dags

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"sync"
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

const CACHE_SIZE = 512

type NodeReader struct {
	repo       Repo
	checkReads bool
	dataCache  *lru.Cache[cid.Cid, []byte]
	fileOpener FileOpenerFn
	cacheMu    sync.Mutex
	fileCache  *lru.Cache[string, *cachedFile]
}

type FileOpenerFn func(ctx context.Context, sourceID id.SourceID, path string) (fs.File, error)

type cachedFile struct {
	f  fs.File
	mu sync.Mutex
}

func (cf *cachedFile) Close() {
	if cf.f != nil {
		cf.f.Close()
	}
}

func (nr *NodeReader) acquireFile(ctx context.Context, sourceID id.SourceID, path string) (*cachedFile, error) {
	key := fmt.Sprintf("%s:%s", sourceID.String(), path)

	nr.cacheMu.Lock()
	if cf, ok := nr.fileCache.Get(key); ok {
		nr.cacheMu.Unlock()
		return cf, nil
	}
	nr.cacheMu.Unlock()

	f, err := nr.fileOpener(ctx, sourceID, path)
	if err != nil {
		return nil, err
	}
	cf := &cachedFile{f: f}

	nr.cacheMu.Lock()
	nr.fileCache.Add(key, cf)
	nr.cacheMu.Unlock()

	return cf, nil
}

func NewNodeReader(repo Repo, fileOpener FileOpenerFn, checkReads bool) (*NodeReader, error) {
	cache, err := lru.New[cid.Cid, []byte](CACHE_SIZE)
	if err != nil {
		return nil, err
	}
	fileCache, err := lru.NewWithEvict[string, *cachedFile](CACHE_SIZE, func(_ string, cf *cachedFile) {
		if cf != nil {
			cf.Close()
		}
	})
	if err != nil {
		return nil, err
	}

	return &NodeReader{
		repo:       repo,
		checkReads: checkReads,
		dataCache:  cache,
		fileOpener: fileOpener,
		fileCache:  fileCache,
	}, nil
}

func (nr *NodeReader) AddToCache(node model.Node, data []byte) {
	if node == nil || data == nil {
		return
	}
	nr.cacheMu.Lock()
	nr.dataCache.ContainsOrAdd(node.CID(), data)
	nr.cacheMu.Unlock()
}

func (nr *NodeReader) GetData(ctx context.Context, node model.Node) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "get-node-data", trace.WithAttributes(
		attribute.String("node.cid", node.CID().String()),
		attribute.Int("node.size", int(node.Size())),
	))
	defer span.End()

	nr.cacheMu.Lock()
	data, ok := nr.dataCache.Get(node.CID())
	nr.cacheMu.Unlock()
	if ok {
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
	nr.cacheMu.Lock()
	nr.dataCache.Add(node.CID(), data)
	nr.cacheMu.Unlock()
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

	cf, err := nr.acquireFile(ctx, node.SourceID(), node.Path())
	if err != nil {
		return nil, err
	}

	cf.mu.Lock()
	defer cf.mu.Unlock()

	seeker, ok := cf.f.(io.ReadSeeker)
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
