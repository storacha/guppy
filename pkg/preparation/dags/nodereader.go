package dags

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

const (
	// this cache is for data read from files, we read BLOCK_SIZE from each file, so this will never grow beyond
	// 1 GiB
	DataCacheSize       = 1024
	FileHandleCacheSize = 1024
)

type NodeReader struct {
	repo       Repo
	checkReads bool
	dataCache  *lru.Cache[cid.Cid, []byte]
	// TODO:(forrest) we may consider adding a "close" method to the `NodeReader` that closes all cached files when done
	fileCache  *lru.Cache[string, *cachedFile]
	fileOpener FileOpenerFn
}

type FileOpenerFn func(ctx context.Context, sourceID id.SourceID, path string) (fs.File, error)

type cachedFile struct {
	mu   sync.Mutex
	file fs.File
}

func NewNodeReader(repo Repo, fileOpener FileOpenerFn, checkReads bool) (*NodeReader, error) {
	dataCache, err := lru.New[cid.Cid, []byte](DataCacheSize)
	if err != nil {
		return nil, err
	}
	fileCache, err := lru.NewWithEvict[string, *cachedFile](FileHandleCacheSize, func(name string, cf *cachedFile) {
		if cf != nil && cf.file != nil {
			if err := cf.file.Close(); err != nil {
				log.Errorw("error closing cached file", "name", name, "err", err)
			}
		}
	})
	if err != nil {
		return nil, err
	}

	return &NodeReader{
		repo:       repo,
		checkReads: checkReads,
		dataCache:  dataCache,
		fileCache:  fileCache,
		fileOpener: fileOpener,
	}, nil
}

func (nr *NodeReader) getFile(ctx context.Context, sourceID id.SourceID, path string) (*cachedFile, error) {
	key := fmt.Sprintf("%s:%s", sourceID, path)

	if cf, ok := nr.fileCache.Get(key); ok {
		return cf, nil
	}

	f, err := nr.fileOpener(ctx, sourceID, path)
	if err != nil {
		return nil, err
	}
	cf := &cachedFile{file: f}
	nr.fileCache.Add(key, cf)
	return cf, nil
}

func (nr *NodeReader) GetData(ctx context.Context, node model.Node) ([]byte, error) {
	if data, ok := nr.dataCache.Get(node.CID()); ok {
		return data, nil
	}
	data, err := nr.getData(ctx, node)
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
	cf, err := nr.getFile(ctx, node.SourceID(), node.Path())
	if err != nil {
		return nil, err
	}
	cf.mu.Lock()
	defer cf.mu.Unlock()

	seeker, ok := cf.file.(io.ReadSeeker)
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
	return data, nil
}

func (nr *NodeReader) getUnixFSNodeData(ctx context.Context, node *model.UnixFSNode) ([]byte, error) {
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
