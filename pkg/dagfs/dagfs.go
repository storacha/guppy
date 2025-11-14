package dagfs

import (
	"context"
	"fmt"
	"io/fs"
	"path"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	ipldfmt "github.com/ipfs/go-ipld-format"
)

// New creates a new dagFS, a [fs.FS] implementation backed by a UnixFS DAG. The
// provided rootCID is used as the root of the filesystem. Blocks are fetched
// using the provided dagService.
func New(ctx context.Context, dagService ipldfmt.DAGService, rootCID cid.Cid) *dagFS {
	return &dagFS{
		ctx:        ctx,
		dagService: dagService,
		rootCID:    rootCID,
	}
}

type dagFS struct {
	ctx        context.Context
	dagService ipldfmt.DAGService
	rootCID    cid.Cid
}

var _ fs.FS = (*dagFS)(nil)

func (dfs *dagFS) Open(fullPath string) (fs.File, error) {
	if !fs.ValidPath(fullPath) {
		return nil, &fs.PathError{
			Op:   "open",
			Path: fullPath,
			Err:  fs.ErrInvalid,
		}
	}
	return dfs.open(fullPath)
}

func (dfs *dagFS) open(fullPath string) (fs.File, error) {
	if fullPath == "." {
		// Open root directory
		rootNode, err := dfs.dagService.Get(dfs.ctx, dfs.rootCID)
		if err != nil {
			return nil, fmt.Errorf("failed to get root node: %w", err)
		}
		return dfs.openNode(rootNode, fullPath)
	} else {
		dirPath, name := path.Split(fullPath)
		dirPath = path.Clean(dirPath)

		dirFile, err := dfs.open(dirPath)
		if err != nil {
			if pe, ok := err.(*fs.PathError); ok {
				e := *pe
				e.Path = fullPath
				return nil, &e
			}
		}

		dir, ok := dirFile.(*dir)
		if !ok {
			return nil, &fs.PathError{
				Op:   "open",
				Path: fullPath,
				Err:  fs.ErrNotExist,
			}
		}

		childNode, err := dir.uioDir.Find(dfs.ctx, name)
		if err != nil {
			return nil, &fs.PathError{
				Op:   "open",
				Path: fullPath,
				Err:  err,
			}
		}

		return dfs.openNode(childNode, name)
	}
}

func (dfs *dagFS) openNode(node ipldfmt.Node, name string) (fs.File, error) {
	switch node := node.(type) {
	case *merkledag.ProtoNode:
		fsNode, err := unixfs.ExtractFSNode(node)
		if err != nil {
			return nil, fmt.Errorf("failed to extract UnixFS node: %w", err)
		}

		if fsNode.IsDir() {
			uioDir, err := uio.NewDirectoryFromNode(dfs.dagService, node)
			if err != nil {
				return nil, fmt.Errorf("failed to create directory from root node: %w", err)
			}
			return &dir{
				ctx:        dfs.ctx,
				uioDir:     uioDir,
				name:       name,
				dagService: dfs.dagService,
			}, nil
		} else {
			dagReader, err := uio.NewDagReader(dfs.ctx, node, dfs.dagService)
			if err != nil {
				return nil, fmt.Errorf("failed to create file reader: %w", err)
			}
			return &ufsFile{
				name:      name,
				fsNode:    fsNode,
				DagReader: dagReader,
			}, nil
		}

	case *merkledag.RawNode:
		dagReader, err := uio.NewDagReader(dfs.ctx, node, dfs.dagService)
		if err != nil {
			return nil, fmt.Errorf("failed to create file reader: %w", err)
		}
		return &rawFile{
			name:      name,
			DagReader: dagReader,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported node type for file %s: %T", name, node)
	}

}
