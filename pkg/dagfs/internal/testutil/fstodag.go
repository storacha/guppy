package testutil

import (
	"context"
	"io/fs"
	"path"

	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	ipld "github.com/ipfs/go-ipld-format"
)

// BuildDAGFromFS creates a UnixFS DAG from an fs.FS filesystem.
// It recursively walks the filesystem, creating directory nodes and file nodes.
// Returns the root node of the created DAG.
//
// Example usage:
//
//	ds := mdtest.Mock()
//	fsys := afero.NewIOFS(memFS)
//	rootNode, err := BuildDAGFromFS(context.Background(), ds, fsys)
func BuildDAGFromFS(ctx context.Context, dagService ipld.DAGService, fsys fs.FS, rawLeaves bool) (ipld.Node, error) {
	return buildDAGFromDir(ctx, dagService, fsys, ".", rawLeaves)
}

// buildDAGFromDir recursively builds a DAG from a directory in the filesystem
func buildDAGFromDir(ctx context.Context, dagService ipld.DAGService, fsys fs.FS, dirPath string, rawLeaves bool) (ipld.Node, error) {
	// Create a new directory
	dir, err := uio.NewDirectory(dagService)
	if err != nil {
		return nil, err
	}

	// Read directory entries
	entries, err := fs.ReadDir(fsys, dirPath)
	if err != nil {
		return nil, err
	}

	// Process each entry
	for _, entry := range entries {
		entryPath := path.Join(dirPath, entry.Name())
		if dirPath == "." {
			entryPath = entry.Name()
		}

		var childNode ipld.Node

		if entry.IsDir() {
			// Recursively build subdirectory
			childNode, err = buildDAGFromDir(ctx, dagService, fsys, entryPath, rawLeaves)
			if err != nil {
				return nil, err
			}
		} else {
			// Build file node
			childNode, err = buildDAGFromFile(dagService, fsys, entryPath, rawLeaves)
			if err != nil {
				return nil, err
			}
		}

		// Add child to directory
		err = dir.AddChild(ctx, entry.Name(), childNode)
		if err != nil {
			return nil, err
		}
	}

	// Get the directory node
	node, err := dir.GetNode()
	if err != nil {
		return nil, err
	}

	// Add the directory node to the DAG service
	err = dagService.Add(ctx, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// buildDAGFromFile creates a UnixFS file node from a file in the filesystem
func buildDAGFromFile(dagService ipld.DAGService, fsys fs.FS, filePath string, rawLeaves bool) (ipld.Node, error) {
	// Open the file
	file, err := fsys.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create a splitter from the file
	spl := chunker.DefaultSplitter(file)

	// Build the DAG from the file with RawLeaves enabled
	// This creates RawNodes for leaf blocks (small files)
	params := helpers.DagBuilderParams{
		Dagserv:   dagService,
		RawLeaves: rawLeaves,
		Maxlinks:  helpers.DefaultLinksPerBlock,
	}
	db, err := params.New(spl)
	if err != nil {
		return nil, err
	}

	return balanced.Layout(db)
}
