package directory

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/guppy/pkg/preparation/dag/model"
	"github.com/storacha/guppy/pkg/preparation/dag/unixfs"
)

// VisitUnixFSNodeVisitor is an interface for visiting file nodes during the DAG building process.
type VisitUnixFSNodeVisitor interface {
	VisitUnixFSNode(cid cid.Cid, size uint64, ufsData []byte, links []dagpb.PBLink, data []byte) error
}

const defaultShardWidth = 256

// BuildUnixFSDirectory creates a dag of ipld Nodes representing file data,
// visiting each node with the provided visitor.
// This code is adapted from https://github.com/ipfs/go-unixfsnode/blob/main/data/builder/directory.go
// but is tailored to the dag walking process we need for database storage
func BuildUnixFSDirectory(linkParams []model.LinkParams, useHAMTDirectorySize uint64, visitor VisitUnixFSNodeVisitor) (cid.Cid, error) {
	if len(linkParams) > int(useHAMTDirectorySize) {
		return BuildUnixFSShardedDirectory(defaultShardWidth, multihash.MURMUR3X64_64, linkParams, visitor)
	}
	ufsNode, err := builder.BuildUnixFS(func(b *builder.Builder) {
		builder.DataType(b, data.Data_Directory)
	})
	if err != nil {
		return cid.Undef, fmt.Errorf("building unixfs: %w", err)
	}
	ufsData := data.EncodeUnixFSData(ufsNode)
	links, err := toLinks(linkParams)
	if err != nil {
		return cid.Undef, fmt.Errorf("converting links: %w", err)
	}
	nd, err := unixfs.BuildNode(ufsData, links)
	if err != nil {
		return cid.Undef, fmt.Errorf("building unixfs node: %w", err)
	}
	c, data, err := unixfs.WritePBNode(nd)
	if err != nil {
		return cid.Undef, fmt.Errorf("writing pb node: %w", err)
	}
	if err := visitor.VisitUnixFSNode(c, uint64(len(data)), ufsData, links, data); err != nil {
		return cid.Undef, fmt.Errorf("visiting unixfs node: %w", err)
	}
	return c, nil
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
