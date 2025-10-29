package shards

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	ipldcar "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-varint"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-ucanto/did"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var log = logging.Logger("preparation/shards")

// A CAR header with zero roots.
var noRootsHeader []byte

func init() {
	var err error
	noRootsHeaderWithoutLength, err := cbor.Encode(
		ipldcar.CarHeader{
			Roots:   nil,
			Version: 1,
		},
	)
	if err != nil {
		panic(fmt.Sprintf("failed to encode CAR header: %v", err))
	}

	var buf bytes.Buffer
	err = util.LdWrite(&buf, noRootsHeaderWithoutLength)
	if err != nil {
		panic(fmt.Sprintf("failed to length-delimit CAR header: %v", err))
	}

	noRootsHeader = buf.Bytes()
}

type NodeDataGetter interface {
	GetData(ctx context.Context, node dagsmodel.Node) ([]byte, error)
}

// API provides methods to interact with the Shards in the repository.
type API struct {
	Repo             Repo
	NodeReader       NodeDataGetter
	MaxNodesPerIndex int
}

var _ uploads.AddNodeToUploadShardsFunc = API{}.AddNodeToUploadShards
var _ uploads.CloseUploadShardsFunc = API{}.CloseUploadShards
var _ storacha.CarForShardFunc = API{}.CarForShard
var _ storacha.IndexesForUploadFunc = API{}.IndexesForUpload

func (a API) AddNodeToUploadShards(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, nodeCID cid.Cid) (bool, error) {
	space, err := a.Repo.GetSpaceByDID(ctx, spaceDID)
	if err != nil {
		return false, fmt.Errorf("failed to get space %s: %w", spaceDID, err)
	}

	openShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.ShardStateOpen)
	if err != nil {
		return false, fmt.Errorf("failed to get open shards for upload %s: %w", uploadID, err)
	}

	node, err := a.Repo.FindNodeByCIDAndSpaceDID(ctx, nodeCID, spaceDID)
	if err != nil {
		return false, fmt.Errorf("failed to find node %s: %w", nodeCID, err)
	}
	if node == nil {
		return false, fmt.Errorf("node %s not found", nodeCID)
	}

	var shard *model.Shard
	var closed bool

	// Look for an open shard that has room for the node, and close any that don't
	// have room. (There should only be at most one open shard, but there's no
	// harm handling multiple if they exist.)
	for _, s := range openShards {
		hasRoom, err := roomInShard(s, node, space)
		if err != nil {
			return false, fmt.Errorf("failed to check room in shard %s for node %s: %w", s.ID(), nodeCID, err)
		}
		if hasRoom {
			shard = s
			break
		}
		s.Close()
		if err := a.Repo.UpdateShard(ctx, s); err != nil {
			return false, fmt.Errorf("updating shard: %w", err)
		}
		closed = true
	}

	// If no such shard exists, create a new one
	if shard == nil {
		shard, err = a.Repo.CreateShard(ctx, uploadID, uint64(len(noRootsHeader)))
		if err != nil {
			return false, fmt.Errorf("failed to create new shard for upload %s: %w", uploadID, err)
		}
		hasRoom, err := roomInShard(shard, node, space)
		if err != nil {
			return false, fmt.Errorf("failed to check room in new shard for node %s: %w", nodeCID, err)
		}
		if !hasRoom {
			return false, fmt.Errorf("node %s (%d bytes) too large to fit in new shard for upload %s (shard size %d bytes)", nodeCID, node.Size(), uploadID, space.ShardSize())
		}
	}

	err = a.Repo.AddNodeToShard(ctx, shard.ID(), nodeCID, spaceDID, nodeEncodingLength(node)-node.Size())
	if err != nil {
		return false, fmt.Errorf("failed to add node %s to shard %s for upload %s: %w", nodeCID, shard.ID(), uploadID, err)
	}
	return closed, nil
}

func roomInShard(shard *model.Shard, node dagsmodel.Node, space *spacesmodel.Space) (bool, error) {
	nodeSize := nodeEncodingLength(node)

	if shard.Size()+nodeSize > space.ShardSize() {
		return false, nil // No room in the shard
	}

	return true, nil
}

func nodeEncodingLength(node dagsmodel.Node) uint64 {
	cid := node.CID()
	blockSize := node.Size()
	pllen := uint64(len(cidlink.Link{Cid: cid}.Binary())) + blockSize
	vilen := uint64(varint.UvarintSize(uint64(pllen)))
	return pllen + vilen
}

func (a API) CloseUploadShards(ctx context.Context, uploadID id.UploadID) (bool, error) {
	openShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.ShardStateOpen)
	if err != nil {
		return false, fmt.Errorf("failed to get open shards for upload %s: %w", uploadID, err)
	}

	var closed bool

	for _, s := range openShards {
		s.Close()
		if err := a.Repo.UpdateShard(ctx, s); err != nil {
			return false, fmt.Errorf("updating shard %s for upload %s: %w", s.ID(), uploadID, err)
		}
		closed = true
	}

	return closed, nil
}

func (a API) CarForShard(ctx context.Context, shardID id.ShardID) (io.Reader, error) {
	readers := []io.Reader{bytes.NewReader(noRootsHeader)}
	err := a.Repo.ForEachNode(ctx, shardID, func(node dagsmodel.Node, _ uint64) error {
		lengthReader := bytes.NewReader(lengthVarint(uint64(node.CID().ByteLen()) + node.Size()))
		cidReader := bytes.NewReader(node.CID().Bytes())
		newReader := NewLazyReader(func() ([]byte, error) {
			data, err := a.NodeReader.GetData(ctx, node)
			if err != nil {
				log.Debug("Error getting data for node ", node.CID(), ": ", err)
				return nil, a.makeErrBadNodes(ctx, shardID)
			}
			return data, nil
		})

		readers = append(readers, lengthReader, cidReader, newReader)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to iterate over nodes in shard %s: %w", shardID, err)
	}

	return io.MultiReader(readers...), nil
}

// makeErrBadNodes attempts to read data from all nodes in the given shard, and
// returns an [ErrBadNodes] for every one that fails. This is a relatively
// expensive check, so it should only be used once we know that we're failing.
// By communicating upstream all failing nodes from the shard, we can handle
// them all at once, and avoid having to restart the upload again for each bad
// node.
func (a API) makeErrBadNodes(ctx context.Context, shardID id.ShardID) error {
	// Collect the nodes first, because we can't read data for each node while
	// holding the lock on the database that ForEachNode has. This means holding a
	// bunch of nodes in memory, but it's limited to the size of a shard.
	var nodes []dagsmodel.Node
	err := a.Repo.ForEachNode(ctx, shardID, func(node dagsmodel.Node, _ uint64) error {
		nodes = append(nodes, node)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to iterate over nodes in shard %s: %w", shardID, err)
	}

	var errs []types.ErrBadNode
	for _, node := range nodes {
		_, err := a.NodeReader.GetData(ctx, node)
		if err != nil {
			errs = append(errs, types.NewErrBadNode(node.CID(), err))
		}
	}

	return types.NewErrBadNodes(errs)
}

func lengthVarint(size uint64) []byte {
	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, size)
	return buf[:n]
}

// LazyReader calls a function to provide its bytes the first time it's
// necessary, then holds the value buffered in memory.
type LazyReader struct {
	fn func() ([]byte, error)
	r  *bytes.Reader
}

// LazyReader implements io.Reader. It could implement anything that
// [bytes.Reader] does, but we'll have to implement each method we want
// separately.
var _ io.Reader = (*LazyReader)(nil)

// NewLazyReader creates a new [LazyReader] with the given function
func NewLazyReader(fn func() ([]byte, error)) *LazyReader {
	return &LazyReader{fn: fn}
}

func (lr *LazyReader) materialize() error {
	if lr.r == nil {
		data, err := lr.fn()
		if err != nil {
			return err
		}
		lr.r = bytes.NewReader(data)
	}
	return nil
}

func (lr *LazyReader) Read(p []byte) (n int, err error) {
	if err := lr.materialize(); err != nil {
		return 0, err
	}
	return lr.r.Read(p)
}

func (a API) IndexesForUpload(ctx context.Context, upload *uploadsmodel.Upload) ([]io.Reader, error) {
	if upload.RootCID() == cid.Undef {
		return nil, fmt.Errorf("no root CID set yet on upload %s", upload.ID())
	}

	shards, err := a.Repo.ShardsForUploadByState(ctx, upload.ID(), model.ShardStateAdded)
	if err != nil {
		return nil, fmt.Errorf("getting added shards for upload %s: %w", upload.ID(), err)
	}

	var indexes []blobindex.ShardedDagIndexView
	// Keep track of how many slices are in the current index, rather than sum
	// them constantly.
	currentIndexSliceCount := 0

	currentIndex := func() blobindex.ShardedDagIndexView {
		return indexes[len(indexes)-1]
	}

	startNewIndex := func() {
		nextIndex := blobindex.NewShardedDagIndexView(cidlink.Link{Cid: upload.RootCID()}, -1)
		indexes = append(indexes, nextIndex)
		currentIndexSliceCount = 0
	}

	startNewIndex()

	for _, s := range shards {
		shardSlices := blobindex.NewMultihashMap[blobindex.Position](-1)

		err := a.Repo.ForEachNode(ctx, s.ID(), func(node dagsmodel.Node, shardOffset uint64) error {
			position := blobindex.Position{
				Offset: shardOffset,
				Length: node.Size(),
			}
			shardSlices.Set(node.CID().Hash(), position)

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to iterate over nodes in shard %s: %w", s.ID(), err)
		}

		if shardSlices.Size() > a.MaxNodesPerIndex {
			return nil, fmt.Errorf("shard %s has %d nodes, exceeding max of %d", s.ID(), shardSlices.Size(), a.MaxNodesPerIndex)
		}

		if currentIndexSliceCount+shardSlices.Size() > a.MaxNodesPerIndex {
			startNewIndex()
		}

		if s.Digest() == nil || len(s.Digest()) == 0 {
			return nil, fmt.Errorf("added shard %s has no digest set", s.ID())
		}

		currentIndex().Shards().Set(s.Digest(), shardSlices)
		currentIndexSliceCount += shardSlices.Size()
	}

	indexReaders := make([]io.Reader, 0, len(indexes))
	for _, index := range indexes {
		indexReader, err := blobindex.Archive(index)
		if err != nil {
			return nil, fmt.Errorf("archiving index for upload %s: %w", upload.ID(), err)
		}
		indexReaders = append(indexReaders, indexReader)
	}

	return indexReaders, nil
}
