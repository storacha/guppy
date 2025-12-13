package shards

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-ucanto/did"

	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/dags/nodereader"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var log = logging.Logger("preparation/shards")

type OpenNodeReaderFunc func() (nodereader.NodeReader, error)

// API provides methods to interact with the Shards in the repository.
type API struct {
	Repo             Repo
	OpenNodeReader   OpenNodeReaderFunc
	MaxNodesPerIndex int
	ShardEncoder     ShardEncoder
}

var _ uploads.AddNodeToUploadShardsFunc = API{}.AddNodeToUploadShards
var _ uploads.CloseUploadShardsFunc = API{}.CloseUploadShards
var _ storacha.ReaderForShardFunc = API{}.ReaderForShard
var _ storacha.IndexesForUploadFunc = API{}.IndexesForUpload

func (a API) AddNodeToUploadShards(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, nodeCID cid.Cid, data []byte) (bool, error) {
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
		hasRoom, err := roomInShard(a.ShardEncoder, s, node, space)
		if err != nil {
			return false, fmt.Errorf("failed to check room in shard %s for node %s: %w", s.ID(), node.CID(), err)
		}
		if hasRoom {
			shard = s
			break
		}
		if err := a.finalizeShardDigests(ctx, s); err != nil {
			return false, fmt.Errorf("finalizing shard %s: %w", s.ID(), err)
		}
		closed = true
	}

	// If no such shard exists, create a new one
	if shard == nil {
		shard, err = a.Repo.CreateShard(ctx,
			uploadID,
			a.ShardEncoder.HeaderEncodingLength(),
			a.ShardEncoder.HeaderDigestState(),
			a.ShardEncoder.HeaderPieceCIDState())

		if err != nil {
			return false, fmt.Errorf("failed to create new shard for upload %s: %w", uploadID, err)
		}
		hasRoom, err := roomInShard(a.ShardEncoder, shard, node, space)
		if err != nil {
			return false, fmt.Errorf("failed to check room in new shard for node %s: %w", node.CID(), err)
		}
		if !hasRoom {
			return false, fmt.Errorf("node %s (%d bytes) too large to fit in new shard for upload %s (shard size %d bytes)", node.CID(), node.Size(), uploadID, space.ShardSize())
		}
	}

	var addNodeOptions []AddNodeToShardOption
	if data != nil {
		digestStateUpdate, err := a.addNodeToDigestState(ctx, shard, node, data)
		if err != nil {
			return false, fmt.Errorf("failed to add node %s to shard %s digest state: %w", node.CID(), shard.ID(), err)
		}
		addNodeOptions = append(addNodeOptions, WithDigestStateUpdate(digestStateUpdate.digestStateUpTo, digestStateUpdate.digestState, digestStateUpdate.pieceCIDState))
	}

	if err := a.Repo.AddNodeToShard(ctx, shard.ID(), node.CID(), spaceDID, a.ShardEncoder.NodeEncodingLength(node)-node.Size(), addNodeOptions...); err != nil {
		return false, fmt.Errorf("failed to add node %s to shard %s for upload %s: %w", node.CID(), shard.ID(), uploadID, err)
	}

	return closed, nil
}

func roomInShard(encoder ShardEncoder, shard *model.Shard, node dagsmodel.Node, space *spacesmodel.Space) (bool, error) {
	nodeSize := encoder.NodeEncodingLength(node)

	if shard.Size()+nodeSize > space.ShardSize() {
		return false, nil // No room in the shard
	}

	return true, nil
}

type digestStateUpdate struct {
	digestStateUpTo uint64
	digestState     []byte
	pieceCIDState   []byte
}

func (a API) addNodeToDigestState(ctx context.Context, shard *model.Shard, node dagsmodel.Node, data []byte) (digestStateUpdate, error) {

	if uint64(len(data)) != node.Size() {
		return digestStateUpdate{}, fmt.Errorf("expected %d bytes for node %s, got %d", node.Size(), node.CID(), len(data))
	}

	hasher, err := a.updatedShardHashState(ctx, shard)
	if err != nil {
		return digestStateUpdate{}, fmt.Errorf("getting updated shard %s hasher: %w", shard.ID(), err)
	}

	err = a.ShardEncoder.WriteNode(ctx, node, data, hasher)
	if err != nil {
		return digestStateUpdate{}, fmt.Errorf("writing node %s to shard %s digest state: %w", node.CID(), shard.ID(), err)
	}

	digestState, pieceCIDState, err := hasher.marshal()
	if err != nil {
		return digestStateUpdate{}, fmt.Errorf("marshaling shard %s digest state: %w", shard.ID(), err)
	}

	return digestStateUpdate{
		digestStateUpTo: shard.Size() + a.ShardEncoder.NodeEncodingLength(node),
		digestState:     digestState,
		pieceCIDState:   pieceCIDState,
	}, nil
}

func (a API) updatedShardHashState(ctx context.Context, shard *model.Shard) (*shardHashState, error) {
	h, err := fromShard(shard)
	if err != nil {
		return nil, fmt.Errorf("getting shard %s hasher: %w", shard.ID(), err)
	}

	if shard.DigestStateUpTo() < shard.Size() {
		err := a.fastWriteShard(ctx, shard.ID(), shard.DigestStateUpTo(), h)
		if err != nil {
			return nil, fmt.Errorf("hashing remaining data for shard %s: %w", shard.ID(), err)
		}
	}

	return h, nil
}

func (a API) finalizeShardDigests(ctx context.Context, shard *model.Shard) error {
	h, err := a.updatedShardHashState(ctx, shard)
	if err != nil {
		return fmt.Errorf("getting updated shard %s hasher: %w", shard.ID(), err)
	}
	shardDigest, pieceCID, err := h.finalize(shard.Size())
	if err != nil {
		return fmt.Errorf("finalizing digests for shard %s: %w", shard.ID(), err)
	}
	if err := shard.Close(shardDigest, pieceCID); err != nil {
		return err
	}
	return a.Repo.UpdateShard(ctx, shard)
}

func (a API) CloseUploadShards(ctx context.Context, uploadID id.UploadID) (bool, error) {
	openShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.ShardStateOpen)
	if err != nil {

		return false, fmt.Errorf("failed to get open shards for upload %s: %w", uploadID, err)
	}

	var closed bool

	for _, s := range openShards {
		if err := a.finalizeShardDigests(ctx, s); err != nil {
			return false, fmt.Errorf("updating shard %s for upload %s: %w", s.ID(), uploadID, err)
		}
		closed = true
	}

	return closed, nil
}

// ReaderForShard uses fastWriteShard connected to a pipe to provide an io.Reader
// for shard data.
func (a API) ReaderForShard(ctx context.Context, shardID id.ShardID) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ctx)
	// cancel when we're done writing or when we hit an error so that worker
	// goroutines exit promptly.
	// NOTE: cancel is invoked in the writer goroutine.
	pr, pw := io.Pipe()

	go func() {
		defer cancel()
		defer pw.Close()
		err := a.fastWriteShard(ctx, shardID, 0, pw)
		if err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	return pr, nil
}

// Fast write streams data from a shard to the given io.Writer
// using multiple workers to fetch block data in parallel.
//  1. Spawning workers to fetch block data and enqueue ordered results.
//  2. Writing the shard header is starting at offset 0, then draining results in index order while
//     preserving ordering via a small pending map.
//  3. Propagating errors/cancellations through the pipe while draining to let
//     workers exit cleanly and bound memory via buffered channels.
func (a API) fastWriteShard(ctx context.Context, shardID id.ShardID, offset uint64, pw io.Writer) error {
	type (
		job struct {
			idx  int
			node dagsmodel.Node
		}
		result struct {
			idx  int
			node dagsmodel.Node
			data []byte
			err  error
		}
	)
	var (
		workerCount = runtime.NumCPU()

		jobs    chan job
		results chan result
	)

	if workerCount < 1 {
		workerCount = 1
	}

	// must have a NodeReader to fetch block data
	if a.OpenNodeReader == nil {
		return fmt.Errorf("no NodeReader configured")
	}

	jobWindow := workerCount * 4
	// Each result carries a whole block (~1MiB), so give results extra headroom
	// to keep workers busy if the writer is momentarily slow.
	resultWindow := jobWindow * 4
	jobs = make(chan job, jobWindow)
	results = make(chan result, resultWindow)
	var workers sync.WaitGroup
	workers.Add(workerCount)

	writeBlock := func(res result) error {
		if res.err != nil {
			return res.err
		}
		if res.data == nil {
			return fmt.Errorf("missing data for node %s", res.node.CID())
		}

		if err := a.ShardEncoder.WriteNode(ctx, res.node, res.data, pw); err != nil {
			return err
		}
		return nil
	}

	nodes, err := a.Repo.NodesByShard(ctx, shardID, offset)
	if err != nil {
		log.Debug("Error getting nodes for shard:", err)

		return err
	}

	nodeReader, err := a.OpenNodeReader()
	if err != nil {
		return fmt.Errorf("failed to open node reader for shard %s: %w", shardID, err)
	}
	defer nodeReader.Close()

	if offset == 0 {
		if err := a.ShardEncoder.WriteHeader(ctx, pw); err != nil {
			return fmt.Errorf("failed to write shard header: %w", err)
		}
	}

	for w := 0; w < workerCount; w++ {
		go func() {
			defer workers.Done()
			for j := range jobs {
				data, err := nodeReader.GetData(ctx, j.node)
				if err != nil {
					// Expand to all bad nodes in the shard to match previous behavior.
					err = a.makeErrBadNodes(ctx, shardID, nodeReader)
				}
				select {
				case <-ctx.Done():
					return
				case results <- result{idx: j.idx, node: j.node, data: data, err: err}:
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for idx, node := range nodes {
			select {
			case <-ctx.Done():
				return
			case jobs <- job{idx: idx, node: node}:
			}
		}
	}()

	go func() {
		workers.Wait()
		close(results)
	}()

	pending := make(map[int]result)
	next := 0
	resultsClosed := false
	drainOnly := false

	var closeErr error
	for {
		if resultsClosed && (len(pending) == 0 || drainOnly) {
			return closeErr
		}

		var (
			res result
			ok  bool
		)

		select {
		case <-ctx.Done():
			// Stop writing, but continue draining results so workers can exit.
			drainOnly = true
			closeErr = ctx.Err()
			continue
		case res, ok = <-results:
			if !ok {
				resultsClosed = true
				continue
			}
		}

		if drainOnly {
			continue
		}

		if res.err != nil {
			return fmt.Errorf("failed to write node %s: %w", res.node.CID(), res.err)
		}

		pending[res.idx] = res
		for {
			pendingRes, ok := pending[next]
			if !ok {
				break
			}

			if err := writeBlock(pendingRes); err != nil {
				return fmt.Errorf("failed to write node %s: %w", pendingRes.node.CID(), err)
			}
			delete(pending, next)
			next++
		}
	}
}

// makeErrBadNodes attempts to read data from all nodes in the given shard, and
// returns an [ErrBadNodes] for every one that fails. This is a relatively
// expensive check, so it should only be used once we know that we're failing.
// By communicating upstream all failing nodes from the shard, we can handle
// them all at once, and avoid having to restart the upload again for each bad
// node.
func (a API) makeErrBadNodes(ctx context.Context, shardID id.ShardID, nodeReader nodereader.NodeReader) error {
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

	var errs []types.BadNodeError
	goodCIDs := cid.NewSet()
	for _, node := range nodes {
		_, err := nodeReader.GetData(ctx, node)
		if err != nil {
			errs = append(errs, types.NewBadNodeError(node.CID(), err))
		} else {
			goodCIDs.Add(node.CID())
		}
	}

	return types.NewBadNodesError(errs, shardID, goodCIDs)
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

func (a API) RemoveShard(ctx context.Context, shardID id.ShardID) error {
	return a.Repo.DeleteShard(ctx, shardID)
}
