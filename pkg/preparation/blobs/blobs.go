package blobs

import (
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

	"github.com/storacha/guppy/internal/ctxutil"
	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/dags/nodereader"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
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
var _ uploads.AddShardsToUploadIndexesFunc = API{}.AddShardsToUploadIndexes
var _ uploads.CloseUploadShardsFunc = API{}.CloseUploadShards
var _ storacha.ReaderForShardFunc = API{}.ReaderForShard
var _ storacha.ReaderForIndexFunc = API{}.ReaderForIndex

func (a API) AddNodeToUploadShards(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, nodeCID cid.Cid, data []byte, shardCB func(shard *model.Shard) error) error {
	space, err := a.Repo.GetSpaceByDID(ctx, spaceDID)
	if err != nil {
		return fmt.Errorf("failed to get space %s: %w", spaceDID, err)
	}

	openShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.BlobStateOpen)
	if err != nil {
		return fmt.Errorf("failed to get open shards for upload %s: %w", uploadID, err)
	}

	node, err := a.Repo.FindNodeByCIDAndSpaceDID(ctx, nodeCID, spaceDID)
	if err != nil {
		return fmt.Errorf("failed to find node %s: %w", nodeCID, err)
	}
	if node == nil {
		return fmt.Errorf("node %s not found", nodeCID)
	}

	var shard *model.Shard

	// Look for an open shard that has room for the node, and close any that don't
	// have room. (There should only be at most one open shard, but there's no
	// harm handling multiple if they exist.)
	for _, s := range openShards {
		hasRoom, err := a.roomInShard(a.ShardEncoder, s, node, space)
		if err != nil {
			return fmt.Errorf("failed to check room in shard %s for node %s: %w", s.ID(), node.CID(), err)
		}
		if hasRoom {
			shard = s
			break
		}
		if err := a.closeShard(ctx, s); err != nil {
			return fmt.Errorf("closing shard %s: %w", s.ID(), err)
		}
		if shardCB != nil {
			err = shardCB(s)
			if err != nil {
				return fmt.Errorf("calling shardCB for closed shard %s: %w", s.ID(), err)
			}
		}
	}

	// If no such shard exists, create a new one
	if shard == nil {
		shard, err = a.Repo.CreateShard(ctx,
			uploadID,
			a.ShardEncoder.HeaderEncodingLength(),
			a.ShardEncoder.HeaderDigestState(),
			a.ShardEncoder.HeaderPieceCIDState())

		if err != nil {
			return fmt.Errorf("failed to create new shard for upload %s: %w", uploadID, err)
		}
		hasRoom, err := a.roomInShard(a.ShardEncoder, shard, node, space)
		if err != nil {
			return fmt.Errorf("failed to check room in new shard for node %s: %w", node.CID(), err)
		}
		if !hasRoom {
			return fmt.Errorf("node %s (%d bytes) too large to fit in new shard for upload %s (shard size %d bytes)", node.CID(), node.Size(), uploadID, space.ShardSize())
		}
	}

	var addNodeOptions []AddNodeToShardOption
	if data != nil {
		digestStateUpdate, err := a.addNodeToDigestState(ctx, shard, node, data)
		if err != nil {
			return fmt.Errorf("failed to add node %s to shard %s digest state: %w", node.CID(), shard.ID(), err)
		}
		addNodeOptions = append(addNodeOptions, WithDigestStateUpdate(digestStateUpdate.digestStateUpTo, digestStateUpdate.digestState, digestStateUpdate.pieceCIDState))
	}

	if err := a.Repo.AddNodeToShard(ctx, shard.ID(), node.CID(), spaceDID, uploadID, a.ShardEncoder.NodeEncodingLength(node)-node.Size(), addNodeOptions...); err != nil {
		return fmt.Errorf("failed to add node %s to shard %s for upload %s: %w", node.CID(), shard.ID(), uploadID, err)
	}

	return nil
}

// AddNodesToUploadShards assigns all unsharded nodes for an upload to shards.
// It fetches nodes that haven't been assigned to shards yet, then iterates through
// them calling AddNodeToUploadShards for each.
func (a API) AddNodesToUploadShards(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, shardCB func(shard *model.Shard) error) error {
	// Get all nodes not yet in shards
	nodeCIDs, err := a.Repo.NodesNotInShards(ctx, uploadID, spaceDID)
	if err != nil {
		return fmt.Errorf("failed to get nodes not in shards for upload %s: %w", uploadID, err)
	}

	if len(nodeCIDs) == 0 {
		return nil // No work to do
	}

	// Process each node by adding it to a shard
	for _, nodeCID := range nodeCIDs {
		// AddNodeToUploadShards handles finding/creating shards and assigns the node
		// Note: we pass nil for data since we don't have it available in this flow.
		// The digest state will be calculated later when reading the shard.
		err := a.AddNodeToUploadShards(ctx, uploadID, spaceDID, nodeCID, nil, shardCB)
		if err != nil {
			return fmt.Errorf("failed to add node %s to shard for upload %s: %w", nodeCID, uploadID, err)
		}
	}

	return nil
}

var _ uploads.AddNodesToUploadShardsFunc = API{}.AddNodesToUploadShards

func (a API) roomInShard(encoder ShardEncoder, shard *model.Shard, node dagsmodel.Node, space *spacesmodel.Space) (bool, error) {
	nodeSize := encoder.NodeEncodingLength(node)

	if shard.Size()+nodeSize > space.ShardSize() {
		return false, nil // No room in the shard
	}

	if a.MaxNodesPerIndex > 0 && shard.SliceCount() >= a.MaxNodesPerIndex {
		return false, nil // Shard has reached maximum node count
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

func (a API) closeShard(ctx context.Context, shard *model.Shard) error {
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

func (a API) CloseUploadShards(ctx context.Context, uploadID id.UploadID, shardCB func(shard *model.Shard) error) error {
	openShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.BlobStateOpen)
	if err != nil {

		return fmt.Errorf("failed to get open shards for upload %s: %w", uploadID, err)
	}

	for _, s := range openShards {
		if err := a.closeShard(ctx, s); err != nil {
			return fmt.Errorf("updating shard %s for upload %s: %w", s.ID(), uploadID, err)
		}
		if shardCB != nil {
			err = shardCB(s)
			if err != nil {
				return fmt.Errorf("calling shardCB for closed shard %s: %w", s.ID(), err)
			}
		}
	}

	return nil
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
			closeErr = ctxutil.Cause(ctx)
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
	nodes, err := a.Repo.NodesByShard(ctx, shardID, 0)
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

func (a API) ReaderForIndex(ctx context.Context, indexID id.IndexID) (io.ReadCloser, error) {
	index, err := a.Repo.GetIndexByID(ctx, indexID)
	if err != nil {
		return nil, fmt.Errorf("getting index %s: %w", indexID, err)
	}

	// Get the upload to retrieve the root CID
	upload, err := a.Repo.GetUploadByID(ctx, index.UploadID())
	if err != nil {
		return nil, fmt.Errorf("getting upload for index %s: %w", indexID, err)
	}
	if upload.RootCID() == cid.Undef {
		return nil, fmt.Errorf("no root CID set yet for upload %s (index %s)", upload.ID(), indexID)
	}

	// Build the index by reading shards from the database
	indexView := blobindex.NewShardedDagIndexView(cidlink.Link{Cid: upload.RootCID()}, -1)

	// Query shards that belong to this index
	shards, err := a.Repo.ShardsForIndex(ctx, indexID)
	if err != nil {
		return nil, fmt.Errorf("getting shards for index %s: %w", indexID, err)
	}

	// Add all shards in this index
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
			return nil, fmt.Errorf("iterating nodes in shard %s: %w", s.ID(), err)
		}

		if s.Digest() == nil || len(s.Digest()) == 0 {
			return nil, fmt.Errorf("shard %s has no digest set", s.ID())
		}
		indexView.Shards().Set(s.Digest(), shardSlices)
	}

	archReader, err := blobindex.Archive(indexView)
	if err != nil {
		return nil, fmt.Errorf("archiving index %s: %w", indexID, err)
	}

	return io.NopCloser(archReader), nil
}

func (a API) roomInIndex(index *model.Index, shard *model.Shard) (bool, error) {
	if a.MaxNodesPerIndex > 0 && index.SliceCount()+shard.SliceCount() > a.MaxNodesPerIndex {
		return false, nil // Index would exceed maximum slice count
	}

	return true, nil
}

func (a API) closeIndex(ctx context.Context, index *model.Index) error {
	if err := index.Close(); err != nil {
		return err
	}

	return a.Repo.UpdateIndex(ctx, index)
}

func (a API) AddShardsToUploadIndexes(ctx context.Context, uploadID id.UploadID, indexCB func(index *model.Index) error) error {
	shardsToIndex, err := a.Repo.ShardsNotInIndexes(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get shards not in indexes for upload %s: %w", uploadID, err)
	}

	if len(shardsToIndex) == 0 {
		return nil // No work to do
	}

	// Process each shard by adding it to an index
	for _, shardID := range shardsToIndex {
		// AddShardToUploadIndexes handles finding/creating indexes and assigns the shard
		err := a.AddShardToUploadIndexes(ctx, uploadID, shardID, indexCB)
		if err != nil {
			return fmt.Errorf("failed to add shard %s to index for upload %s: %w", shardID, uploadID, err)
		}
	}
	return nil
}

func (a API) AddShardToUploadIndexes(ctx context.Context, uploadID id.UploadID, shardID id.ShardID, indexCB func(index *model.Index) error) error {
	openIndexes, err := a.Repo.IndexesForUploadByState(ctx, uploadID, model.BlobStateOpen)
	if err != nil {
		return fmt.Errorf("failed to get open indexes for upload %s: %w", uploadID, err)
	}

	shard, err := a.Repo.GetShardByID(ctx, shardID)
	if err != nil {
		return fmt.Errorf("failed to find shard %s: %w", shardID, err)
	}
	if shard == nil {
		return fmt.Errorf("shard %s not found", shardID)
	}

	var index *model.Index

	// Look for an open index that has room for the shard.
	// (There should only be at most one open index, but there's no harm handling multiple if they exist.)
	for _, idx := range openIndexes {
		hasRoom, err := a.roomInIndex(idx, shard)
		if err != nil {
			return fmt.Errorf("failed to check room in index %s for shard %s: %w", idx.ID(), shard.CID(), err)
		}
		if hasRoom {
			index = idx
			break
		}
		if err := a.closeIndex(ctx, idx); err != nil {
			return fmt.Errorf("closing index %s: %w", idx.ID(), err)
		}
		if indexCB != nil {
			err = indexCB(idx)
			if err != nil {
				return fmt.Errorf("calling indexCB for closed index %s: %w", idx.ID(), err)
			}
		}
	}

	// If no such index exists, create a new one
	if index == nil {
		index, err = a.Repo.CreateIndex(ctx, uploadID)
		if err != nil {
			return fmt.Errorf("failed to create new index for upload %s: %w", uploadID, err)
		}
		hasRoom, err := a.roomInIndex(index, shard)
		if err != nil {
			return fmt.Errorf("failed to check room in new index for shard %s: %w", shard.CID(), err)
		}
		if !hasRoom {
			return fmt.Errorf("shard %s (%d slices) too large to fit in new index for upload %s (MaxNodesPerIndex: %d)", shard.CID(), shard.SliceCount(), uploadID, a.MaxNodesPerIndex)
		}
	}

	if err := a.Repo.AddShardToIndex(ctx, index.ID(), shard.ID()); err != nil {
		return fmt.Errorf("failed to add shard %s to index %s for upload %s: %w", shard.ID(), index.ID(), uploadID, err)
	}

	return nil
}

func (a API) CloseUploadIndexes(ctx context.Context, uploadID id.UploadID, indexCB func(index *model.Index) error) error {
	openIndexes, err := a.Repo.IndexesForUploadByState(ctx, uploadID, model.BlobStateOpen)
	if err != nil {

		return fmt.Errorf("failed to get open indexes for upload %s: %w", uploadID, err)
	}

	for _, s := range openIndexes {
		if err := a.closeIndex(ctx, s); err != nil {
			return fmt.Errorf("updating index %s for upload %s: %w", s.ID(), uploadID, err)
		}
		if indexCB != nil {
			err = indexCB(s)
			if err != nil {
				return fmt.Errorf("calling indexCB for closed index %s: %w", s.ID(), err)
			}
		}
	}

	return nil
}

func (a API) RemoveShard(ctx context.Context, shardID id.ShardID) error {
	return a.Repo.DeleteShard(ctx, shardID)
}
