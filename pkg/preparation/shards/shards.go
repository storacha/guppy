package shards

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"runtime"
	"sync"

	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipldcar "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
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

	mu           sync.Mutex
	shardHashers map[id.ShardID]*shardHashState
}

var _ uploads.AddNodeToUploadShardsFunc = (*API)(nil).AddNodeToUploadShards
var _ uploads.CloseUploadShardsFunc = (*API)(nil).CloseUploadShards
var _ storacha.CarForShardFunc = (*API)(nil).CarForShard
var _ storacha.IndexesForUploadFunc = (*API)(nil).IndexesForUpload

func (a *API) AddNodeToUploadShards(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, nodeCID cid.Cid, data []byte) (bool, error) {
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

	if data == nil && a.NodeReader != nil {
		data, err = a.NodeReader.GetData(ctx, node)
		if err != nil {
			return false, fmt.Errorf("failed to load data for node %s: %w", nodeCID, err)
		}
	}

	var shard *model.Shard
	var closed bool

	// Look for an open shard that has room for the node, and close any that don't
	// have room. (There should only be at most one open shard, but there's no
	// harm handling multiple if they exist.)
	for _, s := range openShards {
		hasRoom, err := roomInShard(s, node, space)
		if err != nil {
			return false, fmt.Errorf("failed to check room in shard %s for node %s: %w", s.ID(), node.CID(), err)
		}
		if hasRoom {
			shard = s
			break
		}
		s.Close()
		if err := a.finalizeShardDigests(ctx, s); err != nil {
			return false, fmt.Errorf("finalizing shard %s: %w", s.ID(), err)
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
			return false, fmt.Errorf("failed to check room in new shard for node %s: %w", node.CID(), err)
		}
		if !hasRoom {
			return false, fmt.Errorf("node %s (%d bytes) too large to fit in new shard for upload %s (shard size %d bytes)", node.CID(), node.Size(), uploadID, space.ShardSize())
		}
		a.initShardHasher(shard)
	}

	if err := a.Repo.AddNodeToShard(ctx, shard.ID(), node.CID(), spaceDID, nodeEncodingLength(node)-node.Size()); err != nil {
		return false, fmt.Errorf("failed to add node %s to shard %s for upload %s: %w", node.CID(), shard.ID(), uploadID, err)
	}
	if err := a.feedShardHasher(shard.ID(), node, data); err != nil {
		return false, fmt.Errorf("failed to hash node %s for shard %s: %w", node.CID(), shard.ID(), err)
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

type shardHashState struct {
	carHash   hash.Hash
	commpCalc *commp.Calc
}

func newShardHashState() *shardHashState {
	return &shardHashState{
		carHash:   sha256.New(),
		commpCalc: &commp.Calc{},
	}
}

func (s *shardHashState) write(parts ...[]byte) error {
	for _, p := range parts {
		if _, err := s.carHash.Write(p); err != nil {
			return err
		}
		if _, err := s.commpCalc.Write(p); err != nil {
			return err
		}
	}
	return nil
}

func (s *shardHashState) finalize(shardSize uint64) (multihash.Multihash, cid.Cid, error) {
	carDigest, err := multihash.Encode(s.carHash.Sum(nil), multihash.SHA2_256)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("encoding car digest: %w", err)
	}

	pieceDigest, _, err := s.commpCalc.Digest()
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("computing piece digest: %w", err)
	}

	pieceCID, err := commcid.DataCommitmentToPieceCidv2(pieceDigest, shardSize)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("computing piece CID: %w", err)
	}

	return carDigest, pieceCID, nil
}

func (a *API) initShardHasher(shard *model.Shard) {
	// Only initialize when the shard is brand new (header only), otherwise we
	// risk missing earlier bytes. Older shards will be re-hashed on close.
	if shard.Size() != uint64(len(noRootsHeader)) || a.NodeReader == nil {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.shardHashers == nil {
		a.shardHashers = make(map[id.ShardID]*shardHashState)
	}
	if _, ok := a.shardHashers[shard.ID()]; ok {
		return
	}

	h := newShardHashState()
	_ = h.write(noRootsHeader)
	a.shardHashers[shard.ID()] = h
}

func (a *API) getShardHasher(shardID id.ShardID) *shardHashState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.shardHashers[shardID]
}

func (a *API) popShardHasher(shardID id.ShardID) *shardHashState {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.shardHashers == nil {
		return nil
	}
	h := a.shardHashers[shardID]
	delete(a.shardHashers, shardID)
	return h
}

func (a *API) feedShardHasher(shardID id.ShardID, node dagsmodel.Node, data []byte) error {
	h := a.getShardHasher(shardID)
	if h == nil {
		return nil
	}

	if data == nil {
		return nil
	}

	if uint64(len(data)) != node.Size() {
		return fmt.Errorf("expected %d bytes for node %s, got %d", node.Size(), node.CID(), len(data))
	}

	prefix := lengthVarint(uint64(node.CID().ByteLen()) + node.Size())
	return h.write(prefix, node.CID().Bytes(), data)
}

func (a *API) computeShardDigests(ctx context.Context, shard *model.Shard) (multihash.Multihash, cid.Cid, error) {
	if h := a.popShardHasher(shard.ID()); h != nil {
		return h.finalize(shard.Size())
	}

	if a.NodeReader == nil {
		return nil, cid.Undef, fmt.Errorf("no node reader available to compute shard %s digests", shard.ID())
	}

	car, err := a.CarForShard(ctx, shard.ID())
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("rehydrating shard %s: %w", shard.ID(), err)
	}

	h := newShardHashState()
	if _, err := io.Copy(io.MultiWriter(h.carHash, h.commpCalc), car); err != nil {
		return nil, cid.Undef, fmt.Errorf("rehashing shard %s: %w", shard.ID(), err)
	}

	return h.finalize(shard.Size())
}

func (a *API) finalizeShardDigests(ctx context.Context, shard *model.Shard) error {
	carDigest, pieceCID, err := a.computeShardDigests(ctx, shard)
	if err == nil {
		if err := shard.SetDigests(carDigest, pieceCID); err != nil {
			return err
		}
	} else if a.NodeReader != nil {
		// If we had a node reader, propagate the error; otherwise leave digests unset.
		return err
	}

	return a.Repo.UpdateShard(ctx, shard)
}

func (a *API) CloseUploadShards(ctx context.Context, uploadID id.UploadID) (bool, error) {
	openShards, err := a.Repo.ShardsForUploadByState(ctx, uploadID, model.ShardStateOpen)
	if err != nil {
		return false, fmt.Errorf("failed to get open shards for upload %s: %w", uploadID, err)
	}

	var closed bool

	for _, s := range openShards {
		s.Close()
		if err := a.finalizeShardDigests(ctx, s); err != nil {
			return false, fmt.Errorf("updating shard %s for upload %s: %w", s.ID(), uploadID, err)
		}
		closed = true
	}

	return closed, nil
}

// CarForShard streams a CAR by:
//  1. Spawning workers to fetch block data and enqueue ordered results.
//  2. Writing the CAR header, then draining results in index order while
//     preserving ordering via a small pending map.
//  3. Propagating errors/cancellations through the pipe while draining to let
//     workers exit cleanly and bound memory via buffered channels.
func (a *API) CarForShard(ctx context.Context, shardID id.ShardID) (io.Reader, error) {
	// Collect nodes up front so DB iteration isn't held during block fetch.
	nodes := make([]dagsmodel.Node, 0, 2048)
	if err := a.Repo.ForEachNode(ctx, shardID, func(node dagsmodel.Node, _ uint64) error {
		nodes = append(nodes, node)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to iterate over nodes in shard %s: %w", shardID, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	// cancel when we're done writing or when we hit an error so that worker
	// goroutines exit promptly.
	// NOTE: cancel is invoked in the writer goroutine.
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
		pr, pw = io.Pipe()

		workerCount = runtime.NumCPU()

		jobs    chan job
		results chan result
	)

	if workerCount < 1 {
		workerCount = 1
	}

	jobWindow := workerCount * 4
	// Each result carries a whole block (~1MiB), so give results extra headroom
	// to keep workers busy if the writer is momentarily slow.
	resultWindow := jobWindow * 4
	jobs = make(chan job, jobWindow)
	results = make(chan result, resultWindow)
	var workers sync.WaitGroup
	workers.Add(workerCount)

	writeNode := func(w io.Writer, node dagsmodel.Node, data []byte) error {
		// Write length-prefix, cid bytes, then the block data directly to avoid
		// the extra allocations incurred by io.MultiReader per block.
		if _, err := w.Write(lengthVarint(uint64(node.CID().ByteLen()) + node.Size())); err != nil {
			return err
		}
		if _, err := w.Write(node.CID().Bytes()); err != nil {
			return err
		}
		_, err := w.Write(data)
		return err
	}

	writeBlock := func(res result) error {
		if res.err != nil {
			return res.err
		}
		if res.data == nil {
			return fmt.Errorf("missing data for node %s", res.node.CID())
		}

		if err := writeNode(pw, res.node, res.data); err != nil {
			return err
		}
		return nil
	}

	go func() {
		defer cancel()
		defer pw.Close()

		if _, err := pw.Write(noRootsHeader); err != nil {
			pw.CloseWithError(fmt.Errorf("failed to write CAR header: %w", err))
			return
		}

		for w := 0; w < workerCount; w++ {
			go func() {
				defer workers.Done()
				for j := range jobs {
					data, err := a.NodeReader.GetData(ctx, j.node)
					if err != nil {
						// Expand to all bad nodes in the shard to match previous behavior.
						err = a.makeErrBadNodes(ctx, shardID)
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

		for {
			if resultsClosed && len(pending) == 0 {
				return
			}

			var (
				res result
				ok  bool
			)

			select {
			case <-ctx.Done():
				// Stop writing, but continue draining results so workers can exit.
				drainOnly = true
				_ = pw.CloseWithError(ctx.Err())
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
				pw.CloseWithError(fmt.Errorf("failed to write node %s: %w", res.node.CID(), res.err))
				return
			}

			pending[res.idx] = res
			for {
				pendingRes, ok := pending[next]
				if !ok {
					break
				}

				if err := writeBlock(pendingRes); err != nil {
					pw.CloseWithError(fmt.Errorf("failed to write node %s: %w", pendingRes.node.CID(), err))
					return
				}
				delete(pending, next)
				next++
			}
		}
	}()

	return pr, nil
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

func (a *API) IndexesForUpload(ctx context.Context, upload *uploadsmodel.Upload) ([]io.Reader, error) {
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
