package uploads

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/did"

	cbor "github.com/ipfs/go-ipld-cbor"
	ipldcar "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/storacha/guppy/pkg/preparation/bettererrgroup"
	dagmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var (
	log    = logging.Logger("preparation/uploads")
	tracer = otel.Tracer("preparation/uploads")
)

type ExecuteScanFunc func(ctx context.Context, uploadID id.UploadID, nodeCB func(node scanmodel.FSEntry) error) error
type ExecuteDagScansForUploadFunc func(ctx context.Context, uploadID id.UploadID, nodeCB func(node dagmodel.Node, data []byte) error) error
type AddNodeToUploadShardsFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID, nodeCID cid.Cid) (id.ShardID, []id.ShardID, error)
type CloseUploadShardsFunc func(ctx context.Context, uploadID id.UploadID) ([]id.ShardID, error)
type AddShardsForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error
type AddIndexesForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error
type AddStorachaUploadForUploadFunc func(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) error
type RemoveBadFSEntryFunc func(ctx context.Context, spaceDID did.DID, fsEntryID id.FSEntryID) error
type RemoveBadNodesFunc func(ctx context.Context, spaceDID did.DID, nodeCIDs []cid.Cid) error

type API struct {
	Repo                       Repo
	ExecuteScan                ExecuteScanFunc
	ExecuteDagScansForUpload   ExecuteDagScansForUploadFunc
	AddShardsForUpload         AddShardsForUploadFunc
	AddIndexesForUpload        AddIndexesForUploadFunc
	AddStorachaUploadForUpload AddStorachaUploadForUploadFunc
	RemoveBadFSEntry           RemoveBadFSEntryFunc
	RemoveBadNodes             RemoveBadNodesFunc

	// AddNodeToUploadShards adds a node to the upload's shards, creating a new
	// shard if necessary. It returns true if an existing open shard was closed,
	// false otherwise.
	AddNodeToUploadShards AddNodeToUploadShardsFunc

	// CloseUploadShards closes any remaining open shard for the upload. It
	// returns true if an existing open shard was in fact closed, false if there
	// was no open shard to close.
	CloseUploadShards CloseUploadShardsFunc
}

type shardBlock struct {
	node dagmodel.Node
	data []byte
}

type shardBuildResult struct {
	carDigest multihash.Multihash
	sha256MH  multihash.Multihash
	piece     []byte
	err       error
}

type shardBuilder struct {
	id     id.ShardID
	tasks  chan shardBlock
	result chan shardBuildResult
}

func newShardBuilder(ctx context.Context, shardID id.ShardID, sem chan struct{}) *shardBuilder {
	tasks := make(chan shardBlock, 16)
	result := make(chan shardBuildResult, 1)

	go func() {
		defer func() { <-sem }()
		defer close(result)

		sha := sha256.New()
		commpCalc := &commp.Calc{}
		mw := io.MultiWriter(sha, commpCalc)

		var (
			res       shardBuildResult
			totalSize uint64
		)

		if _, err := mw.Write(noRootsHeader()); err != nil {
			res.err = err
			result <- res
			return
		}
		totalSize += uint64(len(noRootsHeader()))

		for blk := range tasks {
			select {
			case <-ctx.Done():
				res.err = ctx.Err()
				continue
			default:
			}

			if res.err != nil {
				continue
			}

			if err := writeCarBlock(mw, blk.node, blk.data); err != nil {
				res.err = err
				continue
			}
			cidLen := blk.node.CID().ByteLen()
			totalSize += uint64(cidLen) + blk.node.Size() + uint64(len(lengthVarint(uint64(cidLen)+blk.node.Size())))
		}

		if res.err == nil {
			d := sha.Sum(nil)
			carMH, err := multihash.Sum(d, multihash.SHA2_256, -1)
			if err != nil {
				res.err = err
			} else {
				res.carDigest = carMH
				res.sha256MH = carMH
				if totalSize >= commp.MinPiecePayload && totalSize <= commp.MaxPiecePayload {
					pieceDigest, _, err := commpCalc.Digest()
					if err != nil {
						res.err = err
					} else {
						res.piece = pieceDigest
					}
				}
			}
		}

		result <- res
	}()

	return &shardBuilder{
		id:     shardID,
		tasks:  tasks,
		result: result,
	}
}

func (sb *shardBuilder) addBlock(ctx context.Context, blk shardBlock) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case sb.tasks <- blk:
		return nil
	}
}

func (sb *shardBuilder) close() {
	close(sb.tasks)
}

func (sb *shardBuilder) wait() shardBuildResult {
	return <-sb.result
}

func writeCarBlock(w io.Writer, node dagmodel.Node, data []byte) error {
	cidBytes := node.CID().Bytes()
	length := uint64(len(cidBytes)) + node.Size()
	if _, err := w.Write(lengthVarint(length)); err != nil {
		return err
	}
	if _, err := w.Write(cidBytes); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func lengthVarint(size uint64) []byte {
	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, size)
	return buf[:n]
}

var (
	noRootsHeaderOnce sync.Once
	noRootsHdr        []byte
)

func noRootsHeader() []byte {
	noRootsHeaderOnce.Do(func() {
		noRootsHeaderWithoutLength, err := cbor.Encode(ipldcar.CarHeader{
			Roots:   nil,
			Version: 1,
		})
		if err != nil {
			panic(fmt.Sprintf("failed to encode CAR header: %v", err))
		}
		var buf bytes.Buffer
		if err := util.LdWrite(&buf, noRootsHeaderWithoutLength); err != nil {
			panic(fmt.Sprintf("failed to length-delimit CAR header: %v", err))
		}
		noRootsHdr = buf.Bytes()
	})
	return noRootsHdr
}

// FindOrCreateUploads creates uploads for a given space and its associated sources.
func (a API) FindOrCreateUploads(ctx context.Context, spaceDID did.DID) ([]*model.Upload, error) {
	log.Debugf("Creating uploads for space %s", spaceDID)
	sources, err := a.Repo.ListSpaceSources(ctx, spaceDID)
	if err != nil {
		return nil, err
	}

	log.Debugf("Found %d sources for space %s", len(sources), spaceDID)

	uploads, err := a.Repo.FindOrCreateUploads(ctx, spaceDID, sources)
	if err != nil {
		return nil, err
	}
	log.Debugf("Created %d uploads for space %s", len(uploads), spaceDID)
	return uploads, nil
}

// GetUploadByID retrieves an upload by its unique ID.
func (a API) GetUploadByID(ctx context.Context, uploadID id.UploadID) (*model.Upload, error) {
	return a.Repo.GetUploadByID(ctx, uploadID)
}

// signal signals on a channel that work is available. The channel
// should be buffered (generally with a size of 1). If the channel is full, it
// will not block, as no further signal is needed: two messages saying that work
// is available are the same as one.
func signal(work chan<- struct{}) {
	select {
	case work <- struct{}{}:
	default:
		// channel is full, no need to signal
	}
}

// ExecuteUpload executes the upload process for a given upload, handling its state transitions and processing steps.
func (a API) ExecuteUpload(ctx context.Context, uploadID id.UploadID, spaceDID did.DID) (cid.Cid, error) {
	ctx, span := tracer.Start(ctx, "execute-upload", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer span.End()

	var (
		scansAvailable        = make(chan struct{}, 1)
		dagScansAvailable     = make(chan struct{}, 1)
		closedShardsAvailable = make(chan struct{}, 1)
	)

	// Start the workers
	eg, wCtx := bettererrgroup.WithContext(ctx)
	eg.Go(func() error { return runScanWorker(wCtx, a, uploadID, spaceDID, scansAvailable, dagScansAvailable) })
	eg.Go(func() error {
		return runDAGScanWorker(wCtx, a, uploadID, spaceDID, dagScansAvailable, closedShardsAvailable)
	})
	eg.Go(func() error { return runStorachaWorker(wCtx, a, uploadID, spaceDID, closedShardsAvailable) })

	// Kick them all off. There may be work available in the DB from a previous
	// attempt.
	signal(scansAvailable)
	signal(dagScansAvailable)
	signal(closedShardsAvailable)
	close(scansAvailable)

	log.Debugf("Waiting for workers to finish for upload %s", uploadID)
	workersErr := eg.Wait()

	// Reload the upload to get the latest state from the DB.
	upload, err := a.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return cid.Undef, fmt.Errorf("reloading upload: %w", err)
	}

	return upload.RootCID(), workersErr
}

func runScanWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, scansAvailable <-chan struct{}, dagScansAvailable chan<- struct{}) error {
	ctx, span := tracer.Start(ctx, "scan-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("Scan worker for upload %s exiting", uploadID)
	defer span.End()

	return Worker(
		ctx,
		scansAvailable,

		// doWork
		func() error {
			err := api.ExecuteScan(ctx, uploadID, func(entry scanmodel.FSEntry) error {
				_, isDirectory := entry.(*scanmodel.Directory)
				_, err := api.Repo.CreateDAGScan(ctx, entry.ID(), isDirectory, uploadID, spaceDID)
				if err != nil {
					return fmt.Errorf("creating DAG scan: %w", err)
				}
				signal(dagScansAvailable)
				return nil
			})

			if err != nil {
				return fmt.Errorf("running scans: %w", err)
			}

			return nil
		},

		// finalize
		func() error {
			close(dagScansAvailable)
			return nil
		},
	)
}

// runDAGScanWorker runs the worker that scans files and directories into blocks,
// and buckets them into shards.
func runDAGScanWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, dagScansAvailable <-chan struct{}, closedShardsAvailable chan<- struct{}) error {
	ctx, span := tracer.Start(ctx, "dag-scan-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("DAG scan worker for upload %s exiting", uploadID)
	defer span.End()

	builderSem := make(chan struct{}, 4)
	builders := make(map[id.ShardID]*shardBuilder)
	var mu sync.Mutex

	ensureBuilder := func(shardID id.ShardID) *shardBuilder {
		mu.Lock()
		defer mu.Unlock()
		if sb, ok := builders[shardID]; ok {
			return sb
		}
		builderSem <- struct{}{}
		sb := newShardBuilder(ctx, shardID, builderSem)
		builders[shardID] = sb
		return sb
	}

	closeShard := func(shardID id.ShardID) error {
		mu.Lock()
		sb, ok := builders[shardID]
		if !ok {
			mu.Unlock()
			return nil
		}
		delete(builders, shardID)
		mu.Unlock()

		sb.close()
		res := sb.wait()
		if res.err != nil {
			return fmt.Errorf("finalizing shard %s: %w", shardID, res.err)
		}

		shard, err := api.Repo.GetShardByID(ctx, shardID)
		if err != nil {
			return fmt.Errorf("loading shard %s for digest update: %w", shardID, err)
		}
		shard.SetDigests(res.carDigest, res.sha256MH, res.piece)
		if err := api.Repo.UpdateShard(ctx, shard); err != nil {
			return fmt.Errorf("persisting digests for shard %s: %w", shardID, err)
		}

		signal(closedShardsAvailable)
		return nil
	}

	closeAllBuilders := func() error {
		mu.Lock()
		ids := make([]id.ShardID, 0, len(builders))
		for id := range builders {
			ids = append(ids, id)
		}
		mu.Unlock()

		for _, sid := range ids {
			if err := closeShard(sid); err != nil {
				return err
			}
		}
		return nil
	}

	return Worker(
		ctx,
		dagScansAvailable,

		// doWork
		func() error {
			defer closeAllBuilders()
			err := api.ExecuteDagScansForUpload(ctx, uploadID, func(node dagmodel.Node, data []byte) error {
				log.Debugf("Adding node %s to upload shards for upload %s", node.CID(), uploadID)
				shardID, closedShardIDs, err := api.AddNodeToUploadShards(ctx, uploadID, spaceDID, node.CID())
				if err != nil {
					return fmt.Errorf("adding node to upload shard: %w", err)
				}

				sb := ensureBuilder(shardID)
				if err := sb.addBlock(ctx, shardBlock{node: node, data: data}); err != nil {
					return fmt.Errorf("hashing shard %s: %w", shardID, err)
				}

				for _, cs := range closedShardIDs {
					if err := closeShard(cs); err != nil {
						return err
					}
				}

				return nil
			})

			var badFSEntryErr types.ErrBadFSEntry
			if errors.As(err, &badFSEntryErr) {
				upload, err := api.Repo.GetUploadByID(ctx, uploadID)
				if err != nil {
					return fmt.Errorf("getting upload %s after finding bad FS entry: %w", uploadID, err)
				}

				log.Debug("Removing bad FS entry from upload ", uploadID, ": ", badFSEntryErr.FsEntryID())
				err = api.RemoveBadFSEntry(ctx, upload.SpaceDID(), badFSEntryErr.FsEntryID())
				if err != nil {
					return fmt.Errorf("removing bad FS entry for upload %s: %w", uploadID, err)
				}

				err = upload.Invalidate()
				if err != nil {
					return fmt.Errorf("invalidating upload %s after removing bad FS entry: %w", uploadID, err)
				}

				err = api.Repo.UpdateUpload(ctx, upload)
				if err != nil {
					return fmt.Errorf("updating upload %s after removing bad FS entry: %w", uploadID, err)
				}

				// Bubble the error to fail this attempt entirely. We're now ready for
				// a retry.
				return badFSEntryErr
			}

			if err != nil {
				return fmt.Errorf("running dag scans for upload %s: %w", uploadID, err)
			}

			return nil
		},

		// finalize
		func() error {
			// We're out of nodes, so we can close any open shards for this upload.
			shardsClosed, err := api.CloseUploadShards(ctx, uploadID)
			if err != nil {
				return fmt.Errorf("closing upload shards for upload %s: %w", uploadID, err)
			}

			for _, cs := range shardsClosed {
				if err := closeShard(cs); err != nil {
					return err
				}
			}
			if err := closeAllBuilders(); err != nil {
				return err
			}

			// Reload the upload to get the latest state from the DB.
			upload, err := api.Repo.GetUploadByID(ctx, uploadID)
			if err != nil {
				return fmt.Errorf("reloading upload: %w", err)
			}
			rootCID, err := api.Repo.CIDForFSEntry(ctx, upload.RootFSEntryID())
			if err != nil {
				return fmt.Errorf("retrieving CID for root fs entry: %w", err)
			}
			if err := upload.SetRootCID(rootCID); err != nil {
				return fmt.Errorf("completing DAG generation: %w", err)
			}
			if err := api.Repo.UpdateUpload(ctx, upload); err != nil {
				return fmt.Errorf("updating upload: %w", err)
			}

			close(closedShardsAvailable)

			return nil
		},
	)
}

// runStorachaWorker runs the worker that adds shards and indexes to Storacha.
func runStorachaWorker(ctx context.Context, api API, uploadID id.UploadID, spaceDID did.DID, blobWork <-chan struct{}) error {
	ctx, span := tracer.Start(ctx, "storacha-worker", trace.WithAttributes(
		attribute.String("upload.id", uploadID.String()),
		attribute.String("space.did", spaceDID.String()),
	))
	defer log.Debugf("Storacha worker for upload %s exiting", uploadID)
	defer span.End()

	return Worker(
		ctx,
		blobWork,

		// doWork
		func() error {
			err := api.AddShardsForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				log.Debug("Error adding shards for upload ", uploadID, ": ", err)
				var badNodesErr types.ErrBadNodes
				if errors.As(err, &badNodesErr) {
					upload, err := api.Repo.GetUploadByID(ctx, uploadID)
					if err != nil {
						return fmt.Errorf("getting upload %s after finding bad nodes: %w", uploadID, err)
					}

					var cids []cid.Cid
					for _, e := range badNodesErr.Errs() {
						cids = append(cids, e.CID())
					}
					log.Debug("Removing bad nodes from upload ", uploadID, ": ", cids)
					err = api.RemoveBadNodes(ctx, upload.SpaceDID(), cids)
					if err != nil {
						return fmt.Errorf("removing bad nodes for upload %s: %w", uploadID, err)
					}

					err = upload.Invalidate()
					if err != nil {
						return fmt.Errorf("invalidating upload %s after removing bad nodes: %w", uploadID, err)
					}

					err = api.Repo.UpdateUpload(ctx, upload)
					if err != nil {
						return fmt.Errorf("updating upload %s after removing bad nodes: %w", uploadID, err)
					}

					// Bubble the error to fail this attempt entirely. We're now ready for
					// a retry.
					return badNodesErr
				}
				return fmt.Errorf("`space/blob/add`ing shards for upload %s: %w", uploadID, err)
			}

			return nil
		},

		// finalize
		func() error {
			err := api.AddIndexesForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				return fmt.Errorf("`space/blob/add`ing index for upload %s: %w", uploadID, err)
			}

			err = api.AddStorachaUploadForUpload(ctx, uploadID, spaceDID)
			if err != nil {
				return fmt.Errorf("`upload/add`ing upload %s: %w", uploadID, err)
			}

			return nil
		},
	)
}
