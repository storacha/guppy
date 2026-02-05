package verification

import (
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"slices"

	"github.com/cenkalti/backoff/v5"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/bytemap"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/client/retrieval"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"go.uber.org/zap/zapcore"
)

// maxSpaceBetween is the default maximum space (in bytes) between blocks that
// can still be coalesced into a single request. This value accommodates the
// typical overhead between adjacent blocks in a CAR file, which consists of a
// varint length prefix (1-10 bytes) and a CID (34-37 bytes for CIDv0/CIDv1 with
// SHA2-256).
const maxSpaceBetween = 64

var log = logging.Logger("pkg/verification")

type VerifiedBlock struct {
	Stat BlockStat
}

// ContentRetrieveProofGetterFunc is a function that obtains proofs for content
// retrieval in a given space.
type ContentRetrieveProofGetterFunc func(space did.DID) ([]delegation.Proof, error)

// AuthorizeIndexerRetrievalFunc is a function that authorizes retrievals from
// the indexer by returning a delegation that can be used for retrieval.
type AuthorizeIndexerRetrievalFunc func() (delegation.Delegation, error)

// ShardFinder finds shards and their locations for content retrieval.
type ShardFinder interface {
	FindShard(ctx context.Context, slice multihash.Multihash) (multihash.Multihash, blobindex.Position, error)
	FindLocations(ctx context.Context, shard multihash.Multihash) ([]Location, error)
}

// VerifyDAGRetrieval verifies the retrieval of a DAG starting from the given
// root CID. It uses the provided indexer to find shards and locations, and
// retrieves blocks while verifying their integrity. The function returns a
// sequence of VerifiedBlock results. Note: the getProofs function can be nil,
// in which case no authorization will be sent for retrievals.
func VerifyDAGRetrieval(
	ctx context.Context,
	id principal.Signer,
	getProofs ContentRetrieveProofGetterFunc,
	indexer ShardFinder,
	root cid.Cid,
) iter.Seq2[VerifiedBlock, error] {
	// list of already verified blocks to avoid redundant work
	verified := bytemap.NewByteMap[multihash.Multihash, struct{}](0)

	return func(yield func(VerifiedBlock, error) bool) {
		queue := [][]cid.Cid{{root}}
		for len(queue) > 0 {
			chunk := queue[0]
			queue = queue[1:]

			// filter blocks we may have verified since adding the chunk to the queue
			unverified := []cid.Cid{}
			for _, link := range chunk {
				if !verified.Has(link.Hash()) {
					unverified = append(unverified, link)
				}
			}
			if len(unverified) == 0 {
				continue
			}

			for stat, err := range StatBlocks(ctx, id, getProofs, indexer, unverified) {
				if err != nil {
					yield(VerifiedBlock{}, fmt.Errorf("retrieving block stats: %w", err))
					return
				}
				if len(stat.Links) > 0 {
					queue = append(queue, stat.Links)
				}
				if !yield(VerifiedBlock{Stat: stat}, nil) {
					return
				}
				// mark as verified
				verified.Set(stat.Digest, struct{}{})
			}
		}
	}
}

type BlockStat struct {
	Codec  uint64              // IPLD codec that was used to decode the block.
	Size   uint64              // Size of the block in bytes.
	Digest multihash.Multihash // Multihash digest of the block.
	Links  []cid.Cid           // CIDs linked from this block, deduped.
	Origin Origin              // Where the block was retrieved from.
}

type Origin struct {
	Node     did.DID             // The node that provided the data.
	URL      url.URL             // URL of the shard the data came from.
	Shard    multihash.Multihash // Hash of the shard.
	Position blobindex.Position  // Byte range within the shard.
}

type Slice struct {
	Digest   multihash.Multihash
	Bytes    []byte
	Position blobindex.Position
	Location Location
}

// StatBlocks retrieves block statistics for the given CIDs using the provided
// indexer. It also performs integrity checks on the retrieved blocks. Note:
// the getProofs function can be nil, in which case no authorization will be
// sent for retrievals.
func StatBlocks(ctx context.Context, id principal.Signer, getProofs ContentRetrieveProofGetterFunc, indexer ShardFinder, links []cid.Cid) iter.Seq2[BlockStat, error] {
	return func(yield func(BlockStat, error) bool) {
		shardLocations := bytemap.NewByteMap[multihash.Multihash, []Location](0)
		shardSlices := bytemap.NewByteMap[multihash.Multihash, bytemap.ByteMap[multihash.Multihash, blobindex.Position]](0)
		blockCIDs := bytemap.NewByteMap[multihash.Multihash, cid.Cid](0)

		// find all shard locations for slices and group by shard
		for _, link := range links {
			slice := link.Hash()
			blockCIDs.Set(slice, link)

			shard, pos, err := indexer.FindShard(ctx, slice)
			if err != nil {
				yield(BlockStat{}, fmt.Errorf("finding shard for %q: %w", link, err))
				return
			}

			slices := shardSlices.Get(shard)
			if slices == nil {
				slices = bytemap.NewByteMap[multihash.Multihash, blobindex.Position](0)
				shardSlices.Set(shard, slices)
			}
			slices.Set(link.Hash(), pos)

			if !shardLocations.Has(shard) {
				locations, err := indexer.FindLocations(ctx, shard)
				if err != nil {
					yield(BlockStat{}, fmt.Errorf("finding location for shard %q: %w", digestutil.Format(shard), err))
					return
				}
				shardLocations.Set(shard, locations)
			}
		}

		// for each shard, fetch all slices in batches and yield stats for each block
		for shard, slices := range shardSlices.Iterator() {
			locations := shardLocations.Get(shard)
			for slice, err := range batchFetchSlices(ctx, id, getProofs, shard, locations, slices) {
				if err != nil {
					yield(BlockStat{}, fmt.Errorf("fetching slices for shard %q: %w", digestutil.Format(shard), err))
					return
				}

				err = verifyIntegrity(slice.Digest, slice.Bytes)
				if err != nil {
					yield(BlockStat{}, err)
					return
				}

				cid := blockCIDs.Get(slice.Digest)
				codec := cid.Prefix().Codec
				links, err := extractLinks(codec, slice.Bytes)
				if err != nil {
					yield(BlockStat{}, fmt.Errorf("extracting links for block %q: %w", cid, err))
					return
				}

				if !yield(BlockStat{
					Codec:  codec,
					Size:   uint64(len(slice.Bytes)),
					Digest: slice.Digest,
					Links:  links,
					Origin: Origin{
						Node:     slice.Location.Commitment.Issuer().DID(),
						URL:      slice.Location.Caveats.Location[0],
						Shard:    shard,
						Position: slice.Position,
					},
				}, nil) {
					return
				}
			}
		}
	}
}

func batchFetchSlices(
	ctx context.Context,
	id principal.Signer,
	getProofs ContentRetrieveProofGetterFunc,
	shard multihash.Multihash,
	locations []Location,
	slices bytemap.ByteMap[multihash.Multihash, blobindex.Position],
) iter.Seq2[Slice, error] {
	return func(yield func(Slice, error) bool) {
		batches := batchSlices(slices)
		if log.Level().Enabled(zapcore.Level(logging.LevelDebug)) {
			batchSizes := make([]int, len(batches))
			for i, batch := range batches {
				batchSizes[i] = len(batch)
			}
			log.Debugw("fetching slices in batches", "shard", digestutil.Format(shard), "batches", batchSizes)
		}
		for _, batch := range batches {
			start := slices.Get(batch[0]).Offset
			last := slices.Get(batch[len(batch)-1])
			end := last.Offset + last.Length - 1
			byteRange := contentcap.Range{Start: start, End: end}

			var fetchErr error
			for _, loc := range locations {
				var body io.ReadCloser
				var err error
				if getProofs != nil {
					body, err = authorizedFetch(ctx, id, getProofs, shard, loc, byteRange)
				} else {
					body, err = fetch(ctx, loc, byteRange)
				}
				if err != nil {
					fetchErr = err
					continue
				}

				scratch := make([]byte, maxSpaceBetween)
				for i, digest := range batch {
					pos := slices.Get(digest)
					bytes := make([]byte, pos.Length)
					_, err := io.ReadAtLeast(body, bytes, int(pos.Length))
					if err != nil {
						fetchErr = fmt.Errorf("reading bytes for slice %d in batch: %w", i, err)
						break
					}
					if !yield(Slice{
						Digest:   digest,
						Bytes:    bytes,
						Position: pos,
						Location: loc,
					}, nil) {
						body.Close()
						return
					}
					if i < len(batch)-1 {
						// read space between if there is any
						nextPos := slices.Get(batch[i+1])
						spaceBetween := int(nextPos.Offset - (pos.Offset + pos.Length))
						if spaceBetween > 0 {
							_, err := io.ReadAtLeast(body, scratch[:spaceBetween], spaceBetween)
							if err != nil {
								fetchErr = fmt.Errorf("reading space between slice %d in batch: %w", i, err)
								break
							}
						}
					}
				}
				body.Close()

				if fetchErr != nil {
					continue
				}
				fetchErr = nil
				break
			}
			if fetchErr != nil {
				if !yield(Slice{}, fetchErr) {
					return
				}
			}
		}
	}
}

func batchSlices(items bytemap.ByteMap[multihash.Multihash, blobindex.Position]) [][]multihash.Multihash {
	byteOrderedDigests := slices.Collect(items.Keys())
	slices.SortFunc(byteOrderedDigests, func(a, b multihash.Multihash) int {
		ap := items.Get(a)
		bp := items.Get(b)
		return int(ap.Offset) - int(bp.Offset)
	})
	batches := [][]multihash.Multihash{}
	batch := []multihash.Multihash{}
	var offset uint64
	for _, digest := range byteOrderedDigests {
		pos := items.Get(digest)
		if len(batch) == 0 {
			batch = append(batch, digest)
			offset = pos.Offset + pos.Length
		} else if offset+maxSpaceBetween >= pos.Offset {
			batch = append(batch, digest)
			offset = pos.Offset + pos.Length
		} else {
			batches = append(batches, batch)
			batch = []multihash.Multihash{digest}
			offset = pos.Offset + pos.Length
		}
	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	return batches
}

func authorizedFetch(ctx context.Context, id principal.Signer, getProofs ContentRetrieveProofGetterFunc, shard multihash.Multihash, shardLocation Location, byteRange contentcap.Range) (io.ReadCloser, error) {
	if shardLocation.Caveats.Space == did.Undef {
		return nil, fmt.Errorf("missing space DID in location commitment for shard: %s", digestutil.Format(shard))
	}

	var reqErr error
	for _, url := range shardLocation.Caveats.Location {
		conn, err := retrieval.NewConnection(shardLocation.Commitment.Issuer(), &url)
		if err != nil {
			return nil, fmt.Errorf("creating retrieval connection to %q: %w", url.String(), err)
		}

		body, err := backoff.Retry(ctx, func() (io.ReadCloser, error) {
			proofs, err := getProofs(shardLocation.Caveats.Space)
			if err != nil {
				return nil, fmt.Errorf("getting proofs for retrieval to %q: %w", url.String(), err)
			}

			inv, err := contentcap.Retrieve.Invoke(
				id,
				shardLocation.Commitment.Issuer(),
				shardLocation.Caveats.Space.String(),
				contentcap.RetrieveCaveats{
					Blob:  contentcap.BlobDigest{Digest: shard},
					Range: byteRange,
				},
				delegation.WithProof(proofs...),
			)
			if err != nil {
				return nil, fmt.Errorf("invoking retrieve capability for retrieval to %q: %w", url.String(), err)
			}

			_, hres, err := retrieval.Execute(ctx, inv, conn)
			if err != nil {
				return nil, fmt.Errorf("executing retrieve invocation for retrieval to %q: %w", url.String(), err)
			}
			if hres.Status() != http.StatusOK && hres.Status() != http.StatusPartialContent {
				hres.Body().Close()
				return nil, fmt.Errorf("unexpected status code %d for request to %q", hres.Status(), url.String())
			}
			return hres.Body(), nil
		}, backoff.WithMaxTries(3))
		if err != nil {
			reqErr = err
			continue
		}

		return body, nil
	}
	return nil, reqErr
}

func fetch(ctx context.Context, shardLocation Location, byteRange contentcap.Range) (io.ReadCloser, error) {
	var reqErr error
	for _, url := range shardLocation.Caveats.Location {
		req, err := http.NewRequestWithContext(ctx, "GET", url.String(), nil)
		if err != nil {
			reqErr = fmt.Errorf("creating request for block %q: %w", url.String(), err)
			continue
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", byteRange.Start, byteRange.End))

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			reqErr = fmt.Errorf("performing request for block %q: %w", url.String(), err)
			continue
		}
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusPartialContent {
			res.Body.Close()
			reqErr = fmt.Errorf("unexpected status code %d for request to %q", res.StatusCode, url.String())
			continue
		}
		return res.Body, nil
	}
	return nil, reqErr
}
