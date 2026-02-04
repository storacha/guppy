package verification

import (
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"

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
)

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

// VerifyDAGRetrieval verifies the retrieval of a DAG starting from the given
// root CID. It uses the provided indexer to find shards and locations, and
// retrieves blocks while verifying their integrity. The function returns a
// sequence of VerifiedBlock results. Note: the getProofs function can be nil,
// in which case no authorization will be sent for retrievals.
func VerifyDAGRetrieval(
	ctx context.Context,
	id principal.Signer,
	getProofs ContentRetrieveProofGetterFunc,
	indexer *Indexer,
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

// StatBlocks retrieves block statistics for the given CIDs using the provided
// indexer. It also performs integrity checks on the retrieved blocks. Note:
// the getProofs function can be nil, in which case no authorization will be
// sent for retrievals.
func StatBlocks(ctx context.Context, id principal.Signer, getProofs ContentRetrieveProofGetterFunc, indexer *Indexer, links []cid.Cid) iter.Seq2[BlockStat, error] {
	return func(yield func(BlockStat, error) bool) {
		for _, link := range links {
			slice := link.Hash()
			shard, pos, err := indexer.FindShard(ctx, slice)
			if err != nil {
				yield(BlockStat{}, fmt.Errorf("finding shard for %q: %w", link, err))
				return
			}

			locations, err := indexer.FindLocations(ctx, shard)
			if err != nil {
				yield(BlockStat{}, fmt.Errorf("finding location for shard %q: %w", shard, err))
				return
			}

			var fetchErr error
			for _, loc := range locations {
				var body []byte
				var err error
				if getProofs != nil {
					body, err = authorizedFetch(ctx, id, getProofs, loc, pos)
				} else {
					body, err = fetch(ctx, loc, pos)
				}
				if err != nil {
					fetchErr = err
					continue
				}

				err = verifyIntegrity(slice, body)
				if err != nil {
					fetchErr = err
					continue
				}

				codec := link.Prefix().Codec
				links, err := extractLinks(codec, body)
				if err != nil {
					fetchErr = fmt.Errorf("extracting links for block %q: %w", link, err)
					continue
				}

				if !yield(BlockStat{
					Codec:  codec,
					Size:   pos.Length,
					Digest: slice,
					Links:  links,
					Origin: Origin{
						Node:     loc.Commitment.Issuer().DID(),
						URL:      loc.Caveats.Location[0],
						Shard:    shard,
						Position: pos,
					},
				}, nil) {
					return
				}
				fetchErr = nil
				break
			}
			if fetchErr != nil {
				if !yield(BlockStat{}, fetchErr) {
					return
				}
			}
		}
	}
}

func authorizedFetch(ctx context.Context, id principal.Signer, getProofs ContentRetrieveProofGetterFunc, shardLocation Location, slicePosition blobindex.Position) ([]byte, error) {
	if shardLocation.Caveats.Space == did.Undef {
		return nil, fmt.Errorf("missing space DID in location commitment for shard: %s", digestutil.Format(shardLocation.Caveats.Content.Hash()))
	}

	var reqErr error
	for _, url := range shardLocation.Caveats.Location {
		conn, err := retrieval.NewConnection(shardLocation.Commitment.Issuer(), &url)
		if err != nil {
			return nil, fmt.Errorf("creating retrieval connection to %q: %w", url.String(), err)
		}

		body, err := backoff.Retry(ctx, func() ([]byte, error) {
			proofs, err := getProofs(shardLocation.Caveats.Space)
			if err != nil {
				return nil, fmt.Errorf("getting proofs for retrieval to %q: %w", url.String(), err)
			}

			inv, err := contentcap.Retrieve.Invoke(
				id,
				shardLocation.Commitment.Issuer(),
				shardLocation.Caveats.Space.String(),
				contentcap.RetrieveCaveats{
					Blob: contentcap.BlobDigest{
						Digest: shardLocation.Caveats.Content.Hash(),
					},
					Range: contentcap.Range{
						Start: slicePosition.Offset,
						End:   slicePosition.Offset + slicePosition.Length - 1,
					},
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
			defer hres.Body().Close()

			body, err := io.ReadAll(hres.Body())
			if err != nil {
				return nil, fmt.Errorf("reading response body for request to %q: %w", url.String(), err)
			}
			return body, nil
		}, backoff.WithMaxTries(3))
		if err != nil {
			reqErr = err
			continue
		}

		return body, nil
	}
	return nil, reqErr
}

func fetch(ctx context.Context, shardLocation Location, slicePosition blobindex.Position) ([]byte, error) {
	var reqErr error
	for _, url := range shardLocation.Caveats.Location {
		req, err := http.NewRequestWithContext(ctx, "GET", url.String(), nil)
		if err != nil {
			reqErr = fmt.Errorf("creating request for block %q: %w", url.String(), err)
			continue
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", slicePosition.Offset, slicePosition.Offset+slicePosition.Length-1))

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			reqErr = fmt.Errorf("performing request for block %q: %w", url.String(), err)
			continue
		}
		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err != nil {
			reqErr = fmt.Errorf("reading response body for block %q: %w", url.String(), err)
			continue
		}
		return body, nil
	}
	return nil, reqErr
}
