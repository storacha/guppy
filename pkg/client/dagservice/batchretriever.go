package dagservice

import (
	"context"
	"io"
	"net/url"
	"slices"
	"sync"

	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/guppy/pkg/client/locator"
)

// NewBatchRetriever creates a new batch retriever that can batch multiple
// retrieval requests together when they're contiguous within the same blob. The
// `retriever` will be used to perform retrievals for larger ranges, which will
// then be sliced for the individual requests. `ctx` will be used for the
// retrieval operations.
func NewBatchRetriever(ctx context.Context, retriever Retriever) *batchRetriever {
	return &batchRetriever{
		ctx:            ctx,
		retriever:      retriever,
		blobs:          make(map[url.URL]blob),
		blobsAvailable: make(chan struct{}, 1),
	}
}

type batchRetriever struct {
	ctx       context.Context
	retriever Retriever

	blobs          map[url.URL]blob
	blobsAvailable chan struct{}
	blobsMutex     sync.Mutex

	retrieverMutex sync.Mutex
	workerOnce     sync.Once
}

var _ Retriever = (*batchRetriever)(nil)

type slice struct {
	// The position to read from the blob
	position blobindex.Position

	// Channel to receive the retrieved data or error, then close.
	result chan retrieveResult
}

type blob struct {
	location locator.Location
	slices   []slice
}

type retrieveResult struct {
	data []byte
	err  error
}

// Retrieve retrieves a single slice of data. If multiple retrievals are
// requested that can be batched together, they will be. `ctx` is used while
// waiting for the batch retrieval to complete. The retrieval itself uses the
// `ctx` provided at construction.
func (br *batchRetriever) Retrieve(ctx context.Context, locations []locator.Location) (io.ReadCloser, error) {
	br.ensureWorker()

	result := <-br.addRequest(locations)
	return result.data, result.err
}

func (br *batchRetriever) addRequest(locations []locator.Location) chan retrieveResult {
	br.blobsMutex.Lock()
	defer br.blobsMutex.Unlock()

	// TK: Implement real batching logic.

	// TK: Select location correctly
	location := locations[0]
	// TK: Confirm that we can stick with the first URL
	url := location.Commitment.Nb().Location[0]

	bs, ok := br.blobs[url]
	if !ok {
		bs = blob{
			location: location,
			slices:   []slice{},
		}
	}
	out := make(chan retrieveResult, 1)
	bs.slices = append(bs.slices, slice{
		position: location.Position,
		result:   out,
	})
	br.blobs[url] = bs

	select {
	case br.blobsAvailable <- struct{}{}:
	default:
	}

	return out
}

func (br *batchRetriever) ensureWorker() {
	br.workerOnce.Do(func() {
		go br.worker()
	})
}

func (br *batchRetriever) worker() {
	for blob := range br.popBlobs() {
		for pos, sls := range coalesce(blob.slices) {
			loc := blob.location
			loc.Position = pos
			data, err := br.retriever.Retrieve(br.ctx, []locator.Location{loc})
			for _, sl := range sls {
				sl.result <- retrieveResult{data: data, err: err}
				close(sl.result)
			}
		}
	}
}

func (br *batchRetriever) popBlobs() chan blob {
	out := make(chan blob)

	go func() {
		for {
			br.blobsMutex.Lock()

			if len(br.blobs) == 0 {
				br.blobsMutex.Unlock()
				select {
				case <-br.ctx.Done():
					close(out)
					return
				case <-br.blobsAvailable:
				}
				continue
			}

			// Find one and remove it from the map
			var url url.URL
			var rs blob
			for url, rs = range br.blobs {
				break
			}
			delete(br.blobs, url)

			br.blobsMutex.Unlock()
			select {
			case <-br.ctx.Done():
				close(out)
				return
			case out <- rs:
			}
		}
	}()

	return out
}

// coalesce combines contiguous slices into larger slices to minimize the
// number of retrievals needed.
func coalesce(slcs []slice) func(yield func(blobindex.Position, []slice) bool) {
	return func(yield func(blobindex.Position, []slice) bool) {
		if len(slcs) == 0 {
			return
		}

		// positions := make([]blobindex.Position, len(slcs))
		// for i, sl := range slcs {
		// 	positions[i] = sl.position
		// }

		// Sort by offset
		slices.SortFunc(slcs, func(a, b slice) int {
			return int(a.position.Offset - b.position.Offset)
		})

		var currentChunk blobindex.Position
		var currentSlices []slice

		for _, eachSlice := range slcs {
			// last := &result[len(result)-1]

			// If `eachSlice` begins before or at the end of `last`,
			if eachSlice.position.Offset <= currentChunk.Offset+currentChunk.Length {
				// and extends beyond the end of `currentChunk`, extend `currentChunk`
				if eachSlice.position.Offset+eachSlice.position.Length > currentChunk.Offset+currentChunk.Length {
					currentChunk.Length = (eachSlice.position.Offset + eachSlice.position.Length) - currentChunk.Offset
				}
				currentSlices = append(currentSlices, eachSlice)
			} else {
				// Gap found, yield current chunk
				if !yield(currentChunk, currentSlices) {
					return
				}
				// Start new chunk containing only `eachSlice`
				currentChunk = eachSlice.position
				currentSlices = []slice{eachSlice}
			}
		}
	}
}
