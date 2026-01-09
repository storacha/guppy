package dagservice

import (
	"context"
	"io"
	"net/url"
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
		blobs:          make(map[url.URL]shard),
		blobsAvailable: make(chan struct{}, 1),
	}
}

type batchRetriever struct {
	ctx       context.Context
	retriever Retriever

	requests chan request

	blobs          map[url.URL]shard
	blobsAvailable chan struct{}
	blobsMutex     sync.Mutex

	retrieverMutex sync.Mutex
	workerOnce     sync.Once
}

var _ Retriever = (*batchRetriever)(nil)

type request struct {
	locations []locator.Location
	result    chan retrieveResult
}

type coalescedRequest struct {
	location locator.Location
	slices   []slice
}

type slice struct {
	// The position to read from the blob
	position blobindex.Position

	// Channel to receive the retrieved data or error, then close.
	result chan retrieveResult
}

type shard struct {
	location locator.Location
	slices   []slice
}

type retrieveResult struct {
	data io.ReadCloser
	err  error
}

// Retrieve retrieves a single slice of data. If multiple retrievals are
// requested in sequence that can be batched together, they will be. `ctx` is
// used while waiting for the batch retrieval to complete. The retrieval itself
// uses the `ctx` provided at construction.
func (br *batchRetriever) Retrieve(ctx context.Context, locations []locator.Location) (io.ReadCloser, error) {
	br.ensureWorker()

	resultChan := make(chan retrieveResult, 1)
	req := request{
		locations: locations,
		result:    resultChan,
	}

	select {
	case <-br.ctx.Done():
		return nil, ctx.Err()
	case br.requests <- req:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultChan:
		return result.data, result.err
	}
}

func (br *batchRetriever) ensureWorker() {
	br.workerOnce.Do(func() {
		coalescedRequests := make(chan coalescedRequest)
		go coalesceWorker(br.ctx, br.requests, coalescedRequests)
		go retrieveWorker(br.ctx, br.retriever, coalescedRequests)
	})
}

// When a request appears:
// - If the retrieve worker can take it, let it take it immediately
// - If it's busy, and there's no waiting request, make it the waiting request
// - If it's busy, and there is a waiting request, and it can be coalesced,
// coalesce it into the waiting request
// - If it's busy, and there is a waiting request, but it can't be coalesced

// TK: Actually coalesce requests
func coalesceWorker(ctx context.Context, requests <-chan request, coalescedRequests chan<- coalescedRequest) {
	// The waiting request can be coalesced when possible, or taken by the
	// retriever as soon as it can.
	var waitingRequest coalescedRequest

	// The next request is one we've just received and hope to coalesce with the
	// waiting request.
	var nextRequest request

	// Becomes nil when nextRequest is full.
	reqs := requests

	for {
		select {
		case <-ctx.Done():
			return

		// First, try to send the waiting request to the retriever.
		case coalescedRequests <- waitingRequest:
			waitingRequest = coalescedRequest{}

		// If it can't be sent right now, try to receive a new request.
		case nextRequest = <-reqs:
			// Avoid reading new requests until we've processed this one.
			reqs = nil

			if coalesce(nextRequest, &waitingRequest) {
				reqs = requests
				nextRequest = request{}
			}
		}
	}
}

// Attempts to coalesce `new` into `existing`. Returns true if coalescing was
// successful.
func coalesce(new request, existing *coalescedRequest) bool {
	// TK: Do real logic.
	if existing.location == (locator.Location{}) {
		slices := make([]slice, len(new.locations))
		for i, loc := range new.locations {
			slices[i] = slice{
				position: loc.Position,
				result:   new.result,
			}
		}
		existing.location = new.locations[0]
		existing.slices = slices
		return true
	}

	return false
}

func retrieveWorker(ctx context.Context, retriever Retriever, coalescedRequests <-chan coalescedRequest) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-coalescedRequests:
			data, err := retriever.Retrieve(ctx, []locator.Location{req.location})
			for _, sl := range req.slices {
				if err != nil {
					sl.result <- retrieveResult{data: nil, err: err}
				} else {
					sl.result <- retrieveResult{data: data, err: nil}
				}
				close(sl.result)
			}
		}
	}
}

// // coalesce combines contiguous slices into larger slices to minimize the
// // number of retrievals needed.
// func coalesce(slcs []slice) func(yield func(blobindex.Position, []slice) bool) {
// 	return func(yield func(blobindex.Position, []slice) bool) {
// 		if len(slcs) == 0 {
// 			return
// 		}

// 		// positions := make([]blobindex.Position, len(slcs))
// 		// for i, sl := range slcs {
// 		// 	positions[i] = sl.position
// 		// }

// 		// Sort by offset
// 		slices.SortFunc(slcs, func(a, b slice) int {
// 			return int(a.position.Offset - b.position.Offset)
// 		})

// 		var currentChunk blobindex.Position
// 		var currentSlices []slice

// 		for _, eachSlice := range slcs {
// 			// last := &result[len(result)-1]

// 			// If `eachSlice` begins before or at the end of `last`,
// 			if eachSlice.position.Offset <= currentChunk.Offset+currentChunk.Length {
// 				// and extends beyond the end of `currentChunk`, extend `currentChunk`
// 				if eachSlice.position.Offset+eachSlice.position.Length > currentChunk.Offset+currentChunk.Length {
// 					currentChunk.Length = (eachSlice.position.Offset + eachSlice.position.Length) - currentChunk.Offset
// 				}
// 				currentSlices = append(currentSlices, eachSlice)
// 			} else {
// 				// Gap found, yield current chunk
// 				if !yield(currentChunk, currentSlices) {
// 					return
// 				}
// 				// Start new chunk containing only `eachSlice`
// 				currentChunk = eachSlice.position
// 				currentSlices = []slice{eachSlice}
// 			}
// 		}
// 	}
// }
