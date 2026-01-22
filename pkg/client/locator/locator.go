package locator

import (
	"context"
	"fmt"

	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

type Locator interface {
	// Locate finds and returns the locations of a single block identified by its
	// digest. Returns a [NotFoundError] if no locations are found.
	Locate(ctx context.Context, spaces []did.DID, digest mh.Multihash) ([]Location, error)

	// LocateMany finds the locations of multiple blocks identified by their
	// digests, returning a map from digest to locations. Digests with no found
	// locations will be absent from the returned map.
	LocateMany(ctx context.Context, spaces []did.DID, digests []mh.Multihash) (blobindex.MultihashMap[[]Location], error)
}

type Location struct {
	Commitment ucan.Capability[assert.LocationCaveats]
	Position   blobindex.Position
}

type NotFoundError struct {
	Hash mh.Multihash
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("no locations found for block: %s", digestutil.Format(e.Hash))
}
