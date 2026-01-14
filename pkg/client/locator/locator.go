package locator

import (
	"context"

	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

type Locator interface {
	Locate(ctx context.Context, spaceDID did.DID, digest mh.Multihash) ([]Location, error)
	LocateMany(ctx context.Context, spaceDID did.DID, digests []mh.Multihash) (blobindex.MultihashMap[[]Location], error)
}

type Location struct {
	Commitment ucan.Capability[assert.LocationCaveats]
	Position   blobindex.Position
}
