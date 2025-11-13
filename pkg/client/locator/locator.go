package locator

import (
	"context"

	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-ucanto/ucan"
)

type Locator interface {
	Locate(ctx context.Context, hash mh.Multihash) (Location, error)
}

type Location struct {
	Shard       mh.Multihash
	Commitments []ucan.Capability[assert.LocationCaveats]
	Position    blobindex.Position
}
