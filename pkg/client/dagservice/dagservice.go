package dagservice

import (
	"context"
	"io"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipldfmt "github.com/ipfs/go-ipld-format"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/locator"
)

// Retriever can fetch content from a given [locator.Location]. `Retrieve` does
// no data validation.
type Retriever interface {
	Retrieve(ctx context.Context, location locator.Location) (io.ReadCloser, error)
}

var _ Retriever = (*client.Client)(nil)

func NewDAGService(locator locator.Locator, retriever Retriever, space did.DID, opts ...ExchangeOption) ipldfmt.DAGService {
	return merkledag.NewReadOnlyDagService(
		merkledag.NewDAGService(blockservice.New(
			blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())),
			NewExchange(locator, retriever, space, opts...),
		)),
	)
}
