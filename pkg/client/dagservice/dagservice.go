package dagservice

import (
	"context"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipldfmt "github.com/ipfs/go-ipld-format"
	rclient "github.com/storacha/go-ucanto/client/retrieval"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/locator"
)

type Retriever interface {
	Retrieve(ctx context.Context, space did.DID, location locator.Location, retrievalOpts ...rclient.Option) ([]byte, error)
}

var _ Retriever = (*client.Client)(nil)

func NewDAGService(locator locator.Locator, retriever Retriever, spaces []did.DID) ipldfmt.DAGService {
	return merkledag.NewReadOnlyDagService(
		merkledag.NewDAGService(blockservice.New(
			blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())),
			NewExchange(locator, retriever, spaces),
		)),
	)
}
