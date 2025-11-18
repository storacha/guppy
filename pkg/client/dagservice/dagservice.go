package dagservice

import (
	"github.com/ipfs/boxo/ipld/merkledag"
	ipldfmt "github.com/ipfs/go-ipld-format"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client/locator"
)

func NewDAGService(locator locator.Locator, retriever Retriever, space did.DID) ipldfmt.DAGService {
	return merkledag.NewReadOnlyDagService(NewNodeGetter(locator, retriever, space))
}
