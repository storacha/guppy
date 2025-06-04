package uploadlist

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

const Ability = "upload/list"

type Caveat struct {
	Cursor string
	Size   int64
	Pre    bool
}

var _ ucan.CaveatBuilder = (*Caveat)(nil)

func (c Caveat) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&c, nil)
}

func NewCapability(space did.DID, nb Caveat) ucan.Capability[Caveat] {
	return ucan.NewCapability(Ability, space.String(), nb)
}
