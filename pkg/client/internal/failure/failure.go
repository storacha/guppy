// Package failure defines a drop-in replacement for go-ucanto's failure
// module, with an extended FailureModel that can represent all the fields that
// may appear, so that decoding doesn't fail because of unexpected fields. This
// should ideally move to go-ucanto itself in some form, but it's a bit of a
// hack right now, and the entire issue will go away with the UCAN 1.0 version
// of go-ucanto.
package failure

import (
	// to use go:embed
	_ "embed"
	"fmt"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
	ucanipld "github.com/storacha/go-ucanto/core/ipld"
	ucantofailure "github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/did"
)

//go:embed failure.ipldsch
var failureSchema []byte

// FailureModel is a generic failure
type FailureModel struct {
	Name    *string
	Message string
	Stack   *string

	Audience            *ipld.Node //UCANPrincipal
	Capability          *ipld.Node //Capability
	Cause               *FailureModel
	Causes              *[]FailureModel
	Claimed             *ipld.Node //ParsedCapability
	Delegated           *ipld.Node //Any
	Delegation          *ipld.Node //Delegation
	DelegationErrors    *[]FailureModel
	Did                 *did.DID
	ExpiredAt           *int64
	FailedProofs        *[]FailureModel
	InvalidProofs       *[]FailureModel
	Issuer              *ipld.Node //UCANPrincipal
	Link                *datamodel.Link
	UnknownCapabilities *[]ipld.Node //[]Capability
	ValidAt             *int64
	ErrorString         *bool
}

func (f FailureModel) Error() string {
	return f.Message
}

func (f *FailureModel) ToIPLD() (ipld.Node, error) {
	return ucanipld.WrapWithRecovery(f, typ)
}

var typ schema.Type

func init() {
	ts, err := ipld.LoadSchemaBytes(failureSchema)
	if err != nil {
		panic(fmt.Errorf("loading failure schema: %w", err))
	}
	typ = ts.TypeByName("Failure")
}

func FailureType() schema.Type {
	return typ
}

type failure struct {
	model  FailureModel
	toIPLD func() (ipld.Node, error)
}

func (f failure) Name() string {
	if f.model.Name == nil {
		return ""
	}
	return *f.model.Name
}

func (f failure) Message() string {
	return f.model.Message
}

func (f failure) Error() string {
	return f.model.Message
}

func (f failure) Stack() string {
	if f.model.Stack == nil {
		return ""
	}
	return *f.model.Stack
}

func (f failure) ToIPLD() (ipld.Node, error) {
	if f.toIPLD != nil {
		return f.toIPLD()
	}
	return f.model.ToIPLD()
}

func FromFailureModel(model FailureModel) ucantofailure.IPLDBuilderFailure {
	// treat a zero value failure as a non-error
	if (model == FailureModel{}) {
		return nil
	}
	return failure{model: model}
}
