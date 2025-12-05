package consumer

import (
	_ "embed"
	"fmt"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result/failure"

	uschema "github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/validator"
)

//go:embed consumer.ipldsch
var consumerSchema []byte

var consumerTS = mustLoadTS()

func mustLoadTS() *schema.TypeSystem {
	ts, err := types.LoadSchemaBytes(consumerSchema)
	if err != nil {
		panic(fmt.Errorf("loading consumer schema: %w", err))
	}
	return ts
}

func GetCaveatsType() schema.Type { return consumerTS.TypeByName("GetCaveats") }
func GetOkType() schema.Type      { return consumerTS.TypeByName("GetOk") }

const GetAbility = "consumer/get"

type GetCaveats struct {
	Consumer string
}

func (gc GetCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&gc, GetCaveatsType(), types.Converters...)
}

var GetCaveatsReader = uschema.Struct[GetCaveats](GetCaveatsType(), nil, types.Converters...)

type GetOk struct {
	Consumer string
	Provider string
}

func (go_ GetOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&go_, GetOkType(), types.Converters...)
}

type GetReceipt receipt.Receipt[GetOk, failure.Failure]
type GetReceiptReader receipt.ReceiptReader[GetOk, failure.Failure]

func NewGetReceiptReader() (GetReceiptReader, error) {
	return receipt.NewReceiptReader[GetOk, failure.Failure](consumerSchema)
}

var GetOkReader = uschema.Struct[GetOk](GetOkType(), nil, types.Converters...)

var Get = validator.NewCapability(
	GetAbility,
	uschema.DIDString(),
	GetCaveatsReader,
	validator.DefaultDerives[GetCaveats],
)
