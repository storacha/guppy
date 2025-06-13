package client

import (
	"fmt"

	"github.com/storacha/go-libstoracha/capabilities/access"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	uclient "github.com/storacha/go-ucanto/client"
	udelegation "github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/core/result/failure/datamodel"
	"github.com/storacha/go-ucanto/principal"
	serverdatamodel "github.com/storacha/go-ucanto/server/datamodel"
	"github.com/storacha/guppy/pkg/delegation"
)

// ClaimAccess fetches any stored delegations from the service. This is the
// second step of the Agent authorization process, from the Agent's point of
// view. After the Agent has [RequestAccess]ed, the service will instruct the
// user to confirm the access request out of band, e.g. via email. Once
// confirmed, a delegation will be available on the service for the Agent to
// claim.
func ClaimAccess(issuer principal.Signer, options ...Option) ([]udelegation.Delegation, error) {
	cfg, err := NewConfig(options...)
	if err != nil {
		return nil, err
	}

	caveats := access.ClaimCaveats{}

	inv, err := access.Claim.Invoke(issuer, cfg.conn.ID(), issuer.DID().String(), caveats, convertToInvocationOptions(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("generating invocation: %w", err)
	}

	resp, err := uclient.Execute([]invocation.Invocation{inv}, cfg.conn)
	if err != nil {
		return nil, fmt.Errorf("sending invocation: %w", err)
	}

	reader, err := receipt.NewReceiptReaderFromTypes[access.ClaimOk, serverdatamodel.HandlerExecutionErrorModel](access.ClaimOkType(), serverdatamodel.HandlerExecutionErrorType(), captypes.Converters...)
	if err != nil {
		return nil, fmt.Errorf("generating receipt reader: %w", err)
	}

	rcptlnk, ok := resp.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("receipt not found: %s", inv.Link())
	}

	rcpt, err := reader.Read(rcptlnk, resp.Blocks())
	if err != nil {
		return nil, fmt.Errorf("reading receipt: %w", err)
	}

	claimOk, failErr := result.Unwrap(
		result.MapError(
			result.MapError(
				rcpt.Out(),
				func(errorModel serverdatamodel.HandlerExecutionErrorModel) datamodel.FailureModel {
					return datamodel.FailureModel(errorModel.Cause)
				},
			),
			failure.FromFailureModel,
		),
	)
	if failErr != nil {
		return nil, fmt.Errorf("`access/claim` failed: %w", failErr)
	}

	dels := make([]udelegation.Delegation, 0, len(claimOk.Delegations.Values))
	for _, delBytes := range claimOk.Delegations.Values {
		del, err := delegation.ExtractProof(delBytes)
		if err != nil {
			return nil, fmt.Errorf("extracting delegation: %w", err)
		}
		dels = append(dels, del)
	}

	return dels, nil
}
