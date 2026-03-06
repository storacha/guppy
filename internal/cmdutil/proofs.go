package cmdutil

import (
	"context"
	"fmt"

	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-libstoracha/principalresolver"
	ucan_bs "github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	edverifier "github.com/storacha/go-ucanto/principal/ed25519/verifier"
	"github.com/storacha/go-ucanto/principal/verifier"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
)

// ProofResource finds the resource for a proof, handling the case where the
// delegated resource is "ucan:*" by recursively checking its proofs to find a
// delegation for the specific resource.
func ProofResource(proof delegation.Delegation, ability ucan.Ability) (ucan.Resource, bool) {
	for _, cap := range proof.Capabilities() {
		if validator.ResolveAbility(cap.Can(), ability) == "" {
			continue
		}
		if cap.With() != "ucan:*" {
			return cap.With(), true
		}
		proofs := proof.Proofs()
		if len(proofs) == 0 {
			continue
		}
		bs, err := ucan_bs.NewBlockReader(ucan_bs.WithBlocksIterator(proof.Blocks()))
		if err != nil {
			return "", false
		}
		for _, plink := range proofs {
			p, err := delegation.NewDelegationView(plink, bs)
			if err != nil {
				return "", false
			}
			if r, ok := ProofResource(p, ability); ok {
				return r, true
			}
		}
	}
	return "", false
}

// BuildPruningContext creates a ValidationContext for pruning proofs in
// retrieval auth delegations. The attestorDID should be the upload service DID,
// which is the authority that issues ucan/attest delegations in the proof chain.
func BuildPruningContext(ctx context.Context, attestorDID did.DID) (validator.ValidationContext[contentcap.RetrieveCaveats], error) {
	resolver, err := principalresolver.NewHTTPResolver([]did.DID{attestorDID})
	if err != nil {
		return nil, fmt.Errorf("creating principal resolver: %w", err)
	}

	resolvedKeyDID, err := resolver.ResolveDIDKey(ctx, attestorDID)
	if err != nil {
		return nil, fmt.Errorf("resolving attestor DID key: %w", err)
	}

	keyVerifier, err := edverifier.Parse(resolvedKeyDID.String())
	if err != nil {
		return nil, fmt.Errorf("parsing resolved key DID: %w", err)
	}

	attestor, err := verifier.Wrap(keyVerifier, attestorDID)
	if err != nil {
		return nil, fmt.Errorf("creating attestor verifier: %w", err)
	}

	return validator.NewValidationContext(
		// For client evaluated chains, the authority must be the service that issued the ucan/attest
		// delegations — e.g. the upload-service.
		attestor,
		contentcap.Retrieve,
		validator.IsSelfIssued,
		func(context.Context, validator.Authorization[any]) validator.Revoked {
			return nil
		},
		validator.ProofUnavailable,
		server.ParsePrincipal,
		validator.FailDIDKeyResolution,
		validator.NotExpiredNotTooEarly,
	), nil
}
