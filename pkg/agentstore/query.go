package agentstore

import (
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
)

// Query returns delegations that match the given capability queries.
// If no queries are provided, returns all non-expired delegations.
// Delegations are filtered by:
//   - Expiration: excludes expired delegations
//   - NotBefore: excludes delegations that are not yet valid
//   - Capability matching: if queries are provided, only returns delegations
//     whose capabilities match at least one of the queries
//
// Additionally, this method includes relevant session proofs (ucan/attest delegations)
// that attest to the returned authorizations.
func Query(delegations []delegation.Delegation, queries []CapabilityQuery) []delegation.Delegation {
	now := ucan.Now()

	// Map to track which delegations to include (by CID string)
	authorizations := make(map[string]delegation.Delegation)

	// First pass: collect matching authorizations (non-session-proof delegations)
	for _, del := range delegations {
		// Filter out expired delegations
		if exp := del.Expiration(); exp != nil && *exp < now {
			continue
		}

		// Filter out delegations that are not yet valid
		if del.NotBefore() > now {
			continue
		}

		// Skip session proofs in the first pass
		if isSessionProof(del) {
			continue
		}

		// If no queries, include all non-expired delegations
		if len(queries) == 0 {
			authorizations[del.Link().String()] = del
			continue
		}

		// Check if delegation matches any query
		if matchesAnyQuery(del, queries) {
			authorizations[del.Link().String()] = del
		}
	}

	// Second pass: collect session proofs that attest to the authorizations
	sessionProofs := getSessionProofs(delegations, now)

	for authCID := range authorizations {
		if proofsForAuth, exists := sessionProofs[authCID]; exists {
			// Add all session proofs for this authorization
			for _, sessionProof := range proofsForAuth {
				authorizations[sessionProof.Link().String()] = sessionProof
			}
		}
	}

	// Convert map to slice
	result := make([]delegation.Delegation, 0, len(authorizations))
	for _, del := range authorizations {
		result = append(result, del)
	}

	return result

}

// matchesAnyQuery checks if a delegation's capabilities match any of the provided queries.
func matchesAnyQuery(del delegation.Delegation, queries []CapabilityQuery) bool {
	caps := del.Capabilities()
	for _, query := range queries {
		for _, c := range caps {
			if matchesCapability(c, query) {
				return true
			}
		}
	}
	return false
}

// matchesCapability checks if a capability matches a query using the resolution logic
// from go-ucanto's validator package.
func matchesCapability(cap ucan.Capability[any], query CapabilityQuery) bool {
	// Match ability
	if !matchesAbility(cap.Can(), query.Can) {
		return false
	}

	// Match resource
	if !matchesResource(cap.With(), query.With) {
		return false
	}

	return true
}

// matchesAbility checks if a delegation's ability can authorize the query ability.
// A delegation matches if it has the same ability or a broader wildcard.
// For example, searching for "upload/add" matches delegations with:
//   - "upload/add" (exact match)
//   - "upload/*" (namespace wildcard)
//   - "*" (global wildcard)
func matchesAbility(capAbility, queryAbility ucan.Ability) bool {
	// Exact match
	if capAbility == queryAbility {
		return true
	}

	// Global wildcard in capability
	if capAbility == "*" {
		return true
	}

	// Namespace wildcard in capability (e.g., capability has "upload/*", query is "upload/add")
	if len(capAbility) > 2 && capAbility[len(capAbility)-2:] == "/*" {
		prefix := capAbility[:len(capAbility)-1] // "upload/"
		return len(queryAbility) >= len(prefix) && queryAbility[:len(prefix)] == prefix
	}

	return false
}

// matchesResource checks if a capability's resource matches the query resource.
// A capability matches if:
//   - The query resource is empty (matches any capability resource)
//   - The resources match exactly
//   - The capability has "ucan:*" (matches any query resource)
func matchesResource(capResource, queryResource ucan.Resource) bool {
	return queryResource == "" || capResource == "ucan:*" || queryResource == capResource
}

// isSessionProof checks if a delegation is a session proof (ucan/attest capability).
func isSessionProof(del delegation.Delegation) bool {
	caps := del.Capabilities()
	if len(caps) == 0 {
		return false
	}
	// A session proof has a ucan/attest capability
	return caps[0].Can() == ucancap.AttestAbility
}

// getSessionProofs organizes session proofs by the CID of the authorization they attest to.
// Returns a map from authorization CID string to list of session proof delegations.
func getSessionProofs(delegations []delegation.Delegation, now ucan.UTCUnixTimestamp) map[string][]delegation.Delegation {
	proofs := make(map[string][]delegation.Delegation)

	for _, del := range delegations {
		if !isSessionProof(del) {
			continue
		}

		// Filter out expired session proofs
		if exp := del.Expiration(); exp != nil && *exp < now {
			continue
		}

		// Filter out session proofs that are not yet valid
		if del.NotBefore() > now {
			continue
		}

		caps := del.Capabilities()
		if len(caps) == 0 {
			continue
		}

		// Parse the capability using the ucan/attest parser to get typed access
		source := validator.NewSource(caps[0], del)
		match, err := ucancap.Attest.Match(source)
		if err != nil {
			// If we can't parse it, skip it
			continue
		}

		// Get the proof link from the typed capability
		attestCap := match.Value()
		proofCID := attestCap.Nb().Proof.String()
		proofs[proofCID] = append(proofs[proofCID], del)
	}

	return proofs
}
