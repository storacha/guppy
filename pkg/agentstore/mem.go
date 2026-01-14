package agentstore

import (
	"sync"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
)

type MemStore struct {
	data *AgentData
	mu   sync.RWMutex
}

func NewMemory() (*MemStore, error) {
	s := &MemStore{data: &AgentData{}}
	// check if this store has been initialized with a signer, if it hasn't create one.
	if has, err := s.HasPrincipal(); err != nil {
		return nil, err
	} else if !has {
		signer, err := ed25519.Generate()
		if err != nil {
			return nil, err
		}
		if err := s.SetPrincipal(signer); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *MemStore) HasPrincipal() (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Principal != nil, nil
}

func (s *MemStore) Principal() (principal.Signer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.principal()
}

func (s *MemStore) SetPrincipal(principal principal.Signer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Principal = principal
	return nil
}

func (s *MemStore) Delegations() ([]delegation.Delegation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.delegations()
}

func (s *MemStore) AddDelegations(delegs ...delegation.Delegation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Delegations = append(s.data.Delegations, delegs...)
	return nil
}

func (s *MemStore) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.principal()
	if err != nil {
		return err
	}
	s.data = &AgentData{Principal: p}
	return nil
}

func (s *MemStore) principal() (principal.Signer, error) {
	return s.data.Principal, nil
}

func (s *MemStore) delegations() ([]delegation.Delegation, error) {
	return s.data.Delegations, nil
}

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
func (s *MemStore) Query(queries ...CapabilityQuery) ([]delegation.Delegation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	delegations, err := s.delegations()
	if err != nil {
		return nil, err
	}

	return query(delegations, queries)
}
