package agentstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
)

type FsStore struct {
	path string
	mu   sync.RWMutex
}

const agentStoreFileName = "store.json"

func NewFs(path string) (*FsStore, error) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, fmt.Errorf("directory %q not writable: %w", path, err)
	}
	agentDataFile := filepath.Join(path, agentStoreFileName)
	if _, err := os.Stat(agentDataFile); err != nil {
		if os.IsNotExist(err) {
			signer, err := ed25519.Generate()
			if err != nil {
				return nil, err
			}
			if err := writeToFile(agentDataFile, AgentData{Principal: signer}); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	s := &FsStore{path: agentDataFile}
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

func (s *FsStore) HasPrincipal() (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := readFromFile(s.path)
	if err != nil {
		return false, err
	}
	return data.Principal != nil, nil
}

func (s *FsStore) Principal() (principal.Signer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.principal()
}

func (s *FsStore) SetPrincipal(principal principal.Signer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := readFromFile(s.path)
	if err != nil {
		return fmt.Errorf("error reading %q: %w", s.path, err)
	}
	data.Principal = principal
	return writeToFile(s.path, data)
}

func (s *FsStore) Delegations() ([]delegation.Delegation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.delegations()
}

func (s *FsStore) AddDelegations(delegs ...delegation.Delegation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := readFromFile(s.path)
	if err != nil {
		return fmt.Errorf("error reading %q: %w", s.path, err)
	}
	data.Delegations = append(data.Delegations, delegs...)
	return writeToFile(s.path, data)
}

func (s *FsStore) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.principal()
	if err != nil {
		return err
	}
	data := AgentData{Principal: p}
	return writeToFile(s.path, data)
}

func (s *FsStore) principal() (principal.Signer, error) {
	data, err := readFromFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("error reading %q: %w", s.path, err)
	}
	return data.Principal, nil
}

func (s *FsStore) delegations() ([]delegation.Delegation, error) {
	data, err := readFromFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("error reading %q: %w", s.path, err)
	}
	return data.Delegations, nil
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
func (s *FsStore) Query(queries ...CapabilityQuery) ([]delegation.Delegation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	delegations, err := s.delegations()
	if err != nil {
		return nil, err
	}

	return query(delegations, queries)
}
