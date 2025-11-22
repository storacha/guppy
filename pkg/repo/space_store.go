package repo

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/multiformats/go-multibase"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/guppy/pkg/didmailto"
)

// TODO since we are persisting private keys here, might be worth using a datastore
// instead of a json file
type Space struct {
	DID         did.DID   `json:"did"`
	Name        string    `json:"name"`
	Created     time.Time `json:"created"`
	Registered  bool      `json:"registered,omitempty"`
	Description string    `json:"description,omitempty"`
	PrivateKey  string    `json:"privateKey,omitempty"`
}

func (s Space) NewDelegation(email string) (delegation.Delegation, error) {
	emailDID, err := didmailto.FromEmail(email)
	if err != nil {
		return nil, fmt.Errorf("invalid email address: %w", err)
	}

	spaceSigner, err := signer.Parse(s.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse space private key: %w", err)
	}

	// Grant all necessary capabilities for the space
	capabilities := []ucan.Capability[ucan.NoCaveats]{
		ucan.NewCapability("space/*", s.DID.String(), ucan.NoCaveats{}),
		ucan.NewCapability("upload/*", s.DID.String(), ucan.NoCaveats{}),
		ucan.NewCapability("store/*", s.DID.String(), ucan.NoCaveats{}),
		ucan.NewCapability("index/*", s.DID.String(), ucan.NoCaveats{}),
		ucan.NewCapability("filecoin/*", s.DID.String(), ucan.NoCaveats{}),
		ucan.NewCapability("blob/*", s.DID.String(), ucan.NoCaveats{}),
	}

	return delegation.Delegate(
		spaceSigner,
		emailDID,
		capabilities,
		delegation.WithNoExpiration(),
	)

}

// Sanitized returns the space with the PrivateKey omitted.
func (s Space) Sanitized() Space {
	return Space{
		DID:         s.DID,
		Name:        s.Name,
		Created:     s.Created,
		Registered:  s.Registered,
		Description: s.Description,
	}
}

type SpacesState struct {
	Current *Space           `json:"current,omitempty"`
	Spaces  map[string]Space `json:"spaces"`
}

type SpaceStore struct {
	mu   sync.RWMutex
	path string
}

// SetCurrent sets the provided space as the current.
func (s *SpaceStore) SetCurrent(space Space) error {
	state, err := s.load()
	if err != nil {
		return err
	}

	state.Current = &space

	return s.save(state)
}

// Current returns the current space. An error is returned if the current space is unset.
func (s *SpaceStore) Current() (Space, error) {
	state, err := s.load()
	if err != nil {
		return Space{}, err
	}
	if state.Current == nil {
		return Space{}, fmt.Errorf("no current space exists")
	}
	return *state.Current, nil
}

// State returns the current state of the space store
func (s *SpaceStore) State() (SpacesState, error) {
	return s.load()
}

// Get retrieves the space with the provided name. An error is returned if the
// space doesn't exist in the store
func (s *SpaceStore) Get(name string) (Space, error) {
	state, err := s.load()
	if err != nil {
		return Space{}, err
	}
	space, ok := state.Spaces[name]
	if !ok {
		return Space{}, fmt.Errorf("space '%s' does not exist", name)
	}
	return space, nil
}

// List returns a map of all spaces in the store.
func (s *SpaceStore) List() (map[string]Space, error) {
	state, err := s.load()
	if err != nil {
		return nil, err
	}
	return state.Spaces, nil
}

// CreateSpace creates a space with the provided name and description. An error
// is returned if a space with the provided name already exists.
// If the space created is the first space added to the store, it's set as the current.
func (s *SpaceStore) CreateSpace(name, description string) (Space, error) {
	state, err := s.load()
	if err != nil {
		return Space{}, err
	}
	_, ok := state.Spaces[name]
	if ok {
		return Space{}, fmt.Errorf("space '%s' already exists", name)
	}
	sk, err := signer.Generate()
	if err != nil {
		return Space{}, err
	}

	privKeyStr, err := multibase.Encode(multibase.Base64pad, sk.Encode())
	if err != nil {
		return Space{}, err
	}

	newSpace := Space{
		DID:         sk.DID(),
		Created:     time.Now(),
		PrivateKey:  privKeyStr,
		Name:        name,
		Description: description,
	}

	state.Spaces[name] = newSpace
	// if this is the first space, set as current
	if len(state.Spaces) == 1 {
		state.Current = &newSpace
	}

	if err := s.save(state); err != nil {
		return Space{}, err
	}
	return newSpace, nil
}

// RegisterSpace creates a delegation for the provieded email to the space, returning the delegation.
// An error is returned if the space doesn't exist.
func (s *SpaceStore) RegisterSpace(space Space, email string) (delegation.Delegation, error) {
	state, err := s.load()
	if err != nil {
		return nil, err
	}

	toDelegate, ok := state.Spaces[space.Name]
	if !ok {
		return nil, fmt.Errorf("space '%s' does not exist", space.Name)
	}

	del, err := toDelegate.NewDelegation(email)
	if err != nil {
		return nil, err
	}

	toDelegate.Registered = true
	state.Spaces[toDelegate.Name] = toDelegate

	return del, s.save(state)
}

// init initializes the space store.
func (s *SpaceStore) init() error {
	return s.save(SpacesState{
		Current: new(Space),
		Spaces:  make(map[string]Space),
	})
}

// load returns the current state of the SpaceStore. load is thread safe.
func (s *SpaceStore) load() (SpacesState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var state SpacesState
	data, err := os.ReadFile(s.path)
	if err != nil {
		return SpacesState{}, fmt.Errorf("failed to open space store: %w", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return SpacesState{}, fmt.Errorf("failed to read space store: %w", err)
	}
	return state, nil
}

// save persists the provided state to storage. save is thread safe.
func (s *SpaceStore) save(state SpacesState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to save space store: %w", err)
	}

	return os.WriteFile(s.path, data, 0600)
}
