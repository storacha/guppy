package space

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/multiformats/go-multibase"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/guppy/pkg/didmailto"
)

func generateSpace() (*Space, error) {
	s, err := signer.Generate()
	if err != nil {
		return nil, err
	}

	privKeyStr, err := multibase.Encode(multibase.Base64pad, s.Encode())
	if err != nil {
		return nil, err
	}

	return &Space{
		DID:        s.DID().String(),
		Created:    time.Now(),
		PrivateKey: privKeyStr,
	}, nil
}

func getSpaceConfigPath() (string, error) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	// Use same directory as existing config
	storachaDir := filepath.Join(homedir, ".storacha")
	return filepath.Join(storachaDir, "spaces.json"), nil
}

func loadSpaceConfig() (*SpaceConfig, error) {
	spacePath, err := getSpaceConfigPath()
	if err != nil {
		return nil, err
	}

	var config SpaceConfig
	if data, err := os.ReadFile(spacePath); err == nil {
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse space config: %w", err)
		}
	}

	if config.Spaces == nil {
		config.Spaces = make(map[string]Space)
	}

	return &config, nil
}

func saveSpaceConfig(config *SpaceConfig) error {
	spacePath, err := getSpaceConfigPath()
	if err != nil {
		return err
	}

	datadir := filepath.Dir(spacePath)
	if err := os.MkdirAll(datadir, 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal space config: %w", err)
	}

	return os.WriteFile(spacePath, data, 0600)
}

func addSpaceToConfig(space *Space) error {
	config, err := loadSpaceConfig()
	if err != nil {
		return err
	}

	config.Spaces[space.DID] = *space

	if config.Current == "" {
		config.Current = space.DID
	}

	return saveSpaceConfig(config)
}

func getCurrentSpace() (*Space, error) {
	config, err := loadSpaceConfig()
	if err != nil {
		return nil, err
	}

	if config.Current == "" {
		return nil, fmt.Errorf("no current space set - run 'guppy space create' or 'guppy space use'")
	}

	space, exists := config.Spaces[config.Current]
	if !exists {
		return nil, fmt.Errorf("current space '%s' not found", config.Current)
	}

	return &space, nil
}

func findSpaceByNameOrDID(nameOrDID string) (*Space, error) {
	config, err := loadSpaceConfig()
	if err != nil {
		return nil, err
	}

	// Try DID first
	if space, exists := config.Spaces[nameOrDID]; exists {
		return &space, nil
	}

	// Try name (case-insensitive)
	for _, space := range config.Spaces {
		if strings.EqualFold(space.Name, nameOrDID) {
			return &space, nil
		}
	}

	return nil, fmt.Errorf("space '%s' not found", nameOrDID)
}

func createSpaceToEmailDelegation(space *Space, email string) (delegation.Delegation, error) {
	emailDID, err := didmailto.FromEmail(email)
	if err != nil {
		return nil, fmt.Errorf("invalid email address: %w", err)
	}

	spaceSigner, err := signer.Parse(space.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse space private key: %w", err)
	}

	// Grant all necessary capabilities for the space
	capabilities := []ucan.Capability[ucan.NoCaveats]{
		ucan.NewCapability("space/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("upload/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("store/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("index/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("filecoin/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("blob/*", space.DID, ucan.NoCaveats{}),
	}

	return delegation.Delegate(
		spaceSigner,
		emailDID,
		capabilities,
		delegation.WithNoExpiration(),
	)
}

// sanitizeSpace removes sensitive data for JSON output
func sanitizeSpace(space *Space) interface{} {
	return struct {
		DID         string    `json:"did"`
		Name        string    `json:"name"`
		Created     time.Time `json:"created"`
		Registered  bool      `json:"registered,omitempty"`
		Description string    `json:"description,omitempty"`
	}{
		DID:         space.DID,
		Name:        space.Name,
		Created:     space.Created,
		Registered:  space.Registered,
		Description: space.Description,
	}
}

func saveDelegationToFile(del delegation.Delegation, filepath string) error {
	archive := del.Archive()
	data, err := io.ReadAll(archive)
	if err != nil {
		return fmt.Errorf("failed to read delegation archive: %w", err)
	}

	if err := os.WriteFile(filepath, data, 0600); err != nil {
		return fmt.Errorf("failed to write delegation file: %w", err)
	}

	return nil
}
