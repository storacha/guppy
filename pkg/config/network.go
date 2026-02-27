package config

import (
	"fmt"
	"net/url"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/presets"
)

// NetworkConfig holds network-related configuration that can be specified in
// the config file. All fields are optional. If Name is set, it refers to a
// preset network configuration (forge, hot, warm-staging). Other fields can
// override individual values from the preset.
type NetworkConfig struct {
	// Name is the name of a preset network (forge, hot, warm-staging).
	// If set, other fields override values from the preset.
	Name string `mapstructure:"name" toml:"name"`
	// UploadID is the DID of the upload service.
	UploadID string `mapstructure:"upload_id" toml:"upload_id"`
	// UploadURL is the URL of the upload service.
	UploadURL string `mapstructure:"upload_url" toml:"upload_url"`
	// ReceiptsURL is the URL of the receipts service.
	ReceiptsURL string `mapstructure:"receipts_url" toml:"receipts_url"`
	// IndexerID is the DID of the indexing service.
	IndexerID string `mapstructure:"indexer_id" toml:"indexer_id"`
	// IndexerURL is the URL of the indexing service.
	IndexerURL string `mapstructure:"indexer_url" toml:"indexer_url"`
	// AuthorizedRetrievals indicates whether UCAN authorized retrievals are supported.
	// Use a pointer to distinguish between unset and false.
	AuthorizedRetrievals *bool `mapstructure:"authorized_retrievals" toml:"authorized_retrievals"`
}

// IsEmpty returns true if no network configuration fields are set.
func (n NetworkConfig) IsEmpty() bool {
	return n.Name == "" &&
		n.UploadID == "" &&
		n.UploadURL == "" &&
		n.ReceiptsURL == "" &&
		n.IndexerID == "" &&
		n.IndexerURL == "" &&
		n.AuthorizedRetrievals == nil
}

// Validate validates the network configuration.
func (n NetworkConfig) Validate() error {
	if n.UploadID != "" {
		if _, err := did.Parse(n.UploadID); err != nil {
			return fmt.Errorf("invalid network.upload_id: %w", err)
		}
	}
	if n.UploadURL != "" {
		if _, err := url.Parse(n.UploadURL); err != nil {
			return fmt.Errorf("invalid network.upload_url: %w", err)
		}
	}
	if n.ReceiptsURL != "" {
		if _, err := url.Parse(n.ReceiptsURL); err != nil {
			return fmt.Errorf("invalid network.receipts_url: %w", err)
		}
	}
	if n.IndexerID != "" {
		if _, err := did.Parse(n.IndexerID); err != nil {
			return fmt.Errorf("invalid network.indexer_id: %w", err)
		}
	}
	if n.IndexerURL != "" {
		if _, err := url.Parse(n.IndexerURL); err != nil {
			return fmt.Errorf("invalid network.indexer_url: %w", err)
		}
	}
	return nil
}

// ToPresetConfig converts this config to a presets.NetworkConfig, using the
// specified base preset name as a starting point. If baseName is empty and this
// config's Name is also empty, the default network is used. Config values
// override preset values when set.
func (n NetworkConfig) ToPresetConfig(baseName string) (presets.NetworkConfig, error) {
	// Determine which preset to use as base
	presetName := baseName
	if presetName == "" {
		presetName = n.Name
	}

	// Get the base preset (this also handles STORACHA_* env vars for backward compat)
	network, err := presets.GetNetworkConfig(presetName)
	if err != nil {
		return presets.NetworkConfig{}, err
	}

	// Override with config values if present
	if n.UploadID != "" {
		id, err := did.Parse(n.UploadID)
		if err != nil {
			return presets.NetworkConfig{}, fmt.Errorf("parsing network.upload_id: %w", err)
		}
		network.UploadID = id
		network.Name = "custom"
	}

	if n.UploadURL != "" {
		u, err := url.Parse(n.UploadURL)
		if err != nil {
			return presets.NetworkConfig{}, fmt.Errorf("parsing network.upload_url: %w", err)
		}
		network.UploadURL = *u
		network.Name = "custom"
	}

	if n.ReceiptsURL != "" {
		u, err := url.Parse(n.ReceiptsURL)
		if err != nil {
			return presets.NetworkConfig{}, fmt.Errorf("parsing network.receipts_url: %w", err)
		}
		network.ReceiptsURL = *u
		network.Name = "custom"
	}

	if n.IndexerID != "" {
		id, err := did.Parse(n.IndexerID)
		if err != nil {
			return presets.NetworkConfig{}, fmt.Errorf("parsing network.indexer_id: %w", err)
		}
		network.IndexerID = id
		network.Name = "custom"
	}

	if n.IndexerURL != "" {
		u, err := url.Parse(n.IndexerURL)
		if err != nil {
			return presets.NetworkConfig{}, fmt.Errorf("parsing network.indexer_url: %w", err)
		}
		network.IndexerURL = *u
		network.Name = "custom"
	}

	if n.AuthorizedRetrievals != nil {
		network.AuthorizedRetrievals = *n.AuthorizedRetrievals
		network.Name = "custom"
	}

	return network, nil
}
