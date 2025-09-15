package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config represents the root configuration structure
type Config struct {
	Default *DefaultConfig       `yaml:"default"`
	Spaces  map[string]*Space   `yaml:"spaces"`
	Upload  *UploadConfig       `yaml:"upload"`
	Profiles map[string]*Profile `yaml:"profiles"`
}

// DefaultConfig holds the default settings
type DefaultConfig struct {
	Space      string `yaml:"space"`
	ProofPath  string `yaml:"proof_path"`
	JSONOutput bool   `yaml:"json_output"`
	Verbose    bool   `yaml:"verbose"`
}

// Space represents a space configuration
type Space struct {
	DID       string `yaml:"did"`
	ProofPath string `yaml:"proof_path"`
}

// UploadConfig holds upload-specific settings
type UploadConfig struct {
	DefaultShardSize  int64 `yaml:"default_shard_size"`
	WrapSingleFiles   bool  `yaml:"wrap_single_files"`
	IncludeHidden     bool  `yaml:"include_hidden"`
}

// Profile represents a named set of settings
type Profile struct {
	Space         string `yaml:"space"`
	Verbose       bool   `yaml:"verbose"`
	JSONOutput    bool   `yaml:"json_output"`
	WrapSingleFiles bool `yaml:"wrap_single_files"`
}

// Manager handles configuration loading and saving
type Manager struct {
	config     *Config
	configPath string
}

// DefaultConfig provides default configuration values
func defaultConfig() *Config {
	return &Config{
		Default: &DefaultConfig{
			JSONOutput: false,
			Verbose:    false,
		},
		Spaces:   make(map[string]*Space),
		Upload: &UploadConfig{
			DefaultShardSize: 1073741824, // 1GB
			WrapSingleFiles:  true,
			IncludeHidden:    false,
		},
		Profiles: make(map[string]*Profile),
	}
}

// NewManager creates a new configuration manager
func NewManager() (*Manager, error) {
	configPath, err := findConfigFile()
	if err != nil {
		// If no config file exists, create one with defaults
		configPath, err = createDefaultConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to create default config: %w", err)
		}
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, err
	}

	return &Manager{
		config:     cfg,
		configPath: configPath,
	}, nil
}

// findConfigFile looks for a config file in standard locations
func findConfigFile() (string, error) {
	// Check current directory
	if _, err := os.Stat(".guppy.yaml"); err == nil {
		return ".guppy.yaml", nil
	}

	// Check user's home directory
	home, err := os.UserHomeDir()
	if err == nil {
		path := filepath.Join(home, ".guppy", "config.yaml")
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}

	// Check system-wide location
	if _, err := os.Stat("/etc/guppy/config.yaml"); err == nil {
		return "/etc/guppy/config.yaml", nil
	}

	return "", fmt.Errorf("no config file found")
}

// createDefaultConfig creates a default configuration file
func createDefaultConfig() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}

	configDir := filepath.Join(home, ".guppy")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create config directory: %w", err)
	}

	configPath := filepath.Join(configDir, "config.yaml")
	cfg := defaultConfig()

	if err := saveConfig(configPath, cfg); err != nil {
		return "", fmt.Errorf("failed to save default config: %w", err)
	}

	return configPath, nil
}

// loadConfig loads the configuration from a file
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := defaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return cfg, nil
}

// saveConfig saves the configuration to a file
func saveConfig(path string, cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Get returns the current configuration
func (m *Manager) Get() *Config {
	return m.config
}

// Save saves the current configuration
func (m *Manager) Save() error {
	return saveConfig(m.configPath, m.config)
}

// SetValue sets a configuration value by path
func (m *Manager) SetValue(path string, value interface{}) error {
	return fmt.Errorf("not implemented")
}

// GetValue gets a configuration value by path
func (m *Manager) GetValue(path string) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
