package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigManager(t *testing.T) {
	// Create a temporary directory for test config files
	tmpDir, err := os.MkdirTemp("", "guppy-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set up a test config file
	configPath := filepath.Join(tmpDir, "config.yaml")
	testConfig := &Config{
		Default: &DefaultConfig{
			Space:      "test-space",
			ProofPath:  "/test/path",
			JSONOutput: true,
			Verbose:    true,
		},
		Spaces: map[string]*Space{
			"test": {
				DID:       "did:test:123",
				ProofPath: "/test/proofs",
			},
		},
		Upload: &UploadConfig{
			DefaultShardSize: 1024,
			WrapSingleFiles: true,
			IncludeHidden:   false,
		},
		Profiles: map[string]*Profile{
			"test": {
				Space:      "test",
				Verbose:    true,
				JSONOutput: true,
			},
		},
	}

	// Save test config
	err = saveConfig(configPath, testConfig)
	require.NoError(t, err)

	// Test loading config
	loadedConfig, err := loadConfig(configPath)
	require.NoError(t, err)

	// Verify loaded config matches test config
	assert.Equal(t, testConfig.Default.Space, loadedConfig.Default.Space)
	assert.Equal(t, testConfig.Default.ProofPath, loadedConfig.Default.ProofPath)
	assert.Equal(t, testConfig.Default.JSONOutput, loadedConfig.Default.JSONOutput)
	assert.Equal(t, testConfig.Default.Verbose, loadedConfig.Default.Verbose)

	assert.Equal(t, testConfig.Spaces["test"].DID, loadedConfig.Spaces["test"].DID)
	assert.Equal(t, testConfig.Spaces["test"].ProofPath, loadedConfig.Spaces["test"].ProofPath)

	assert.Equal(t, testConfig.Upload.DefaultShardSize, loadedConfig.Upload.DefaultShardSize)
	assert.Equal(t, testConfig.Upload.WrapSingleFiles, loadedConfig.Upload.WrapSingleFiles)
	assert.Equal(t, testConfig.Upload.IncludeHidden, loadedConfig.Upload.IncludeHidden)

	assert.Equal(t, testConfig.Profiles["test"].Space, loadedConfig.Profiles["test"].Space)
	assert.Equal(t, testConfig.Profiles["test"].Verbose, loadedConfig.Profiles["test"].Verbose)
	assert.Equal(t, testConfig.Profiles["test"].JSONOutput, loadedConfig.Profiles["test"].JSONOutput)
}

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()

	assert.NotNil(t, cfg.Default)
	assert.NotNil(t, cfg.Spaces)
	assert.NotNil(t, cfg.Upload)
	assert.NotNil(t, cfg.Profiles)

	assert.False(t, cfg.Default.JSONOutput)
	assert.False(t, cfg.Default.Verbose)

	assert.Equal(t, int64(1073741824), cfg.Upload.DefaultShardSize)
	assert.True(t, cfg.Upload.WrapSingleFiles)
	assert.False(t, cfg.Upload.IncludeHidden)
}

func TestConfigFileCreation(t *testing.T) {
	// Create a temporary directory for test
	tmpDir, err := os.MkdirTemp("", "guppy-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set home directory to temp directory for test
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", originalHome)

	// Test creating default config
	configPath, err := createDefaultConfig()
	require.NoError(t, err)

	// Verify config file exists
	_, err = os.Stat(configPath)
	assert.NoError(t, err)

	// Verify config file contains valid YAML
	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	assert.NotNil(t, cfg)
}
