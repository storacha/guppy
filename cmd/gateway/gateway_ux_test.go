package gateway_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/storacha/guppy/cmd/gateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serveHelpOutput executes "gateway serve --help" and returns the output string.
func serveHelpOutput(t *testing.T) string {
	t.Helper()
	buf := new(bytes.Buffer)
	cmd := gateway.Cmd
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"serve", "--help"})
	err := cmd.Execute()
	require.NoError(t, err, "gateway serve --help should not error")
	return buf.String()
}

// TestServeHelpExamples validates AC1: help includes an Examples section with
// at least 3 examples covering basic usage, subdomain mode, and advertise URL.
func TestServeHelpExamples(t *testing.T) {
	help := serveHelpOutput(t)

	t.Run("has examples section", func(t *testing.T) {
		assert.Contains(t, help, "Examples:",
			"help output should contain an Examples section")
	})

	t.Run("has at least 3 examples", func(t *testing.T) {
		// Cobra formats examples indented under "Examples:". Count lines
		// that look like example commands (contain "guppy gateway serve").
		exampleLines := 0
		for _, line := range strings.Split(help, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "guppy gateway serve") || strings.HasPrefix(trimmed, "$ guppy gateway serve") {
				exampleLines++
			}
		}
		assert.GreaterOrEqual(t, exampleLines, 3,
			"help output should contain at least 3 example commands")
	})

	t.Run("includes basic usage example", func(t *testing.T) {
		assert.Contains(t, help, "guppy gateway serve",
			"help should include a basic usage example")
	})

	t.Run("includes subdomain mode example", func(t *testing.T) {
		// Look for an example line that contains both --subdomain and --host
		// in a command invocation, not just in the flags section.
		found := false
		for _, line := range strings.Split(help, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.Contains(trimmed, "guppy") && strings.Contains(trimmed, "--subdomain") && strings.Contains(trimmed, "--host") {
				found = true
				break
			}
		}
		assert.True(t, found,
			"examples should include a subdomain mode command with --subdomain and --host")
	})

	t.Run("includes advertise URL example", func(t *testing.T) {
		// Look for an example line with an advertise-url in a command invocation.
		found := false
		for _, line := range strings.Split(help, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.Contains(trimmed, "guppy") && strings.Contains(trimmed, "--advertise-url") {
				found = true
				break
			}
		}
		assert.True(t, found,
			"examples should include an advertise URL command example")
	})
}

// TestServeHelpSubdomainRequiresHost validates AC2: the --subdomain flag
// description mentions that --host is required.
func TestServeHelpSubdomainRequiresHost(t *testing.T) {
	help := serveHelpOutput(t)

	// Find the line(s) describing the --subdomain flag and check they mention --host
	lines := strings.Split(help, "\n")
	foundSubdomainFlag := false
	subdomainDescription := ""
	for i, line := range lines {
		if strings.Contains(line, "--subdomain") && strings.Contains(line, "-s") {
			foundSubdomainFlag = true
			// Collect the description which may span multiple lines
			subdomainDescription = line
			// Check continuation lines (indented lines following the flag line)
			for j := i + 1; j < len(lines); j++ {
				trimmed := strings.TrimSpace(lines[j])
				if trimmed == "" || strings.HasPrefix(trimmed, "-") {
					break
				}
				subdomainDescription += " " + trimmed
			}
			break
		}
	}

	require.True(t, foundSubdomainFlag,
		"help output should contain --subdomain flag description")
	assert.Contains(t, subdomainDescription, "--host",
		"--subdomain flag description should mention that --host is required")
}

// TestServeHelpBlockCacheMemoryGuidance validates AC3: the --block-cache-capacity
// flag description includes memory guidance (approx 1MB per block).
func TestServeHelpBlockCacheMemoryGuidance(t *testing.T) {
	help := serveHelpOutput(t)

	// Find the block-cache-capacity flag description
	lines := strings.Split(help, "\n")
	cacheDescription := ""
	for i, line := range lines {
		if strings.Contains(line, "--block-cache-capacity") {
			cacheDescription = line
			for j := i + 1; j < len(lines); j++ {
				trimmed := strings.TrimSpace(lines[j])
				if trimmed == "" || strings.HasPrefix(trimmed, "-") {
					break
				}
				cacheDescription += " " + trimmed
			}
			break
		}
	}

	require.NotEmpty(t, cacheDescription,
		"help output should contain --block-cache-capacity flag")
	assert.Contains(t, strings.ToLower(cacheDescription), "1mb",
		"--block-cache-capacity description should mention ~1MB per block memory guidance")
}

// TestServeHelpConfigPriority validates AC4: help text documents the
// configuration priority order (flags > env > config file > defaults).
func TestServeHelpConfigPriority(t *testing.T) {
	help := serveHelpOutput(t)

	// The help text should document configuration precedence somewhere
	helpLower := strings.ToLower(help)

	assert.True(t,
		strings.Contains(helpLower, "flag") &&
			strings.Contains(helpLower, "env") &&
			strings.Contains(helpLower, "config") &&
			strings.Contains(helpLower, "default"),
		"help text should document configuration priority mentioning flags, env, config file, and defaults")

	// Verify ordering is correct: flags should appear before env, env before config, etc.
	// A simple check: the word "flag" appears before "env" which appears before "config file"
	// in the priority description
	flagIdx := strings.Index(helpLower, "flag")
	envIdx := strings.Index(helpLower, "env")
	configIdx := -1
	// Look for "config file" or "config" after "env"
	if envIdx >= 0 {
		rest := helpLower[envIdx:]
		idx := strings.Index(rest, "config")
		if idx >= 0 {
			configIdx = envIdx + idx
		}
	}
	defaultIdx := strings.LastIndex(helpLower, "default")

	assert.Greater(t, envIdx, flagIdx,
		"flags should have higher priority than env (appear first in priority docs)")
	assert.Greater(t, configIdx, envIdx,
		"env should have higher priority than config file")
	assert.Greater(t, defaultIdx, configIdx,
		"config file should have higher priority than defaults")
}

// TestServeHelpSubdomainTypoFix validates AC5: the --subdomain flag description
// says "Enable" not "Enabled".
func TestServeHelpSubdomainTypoFix(t *testing.T) {
	help := serveHelpOutput(t)

	// Find the subdomain flag line
	lines := strings.Split(help, "\n")
	for _, line := range lines {
		if strings.Contains(line, "--subdomain") && strings.Contains(line, "-s") {
			// The description should start with "Enable" not "Enabled"
			assert.Contains(t, line, "Enable subdomain",
				"--subdomain flag should say 'Enable' not 'Enabled'")
			assert.NotContains(t, line, "Enabled subdomain",
				"--subdomain flag should not contain the typo 'Enabled subdomain'")
			return
		}
	}
	t.Fatal("--subdomain flag not found in help output")
}

// TestServeSubdomainWithoutHostFails validates AC6: running with --subdomain
// but no --host fails immediately with an error that mentions --host.
func TestServeSubdomainWithoutHostFails(t *testing.T) {
	buf := new(bytes.Buffer)
	cmd := gateway.Cmd
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"serve", "--subdomain"})

	err := cmd.Execute()
	require.Error(t, err,
		"gateway serve --subdomain without --host should fail")

	errMsg := err.Error() + " " + buf.String()
	errMsgLower := strings.ToLower(errMsg)

	assert.Contains(t, errMsgLower, "--host",
		"error message should mention --host flag when --subdomain is used without it")
}

// TestServeAdvertiseURLRequiresHTTPS validates AC7: running with an HTTP
// advertise URL fails immediately with a clear HTTPS-required error.
func TestServeAdvertiseURLRequiresHTTPS(t *testing.T) {
	buf := new(bytes.Buffer)
	cmd := gateway.Cmd
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"serve", "--advertise-url", "http://example.com"})

	err := cmd.Execute()
	require.Error(t, err,
		"gateway serve --advertise-url http://example.com should fail")

	errMsg := err.Error() + " " + buf.String()
	errMsgLower := strings.ToLower(errMsg)

	assert.Contains(t, errMsgLower, "https",
		"error message should mention that HTTPS is required")
}

// TestServeInvalidLogLevelFails validates AC8: running with an invalid
// --log-level fails immediately with a clear error listing valid levels.
func TestServeInvalidLogLevelFails(t *testing.T) {
	buf := new(bytes.Buffer)
	cmd := gateway.Cmd
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"serve", "--log-level", "bogus"})

	err := cmd.Execute()
	require.Error(t, err,
		"gateway serve --log-level=bogus should fail")

	errMsg := err.Error() + " " + buf.String()
	errMsgLower := strings.ToLower(errMsg)

	// Error should list the valid log levels
	assert.Contains(t, errMsgLower, "debug",
		"error message should list 'debug' as a valid log level")
	assert.Contains(t, errMsgLower, "info",
		"error message should list 'info' as a valid log level")
	assert.Contains(t, errMsgLower, "warn",
		"error message should list 'warn' as a valid log level")
	assert.Contains(t, errMsgLower, "error",
		"error message should list 'error' as a valid log level")
}

// TestGatewayConfigCommandExists validates AC9: "gateway config --help" exists
// and describes its purpose.
func TestGatewayConfigCommandExists(t *testing.T) {
	buf := new(bytes.Buffer)
	cmd := gateway.Cmd
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"config", "--help"})

	err := cmd.Execute()
	require.NoError(t, err,
		"gateway config --help should succeed (command must exist)")

	help := buf.String()
	assert.Contains(t, strings.ToLower(help), "config",
		"gateway config help should describe its purpose")
	// Should mention that it shows resolved configuration
	helpLower := strings.ToLower(help)
	assert.True(t,
		strings.Contains(helpLower, "configuration") || strings.Contains(helpLower, "settings"),
		"gateway config help should mention configuration or settings")
}

// TestGatewayConfigShowsResolvedSettings validates AC10: "gateway config"
// outputs the resolved gateway configuration showing all settings and their
// sources.
func TestGatewayConfigShowsResolvedSettings(t *testing.T) {
	buf := new(bytes.Buffer)
	cmd := gateway.Cmd
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"config"})

	err := cmd.Execute()
	require.NoError(t, err,
		"gateway config should succeed")

	output := buf.String()
	outputLower := strings.ToLower(output)

	t.Run("shows port setting", func(t *testing.T) {
		assert.Contains(t, outputLower, "port",
			"config output should show port setting")
	})

	t.Run("shows block cache capacity setting", func(t *testing.T) {
		assert.True(t,
			strings.Contains(outputLower, "block_cache_capacity") ||
				strings.Contains(outputLower, "block-cache-capacity") ||
				strings.Contains(outputLower, "blockcachecapacity"),
			"config output should show block cache capacity setting")
	})

	t.Run("shows subdomain setting", func(t *testing.T) {
		assert.Contains(t, outputLower, "subdomain",
			"config output should show subdomain setting")
	})

	t.Run("shows trusted setting", func(t *testing.T) {
		assert.Contains(t, outputLower, "trusted",
			"config output should show trusted setting")
	})

	t.Run("shows source information", func(t *testing.T) {
		// Each setting should show where its value comes from (flag, env, config file, default)
		assert.True(t,
			strings.Contains(outputLower, "default") ||
				strings.Contains(outputLower, "flag") ||
				strings.Contains(outputLower, "env") ||
				strings.Contains(outputLower, "source"),
			"config output should indicate the source of each setting value")
	})
}
