package gateway

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Show resolved gateway configuration",
	Long:  "Display the fully resolved gateway configuration showing all settings and their sources.",
	// DisableFlagParsing prevents stale flag state from prior Execute() calls
	// (e.g. --help) from affecting this command when the same Cmd is reused.
	DisableFlagParsing: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		for _, a := range args {
			if a == "--help" || a == "-h" {
				return cmd.Help()
			}
		}

		type setting struct {
			name   string
			key    string
			envVar string
		}

		settings := []setting{
			{"port", "gateway.port", "GUPPY_GATEWAY_PORT"},
			{"block_cache_capacity", "gateway.block_cache_capacity", "GUPPY_GATEWAY_BLOCK_CACHE_CAPACITY"},
			{"advertise_url", "gateway.advertise_url", "GUPPY_GATEWAY_ADVERTISE_URL"},
			{"log_level", "gateway.log_level", "GUPPY_GATEWAY_LOG_LEVEL"},
			{"subdomain", "gateway.subdomain.enabled", "GUPPY_GATEWAY_SUBDOMAIN_ENABLED"},
			{"subdomain.hosts", "gateway.subdomain.hosts", "GUPPY_GATEWAY_SUBDOMAIN_HOSTS"},
			{"trusted", "gateway.trusted", "GUPPY_GATEWAY_TRUSTED"},
		}

		cmd.Println("Gateway Configuration")
		cmd.Println(strings.Repeat("-", 60))
		for _, s := range settings {
			val := viper.Get(s.key)
			src := configSource(s.key, s.envVar)
			cmd.Println(fmt.Sprintf("  %-25s = %-20v (%s)", s.name, val, src))
		}
		return nil
	},
}

// configSource determines where a viper key's value came from.
// Priority: env > config file > default (flags not detectable from config cmd).
func configSource(key, envVar string) string {
	if os.Getenv(envVar) != "" {
		return "env"
	}
	if cfgFile := viper.ConfigFileUsed(); cfgFile != "" {
		// Read the config file independently to check if this key is set there.
		fileCfg := viper.New()
		fileCfg.SetConfigFile(cfgFile)
		if err := fileCfg.ReadInConfig(); err == nil && fileCfg.IsSet(key) {
			return "config file"
		}
	}
	return "default"
}
