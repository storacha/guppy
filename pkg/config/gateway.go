package config

import "errors"

type GatewayConfig struct {
	// Port is the port to run the gateway on.
	Port int `mapstructure:"port" flag:"port" toml:"port"`
	// TlsPort is the external TLS port advertised in delegated routing
	// responses. This is the port an external TLS proxy (e.g. nginx) listens
	// on. Set to 0 to omit /tls from the advertised multiaddr.
	TlsPort int `mapstructure:"tls-port" flag:"tls-port" toml:"tls-port"`
	// BlockCacheCapacity defines the number of blocks to cache in memory. Blocks
	// are typically <1MB due to IPFS chunking, so an upper bound for how much
	// memory the cache will utilize is approximately this number multiplied by
	// 1MB. e.g. capacity for 1,000 blocks ~= 1GB of memory.
	BlockCacheCapacity int `mapstructure:"block_cache_capacity" flag:"block-cache-capacity" toml:"block_cache_capacity"`
	// LogLevel sets the logging level for the gateway server (debug, info, warn
	// or error).
	LogLevel string `mapstructure:"log_level" flag:"log-level" toml:"log_level"`
	// Subdomain configures subdomain gateway mode.
	Subdomain SubdomainConfig `mapstructure:"subdomain" toml:"subdomain"`
	// Trusted indicates whether to enable trusted gateway mode, which allows
	// deserialized responses.
	Trusted bool `mapstructure:"trusted" flag:"trusted" toml:"trusted"`
}

func (c GatewayConfig) Validate() error {
	err := validateConfig(c)
	if err != nil {
		return err
	}
	return c.Subdomain.Validate()
}

type SubdomainConfig struct {
	// Enabled indicates whether to enable subdomain gateway mode.
	Enabled bool
	// Hosts is the list of gateway hosts for subdomain mode.
	Hosts []string
}

func (c SubdomainConfig) Validate() error {
	err := validateConfig(c)
	if err != nil {
		return err
	}
	if c.Enabled && len(c.Hosts) == 0 {
		return errors.New("one or more hosts must be specified when subdomain mode is enabled")
	}
	return nil
}
