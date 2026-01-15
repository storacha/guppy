package config

type GatewayConfig struct {
	Port               int  `mapstructure:"port" flag:"port" toml:"port"`
	BlockCacheCapacity int  `mapstructure:"block-cache-capacity" flag:"block-cache-capacity" toml:"block-cache-capacity"`
	Trustless          bool `mapstructure:"trustless" flag:"trustless" toml:"trustless"`
}

func (c GatewayConfig) Validate() error {
	return validateConfig(c)
}
