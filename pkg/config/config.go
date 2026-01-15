package config

import "github.com/spf13/viper"

type Config struct {
	Gateway GatewayConfig `mapstructure:"gateway" flag:"gateway" toml:"gateway"`
}

func (c Config) Validate() error {
	return validateConfig(c)
}

func Load[T Validatable]() (T, error) {
	var out T
	if err := viper.Unmarshal(&out); err != nil {
		return out, err
	}
	if err := out.Validate(); err != nil {
		return out, err
	}
	return out, nil
}
