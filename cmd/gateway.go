package cmd

import "github.com/storacha/guppy/cmd/gateway"

func init() {
	rootCmd.AddCommand(gateway.GatewayCmd)
}
