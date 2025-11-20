package space

import (
	"time"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
)

type Space struct {
	DID         string    `json:"did"`
	Name        string    `json:"name"`
	Created     time.Time `json:"created"`
	Registered  bool      `json:"registered,omitempty"`
	Description string    `json:"description,omitempty"`
	PrivateKey  string    `json:"privateKey,omitempty"`
}

type SpaceConfig struct {
	Current string           `json:"current,omitempty"`
	Spaces  map[string]Space `json:"spaces"`
}

var spaceFlags struct {
	description string
	spaceName   string
	output      string
	json        bool
}

func init() {
	Cmd.AddCommand(createCmd, listCmd, useCmd, infoCmd, registerCmd)

}

var Cmd = &cobra.Command{
	Use:   "space",
	Short: "Manage spaces",
	Long: wordwrap.WrapString(
		"Create and manage Storacha spaces locally. Spaces are isolated storage "+
			"contexts that can be shared with other users via delegations.",
		80),
}
