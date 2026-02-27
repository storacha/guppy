package space

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
)

var listFlags struct {
	jsonOutput bool
}

func init() {
	listCmd.Flags().BoolVar(&listFlags.jsonOutput, "json", false, "Output in JSON format")
}

var listCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List all spaces",
	Long: wordwrap.WrapString(
		"Lists all Storacha spaces stored in the local store.",
		80),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir, cfg.Network)

		spaces, err := c.Spaces()
		if err != nil {
			return fmt.Errorf("retrieving spaces: %w", err)
		}

		if listFlags.jsonOutput {
			type spaceOutput struct {
				ID    string   `json:"id"`
				Names []string `json:"names,omitempty"`
			}
			output := make([]spaceOutput, 0, len(spaces))
			for _, space := range spaces {
				output = append(output, spaceOutput{
					ID:    space.DID().String(),
					Names: space.Names(),
				})
			}

			jsonBytes, err := json.Marshal(output)
			if err != nil {
				return fmt.Errorf("marshaling output: %w", err)
			}
			fmt.Println(string(jsonBytes))
		} else {
			fmt.Printf("%-60s %s\n", "SPACE", "NAME")
			for _, space := range spaces {
				names := space.Names()
				if len(names) > 0 {
					quoted := make([]string, len(names))
					for i, n := range names {
						quoted[i] = fmt.Sprintf("%q", n)
					}
					fmt.Printf("%-60s %s\n", space.DID().String(), strings.Join(quoted, ", "))
				} else {
					fmt.Println(space.DID().String())
				}
			}
		}

		return nil
	},
}
