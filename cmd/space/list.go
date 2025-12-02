package space

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/guppy/internal/cmdutil"
)

var listFlags struct {
	jsonOutput bool
}

var listCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List all spaces",
	Long: wordwrap.WrapString(
		"Lists all Storacha spaces stored in the local store.",
		80),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := cmdutil.MustGetClient(*StorePathP)

		spaces, err := c.Spaces()
		if err != nil {
			return fmt.Errorf("retrieving spaces: %w", err)
		}

		if listFlags.jsonOutput {
			// Build JSON array of space DIDs
			output := make([]map[string]string, 0, len(spaces))
			for _, space := range spaces {
				output = append(output, map[string]string{
					"id": space.DID().String(),
				})
			}

			jsonBytes, err := json.Marshal(output)
			if err != nil {
				return fmt.Errorf("marshaling output: %w", err)
			}
			fmt.Println(string(jsonBytes))
		} else {
			for _, space := range spaces {
				fmt.Println(space.DID().String())
			}
		}

		return nil
	},
}

func init() {
	listCmd.Flags().BoolVar(&listFlags.jsonOutput, "json", false, "Output in JSON format")
	SpaceCmd.AddCommand(listCmd)
}
