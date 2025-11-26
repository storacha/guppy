package space

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/guppy/internal/cmdutil"
)

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List all spaces",
	Long: wordwrap.WrapString(
		"Lists all Storacha spaces stored in the local store.",
		80),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := cmdutil.MustGetClient(*StorePathP)

		spaces, err := c.Spaces()
		if err != nil {
			return fmt.Errorf("retrieving spaces: %w", err)
		}

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

		return nil
	},
}

func init() {
	SpaceCmd.AddCommand(lsCmd)
}
