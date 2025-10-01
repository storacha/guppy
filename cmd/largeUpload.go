//go:build wip

package cmd

import (
	"errors"

	"github.com/spf13/cobra"
	"github.com/storacha/guppy/cmd/internal/largeupload"
)

var largeUploadFlags struct {
	space  string
	resume bool
}

var largeUploadCmd = &cobra.Command{
	Use:   "large-upload",
	Short: "WIP - Upload a large amount of data to the service",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		root := cmd.Flags().Arg(0)
		if root == "" {
			return errors.New("Path cannot be empty")
		}

		return largeupload.Action(cmd.Context(), largeUploadFlags.space, largeUploadFlags.resume, root)
	},
}

func init() {
	rootCmd.AddCommand(largeUploadCmd)

	largeUploadCmd.Flags().StringVar(&largeUploadFlags.space, "space", "", "DID of the space to upload to")
	largeUploadCmd.MarkFlagRequired("space")
	largeUploadCmd.Flags().BoolVar(&largeUploadFlags.resume, "resume", false, "Resume a previously interrupted upload")
}
