//go:build wip

package cmd

import (
	"errors"

	"github.com/spf13/cobra"
	"github.com/storacha/guppy/cmd/internal/largeupload"
)

var largeUploadCmd = &cobra.Command{
	Use:   "large-upload",
	Short: "WIP - Upload a large amount of data to the service",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		space, err := cmd.Flags().GetString("space")
		if err != nil {
			return err
		}

		resumeUpload, err := cmd.Flags().GetBool("resume")
		if err != nil {
			return err
		}

		root := cmd.Flags().Arg(0)
		if root == "" {
			return errors.New("Path cannot be empty")
		}

		return largeupload.Action(cmd.Context(), space, resumeUpload, root)
	},
}

func init() {
	rootCmd.AddCommand(largeUploadCmd)

	largeUploadCmd.Flags().String("space", "", "DID of the space to upload to")
	largeUploadCmd.MarkFlagRequired("space")
	largeUploadCmd.Flags().Bool("resume", false, "Resume a previously interrupted upload")
}
