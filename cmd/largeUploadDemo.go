//go:build wip

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/storacha/guppy/cmd/internal/largeupload"
)

var largeUploadDemoCmd = &cobra.Command{
	Use: "large-upload-demo",
	RunE: func(cmd *cobra.Command, args []string) error {
		resumeUpload, err := cmd.Flags().GetBool("resume")
		if err != nil {
			return err
		}

		alterMetadata, err := cmd.Flags().GetBool("alter-metadata")
		if err != nil {
			return err
		}

		alterData, err := cmd.Flags().GetBool("alter-data")
		if err != nil {
			return err
		}
		return largeupload.Demo(cmd.Context(), resumeUpload, alterMetadata, alterData)
	},
}

func init() {
	rootCmd.AddCommand(largeUploadDemoCmd)

	largeUploadDemoCmd.Flags().Bool("resume", false, "Resume a previously interrupted upload")
	largeUploadDemoCmd.Flags().Bool("alter-metadata", false, "Cause an error during DAG generation by changing the modification time in a file on the \"filesystem\" between file opens.")
	largeUploadDemoCmd.Flags().Bool("alter-data", false, "Cause an error during shard generation by changing the data in a file on the \"filesystem\" between file opens.")
}
