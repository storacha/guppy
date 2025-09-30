//go:build wip

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/storacha/guppy/cmd/internal/largeupload"
)

var largeUploadDemoFlags struct {
	resume        bool
	alterMetadata bool
	alterData     bool
}

var largeUploadDemoCmd = &cobra.Command{
	Use: "large-upload-demo",
	RunE: func(cmd *cobra.Command, args []string) error {
		return largeupload.Demo(cmd.Context(), largeUploadDemoFlags.resume, largeUploadDemoFlags.alterMetadata, largeUploadDemoFlags.alterData)
	},
}

func init() {
	rootCmd.AddCommand(largeUploadDemoCmd)

	largeUploadDemoCmd.Flags().BoolVar(&largeUploadDemoFlags.resume, "resume", false, "Resume a previously interrupted upload")
	largeUploadDemoCmd.Flags().BoolVar(&largeUploadDemoFlags.alterMetadata, "alter-metadata", false, "Cause an error during DAG generation by changing the modification time in a file on the \"filesystem\" between file opens.")
	largeUploadDemoCmd.Flags().BoolVar(&largeUploadDemoFlags.alterData, "alter-data", false, "Cause an error during shard generation by changing the data in a file on the \"filesystem\" between file opens.")
}
