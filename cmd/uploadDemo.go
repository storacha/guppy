//go:build wip

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/storacha/guppy/cmd/internal/upload"
)

var uploadDemoFlags struct {
	resume        bool
	alterMetadata bool
	alterData     bool
}

var uploadDemoCmd = &cobra.Command{
	Use: "upload-demo",
	RunE: func(cmd *cobra.Command, args []string) error {
		return upload.Demo(cmd.Context(), uploadDemoFlags.resume, uploadDemoFlags.alterMetadata, uploadDemoFlags.alterData)
	},
}

func init() {
	rootCmd.AddCommand(uploadDemoCmd)

	uploadDemoCmd.Flags().BoolVar(&uploadDemoFlags.resume, "resume", false, "Resume a previously interrupted upload")
	uploadDemoCmd.Flags().BoolVar(&uploadDemoFlags.alterMetadata, "alter-metadata", false, "Cause an error during DAG generation by changing the modification time in a file on the \"filesystem\" between file opens.")
	uploadDemoCmd.Flags().BoolVar(&uploadDemoFlags.alterData, "alter-data", false, "Cause an error during shard generation by changing the data in a file on the \"filesystem\" between file opens.")
}
