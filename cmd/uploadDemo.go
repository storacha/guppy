//go:build wip

package cmd

import (
	"errors"

	"github.com/spf13/cobra"
	"github.com/storacha/guppy/cmd/internal/upload/demo"
	"github.com/storacha/guppy/cmd/internal/upload/repo"
)

var uploadDemoFlags struct {
	alterMetadata bool
	alterData     bool
}

var uploadDemoCmd = &cobra.Command{
	Use:     "upload-demo <space-name>",
	Aliases: []string{"up-demo"},
	Short:   "WIP - Demo uploading data to the service using a fake filesystem",
	Long: `Runs a demo upload using a fake filesystem. A "space" is selected based on a
given string, which is simply hashed into a private key. That is, the name
itself is meaningless, but using the same name will result in the same space
being used, which allows testing resuming failed or interrupted uploads.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		repo, _, err := repo.Make(ctx, "guppy-demo.db")
		if err != nil {
			return err
		}
		// Currently leads to a race condition with the app still running delayed DB
		// queries. We can deal with this issue later, since the process ends at the
		// end of this function anyhow.
		// defer closeDb()

		spaceName := cmd.Flags().Arg(0)
		if spaceName == "" {
			return errors.New("Space name cannot be empty")
		}

		return demo.Demo(ctx, repo, spaceName, uploadDemoFlags.alterMetadata, uploadDemoFlags.alterData)
	},
}

func init() {
	rootCmd.AddCommand(uploadDemoCmd)

	uploadDemoCmd.Flags().BoolVar(&uploadDemoFlags.alterMetadata, "alter-metadata", false, "Cause an error during DAG generation by changing the modification time in a file on the \"filesystem\" between file opens.")
	uploadDemoCmd.Flags().BoolVar(&uploadDemoFlags.alterData, "alter-data", false, "Cause an error during shard generation by changing the data in a file on the \"filesystem\" between file opens.")
}
