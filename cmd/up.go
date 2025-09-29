package cmd

import (
	"github.com/spf13/cobra"
	"github.com/storacha/guppy/cmd/internal/upload"
)

// upCmd represents the up command
var upCmd = &cobra.Command{
	Use:   "up",
	Short: "Store files to the service and register an upload",
	Long:  `Stores files to the service and registers an upload.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		spaceDID, err := cmd.Flags().GetString("space")
		if err != nil {
			return err
		}
		proofPath, err := cmd.Flags().GetString("proof")
		if err != nil {
			return err
		}
		carPath, err := cmd.Flags().GetString("car")
		if err != nil {
			return err
		}

		filePaths := cmd.Flags().Args()

		hidden, err := cmd.Flags().GetBool("hidden")
		if err != nil {
			return err
		}
		json, err := cmd.Flags().GetBool("json")
		if err != nil {
			return err
		}
		verbose, err := cmd.Flags().GetBool("verbose")
		if err != nil {
			return err
		}
		wrap, err := cmd.Flags().GetBool("wrap")
		if err != nil {
			return err
		}
		shardSize, err := cmd.Flags().GetInt("shard-size")
		if err != nil {
			return err
		}

		return upload.Upload(cmd.Context(), spaceDID, proofPath, carPath, filePaths, hidden, json, verbose, wrap, shardSize)
	},
}

func init() {
	rootCmd.AddCommand(upCmd)

	upCmd.Flags().String("space", "", "DID of the space to upload to")
	upCmd.Flags().String("proof", "", "Path to archive (CAR) containing UCAN proofs for the operation")
	upCmd.Flags().StringP("car", "c", "", "Path to CAR file to upload")

	upCmd.Flags().BoolP("hidden", "H", false, "Include paths that start with \".\"")
	upCmd.Flags().BoolP("json", "j", false, "Format as newline delimited JSON")
	upCmd.Flags().BoolP("verbose", "v", false, "Output more details")
	upCmd.Flags().Bool("wrap", true, "Wrap single input file in a directory (Has no effect on directory or CAR uploads. Use --wrap false to disable)")
	upCmd.Flags().Int("shard-size", 0, "Maximum size of each shard CAR in the upload (in bytes)")
}
