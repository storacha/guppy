package cmd

import (
	"github.com/spf13/cobra"
	"github.com/storacha/guppy/cmd/internal/upload"
)

// upCmd represents the up command
var upFlags struct {
	spaceDID  string
	proofPath string
	carPath   string
	hidden    bool
	json      bool
	verbose   bool
	wrap      bool
	shardSize int
}

var upCmd = &cobra.Command{
	Use:   "up",
	Short: "Store files to the service and register an upload",
	Long:  `Stores files to the service and registers an upload.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		filePaths := cmd.Flags().Args()
		return upload.Upload(cmd.Context(), upFlags.spaceDID, upFlags.proofPath, upFlags.carPath, filePaths, upFlags.hidden, upFlags.json, upFlags.verbose, upFlags.wrap, upFlags.shardSize)
	},
}

func init() {
	rootCmd.AddCommand(upCmd)

	upCmd.Flags().StringVar(&upFlags.spaceDID, "space", "", "DID of the space to upload to")
	upCmd.Flags().StringVar(&upFlags.proofPath, "proof", "", "Path to archive (CAR) containing UCAN proofs for the operation")
	upCmd.Flags().StringVarP(&upFlags.carPath, "car", "c", "", "Path to CAR file to upload")

	upCmd.Flags().BoolVarP(&upFlags.hidden, "hidden", "H", false, "Include paths that start with \".\"")
	upCmd.Flags().BoolVarP(&upFlags.json, "json", "j", false, "Format as newline delimited JSON")
	upCmd.Flags().BoolVarP(&upFlags.verbose, "verbose", "v", false, "Output more details")
	upCmd.Flags().BoolVar(&upFlags.wrap, "wrap", true, "Wrap single input file in a directory (Has no effect on directory or CAR uploads. Use --wrap false to disable)")
	upCmd.Flags().IntVar(&upFlags.shardSize, "shard-size", 0, "Maximum size of each shard CAR in the upload (in bytes)")
}
