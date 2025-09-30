//go:build wip

package cmd

import (
	"errors"

	"github.com/spf13/cobra"
	"github.com/storacha/guppy/cmd/internal/upload"
)

var uploadFlags struct {
	space  string
	resume bool
}

var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "WIP - Upload data to the service",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		root := cmd.Flags().Arg(0)
		if root == "" {
			return errors.New("Path cannot be empty")
		}

		return upload.Action(cmd.Context(), uploadFlags.space, uploadFlags.resume, root)
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)

	uploadCmd.Flags().StringVar(&uploadFlags.space, "space", "", "DID of the space to upload to")
	uploadCmd.MarkFlagRequired("space")
	uploadCmd.Flags().BoolVar(&uploadFlags.resume, "resume", false, "Resume a previously interrupted upload")
}
