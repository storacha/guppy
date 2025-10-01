//go:build wip

package cmd

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/cmd/internal/upload"
	"github.com/storacha/guppy/cmd/internal/upload/repo"
)

var uploadCmd = &cobra.Command{
	Use:     "upload <space>",
	Aliases: []string{"up"},
	Short:   "WIP - Upload data to the service",
	Args:    cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		space := cmd.Flags().Arg(0)
		if space == "" {
			return errors.New("Space cannot be empty")
		}
		spaceDID, err := did.Parse(space)
		if err != nil {
			return fmt.Errorf("parsing space DID: %w", err)
		}

		return upload.Action(cmd.Context(), spaceDID)
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)
}

var uploadAddSourceFlags struct {
	space  string
	resume bool
}

var uploadAddSourceCmd = &cobra.Command{
	Use:     "add-space <space> <path>",
	Aliases: []string{"up"},
	Short:   "WIP - Upload data to the service",
	Args:    cobra.ExactArgs(2),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		repo, closeDb, err := repo.Make(ctx, "guppy.db")
		if err != nil {
			return err
		}
		defer closeDb()

		space := cmd.Flags().Arg(0)
		if space == "" {
			return errors.New("Space cannot be empty")
		}

		path := cmd.Flags().Arg(1)
		if path == "" {
			return errors.New("Path cannot be empty")
		}

		spaceDID, err := did.Parse(space)
		if err != nil {
			return fmt.Errorf("parsing space DID: %w", err)
		}

		return upload.AddSource(ctx, repo, spaceDID, path)
	},
}

func init() {
	uploadCmd.AddCommand(uploadAddSourceCmd)
}
