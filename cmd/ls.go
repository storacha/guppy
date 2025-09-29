package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/guppy/internal/cmdutil"
)

var lsCmd = &cobra.Command{
	Use:     "ls <space-did>",
	Aliases: []string{"list"},
	Short:   "List uploads in a space",
	Long: `Lists all uploads in the given space as CIDs, one on each line. With
--shards flag, lists shard CIDs below each upload root CID, indented.`,
	Example: fmt.Sprintf("  %s ls did:key:z6MksCX5PdUgHv83cmDE2DfCrR1WHG9MmZPRKSvTi8Ca297V", rootCmd.Name()),
	Args:    cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		spaceDID := cmd.Flags().Arg(0)

		proofsPath, err := cmd.Flags().GetString("proof")
		if err != nil {
			return err
		}

		showShards, err := cmd.Flags().GetBool("shards")
		if err != nil {
			return err
		}

		space := cmdutil.MustParseDID(spaceDID)

		proofs := []delegation.Delegation{}
		if proofsPath != "" {
			proof := cmdutil.MustGetProof(proofsPath)
			proofs = append(proofs, proof)
		}

		c := cmdutil.MustGetClient(proofs...)

		listOk, err := c.UploadList(
			cmd.Context(),
			space,
			uploadcap.ListCaveats{})
		if err != nil {
			return err
		}

		for _, r := range listOk.Results {
			fmt.Printf("%s\n", r.Root)
			if showShards {
				for _, s := range r.Shards {
					fmt.Printf("\t%s\n", s)
				}
			}
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(lsCmd)

	lsCmd.Flags().String("space", "", "DID of the space to list")
	lsCmd.Flags().String("proof", "", "Path to archive (CAR) containing UCAN proofs for this operation.")
	lsCmd.Flags().Bool("shards", false, "Display shard CIDs under each upload root.")
}
