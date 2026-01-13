package cmd

import (
	"fmt"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
)

var rmFlags struct {
	proofsPath string
}

var rmCmd = &cobra.Command{
	Use:   "rm <space> <root>",
	Short: "Remove an upload from the uploads listing",
	Long:  "Remove an upload from the uploads listing. This does not remove the underlying data from IPFS or space storage.",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		spaceDID, err := did.Parse(args[0])
		if err != nil {
			return fmt.Errorf("parsing space DID: %w", err)
		}

		rootCID, err := cid.Parse(args[1])
		if err != nil {
			return fmt.Errorf("parsing root CID: %w", err)
		}

		proofs := []delegation.Delegation{}
		if rmFlags.proofsPath != "" {
			proof := cmdutil.MustGetProof(rmFlags.proofsPath)
			proofs = append(proofs, proof)
		}

		c := cmdutil.MustGetClient(storePath, proofs...)

		_, err = c.UploadRemove(ctx, spaceDID, cidlink.Link{Cid: rootCID})
		if err != nil {
			return err
		}

		fmt.Printf("Removed upload %s from space %s\n", rootCID, spaceDID)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(rmCmd)

	rmCmd.Flags().StringVar(&rmFlags.proofsPath, "proof", "", "Path to archive (CAR) containing UCAN proofs for this operation.")
}
