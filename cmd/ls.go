package cmd

import (
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/repo"
)

var lsFlags struct {
	proofsPath string
	showShards bool
}

func init() {
	lsCmd.Flags().StringVar(&lsFlags.proofsPath, "proof", "", "Path to archive (CAR) containing UCAN proofs for this operation.")
	lsCmd.Flags().BoolVar(&lsFlags.showShards, "shards", false, "Display shard CIDs under each upload root.")
}

var lsCmd = &cobra.Command{
	Use:     "ls <space-did>",
	Aliases: []string{"list"},
	Short:   "List uploads in a space",
	Long: wordwrap.WrapString(
		"Lists all uploads in the given space as CIDs, one on each line. With "+
			"`--shards` flag, lists shard CIDs below each upload root CID, indented.",
		80),
	Example: fmt.Sprintf("  %s ls did:key:z6MksCX5PdUgHv83cmDE2DfCrR1WHG9MmZPRKSvTi8Ca297V", rootCmd.Name()),
	Args:    cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		spaceDID, err := did.Parse(cmd.Flags().Arg(0))
		if err != nil {
			return fmt.Errorf("parsing space DID: %w", err)
		}

		var proofs []delegation.Delegation
		if lsFlags.proofsPath != "" {
			proof := cmdutil.MustGetProof(lsFlags.proofsPath)
			proofs = append(proofs, proof)
		}

		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		repo, err := repo.Open(cfg.Repo)

		c, err := cmdutil.NewClient(cfg.Network, repo, proofs...)
		if err != nil {
			return fmt.Errorf("creating client: %w", err)
		}

		var cursor *string
		for {
			listOk, err := c.UploadList(
				cmd.Context(),
				spaceDID,
				uploadcap.ListCaveats{Cursor: cursor})
			if err != nil {
				return err
			}

			for _, r := range listOk.Results {
				cmd.Printf("%s\n", r.Root)
				if lsFlags.showShards {
					for _, s := range r.Shards {
						cmd.Printf("\t%s\n", s)
					}
				}
			}

			if listOk.Cursor == nil {
				break
			}
			cursor = listOk.Cursor
		}

		return nil
	},
}
