package cmd

import (
	"fmt"
	"io/fs"
	"os"
	"text/tabwriter"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/capabilities/consumer"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	"github.com/storacha/guppy/pkg/dagfs"
)

var exploreFlags struct {
	space string
}

var exploreCmd = &cobra.Command{
	Use:   "explore <root-cid>",
	Short: "Explore a file tree from a CID",
	Long:  "Traverses the UnixFS DAG rooted at the given CID and lists all files and directories.",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		rootCid, err := cid.Decode(args[0])
		if err != nil {
			return fmt.Errorf("invalid CID: %w", err)
		}

		c := cmdutil.MustGetClient(storePath)

		var spaceDID did.DID
		if exploreFlags.space != "" {
			spaceDID, err = did.Parse(exploreFlags.space)
			if err != nil {
				return fmt.Errorf("invalid space DID: %w", err)
			}
		} else {
			return fmt.Errorf("space DID is required. Use --space <did>")
		}

		indexer, indexerPrincipal := cmdutil.MustGetIndexClient()

		pfs := make([]delegation.Proof, 0, len(c.Proofs()))
		for _, del := range c.Proofs() {
			pfs = append(pfs, delegation.FromDelegation(del))
		}

		retrievalAuth, err := consumer.Get.Delegate(
			c.Issuer(),
			indexerPrincipal,
			spaceDID.String(),
			consumer.GetCaveats{
				Consumer: c.Issuer().DID().String(),
			},
			delegation.WithProof(pfs...),
			delegation.WithExpiration(int(time.Now().Add(30*time.Second).Unix())),
		)
		if err != nil {
			return fmt.Errorf("delegating %s: %w", consumer.GetAbility, err)
		}

		loc := locator.NewIndexLocator(indexer, []delegation.Delegation{retrievalAuth})

		dagSvc := dagservice.NewDAGService(
			loc,
			c,
			spaceDID,
		)

		dfs := dagfs.New(ctx, dagSvc, rootCid)

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "MODE\tSIZE\tNAME")

		err = fs.WalkDir(dfs, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", path, err)
				return nil
			}

			info, err := d.Info()
			if err != nil {
				return err
			}

			fmt.Fprintf(w, "%s\t%d\t%s\n", d.Type(), info.Size(), path)
			return nil
		})

		w.Flush()
		return err
	},
}

func init() {
	exploreCmd.Flags().StringVarP(&exploreFlags.space, "space", "s", "", "Space DID (required)")
	rootCmd.AddCommand(exploreCmd)
}
