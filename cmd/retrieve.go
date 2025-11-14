package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/spf13/cobra"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	"github.com/storacha/guppy/pkg/dagfs"
)

func must[T any](ret T, err error) T {
	if err != nil {
		panic(err)
	}
	return ret
}

var retrieveCmd = &cobra.Command{
	Use:     "retrieve <space> <CID> <output-path>",
	Aliases: []string{"get"},
	Short:   "Get a file or directory by its CID",
	Long:    "Retrieves a file or directory from the local storage by its CID.",
	Args:    cobra.ExactArgs(3),

	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		repo, err := makeRepo(ctx)
		if err != nil {
			return err
		}
		defer repo.Close()

		c := cmdutil.MustGetClient(storePath)
		space, err := did.Parse(args[0])
		if err != nil {
			return fmt.Errorf("invalid space DID: %w", err)
		}
		cid, err := cid.Parse(args[1])
		if err != nil {
			return fmt.Errorf("invalid CID: %w", err)
		}
		outputPath := args[2]

		indexer, indexerPrincipal := cmdutil.MustGetIndexClient()

		pfs := make([]delegation.Proof, 0, len(c.Proofs()))
		for _, del := range c.Proofs() {
			pfs = append(pfs, delegation.FromDelegation(del))
		}

		// Allow the indexing service to retrieve indexes
		retrievalAuth, err := contentcap.Retrieve.Delegate(
			c.Issuer(),
			indexerPrincipal,
			space.DID().String(),
			contentcap.RetrieveCaveats{},
			delegation.WithProof(pfs...),
			delegation.WithExpiration(int(time.Now().Add(30*time.Second).Unix())),
		)

		locator := locator.NewIndexLocator(indexer, []delegation.Delegation{retrievalAuth})
		ds := dagservice.NewDAGService(locator, c, space)
		retrievedFs := dagfs.New(ctx, ds, cid)

		err = os.CopyFS(outputPath, retrievedFs)
		if err != nil {
			return fmt.Errorf("copying retrieved filesystem: %w", err)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(retrieveCmd)
}
