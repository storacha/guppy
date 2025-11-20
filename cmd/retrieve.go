package cmd

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	"github.com/storacha/guppy/pkg/dagfs"
)

var retrieveCmd = &cobra.Command{
	Use:     "retrieve <space> <content-path> <output-path>",
	Aliases: []string{"get"},
	Short:   "Get a file or directory by its CID",
	Long: wordwrap.WrapString(
		"Retrieves a file or directory from a space. The specified file or "+
			"directory will be written to <output-path>. <content-path> can take "+
			"several forms:\n\n"+
			"* /ipfs/<cid>[/<subpath>]\n"+
			"* ipfs://<cid>[/<subpath>]\n"+
			"* <cid>[/<subpath>]",
		80),
	Args: cobra.ExactArgs(3),

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

		pathCID, subpath, err := cmdutil.ContentPath(args[1])
		if err != nil {
			return fmt.Errorf("parsing content path: %w", err)
		}
		if subpath == "" {
			subpath = "."
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
		if err != nil {
			return fmt.Errorf("delegating %s: %w", contentcap.RetrieveAbility, err)
		}

		locator := locator.NewIndexLocator(indexer, []delegation.Delegation{retrievalAuth})
		ds := dagservice.NewDAGService(locator, c, space)
		retrievedFs := dagfs.New(ctx, ds, pathCID)

		file, err := retrievedFs.Open(subpath)
		if err != nil {
			return fmt.Errorf("opening path in retrieved filesystem: %w", err)
		}
		defer file.Close()

		// If it's a directory, copy the whole directory. If it's a file, copy the
		// file.
		if _, ok := file.(fs.ReadDirFile); ok {
			pathedFs, err := fs.Sub(retrievedFs, subpath)
			if err != nil {
				return fmt.Errorf("sub filesystem: %w", err)
			}

			err = os.CopyFS(outputPath, pathedFs)
			if err != nil {
				return fmt.Errorf("copying retrieved filesystem: %w", err)
			}
		} else {
			outFile, err := os.Create(outputPath)
			if err != nil {
				return fmt.Errorf("creating output file: %w", err)
			}
			defer outFile.Close()

			_, err = io.Copy(outFile, file)
			if err != nil {
				return fmt.Errorf("writing to output file: %w", err)
			}
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(retrieveCmd)
}
