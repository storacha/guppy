package cmd

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
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
	Long:    "Retrieves a file or directory from the space by its CID.",
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

		rootInfo, err := fs.Stat(retrievedFs, ".")
		if err != nil {
			return fmt.Errorf("stat retrieved root: %w", err)
		}

		if rootInfo.IsDir() {
			if err := os.MkdirAll(outputPath, 0o755); err != nil {
				return fmt.Errorf("creating output path: %w", err)
			}

			if err := os.CopyFS(outputPath, retrievedFs); err != nil {
				return fmt.Errorf("copying retrieved filesystem: %w", err)
			}
			return nil
		}

		// Root CID is a file; write it either to the provided path or inside it if it's a directory.
		destPath := outputPath
		if info, err := os.Stat(outputPath); err == nil && info.IsDir() {
			destPath = filepath.Join(outputPath, cid.String())
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat output path: %w", err)
		}

		if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
			return fmt.Errorf("creating output directory: %w", err)
		}

		out, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return fmt.Errorf("opening output file: %w", err)
		}
		defer out.Close()

		in, err := retrievedFs.Open(".")
		if err != nil {
			return fmt.Errorf("opening retrieved file: %w", err)
		}
		defer in.Close()

		if _, err := io.Copy(out, in); err != nil {
			return fmt.Errorf("writing retrieved file: %w", err)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(retrieveCmd)
}
