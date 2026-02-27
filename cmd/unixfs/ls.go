package unixfs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/spf13/cobra"
	"github.com/storacha/go-libstoracha/capabilities/consumer"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/dagfs"
)

var lsFlags struct {
	long bool
}

var lsCmd = &cobra.Command{
	Use:   "ls <space-did> <cid-path>",
	Short: "List directory contents",
	Long:  "Lists files and directories in a UnixFS tree. Supports shallow listing and incremental output.",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		spaceDidStr := args[0]
		cidPathStr := args[1]

		spaceDID, err := did.Parse(spaceDidStr)
		if err != nil {
			return fmt.Errorf("invalid space DID: %w", err)
		}

		parts := strings.SplitN(cidPathStr, "/", 2)
		rootCidStr := parts[0]
		subPath := ""
		if len(parts) > 1 {
			subPath = parts[1]
		}

		rootCid, err := cid.Decode(rootCidStr)
		if err != nil {
			return fmt.Errorf("invalid root CID: %w", err)
		}

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}

		c := cmdutil.MustGetClient(cfg.Repo.Dir, cfg.Network)
		indexer, indexerPrincipal := cmdutil.MustGetIndexClient(cfg.Network)

		proofs, err := c.Proofs()
		if err != nil {
			return err
		}

		pfs := make([]delegation.Proof, 0, len(proofs))
		for _, del := range proofs {
			pfs = append(pfs, delegation.FromDelegation(del))
		}

		retrievalAuth, err := consumer.Get.Delegate(
			c.Issuer(),
			indexerPrincipal,
			indexerPrincipal.DID().String(),
			consumer.GetCaveats{
				Consumer: spaceDID.String(),
			},
			delegation.WithProof(pfs...),
			delegation.WithExpiration(int(time.Now().Add(30*time.Second).Unix())),
		)
		if err != nil {
			return fmt.Errorf("delegating capability: %w", err)
		}

		loc := locator.NewIndexLocator(indexer, locator.AuthorizeRetrievalFunc(func(spaces []did.DID) (delegation.Delegation, error) {
			return retrievalAuth, nil
		}))

		dagSvc := dagservice.NewDAGService(loc, c, []did.DID{spaceDID})
		dfs := dagfs.New(ctx, dagSvc, rootCid)

		targetPath := subPath
		if targetPath == "" {
			targetPath = "."
		}

		f, err := dfs.Open(targetPath)
		if err != nil {
			return fmt.Errorf("opening path %s: %w", targetPath, err)
		}
		defer f.Close()

		stat, err := f.Stat()
		if err != nil {
			return err
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

		printEntry := func(info fs.FileInfo) {
			if lsFlags.long {
				fmt.Fprintf(w, "%s\t%d\t%s\t%s\n",
					info.Mode(),
					info.Size(),
					info.ModTime().Format("Jan 02 15:04"),
					info.Name(),
				)
			} else {
				fmt.Println(info.Name())
			}
		}

		if !stat.IsDir() {
			printEntry(stat)
			w.Flush()
			return nil
		}

		readDirFile, ok := f.(fs.ReadDirFile)
		if !ok {
			return fmt.Errorf("directory does not support reading entries")
		}

		for {
			entries, err := readDirFile.ReadDir(20)
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("reading directory: %w", err)
			}

			for _, entry := range entries {
				info, err := entry.Info()
				if err != nil {
					continue
				}
				printEntry(info)
			}
			w.Flush()
		}

		return nil
	},
}

func init() {
	lsCmd.Flags().BoolVarP(&lsFlags.long, "long", "l", false, "Use long listing format")
	Cmd.AddCommand(lsCmd)
}
