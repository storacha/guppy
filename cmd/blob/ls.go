package blob

import (
	"bytes"

	"github.com/dustin/go-humanize"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/core/delegation"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/config"
)

const pageSize = 1000

var lsFlags struct {
	proofsPath string
	long       bool
	human      bool
	json       bool
}

func init() {
	lsCmd.Flags().StringVar(&lsFlags.proofsPath, "proof", "", "Path to archive (CAR) containing UCAN proofs for this operation.")
	lsCmd.Flags().BoolVarP(&lsFlags.long, "long", "l", false, "Display detailed information about blobs.")
	lsCmd.Flags().BoolVarP(&lsFlags.human, "human", "H", false, "Display blob sizes in human-readable format (only applicable when used with --long).")
	lsCmd.Flags().BoolVar(&lsFlags.json, "json", false, "Output as newline delimited JSON.")
}

var lsCmd = &cobra.Command{
	Use:     "ls <space>",
	Aliases: []string{"list"},
	Short:   "List blobs in a space",
	Long: wordwrap.WrapString(
		"Lists all blobs in the given space as multibase base58btc encoded strings,"+
			" one on each line. The space can be specified by DID or by name.",
		80),
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		proofs := []delegation.Delegation{}
		if lsFlags.proofsPath != "" {
			proof := cmdutil.MustGetProof(lsFlags.proofsPath)
			proofs = append(proofs, proof)
		}

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir, client.WithAdditionalProofs(proofs...))

		spaceDID, err := cmdutil.ResolveSpace(c, args[0])
		if err != nil {
			return err
		}

		var cursor *string
		size := uint64(pageSize)
		for {
			listOk, err := c.SpaceBlobList(
				cmd.Context(),
				spaceDID,
				spaceblobcap.ListCaveats{Cursor: cursor, Size: &size})
			if err != nil {
				return err
			}

			for _, r := range listOk.Results {
				if lsFlags.json {
					n, err := r.ToIPLD()
					cobra.CheckErr(err)
					var buf bytes.Buffer
					err = dagjson.Encode(n, &buf)
					cobra.CheckErr(err)
					cmd.Println(buf.String())
				} else if lsFlags.long {
					if lsFlags.human {
						cmd.Printf("%s\t%s\n", digestutil.Format(r.Blob.Digest), humanize.IBytes(r.Blob.Size))
					} else {
						cmd.Printf("%s\t%d\n", digestutil.Format(r.Blob.Digest), r.Blob.Size)
					}
				} else {
					cmd.Println(digestutil.Format(r.Blob.Digest))
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
