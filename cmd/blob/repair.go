package blob

import (
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/multiformats/go-multihash"
	"github.com/spf13/cobra"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	fdm "github.com/storacha/go-ucanto/core/result/failure/datamodel"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/validator"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/receipt"
	indexer_types "github.com/storacha/indexing-service/pkg/types"
)

const minReplicas = 3

var repairFlags struct {
	ignore []string
}

func init() {
	repairCmd.Flags().StringSliceVarP(&repairFlags.ignore, "ignore", "i", []string{}, "DIDs of storage providers to ignore in results.")
}

var repairCmd = &cobra.Command{
	Use:   "repair <space>",
	Short: "Repair blobs in a space with insufficient replicas",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir)

		indexer, _ := cmdutil.MustGetIndexClient()

		space, err := cmdutil.ResolveSpace(c, args[0])
		if err != nil {
			return err
		}

		ignore := map[did.DID]struct{}{}
		for _, didStr := range repairFlags.ignore {
			did, err := did.Parse(didStr)
			cobra.CheckErr(err)
			ignore[did] = struct{}{}
		}

		var i int
		var cursor *string
		size := uint64(pageSize)
		for {
			listOk, err := c.SpaceBlobList(
				cmd.Context(),
				space,
				spaceblobcap.ListCaveats{Cursor: cursor, Size: &size})
			if err != nil {
				return err
			}

			for _, r := range listOk.Results {
				i++
				queryResult, err := indexer.QueryClaims(cmd.Context(), indexer_types.Query{
					Type:   indexer_types.QueryTypeLocation,
					Hashes: []multihash.Multihash{r.Blob.Digest},
				})
				cobra.CheckErr(err)

				bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(queryResult.Blocks()))
				cobra.CheckErr(err)

				var commitments []delegation.Delegation
				for _, claimID := range queryResult.Claims() {
					claim, err := delegation.NewDelegationView(claimID, bs)
					cobra.CheckErr(err)
					_, err = assert.Location.Match(validator.NewSource(claim.Capabilities()[0], claim))
					if err != nil {
						continue
					}
					commitments = append(commitments, claim)
				}

				var filteredCommitments []delegation.Delegation
				for _, commL := range commitments {
					if _, ok := ignore[commL.Issuer().DID()]; !ok {
						filteredCommitments = append(filteredCommitments, commL)
					}
				}

				needRepair := len(commitments) < minReplicas || len(filteredCommitments) < minReplicas
				replicas := minReplicas

				if len(filteredCommitments) < minReplicas {
					replicas = minReplicas + (len(commitments) - len(filteredCommitments))
				}

				if !needRepair {
					cmd.Println(fmt.Sprintf("%d: %s does not need repair", i, digestutil.Format(r.Blob.Digest)))
					continue
				}

				cmd.Println(fmt.Sprintf("%d: %s", i, digestutil.Format(r.Blob.Digest)))
				res, _, err := c.SpaceBlobReplicate(
					cmd.Context(),
					space,
					r.Blob,
					uint(replicas),
					commitments[rand.IntN(len(commitments))],
				)
				if err != nil {
					cmd.PrintErrln(fmt.Sprintf(" - error invoking replication: %s", err))
					continue
				}
				for _, site := range res.Site {
					cmd.PrintErrln(fmt.Sprintf(" - transfer task: %s", site.UcanAwait.Link))

					r, err := c.Receipts().Fetch(cmd.Context(), site.UcanAwait.Link)
					if err != nil {
						if errors.Is(err, receipt.ErrNotFound) {
							continue
						}
						cobra.CheckErr(err)
					}

					_, x := result.Unwrap(r.Out())
					if x != nil {
						failure := fdm.Bind(x)
						cmd.PrintErrln(fmt.Sprintf("   - transfer failed: %s", failure.Message))
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
