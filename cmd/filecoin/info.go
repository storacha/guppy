package filecoin

import (
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-wordwrap"
	mh "github.com/multiformats/go-multihash"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
)

var infoFlags struct {
	jsonOutput bool
	space      string
}

var infoCmd = &cobra.Command{
	Use:   "info <piece-cid>",
	Short: "Get information about a piece",
	Long: wordwrap.WrapString(
		"Gets information about a piece's storage status. You must provide the Space DID associated with the content.",
		80),
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if infoFlags.space == "" {
			return fmt.Errorf("space DID is required. Use --space <did>")
		}
		spaceDID, err := did.Parse(infoFlags.space)
		if err != nil {
			return fmt.Errorf("invalid space DID: %w", err)
		}

		pieceCid, err := cid.Decode(args[0])
		if err != nil {
			return fmt.Errorf("invalid piece CID: %w", err)
		}

		decodedMh, err := mh.Decode(pieceCid.Hash())
		if err != nil {
			return fmt.Errorf("decoding multihash: %w", err)
		}

		if pieceCid.Type() != cid.Raw || decodedMh.Code != 0x1011 {
			newMh, err := mh.Encode(decodedMh.Digest, 0x1011)
			if err != nil {
				return fmt.Errorf("encoding multihash: %w", err)
			}
			pieceCid = cid.NewCidV1(cid.Raw, newMh)
		}

		c := cmdutil.MustGetClient(*StorePathP)

		result, err := c.FilecoinInfo(cmd.Context(), pieceCid, spaceDID)
		if err != nil {
			return fmt.Errorf("getting filecoin info: %w", err)
		}

		// 4. Output
		if infoFlags.jsonOutput {
			jsonBytes, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return fmt.Errorf("marshaling output: %w", err)
			}
			fmt.Println(string(jsonBytes))
		} else {
			fmt.Printf("Piece: %s\n", result.Piece.String())

			if len(result.Aggregates) > 0 {
				fmt.Println("\nAggregates:")
				for _, agg := range result.Aggregates {
					fmt.Printf("  CID: %s\n", agg.Aggregate.String())
				}
			}

			if len(result.Deals) > 0 {
				fmt.Println("\nDeals:")
				for _, deal := range result.Deals {
					fmt.Printf("  - Provider: %s\n", deal.Provider)
					fmt.Printf("    Deal ID: %d\n", deal.Aux.DataSource.DealID)
				}
			} else {
				fmt.Println("\nNo active deals found yet.")
			}
		}

		return nil
	},
}

func init() {
	infoCmd.Flags().BoolVar(&infoFlags.jsonOutput, "json", false, "Output in JSON format")
	infoCmd.Flags().StringVarP(&infoFlags.space, "space", "s", "", "Space DID (required)")
	FilecoinCmd.AddCommand(infoCmd)
}
