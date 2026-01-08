package gateway

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/gateway/assets"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/labstack/echo/v4"
	"github.com/spf13/cobra"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
)

var serveCmd = &cobra.Command{
	Use:   "serve <space-did>",
	Short: "Start a Storacha Network gateway",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		guppyDirPath, _ := cmd.Flags().GetString("guppy-dir")
		storePath := filepath.Join(guppyDirPath, "store.json")

		client := cmdutil.MustGetClient(storePath)
		space, err := did.Parse(args[0])
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("invalid space DID: %w", err)
		}

		indexer, indexerPrincipal := cmdutil.MustGetIndexClient()

		pfs := make([]delegation.Proof, 0, len(client.Proofs()))
		for _, del := range client.Proofs() {
			pfs = append(pfs, delegation.FromDelegation(del))
		}

		locator := locator.NewIndexLocator(indexer, func(space did.DID) (delegation.Delegation, error) {
			// Allow the indexing service to retrieve indexes
			return contentcap.Retrieve.Delegate(
				client.Issuer(),
				indexerPrincipal,
				space.DID().String(),
				contentcap.RetrieveCaveats{},
				delegation.WithProof(pfs...),
				delegation.WithExpiration(int(time.Now().Add(30*time.Second).Unix())),
			)
		})
		exchange := dagservice.NewExchange(locator, client, space)

		gwConf := gateway.Config{
			NoDNSLink: true,
			Menu: []assets.MenuItem{
				{
					Title: "Storacha Network",
					URL:   "https://storacha.network",
				},
			},
		}

		// TODO: what is this for? is it a cache, does it need to expire?
		blockStore := blockstore.NewBlockstore(sync.MutexWrap(datastore.NewMapDatastore()))
		blockService := blockservice.New(blockStore, exchange)

		backend, err := gateway.NewBlocksBackend(blockService)
		cobra.CheckErr(err)

		ipfsHandler := gateway.NewHandler(gwConf, backend)

		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		go func() {
			<-timer.C
			cmd.Printf("Gateway is running at http://localhost:%s", "3000")
		}()

		e := echo.New()
		e.GET("/ipfs/*", echo.WrapHandler(ipfsHandler))
		e.Logger.Fatal(e.Start(":3000"))
		return nil
	},
}

func init() {
	GatewayCmd.AddCommand(serveCmd)
}
