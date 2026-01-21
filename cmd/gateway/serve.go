package gateway

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/gateway/assets"
	logging "github.com/ipfs/go-log/v2"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	arc "github.com/storacha/go-ds-arc"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/build"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	"github.com/storacha/guppy/pkg/config"
)

const (
	// port is the default port to run the gateway on.
	port = 3000
	// blockCacheCapacity defines the default number of blocks to cache in memory.
	// Blocks are typically <1MB due to IPFS chunking, so an upper bound for how
	// much memory the cache will utilize is approximately this number multiplied
	// by 1MB. e.g. capacity for 1,000 blocks ~= 1GB of memory.
	blockCacheCapacity = 1000
	// subdomainEnabled indicates whether to enable subdomain gateway mode, which
	// is disabled by default.
	subdomainEnabled = false
	// trustedEnabled indicates whether to enable trusted gateway mode, which
	// allows deserialized responses. It is enabled by default.
	trustedEnabled = true
)

//go:embed static/index.html
var indexHTML []byte

var log = logging.Logger("cmd/gateway")

var serveCmd = &cobra.Command{
	Use:   "serve [space-did...]",
	Short: "Start a Storacha Network gateway",
	Long: wordwrap.WrapString(
		"Start an IPFS Gateway that operates on the Storacha Network. By default "+
			"it serves data from all authorized spaces. One or more space DIDs can "+
			"be specified to restrict content served to those spaces only.",
		80),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}
		cobra.CheckErr(logging.SetLogLevel("cmd/gateway", cfg.Gateway.LogLevel))

		indexHTML = []byte(strings.Replace(string(indexHTML), "{{.Version}}", build.Version, -1))
		guppyDirPath, _ := cmd.Flags().GetString("guppy-dir")
		storePath := filepath.Join(guppyDirPath, "store.json")

		c := cmdutil.MustGetClient(storePath)
		allProofs := c.Proofs(client.CapabilityQuery{Can: contentcap.RetrieveAbility})
		authdSpaces := map[did.DID]struct{}{}
		for _, proof := range allProofs {
			for _, cap := range proof.Capabilities() {
				if validator.ResolveAbility(cap.Can(), contentcap.RetrieveAbility) != "" {
					spaceDID, err := did.Parse(cap.With())
					if err == nil {
						authdSpaces[spaceDID] = struct{}{}
					}
				}
			}
		}
		log.Debugw("found authorizations in proofs", "spaces", slices.Collect(maps.Keys(authdSpaces)))

		spaces := make([]did.DID, 0, len(args))
		for _, arg := range args {
			space, err := did.Parse(arg)
			if err != nil {
				cmd.SilenceUsage = false
				return fmt.Errorf("invalid space DID: %w", err)
			}
			if _, ok := authdSpaces[space]; !ok {
				return fmt.Errorf("missing %q proof for space: %s", contentcap.RetrieveAbility, space)
			}
			spaces = append(spaces, space)
		}
		if len(spaces) == 0 {
			spaces = slices.Collect(maps.Keys(authdSpaces))
		}

		indexer, indexerPrincipal := cmdutil.MustGetIndexClient()
		locator := locator.NewIndexLocator(indexer, func(spaces []did.DID) (delegation.Delegation, error) {
			queries := make([]client.CapabilityQuery, 0, len(spaces))
			for _, space := range spaces {
				queries = append(queries, client.CapabilityQuery{
					Can:  contentcap.RetrieveAbility,
					With: space.String(),
				})
			}

			var pfs []delegation.Proof
			for _, del := range c.Proofs(queries...) {
				pfs = append(pfs, delegation.FromDelegation(del))
			}

			caps := make([]ucan.Capability[ucan.NoCaveats], 0, len(spaces))
			for _, space := range spaces {
				caps = append(caps, ucan.NewCapability(contentcap.RetrieveAbility, space.String(), ucan.NoCaveats{}))
			}

			opts := []delegation.Option{
				delegation.WithProof(pfs...),
				delegation.WithExpiration(int(time.Now().Add(30 * time.Second).Unix())),
			}

			return delegation.Delegate(c.Issuer(), indexerPrincipal, caps, opts...)
		})
		exchange := dagservice.NewExchange(locator, c, spaces)

		pubGws := map[string]*gateway.PublicGateway{}
		if cfg.Gateway.Subdomain.Enabled {
			log.Infow("subdomain gateway enabled", "hosts", cfg.Gateway.Subdomain.Hosts)
			for _, host := range cfg.Gateway.Subdomain.Hosts {
				pubGws[host] = &gateway.PublicGateway{
					Paths:                 []string{"/ipfs"},
					UseSubdomains:         true,
					NoDNSLink:             true,
					DeserializedResponses: cfg.Gateway.Trusted,
				}
			}
		}

		if cfg.Gateway.Trusted {
			log.Info("trusted gateway enabled")
		}

		gwConf := gateway.Config{
			DeserializedResponses: cfg.Gateway.Trusted,
			Menu: []assets.MenuItem{
				{
					Title: "Storacha Network",
					URL:   "https://storacha.network",
				},
			},
			NoDNSLink:      true,
			PublicGateways: pubGws,
		}

		blockStore := blockstore.NewBlockstore(arc.New(cfg.Gateway.BlockCacheCapacity))
		blockService := blockservice.New(blockStore, exchange)
		backend, err := gateway.NewBlocksBackend(blockService)
		cobra.CheckErr(err)

		ipfsHandler := gateway.NewHandler(gwConf, backend)
		ipfsHandler = gateway.NewHostnameHandler(gwConf, backend, ipfsHandler)
		ipfsHandler = gateway.NewHeaders(nil).ApplyCors().Wrap(ipfsHandler)

		e := echo.New()
		e.HideBanner = true
		e.HidePort = true

		e.Use(requestLogger(log))
		e.Use(middleware.Recover())

		if cfg.Gateway.Subdomain.Enabled {
			echoHandler := echo.WrapHandler(ipfsHandler)
			e.GET("/*", func(c echo.Context) error {
				r := c.Request()
				host := r.Host
				if xHost := r.Header.Get("X-Forwarded-Host"); xHost != "" {
					host = xHost // support X-Forwarded-Host if added by a reverse proxy
				}
				if r.URL.Path == "/" && !strings.Contains(host, ".ipfs.") {
					return rootHandler(c)
				}
				return echoHandler(c)
			})
		} else {
			e.GET("/", rootHandler)
			e.GET("/ipfs/*", echo.WrapHandler(ipfsHandler))
		}

		// print banner after short delay to ensure it only appears if no errors
		// occurred during startup
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		go func() {
			<-timer.C
			var hosts []string
			if cfg.Gateway.Subdomain.Enabled {
				hosts = cfg.Gateway.Subdomain.Hosts
			}
			cmd.Println(banner(build.Version, cfg.Gateway.Port, c.DID(), spaces, hosts))
		}()

		// shut down the server gracefully on context cancellation
		go func() {
			<-cmd.Context().Done()
			cmd.Println("\nShutting down server...")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			if err := e.Shutdown(ctx); err != nil {
				cmd.PrintErrf("shutting down server: %s", err.Error())
			}
		}()

		addr := fmt.Sprintf(":%d", cfg.Gateway.Port)
		if err := e.Start(addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("closing server: %w", err)
		}
		return nil
	},
}

func init() {
	serveCmd.Flags().IntP("block-cache-capacity", "c", blockCacheCapacity, "Number of blocks to cache in memory")
	cobra.CheckErr(viper.BindPFlag("gateway.block_cache_capacity", serveCmd.Flags().Lookup("block-cache-capacity")))

	serveCmd.Flags().IntP("port", "p", port, "Port to run the HTTP server on")
	cobra.CheckErr(viper.BindPFlag("gateway.port", serveCmd.Flags().Lookup("port")))

	serveCmd.Flags().BoolP("subdomain", "s", subdomainEnabled, "Enabled subdomain gateway mode (e.g. <cid>.ipfs.<gateway-host>)")
	cobra.CheckErr(viper.BindPFlag("gateway.subdomain.enabled", serveCmd.Flags().Lookup("subdomain")))

	serveCmd.Flags().StringSlice("host", []string{}, "Gateway host(s) for subdomain mode (required if subdomain mode is enabled)")
	cobra.CheckErr(viper.BindPFlag("gateway.subdomain.hosts", serveCmd.Flags().Lookup("host")))

	serveCmd.Flags().BoolP("trusted", "t", trustedEnabled, "Enable trusted gateway mode (allows deserialized responses)")
	cobra.CheckErr(viper.BindPFlag("gateway.trusted", serveCmd.Flags().Lookup("trusted")))

	serveCmd.Flags().String("log-level", "warn", "Logging level for the gateway server (debug, info, warn, error)")
	cobra.CheckErr(viper.BindPFlag("gateway.log_level", serveCmd.Flags().Lookup("log-level")))

	GatewayCmd.AddCommand(serveCmd)
}

func rootHandler(c echo.Context) error {
	return c.Blob(http.StatusOK, "text/html; charset=utf-8", indexHTML)
}
