package gateway

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/gateway/assets"
	logging "github.com/ipfs/go-log/v2"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/labstack/gommon/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	arc "github.com/storacha/go-ds-arc"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/build"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	"github.com/storacha/guppy/pkg/config"
	"go.uber.org/zap"
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
	Use:   "serve <space-did>",
	Short: "Start a Storacha Network gateway",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

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
			cmd.Println(banner(build.Version, cfg.Gateway.Port, client.DID(), space))
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
	logging.SetLogLevel("cmd/gateway", "info")

	serveCmd.Flags().IntP("block-cache-capacity", "c", blockCacheCapacity, "Number of blocks to cache in memory")
	cobra.CheckErr(viper.BindPFlag("gateway.block-cache-capacity", serveCmd.Flags().Lookup("block-cache-capacity")))

	serveCmd.Flags().IntP("port", "p", port, "Port to run the HTTP server on")
	cobra.CheckErr(viper.BindPFlag("gateway.port", serveCmd.Flags().Lookup("port")))

	serveCmd.Flags().BoolP("subdomain", "s", subdomainEnabled, "Enabled subdomain gateway mode (e.g. <cid>.ipfs.<gateway-host>)")
	cobra.CheckErr(viper.BindPFlag("gateway.subdomain.enabled", serveCmd.Flags().Lookup("subdomain")))

	serveCmd.Flags().StringSlice("host", []string{}, "Gateway host(s) for subdomain mode (required if subdomain mode is enabled)")
	cobra.CheckErr(viper.BindPFlag("gateway.subdomain.hosts", serveCmd.Flags().Lookup("host")))

	serveCmd.Flags().BoolP("trusted", "t", trustedEnabled, "Enable trusted gateway mode (allows deserialized responses)")
	cobra.CheckErr(viper.BindPFlag("gateway.trusted", serveCmd.Flags().Lookup("trusted")))

	GatewayCmd.AddCommand(serveCmd)

	indexHTML = []byte(strings.Replace(string(indexHTML), "{{.Version}}", build.Version, -1))
}

func banner(version string, port int, id did.DID, space did.DID) string {
	return fmt.Sprintf(
		`
%s ▄▖           
  ▌ ▌▌▛▌▌▌▌▀▌▌▌
  ▙▌▙▌▙▌▚▚▘█▌▙▌
      ▌      ▄▌ %s

High performance IPFS Gateway
%s
------------------------------
Server %s
Space  %s
------------------------------
⇨ HTTP server started on %s`,
		color.Cyan("⬢"),
		color.Red(version),
		color.Blue("https://storacha.network"),
		color.Grey(id.String()),
		color.Grey(space.String()),
		color.Green(fmt.Sprintf("http://localhost:%d", port)),
	)
}

func requestLogger(logger *logging.ZapEventLogger) echo.MiddlewareFunc {
	return middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogMethod:        true,
		LogLatency:       true,
		LogRemoteIP:      true,
		LogHost:          true,
		LogURI:           true,
		LogUserAgent:     true,
		LogStatus:        true,
		LogContentLength: true,
		LogResponseSize:  true,
		LogHeaders:       []string{},
		LogError:         true,
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			fields := []zap.Field{
				zap.Int("status", v.Status),
				zap.String("method", v.Method),
				zap.String("uri", v.URI),
				zap.String("host", v.Host),
				zap.String("remote_ip", v.RemoteIP),
				zap.Duration("latency", v.Latency),
				zap.String("user_agent", v.UserAgent),
				zap.String("content_length", v.ContentLength),
				zap.Int64("response_size", v.ResponseSize),
				zap.Reflect("headers", v.Headers),
			}
			if v.Error != nil {
				fields = append(fields, zap.Error(v.Error))
			}
			switch {
			case v.Status >= http.StatusInternalServerError:
				logger.WithOptions(zap.Fields(fields...)).Error("server error")
			case v.Status >= http.StatusBadRequest:
				logger.WithOptions(zap.Fields(fields...)).Warn("client error")
			default:
				logger.WithOptions(zap.Fields(fields...)).Info("request")
			}
			return nil
		},
	})
}

func rootHandler(c echo.Context) error {
	return c.Blob(http.StatusOK, "text/html; charset=utf-8", indexHTML)
}
