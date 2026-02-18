package cmd

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/storacha/guppy/cmd/blob"
	"github.com/storacha/guppy/cmd/unixfs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/storacha/guppy/cmd/account"
	"github.com/storacha/guppy/cmd/delegation"
	"github.com/storacha/guppy/cmd/gateway"
	"github.com/storacha/guppy/cmd/proof"
	"github.com/storacha/guppy/cmd/space"
	"github.com/storacha/guppy/cmd/upload"
)

var (
	log    = logging.Logger("cmd")
	tracer = otel.Tracer("cmd")
	// path to guppy config file relative to user config directory
	configFilePath = path.Join("guppy", "config.toml")
)

var (
	cfgFile      string
	guppyDirPath string
)

var rootCmd = &cobra.Command{
	Use:   "guppy",
	Short: "Interact with the Storacha Network",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		span := trace.SpanFromContext(cmd.Context())
		setSpanAttributes(cmd, span)
	},
	// We handle errors ourselves when they're returned from ExecuteContext.
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	cobra.OnInitialize(initConfig)
	cobra.EnableTraverseRunHooks = true
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	// default storacha dir: ~/.storacha/guppy
	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(fmt.Errorf("failed to get user home directory: %w", err))
	}

	rootCmd.AddCommand(unixfs.Cmd)
	rootCmd.PersistentFlags().StringVar(
		&guppyDirPath,
		"guppy-dir",
		"",
		"Guppy Directory",
	)

	unixfs.StorePathP = &guppyDirPath

	rootCmd.PersistentFlags().String(
		"data-dir",
		filepath.Join(homedir, ".storacha/guppy"),
		"Directory containing the config and data store (default: ~/.storacha/guppy)",
	)
	cobra.CheckErr(viper.BindPFlag("repo.data_dir", rootCmd.PersistentFlags().Lookup("data-dir")))

	rootCmd.PersistentFlags().String(
		"database-url",
		"",
		"PostgreSQL connection URL (e.g., postgres://user:pass@host:5432/dbname). If set, uses PostgreSQL instead of SQLite. The database should not be shared with other processes.",
	)
	cobra.CheckErr(viper.BindPFlag("repo.database_url", rootCmd.PersistentFlags().Lookup("database-url")))

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Config file path. Attempts to load from user config directory if not set e.g. ~/.config/"+configFilePath)

	rootCmd.PersistentFlags().Bool("ui", false, "Use the guppy UI")

	// Add Commands
	rootCmd.AddCommand(
		whoamiCmd,
		versionCmd,
		retrieveCmd,
		resetCmd,
		lsCmd,
		loginCmd,
		upload.Cmd,
		space.Cmd,
		proof.Cmd,
		gateway.Cmd,
		delegation.Cmd,
		account.Cmd,
		blob.Cmd,
	)
}

func initConfig() {
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("GUPPY")

	if cfgFile == "" {
		if configDir, err := os.UserConfigDir(); err == nil {
			defaultCfgFile := path.Join(configDir, configFilePath)
			if inf, err := os.Stat(defaultCfgFile); err == nil && !inf.IsDir() {
				log.Infof("loading config automatically from: %s", defaultCfgFile)
				cfgFile = defaultCfgFile
			}
		}
	}

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		cobra.CheckErr(viper.ReadInConfig())
	} else {
		// otherwise look for config.toml in current directory
		viper.SetConfigName("config")
		viper.SetConfigType("toml")
		viper.AddConfigPath(".")
		// Don't error if config file is not found - it's optional
		_ = viper.ReadInConfig()
	}
}

// ExecuteContext adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func ExecuteContext(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "cli")
	defer span.End()

	return rootCmd.ExecuteContext(ctx)
}
