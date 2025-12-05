package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/storacha/guppy/cmd/account"
	"github.com/storacha/guppy/cmd/space"
	"github.com/storacha/guppy/pkg/config"
)

var (
	log    = logging.Logger("cmd")
	tracer = otel.Tracer("cmd")
)

var rootCmd = &cobra.Command{
	Use:   "guppy",
	Short: "Interact with the Storacha Network",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		span := trace.SpanFromContext(cmd.Context())
		setSpanAttributes(cmd, span)

		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}
		return os.MkdirAll(cfg.Repo.DataDir, os.ModePerm)
	},
	// We handle errors ourselves when they're returned from ExecuteContext.
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	// Register flags/commands before Cobra parses args so subcommands are available.
	initFlags()
	cobra.OnInitialize(initConfig)
}

var cfgFile string
var defaultConfigFilePath = filepath.Join("guppy", "config.toml")

func initFlags() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "",
		"Config file path. Attempts to load from user config directory if not set e.g. ~/.config/"+defaultConfigFilePath)

	rootCmd.PersistentFlags().String("data-dir", filepath.Join(lo.Must(os.UserHomeDir()), ".storacha/guppy"),
		"Directory containing the config and data store (default: ~/.storacha/guppy)",
	)
	// bind flag to config
	cobra.CheckErr(viper.BindPFlag("repo.data_dir", rootCmd.PersistentFlags().Lookup("data-dir")))

	rootCmd.PersistentFlags().String("key-file", "", "Path to a PEM file containing ed25519 private key")
	// bind flag to config
	cobra.CheckErr(viper.BindPFlag("identity.key_file", rootCmd.PersistentFlags().Lookup("key-file")))
	// completions
	cobra.CheckErr(rootCmd.MarkPersistentFlagFilename("key-file", "pem"))

	rootCmd.AddCommand(
		account.Cmd,
		loginCmd,
		lsCmd,
		resetCmd,
		retrieveCmd,
		space.Cmd,
		uploadCmd,
		versionCmd,
		whoamiCmd,
	)
}

func initConfig() {
	viper.AutomaticEnv()
	// the two options below will cause keys bound to viper, e.g. identity.key_file to be searched for as
	// GUPPY_IDENTITY_KEY_FILE
	viper.SetEnvPrefix("GUPPY")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// if no flag was provided look in $HOME/.config/guppy/config.toml
	if cfgFile == "" {
		if configDir, err := os.UserConfigDir(); err == nil {
			defaultCfgFile := filepath.Join(configDir, defaultConfigFilePath)
			if inf, err := os.Stat(defaultCfgFile); err == nil && !inf.IsDir() {
				log.Infof("loading config automatically from: %s", defaultCfgFile)
				cfgFile = defaultCfgFile
			}
		}
	}

	// a flag was provided, or we found a config file in the default location
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		cobra.CheckErr(viper.ReadInConfig())
	}
}

// ExecuteContext adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func ExecuteContext(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "cli")
	defer span.End()

	return rootCmd.ExecuteContext(ctx)
}

// commandPath returns the command path for a `cobra.Command`. Where
// `cmd.CommandPath()` returns a concatenated string, this returns a slice of
// the individual commands in the path.
func commandPath(c *cobra.Command) []string {
	var path []string
	if c.HasParent() {
		path = commandPath(c.Parent())
	}
	path = append(path, c.Name())
	return path
}

// setSpanAttributes sets attributes on the provided span based on the command
// and its flags. It will set:
//   - command.path: the full path of the command as a string slice
//   - command.flag.<flag-name>: the value of each flag, as the appropriate type
func setSpanAttributes(cmd *cobra.Command, span trace.Span) {
	attrs := []attribute.KeyValue{
		attribute.StringSlice("command.path", commandPath(cmd)),
	}
	cmd.Flags().Visit(func(f *pflag.Flag) {
		var err error
		k := "command.flag." + f.Name

		var attr attribute.KeyValue
		switch f.Value.Type() {
		case "bool":
			var v bool
			v, err = cmd.Flags().GetBool(f.Name)
			attr = attribute.Bool(k, v)
		case "boolSlice":
			var v []bool
			v, err = cmd.Flags().GetBoolSlice(f.Name)
			attr = attribute.BoolSlice(k, v)
		case "int":
			var v int
			v, err = cmd.Flags().GetInt(f.Name)
			attr = attribute.Int(k, v)
		case "intSlice":
			var v []int
			v, err = cmd.Flags().GetIntSlice(f.Name)
			attr = attribute.IntSlice(k, v)
		case "int64":
			var v int64
			v, err = cmd.Flags().GetInt64(f.Name)
			attr = attribute.Int64(k, v)
		case "int64Slice":
			var v []int64
			v, err = cmd.Flags().GetInt64Slice(f.Name)
			attr = attribute.Int64Slice(k, v)
		case "float64":
			var v float64
			v, err = cmd.Flags().GetFloat64(f.Name)
			attr = attribute.Float64(k, v)
		case "float64Slice":
			var v []float64
			v, err = cmd.Flags().GetFloat64Slice(f.Name)
			attr = attribute.Float64Slice(k, v)
		case "string":
			var v string
			v, err = cmd.Flags().GetString(f.Name)
			attr = attribute.String(k, v)
		case "stringSlice":
			var v []string
			v, err = cmd.Flags().GetStringSlice(f.Name)
			attr = attribute.StringSlice(k, v)
		default:
			attr = attribute.String(k, f.Value.String())
		}
		if err != nil {
			log.Warnf("getting flag %q value %v for telemetry: %v", f.Name, f.Value, err)
			return
		}

		attrs = append(attrs, attr)
	})

	span.SetAttributes(attrs...)
}
