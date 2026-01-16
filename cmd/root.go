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
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	log    = logging.Logger("cmd")
	tracer = otel.Tracer("cmd")
	// path to guppy config file relative to user config directory
	configFilePath = path.Join("guppy", "config.toml")
)

var (
	guppyDirPath string
	storePath    string
	cfgFile      string
)

var rootCmd = &cobra.Command{
	Use:   "guppy",
	Short: "Interact with the Storacha Network",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		span := trace.SpanFromContext(cmd.Context())
		setSpanAttributes(cmd, span)

		storePath = filepath.Join(guppyDirPath, "store.json")
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

	rootCmd.PersistentFlags().StringVar(
		&guppyDirPath,
		"guppy-dir",
		filepath.Join(homedir, ".storacha/guppy"),
		"Directory containing the config and data store (default: ~/.storacha/guppy)",
	)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Config file path. Attempts to load from user config directory if not set e.g. ~/.config/"+configFilePath)

	rootCmd.PersistentFlags().Bool("ui", false, "Use the guppy UI")
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
