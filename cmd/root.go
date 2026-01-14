package cmd

import (
	"context"
	"fmt"
	"os"
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
	cobra.EnableTraverseRunHooks = true
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	cobra.OnInitialize(initRootFlags, initConfig)
}

var cfgFilePath string

func initRootFlags() {
	// default storacha dir: ~/.storacha/guppy
	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(fmt.Errorf("failed to get user home directory: %w", err))
	}

	rootCmd.PersistentFlags().StringVar(
		&cfgFilePath,
		"config",
		"",
		"Path to the config file",
	)

	// this flag is hidden and deprecated in favor of "data-dir", though it will continue to work with a warning.
	rootCmd.PersistentFlags().String(
		"guppy-dir",
		filepath.Join(homedir, ".storacha/guppy"),
		"Directory containing the config and data store (default: ~/.storacha/guppy)",
	)
	cobra.CheckErr(rootCmd.PersistentFlags().MarkDeprecated("guppy-dir",
		"guppy-dir is deprecated please use data-dir instead"))
	cobra.CheckErr(rootCmd.PersistentFlags().MarkHidden("guppy-dir"))
	cobra.CheckErr(viper.BindPFlag("repo.data_dir", rootCmd.PersistentFlags().Lookup("guppy-dir")))

	rootCmd.PersistentFlags().String(
		"data-dir",
		filepath.Join(homedir, ".storacha/guppy"),
		"Directory containing the config and data store (default: ~/.storacha/guppy)",
	)
	cobra.CheckErr(viper.BindPFlag("repo.data_dir", rootCmd.PersistentFlags().Lookup("data-dir")))
	// ensure only the data-dir or (deprecate) guppy-dir are used, never both
	rootCmd.MarkFlagsMutuallyExclusive("guppy-dir", "data-dir")

	rootCmd.PersistentFlags().String(
		"upload-service-url",
		"https://up.forge.storacha.network",
		"URL of upload service",
	)
	cobra.CheckErr(viper.BindPFlag("client.upload_service.url", rootCmd.PersistentFlags().Lookup("upload-service-url")))
	cobra.CheckErr(viper.BindEnv("client.upload_service.url", "STORACHA_SERVICE_URL"))

	rootCmd.PersistentFlags().String(
		"upload-service-did",
		"did:web:up.forge.storacha.network",
		"DID of upload service",
	)
	cobra.CheckErr(viper.BindPFlag("client.upload_service.did", rootCmd.PersistentFlags().Lookup("upload-service-did")))
	cobra.CheckErr(viper.BindEnv("client.upload_service.url", "STORACHA_SERVICE_DID"))

	rootCmd.PersistentFlags().String(
		"indexer-service-url",
		"https://indexer.forge.storacha.network",
		"URL of indexer service",
	)
	cobra.CheckErr(viper.BindPFlag("indexer.url", rootCmd.PersistentFlags().Lookup("indexer-service-url")))
	cobra.CheckErr(viper.BindEnv("indexer.url", "STORACHA_INDEXING_SERVICE_URL"))

	rootCmd.PersistentFlags().String(
		"indexer-service-did",
		"did:web:indexer.forge.storacha.network",
		"DID of indexer service",
	)
	cobra.CheckErr(viper.BindPFlag("indexer.did", rootCmd.PersistentFlags().Lookup("indexer-service-did")))
	cobra.CheckErr(viper.BindEnv("indexer.url", "STORACHA_INDEXING_SERVICE_DID"))
}

func initConfig() {
	// check if environment variables match any of the existing keys
	// as an example a key is 'repo.data_dir'
	viper.AutomaticEnv()
	// when checking for env vars, rename keys searched for from 'repo.data_dir' to 'repo_data_dir'
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	// when checking for env vars, search for keys prefixed with GUPPY
	viper.SetEnvPrefix("GUPPY")

	// when searching for a config file look for files names "guppy-config.yaml"
	viper.SetConfigName("guppy-config")
	viper.SetConfigType("yaml")

	// if no config file was provided, first look in the current directory _then_ look in
	// $XDG_CONFIG_HOME/guppy/
	if cfgFilePath == "" {
		viper.AddConfigPath(".")
		if configDir, err := os.UserConfigDir(); err == nil {
			defaultCfgFile := filepath.Join(configDir, "guppy")
			viper.AddConfigPath(defaultCfgFile)
		}
	} else {
		// else a config was provided over the cli via a flag, read it in directly
		viper.SetConfigFile(cfgFilePath)
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
