package cmd

import (
	"context"
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

	"github.com/storacha/guppy/cmd/space"
	"github.com/storacha/guppy/cmd/upload"
)

var (
	log    = logging.Logger("cmd")
	tracer = otel.Tracer("cmd")
)

// DefaultDataDirPath defines the default data dir of guppy: "$HOME/.storacha/guppy"
var DefaultDataDirPath = filepath.Join(lo.Must(os.UserHomeDir()), ".storacha/guppy")

const (
	defaultServiceName = "staging.up.warm.storacha.network"
	defaultIndexerName = "staging.indexer.warm.storacha.network"
)

func init() {
	// root flags
	rootCmd.PersistentFlags().String("config", "", "path to a config file")

	// root flags w/ config binding
	rootCmd.PersistentFlags().String("data-dir", DefaultDataDirPath, "path to a data dir containing guppy state")
	cobra.CheckErr(viper.BindPFlag("repo.dir", rootCmd.PersistentFlags().Lookup("data-dir")))

	rootCmd.PersistentFlags().String("upload-service", defaultServiceName, "name of service to use for storacha upload")
	cobra.CheckErr(viper.BindPFlag("network.upload_service", rootCmd.PersistentFlags().Lookup("upload-service")))

	rootCmd.PersistentFlags().String("indexer-service", defaultIndexerName, "name of service to use for storacha indexing")
	cobra.CheckErr(viper.BindPFlag("network.indexer_service", rootCmd.PersistentFlags().Lookup("indexer-service")))

	// root commands
	rootCmd.AddCommand(loginCmd, upload.Cmd, lsCmd, resetCmd, retrieveCmd, space.Cmd, whoamiCmd)
}

func initViper() {
	viper.AutomaticEnv()
	// replace . and - with _
	viper.EnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
}

var rootCmd = &cobra.Command{
	Use:           "guppy",
	Short:         "Interact with the Storacha Network",
	SilenceErrors: true, // We handle errors ourselves when they're returned from ExecuteContext.
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		span := trace.SpanFromContext(cmd.Context())
		// TODO(forrest): I think this will need to re-embed the span back into the context to have desired effect.
		setSpanAttributes(cmd, span)
	},
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
