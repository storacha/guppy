package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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
}

func init() {
	rootCmd.AddCommand(uploadCmd)

	// default storacha dir: ~/.storacha
	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(fmt.Errorf("failed to get user home directory: %w", err))
	}
	defaultStorachaDir := filepath.Join(homedir, ".storacha")

	uploadCmd.PersistentFlags().StringVar(&uploadDbPath, "storachaDir", defaultStorachaDir, "Directory containing the config and data store (default: ~/.storacha)")

	// We could make this configurable in the future, but for now it seems like
	// too many options for likely no value.
	storePath = filepath.Join(defaultStorachaDir, "store.json")
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
