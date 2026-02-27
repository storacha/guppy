package delegation

import (
	"fmt"
	"io"
	"maps"
	"os"
	"slices"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/config"
)

var createFlags struct {
	can        []string
	expiration int
	output     string
}

func init() {
	createCmd.Flags().StringArrayVarP(&createFlags.can, "can", "c", nil, "One or more abilities to delegate.")
	createCmd.Flags().IntVarP(&createFlags.expiration, "expiration", "e", 0, "Unix timestamp when the delegation is no longer valid. Zero indicates no expiration.")
	createCmd.Flags().StringVarP(&createFlags.output, "output", "o", "", "Path to write the delegation CAR file to. If not specified, outputs to stdout.")
}

var createCmd = &cobra.Command{
	Use:   "create <space> <audience-did>",
	Short: "Delegate capabilities for a space to others.",
	Long: wordwrap.WrapString(
		"Output a CAR encoded UCAN that delegates capabilities for a space to the audience. "+
			"The space can be specified by DID or by name.",
		80),
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		aud, err := did.Parse(args[1])
		if err != nil {
			return fmt.Errorf("parsing audience DID: %w", err)
		}
		if len(createFlags.can) == 0 {
			return fmt.Errorf("at least one capability must be specified with --can")
		}

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		c := cmdutil.MustGetClient(cfg.Repo.Dir, cfg.Network)

		space, err := cmdutil.ResolveSpace(c, args[0])
		if err != nil {
			return err
		}
		caps := make([]ucan.Capability[ucan.NoCaveats], 0, len(createFlags.can))
		proofs := map[string]delegation.Proof{}
		for _, can := range createFlags.can {
			query := agentstore.CapabilityQuery{Can: can, With: space.String()}
			dlgs, err := c.Proofs(query)
			if err != nil {
				return err
			}
			if len(dlgs) == 0 {
				return fmt.Errorf("no delegations found for ability %q with space %q", can, space.String())
			}
			for _, d := range dlgs {
				proofs[d.Link().String()] = delegation.FromDelegation(d)
			}
			cap := ucan.NewCapability(can, space.String(), ucan.NoCaveats{})
			caps = append(caps, cap)
		}

		opts := []delegation.Option{
			delegation.WithProof(slices.Collect(maps.Values(proofs))...),
		}
		if createFlags.expiration > 0 {
			opts = append(opts, delegation.WithExpiration(createFlags.expiration))
		} else {
			opts = append(opts, delegation.WithNoExpiration())
		}

		dlg, err := delegation.Delegate(c.Issuer(), aud, caps, opts...)
		if err != nil {
			return fmt.Errorf("creating delegation: %w", err)
		}

		if createFlags.output != "" {
			data, err := io.ReadAll(delegation.Archive(dlg))
			if err != nil {
				return fmt.Errorf("reading delegation archive: %w", err)
			}
			err = os.WriteFile(createFlags.output, data, 0644)
			if err != nil {
				return fmt.Errorf("writing delegation archive: %w", err)
			}
		} else {
			out, err := delegation.Format(dlg)
			if err != nil {
				return fmt.Errorf("formatting delegation: %w", err)
			}
			fmt.Println(out)
		}

		return nil
	},
}
