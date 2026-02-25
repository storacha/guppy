package cmd

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-libstoracha/principalresolver"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	edverifier "github.com/storacha/go-ucanto/principal/ed25519/verifier"
	"github.com/storacha/go-ucanto/principal/verifier"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/client/dagservice"
	"github.com/storacha/guppy/pkg/client/locator"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/dagfs"
)

func init() {
	retrieveCmd.Flags().StringP("network", "n", "", "Network to retrieve content from.")
	retrieveCmd.Flags().MarkHidden("network")
}

var retrieveCmd = &cobra.Command{
	Use:     "retrieve <space> <content-path> <output-path>",
	Aliases: []string{"get"},
	Short:   "Get a file or directory by its CID",
	Long: wordwrap.WrapString(
		"Retrieves a file or directory from a space. The specified file or "+
			"directory will be written to <output-path>. The space can be specified "+
			"by DID or by name. <content-path> can take several forms:\n\n"+
			"* /ipfs/<cid>[/<subpath>]\n"+
			"* ipfs://<cid>[/<subpath>]\n"+
			"* <cid>[/<subpath>]",
		80),
	Args: cobra.ExactArgs(3),

	RunE: func(cmd *cobra.Command, args []string) (retErr error) {
		ctx := cmd.Context()
		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}

		c := cmdutil.MustGetClient(cfg.Repo.Dir)
		space, err := cmdutil.ResolveSpace(c, args[0])
		if err != nil {
			return err
		}

		pathCID, subpath, err := cmdutil.ContentPath(args[1])
		if err != nil {
			cmd.SilenceUsage = false
			return fmt.Errorf("parsing content path: %w", err)
		}
		if subpath == "" {
			subpath = "."
		}

		outputPath := args[2]

		indexer, indexerPrincipal := cmdutil.MustGetIndexClient()

		ctx, span := tracer.Start(ctx, "retrieve", trace.WithAttributes(
			attribute.String("retrieval.space", space.DID().String()),
			attribute.String("retrieval.cid", pathCID.String()),
			attribute.String("retrieval.subpath", subpath),
			attribute.String("retrieval.output_path", outputPath),
		))
		defer span.End()
		defer func() {
			if retErr != nil {
				span.RecordError(retErr)
				span.SetStatus(codes.Error, "")
			}
		}()

		networkName, _ := cmd.Flags().GetString("network")
		network := cmdutil.MustGetNetworkConfig(networkName)
		pruningCtx, err := buildPruningContext(ctx, network.UploadID)
		if err != nil {
			return err
		}

		locator := locator.NewIndexLocator(indexer, func(spaces []did.DID) (delegation.Delegation, error) {
			queries := make([]agentstore.CapabilityQuery, 0, len(spaces))
			for _, space := range spaces {
				queries = append(queries, agentstore.CapabilityQuery{
					Can:  contentcap.Retrieve.Can(),
					With: space.String(),
				})
			}

			var pfs []delegation.Proof
			res, err := c.Proofs(queries...)
			if err != nil {
				return nil, err
			}
			for _, del := range res {
				pfs = append(pfs, delegation.FromDelegation(del))
			}

			// Allow the indexing service to retrieve indexes
			// Prune proofs from the delegation to avoid sending large delegations to the indexer
			draftDlg, err := contentcap.Retrieve.Delegate(
				c.Issuer(),
				indexerPrincipal,
				space.DID().String(),
				contentcap.RetrieveCaveats{},
				delegation.WithProof(pfs...),
				delegation.WithExpiration(int(time.Now().Add(30*time.Second).Unix())),
			)
			if err != nil {
				return nil, fmt.Errorf("creating delegation to indexer: %w", err)
			}

			pfs, unauth := validator.PruneProofs(ctx, draftDlg, pruningCtx)
			if unauth != nil {
				return nil, unauth
			}

			return delegation.Delegate(
				c.Issuer(),
				indexerPrincipal,
				[]ucan.Capability[ucan.NoCaveats]{
					ucan.NewCapability(contentcap.Retrieve.Can(), space.DID().String(), ucan.NoCaveats{}),
				},
				delegation.WithProof(pfs...),
				delegation.WithExpiration(int(time.Now().Add(30*time.Second).Unix())),
			)
		})
		ds := dagservice.NewDAGService(locator, c, []did.DID{space})
		retrievedFs := dagfs.New(ctx, ds, pathCID)

		file, err := retrievedFs.Open(subpath)
		if err != nil {
			return fmt.Errorf("opening path in retrieved filesystem: %w", err)
		}
		defer file.Close()

		// If it's a directory, copy the whole directory. If it's a file, copy the
		// file.
		if _, ok := file.(fs.ReadDirFile); ok {
			span.SetAttributes(
				attribute.Bool("retrieval.directory", true),
			)
			pathedFs, err := fs.Sub(retrievedFs, subpath)
			if err != nil {
				return fmt.Errorf("sub filesystem: %w", err)
			}

			err = os.CopyFS(outputPath, pathedFs)
			if err != nil {
				return fmt.Errorf("copying retrieved filesystem: %w", err)
			}
		} else {
			span.SetAttributes(
				attribute.Bool("retrieval.directory", false),
			)
			outFile, err := os.Create(outputPath)
			if err != nil {
				return fmt.Errorf("creating output file: %w", err)
			}
			defer outFile.Close()

			_, err = io.Copy(outFile, file)
			if err != nil {
				return fmt.Errorf("writing to output file: %w", err)
			}
		}

		return nil
	},
}

func buildPruningContext(ctx context.Context, attestorDID did.DID) (validator.ValidationContext[contentcap.RetrieveCaveats], error) {
	resolver, err := principalresolver.NewHTTPResolver([]did.DID{attestorDID})
	if err != nil {
		return nil, fmt.Errorf("creating principal resolver: %w", err)
	}

	resolvedKeyDID, unresolvedErr := resolver.ResolveDIDKey(ctx, attestorDID)
	if unresolvedErr != nil {
		return nil, fmt.Errorf("resolving attestor DID key: %w", unresolvedErr)
	}

	keyVerifier, err := edverifier.Parse(resolvedKeyDID.String())
	if err != nil {
		return nil, fmt.Errorf("parsing resolved key DID: %w", err)
	}

	attestor, err := verifier.Wrap(keyVerifier, attestorDID)
	if err != nil {
		return nil, fmt.Errorf("creating attestor verifier: %w", err)
	}

	return validator.NewValidationContext(
		// For client evaluated chains, the authority must be the service that issued the ucan/attest
		// delegations — e.g. the upload-service.
		attestor,
		contentcap.Retrieve,
		validator.IsSelfIssued,
		func(context.Context, validator.Authorization[any]) validator.Revoked {
			return nil
		},
		validator.ProofUnavailable,
		server.ParsePrincipal,
		validator.FailDIDKeyResolution,
		validator.NotExpiredNotTooEarly,
	), nil
}
