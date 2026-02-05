package upload

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/cmd/internal/upload/ui"
	"github.com/storacha/guppy/cmd/upload/source"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/bus"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var log = logging.Logger("cmd/upload")

var rootFlags struct {
	all         bool
	retry       bool
	parallelism uint64
}

func init() {
	Cmd.Flags().BoolVar(&rootFlags.all, "all", false, "Upload all sources (even if arguments are provided)")
	Cmd.Flags().BoolVar(&rootFlags.retry, "retry", false, "Auto-retry failed uploads")
	Cmd.Flags().Uint64Var(&rootFlags.parallelism, "parallelism", 6, "Number of parallel shard uploads to perform concurrently")

	Cmd.AddCommand(source.Cmd)
}

var Cmd = &cobra.Command{
	Use:     "upload <space> [source-path-or-name...]",
	Aliases: []string{"up"},
	Short:   "Upload data to a Storacha space",
	Long: wordwrap.WrapString(
		"Uploads data to a Storacha space. By default, this will upload all sources "+
			"added to the space. You can optionally specify one or more source paths "+
			"or names to upload only those specific sources. The space can be specified "+
			"by DID or by name.",
		80),
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		spaceArg := args[0]
		if spaceArg == "" {
			cmd.SilenceUsage = false
			return fmt.Errorf("space required")
		}

		useUI, err := cmd.Parent().PersistentFlags().GetBool("ui")
		if err != nil {
			return fmt.Errorf("getting 'ui' flag: %w", err)
		}

		requestedSources := args[1:]

		// The command line was valid. Past here, errors do not mean the user needs
		// to see the usage.
		cmd.SilenceUsage = true

		eb := bus.New()

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}
		repo, err := preparation.OpenRepo(ctx, cfg.Repo.DatabasePath(), sqlrepo.WithEventBus(eb))
		if err != nil {
			return err
		}
		// TODO: FIXME!
		// Currently leads to a race condition with the app still running delayed DB
		// queries. We can deal with this issue later, since the process ends at the
		// end of this function anyhow.
		// defer repo.Close()

		client := cmdutil.MustGetClient(cfg.Repo.Dir)
		spaceDID, err := cmdutil.ResolveSpace(client, spaceArg)
		if err != nil {
			return err
		}

		api := preparation.NewAPI(repo, client,
			preparation.WithBlobUploadParallelism(int(rootFlags.parallelism)),
			preparation.WithEventBus(eb),
		)
		allUploads, err := api.FindOrCreateUploads(ctx, spaceDID)
		if err != nil {
			return fmt.Errorf("command failed to create uploads: %w", err)
		}

		if len(allUploads) == 0 {
			fmt.Printf("No sources found for space. Add a source first with:\n\n$ %s %s <path>\n\n",
				source.AddCmd.CommandPath(),
				spaceDID)
			return cmdutil.NewHandledCliError(fmt.Errorf("no uploads found for space %s", spaceDID))
		}

		var uploadsToRun []*uploadsmodel.Upload

		if len(requestedSources) == 0 || rootFlags.all {
			uploadsToRun = allUploads
		} else {
			reqMap := make(map[string]bool)
			for _, p := range requestedSources {
				reqMap[filepath.Clean(p)] = true
				reqMap[p] = true
			}

			for _, u := range allUploads {
				src, err := repo.GetSourceByID(ctx, u.SourceID())
				if err != nil {
					return fmt.Errorf("getting source info: %w", err)
				}

				cleanPath := filepath.Clean(src.Path())
				name := src.Name()

				if reqMap[cleanPath] || reqMap[name] {
					uploadsToRun = append(uploadsToRun, u)
					delete(reqMap, cleanPath)
					delete(reqMap, name)
				}
			}

			if len(reqMap) > 0 {
				for p := range reqMap {
					return fmt.Errorf("source not found in space: %s (did you add it with 'guppy upload sources add'?)", p)
				}
			}
		}

		if useUI {
			return ui.RunUploadUI(ctx, repo, api, uploadsToRun, rootFlags.retry, eb)
		}
		// UI disabled, log at info level
		logging.SetAllLoggers(logging.LevelInfo)

		type uploadResult struct {
			upload   *uploadsmodel.Upload
			cid      cid.Cid
			attempts int
		}

		type uploadFailure struct {
			upload   *uploadsmodel.Upload
			err      error
			attempts int
		}

		var completedUploads []uploadResult
		var failedUploads []uploadFailure
		for _, u := range uploadsToRun {
			start := time.Now()
			log.Infow("Starting upload", "upload", u.ID())
			attempt := 0
			var uploadCID cid.Cid
			var lastErr error

			for {
				attempt++
				uploadCID, err = api.ExecuteUpload(ctx, u)
				if err == nil {
					lastErr = nil
					break
				}

				var re types.RetriableError
				if errors.As(err, &re) {
					lastErr = err
					if rootFlags.retry {
						log.Warnw("Retriable upload error encountered, retrying", "upload", u.ID(), "attempt", attempt,
							"err", err)
						continue
					}

					log.Errorw("Retriable upload error encountered (retry disabled)", "upload", u.ID(), "attempt",
						attempt, "err", err)
					break
				}

				lastErr = err
				log.Errorw("Upload failed with non-retriable error", "upload", u.ID(), "attempt", attempt, "err", err)
				break
			}

			if lastErr != nil {
				failedUploads = append(failedUploads, uploadFailure{
					upload:   u,
					err:      lastErr,
					attempts: attempt,
				})
				log.Errorw("Upload failed", "upload", u.ID(), "duration", time.Since(start), "attempts", attempt, "err",
					lastErr)
				continue
			}

			completedUploads = append(completedUploads, uploadResult{
				upload:   u,
				cid:      uploadCID,
				attempts: attempt,
			})
			log.Infow("Completed upload", "upload", u.ID(), "cid", uploadCID.String(), "duration", time.Since(start), "attempts", attempt)
		}

		for _, u := range completedUploads {
			cmd.Printf("Upload completed successfully: %s\n", u.cid.String())
		}

		if len(failedUploads) > 0 {
			cmd.Println("Uploads failed:")
			for _, u := range failedUploads {
				cmd.Printf("- %s: %v\n", u.upload.ID(), u.err)
			}
			return cmdutil.NewHandledCliError(fmt.Errorf("%d upload(s) failed", len(failedUploads)))
		}
		return nil
	},
}
