package upload

import (
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/cmd/internal/upload/ui"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/repo"
)

var Cmd = &cobra.Command{
	Use:     "upload <space>",
	Aliases: []string{"up"},
	Short:   "Upload data to a Storacha space",
	Long: wordwrap.WrapString(
		"Uploads data to a Storacha space. This will produce one upload in the "+
			"space for each source added to the space with `upload sources add`. If "+
			"no sources have been added to the space yet, the command will exit "+
			"with an error.",
		80),
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		space := cmd.Flags().Arg(0)
		if space == "" {
			return fmt.Errorf("space cannot be empty")
		}
		spaceDID, err := did.Parse(space)
		if err != nil {
			return fmt.Errorf("parsing space DID: %w", err)
		}

		// The command line was valid. Past here, errors do not mean the user needs
		// to see the usage.
		cmd.SilenceUsage = true

		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		repo, err := repo.Open(cfg.Repo)
		if err != nil {
			return fmt.Errorf("opening repo: %w", err)
		}

		c, err := cmdutil.NewClient(cfg.Network, repo)
		if err != nil {
			return fmt.Errorf("creating client: %w", err)
		}

		sqlRepo, err := repo.OpenSQLRepo(ctx)
		if err != nil {
			return err
		}
		// Currently leads to a race condition with the app still running delayed DB
		// queries. We can deal with this issue later, since the process ends at the
		// end of this function anyhow.
		// defer sqlRepo.Close()

		api := preparation.NewAPI(sqlRepo, c)
		uploads, err := api.FindOrCreateUploads(ctx, spaceDID)
		if err != nil {
			return fmt.Errorf("command failed to create uploads: %w", err)
		}

		if len(uploads) == 0 {
			cmd.Printf("No sources found for space. Add a source first with:\n\n$ %s %s <path>\n\n", sourcesAddCmd.CommandPath(), spaceDID)
			return cmdutil.NewHandledCliError(fmt.Errorf("no uploads found for space %s", spaceDID))
		}

		return ui.RunUploadUI(ctx, sqlRepo, api, uploads)
	},
}

func init() {
	Cmd.AddCommand(sourcesCmd)
}
