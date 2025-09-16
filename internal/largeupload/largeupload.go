package largeupload

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/internal/fakefs"
	"github.com/storacha/guppy/pkg/client"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
	"github.com/urfave/cli/v2"
	_ "modernc.org/sqlite"
)

func makeUpload(ctx context.Context, repo *sqlrepo.Repo, api preparation.API, spaceDID did.DID, root string) (*uploadsmodel.Upload, error) {
	_, err := api.FindOrCreateSpace(ctx, spaceDID, "Large Upload Space")
	if err != nil {
		return nil, fmt.Errorf("command failed to create space: %w", err)
	}

	source, err := api.CreateSource(ctx, "Large Upload Source", root)
	if err != nil {
		return nil, fmt.Errorf("command failed to create source: %w", err)
	}

	err = repo.AddSourceToSpace(ctx, spaceDID, source.ID())
	if err != nil {
		return nil, fmt.Errorf("command failed to add source to space: %w", err)
	}

	uploads, err := api.CreateUploads(ctx, spaceDID)
	if err != nil {
		return nil, fmt.Errorf("command failed to create uploads: %w", err)
	}
	if len(uploads) != 1 {
		return nil, fmt.Errorf("command expected 1 upload, got %d", len(uploads))
	}

	return uploads[0], nil
}

func makeRepo(ctx context.Context) (*sqlrepo.Repo, func() error, error) {
	db, err := sql.Open("sqlite", "guppy.db")
	if err != nil {
		return nil, nil, fmt.Errorf("command failed to open in-memory SQLite database: %w", err)
	}
	db.SetMaxOpenConns(1)

	_, err = db.ExecContext(ctx, sqlrepo.Schema)
	if err != nil {
		return nil, nil, fmt.Errorf("command failed to execute schema: %w", err)
	}

	repo := sqlrepo.New(db)
	return repo, db.Close, nil
}

func Action(cCtx *cli.Context) error {
	spaceDID := cmdutil.MustParseDID(cCtx.String("space"))
	root := cCtx.Args().First()
	if root == "" {
		return fmt.Errorf("command requires a root path argument")
	}

	repo, _, err := makeRepo(cCtx.Context)
	if err != nil {
		return err
	}
	// Currently leads to a race condition with the app still running delayed DB
	// queries. We can deal with this issue later, since the process ends at the
	// end of this function anyhow.
	// defer closeDb()

	api := preparation.NewAPI(repo, cmdutil.MustGetClient(), spaceDID)
	upload, err := makeUpload(cCtx.Context, repo, api, spaceDID, root)
	if err != nil {
		return err
	}

	_, err = tea.NewProgram(newUploadModel(cCtx.Context, repo, api, upload)).Run()
	if err != nil {
		return fmt.Errorf("command failed to run upload UI: %w", err)
	}
	return err
}

type nullTransport struct{}

func (t nullTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	time.Sleep(1 * time.Second)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
	}, nil
}
func Demo(cCtx *cli.Context) error {
	repo, _, err := makeRepo(cCtx.Context)
	if err != nil {
		return err
	}
	// Currently leads to a race condition with the app still running delayed DB
	// queries. We can deal with this issue later, since the process ends at the
	// end of this function anyhow.
	// defer closeDb()

	// Use a fixed key for demo
	space, err := signer.Parse("MgCbD4FANEZRIU8Fcr5Lm0sjFkIJ0VMKo5DbLTCXQnS2JdO0Bb77mmA2VUXfYUdClitsWiHBFvgkxbRgo/+tsNLG6nyM=")
	if err != nil {
		return fmt.Errorf("command failed to parse space key: %w", err)
	}
	spaceDID := space.DID()

	baseClient, err := ctestutil.Client(
		ctestutil.WithClientOptions(
			// Act as space to avoid auth issues
			client.WithPrincipal(space),
		),
		ctestutil.WithSpaceBlobAdd(),

		ctestutil.WithServerOptions(
			server.WithServiceMethod(
				spaceindexcap.Add.Can(),
				server.Provide(
					spaceindexcap.Add,
					func(
						ctx context.Context,
						cap ucan.Capability[spaceindexcap.AddCaveats],
						inv invocation.Invocation,
						context server.InvocationContext,
					) (result.Result[spaceindexcap.AddOk, failure.IPLDBuilderFailure], fx.Effects, error) {
						return result.Ok[spaceindexcap.AddOk, failure.IPLDBuilderFailure](spaceindexcap.AddOk{}), nil, nil
					},
				),
			),
		),

		ctestutil.WithServerOptions(
			server.WithServiceMethod(
				uploadcap.Add.Can(),
				server.Provide(
					uploadcap.Add,
					func(
						ctx context.Context,
						cap ucan.Capability[uploadcap.AddCaveats],
						inv invocation.Invocation,
						context server.InvocationContext,
					) (result.Result[uploadcap.AddOk, failure.IPLDBuilderFailure], fx.Effects, error) {
						return result.Ok[uploadcap.AddOk, failure.IPLDBuilderFailure](uploadcap.AddOk{
							Root:   cap.Nb().Root,
							Shards: cap.Nb().Shards,
						}), nil, nil
					},
				),
			),
		),
	)
	if err != nil {
		return fmt.Errorf("command failed to create client: %w", err)
	}
	customPutClient := &ctestutil.ClientWithCustomPut{
		Client:    baseClient,
		PutClient: &http.Client{Transport: nullTransport{}},
	}
	api := preparation.NewAPI(repo, customPutClient, spaceDID, preparation.WithGetLocalFSForPathFn(func(path string) (fs.FS, error) {
		return fakefs.New(0), nil
	}))

	var upload *uploadsmodel.Upload
	upload, err = makeUpload(cCtx.Context, repo, api, spaceDID, ".")
	if err != nil {
		return err
	}

	_, err = tea.NewProgram(newUploadModel(cCtx.Context, repo, api, upload)).Run()
	if err != nil {
		return fmt.Errorf("command failed to run upload UI: %w", err)
	}

	return err
}
