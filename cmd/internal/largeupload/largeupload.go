package largeupload

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"
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
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
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

func runUploadUI(ctx context.Context, repo *sqlrepo.Repo, api preparation.API, upload *uploadsmodel.Upload) error {
	m, err := tea.NewProgram(newUploadModel(ctx, repo, api, upload)).Run()
	if err != nil {
		return fmt.Errorf("command failed to run upload UI: %w", err)
	}
	um := m.(uploadModel)

	if um.err != nil {
		var errBadFSEntry types.ErrBadFSEntry
		var errBadNodes types.ErrBadNodes
		if errors.As(um.err, &errBadFSEntry) || errors.As(um.err, &errBadNodes) {
			var sb strings.Builder
			sb.WriteString("\nUpload failed due to out-of-date scan:\n")

			if errors.As(um.err, &errBadFSEntry) {
				sb.WriteString(fmt.Sprintf(" - FS entry %s is out of date\n", errBadFSEntry.FsEntryID()))
			}

			if errors.As(um.err, &errBadNodes) {
				for i, ebn := range errBadNodes.Errs() {
					if i >= 3 {
						sb.WriteString(fmt.Sprintf("...and %d more errors\n", len(errBadNodes.Errs())-i))
						break
					}
					sb.WriteString(fmt.Sprintf(" - %s: %v\n", ebn.CID(), ebn.Unwrap()))
				}
			}

			sb.WriteString("\nYou can resume the upload to re-scan and continue.\n")

			fmt.Print(sb.String())
			return nil
		}

		if errors.Is(um.err, context.Canceled) || errors.Is(um.err, Canceled{}) {
			fmt.Println("\nUpload canceled.")
			return nil
		}

		return fmt.Errorf("upload failed: %w", um.err)
	}

	fmt.Println("Upload complete!")

	return nil
}

func Action(ctx context.Context, space string, resumeUpload bool, root string) error {
	spaceDID := cmdutil.MustParseDID(space)

	if root == "" {
		return fmt.Errorf("command requires a root path argument")
	}

	repo, _, err := makeRepo(ctx)
	if err != nil {
		return err
	}
	// Currently leads to a race condition with the app still running delayed DB
	// queries. We can deal with this issue later, since the process ends at the
	// end of this function anyhow.
	// defer closeDb()

	api := preparation.NewAPI(repo, cmdutil.MustGetClient(), spaceDID)

	var upload *uploadsmodel.Upload
	if resumeUpload {
		idBytes, err := os.ReadFile("last-upload-id.txt")
		if err != nil {
			return fmt.Errorf("reading last upload ID file: %w", err)
		}

		id, err := id.Parse(string(idBytes))
		if err != nil {
			return fmt.Errorf("parsing upload ID from file: %w", err)
		}

		upload, err = api.GetUploadByID(ctx, id)
		if err != nil {
			return fmt.Errorf("getting upload by ID: %w", err)
		}
		if upload == nil {
			return fmt.Errorf("no upload found with ID %s", id)
		}
		fmt.Println("Resuming upload", upload.ID())
	} else {
		upload, err = makeUpload(ctx, repo, api, spaceDID, root)
		if err != nil {
			return err
		}

		// Write upload ID to file for easy pasting into resume flag
		err = os.WriteFile("last-upload-id.txt", []byte(upload.ID().String()), 0644)
		if err != nil {
			return fmt.Errorf("writing upload ID to file: %w", err)
		}
	}

	err = runUploadUI(ctx, repo, api, upload)
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

type changingFS struct {
	fs.FS
	changingFile  string
	count         int
	changeModTime bool
	changeData    bool
}

type seekerFile interface {
	fs.File
	io.Seeker
}

type changingFile struct {
	seekerFile
	fs.FileInfo
	parent changingFS
}

type changingDir struct {
	changingFile
	fs fs.FS
}

func (fsys *changingFS) Open(name string) (fs.File, error) {
	f, err := fsys.FS.Open(name)
	if err != nil {
		return nil, err
	}
	sf, ok := f.(seekerFile)
	if !ok {
		return nil, fmt.Errorf("file %s is not seekable", name)
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	cf := changingFile{
		seekerFile: sf,
		FileInfo:   info,
		parent:     *fsys,
	}

	if name == fsys.changingFile {
		defer func() { fsys.count++ }()
		return cf, nil
	}

	if name == path.Dir(fsys.changingFile) {
		subFS, err := fs.Sub(fsys, name)
		if err != nil {
			return nil, err
		}
		return changingDir{
			changingFile: cf,
			fs:           subFS,
		}, nil
	}

	return f, nil
}

func (f changingFile) Read(b []byte) (int, error) {
	n, err := f.seekerFile.Read(b)
	if err != nil && err != io.EOF {
		return n, err
	}

	if f.parent.changeData && n > 0 {
		// Just change the first byte.
		b[0] = (b[0] + byte(f.parent.count))
	}

	return n, err
}

func (f changingFile) Stat() (fs.FileInfo, error) {
	return f, nil
}

func (d changingDir) ReadDir(n int) ([]fs.DirEntry, error) {
	entries, err := d.seekerFile.(fs.ReadDirFile).ReadDir(n)
	if err != nil {
		return nil, err
	}

	var changingDEs []fs.DirEntry
	for _, entry := range entries {
		f, err := d.fs.Open(entry.Name())
		if err != nil {
			return nil, err
		}
		info, err := f.Stat()
		if err != nil {
			return nil, err
		}
		f.Close()
		changingDEs = append(changingDEs, fs.FileInfoToDirEntry(info))

	}
	return changingDEs, nil
}

func (f changingFile) ModTime() time.Time {
	original := f.FileInfo.ModTime()
	if f.parent.changeModTime {
		newTime := original.Add(time.Duration(f.parent.count) * time.Minute)
		return newTime
	}

	return original
}

func newChangingFS(fsys fs.FS, changeModTime, changeData bool) (fs.FS, error) {
	var secondDir string
	root, err := fsys.Open(".")
	if err != nil {
		return nil, err
	}
	defer root.Close()
	rootEntries, err := root.(fs.ReadDirFile).ReadDir(-1)
	if err != nil {
		return nil, err
	}
	for range 2 {
		for _, entry := range rootEntries {
			if entry.IsDir() {
				secondDir = entry.Name()
				break
			}
		}
	}
	if secondDir == "" {
		return nil, fmt.Errorf("no directories found in root")
	}

	var lastDirFirstFile string
	lastDirF, err := fsys.Open(secondDir)
	if err != nil {
		return nil, err
	}
	defer lastDirF.Close()
	lastDirEntries, err := lastDirF.(fs.ReadDirFile).ReadDir(-1)
	if err != nil {
		return nil, err
	}
	for _, entry := range lastDirEntries {
		if !entry.IsDir() {
			lastDirFirstFile = entry.Name()
			break
		}
	}
	if lastDirFirstFile == "" {
		return nil, fmt.Errorf("no files found in last directory %s", secondDir)
	}

	return &changingFS{
		FS:            fsys,
		changingFile:  path.Join(secondDir, lastDirFirstFile),
		changeModTime: changeModTime,
		changeData:    changeData,
	}, nil
}

func Demo(ctx context.Context, resumeUpload, alterMetadata, alterData bool) error {
	repo, _, err := makeRepo(ctx)
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
	fsys, err := newChangingFS(fakefs.New(0), alterMetadata, alterData)
	if err != nil {
		return fmt.Errorf("creating changing FS: %w", err)
	}
	api := preparation.NewAPI(repo, customPutClient, spaceDID, preparation.WithGetLocalFSForPathFn(func(path string) (fs.FS, error) {
		return fsys, nil
	}))

	var upload *uploadsmodel.Upload
	if resumeUpload {
		idBytes, err := os.ReadFile("last-upload-id.txt")
		if err != nil {
			return fmt.Errorf("reading last upload ID file: %w", err)
		}

		id, err := id.Parse(string(idBytes))
		if err != nil {
			return fmt.Errorf("parsing upload ID from file: %w", err)
		}

		upload, err = api.GetUploadByID(ctx, id)
		if err != nil {
			return fmt.Errorf("getting upload by ID: %w", err)
		}
		if upload == nil {
			return fmt.Errorf("no upload found with ID %s", id)
		}
		fmt.Println("Resuming upload", upload.ID())
	} else {
		upload, err = makeUpload(ctx, repo, api, spaceDID, ".")
		if err != nil {
			return err
		}

		// Write upload ID to file for easy pasting into resume flag
		err = os.WriteFile("last-upload-id.txt", []byte(upload.ID().String()), 0644)
		if err != nil {
			return fmt.Errorf("writing upload ID to file: %w", err)
		}
	}

	err = runUploadUI(ctx, repo, api, upload)
	return err
}
