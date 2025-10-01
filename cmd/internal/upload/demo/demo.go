package demo

import (
	"context"
	"crypto/ed25519"
	"crypto/sha512"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path"
	"time"

	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/cmd/internal/upload/ui"
	"github.com/storacha/guppy/internal/fakefs"
	"github.com/storacha/guppy/pkg/client"
	ctestutil "github.com/storacha/guppy/pkg/client/testutil"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
)

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

func Demo(ctx context.Context, repo *sqlrepo.Repo, spaceName string, alterMetadata, alterData bool) error {
	hash := sha512.Sum512_256([]byte(spaceName))
	space, err := signer.FromRaw(ed25519.NewKeyFromSeed(hash[:]))
	if err != nil {
		return fmt.Errorf("command failed to create space key: %w", err)
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
	api := preparation.NewAPI(repo, customPutClient, preparation.WithGetLocalFSForPathFn(func(path string) (fs.FS, error) {
		return fsys, nil
	}))

	uploads, err := api.FindOrCreateUploads(ctx, spaceDID)
	if err != nil {
		return fmt.Errorf("command failed to create uploads: %w", err)
	}

	if len(uploads) == 0 {
		// Try adding the source and running again.

		_, err = api.FindOrCreateSpace(ctx, spaceDID, spaceDID.String())
		if err != nil {
			return fmt.Errorf("command failed to create space: %w", err)
		}

		source, err := api.CreateSource(ctx, ".", ".")
		if err != nil {
			return fmt.Errorf("command failed to create source: %w", err)
		}

		err = repo.AddSourceToSpace(ctx, spaceDID, source.ID())
		if err != nil {
			return fmt.Errorf("command failed to add source to space: %w", err)
		}

		uploads, err = api.FindOrCreateUploads(ctx, spaceDID)
		if err != nil {
			return fmt.Errorf("command failed to create uploads: %w", err)
		}

	}

	return ui.RunUploadUI(ctx, repo, api, uploads)
}
