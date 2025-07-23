package preparation

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/guppy/pkg/preparation/spaces"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/dags"
	"github.com/storacha/guppy/pkg/preparation/scans"
	scansmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/sources"
	sourcesmodel "github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var log = logging.Logger("preparation")

type Repo interface {
	spaces.Repo
	uploads.Repo
	sources.Repo
	scans.Repo
	dags.Repo
	shards.Repo
}

type API struct {
	Spaces spaces.API
	Uploads        uploads.API
	Sources        sources.API
	DAGs           dags.API
	Scans          scans.API
}

// Option is an option configuring the API.
type Option func(cfg *config) error

type config struct {
	getLocalFSForPathFn func(path string) (fs.FS, error)
}

func NewAPI(repo Repo, options ...Option) API {
	cfg := &config{
		getLocalFSForPathFn: func(path string) (fs.FS, error) { return os.DirFS(path), nil },
	}
	for _, opt := range options {
		if err := opt(cfg); err != nil {
			panic(fmt.Sprintf("failed to apply option: %v", err))
		}
	}

	// The dependencies of the APIs involve a cycle, so we need to declare one
	// first and initialize it last.
	var uploadsAPI uploads.API

	spacesAPI := spaces.API{
		Repo: repo,
	}

	sourcesAPI := sources.API{
		Repo:                repo,
		GetLocalFSForPathFn: cfg.getLocalFSForPathFn,
	}

	scansAPI := scans.API{
		Repo: repo,
		// Lazy-evaluate `uploadsAPI`, which isn't initialized yet, but will be.
		UploadSourceLookup: func(ctx context.Context, uploadID id.UploadID) (id.SourceID, error) {
			return uploadsAPI.GetSourceIDForUploadID(ctx, uploadID)
		},
		SourceAccessor: sourcesAPI.AccessByID,
		WalkerFn:       walker.WalkDir,
	}

	dagsAPI := dags.API{
		Repo:         repo,
		FileAccessor: scansAPI.OpenFileByID,
	}

	shardsAPI := shards.API{
		Repo: repo,
	}

	uploadsAPI = uploads.API{
		Repo: repo,
		RunNewScan: func(ctx context.Context, uploadID id.UploadID, fsEntryCb func(id id.FSEntryID, isDirectory bool) error) (id.FSEntryID, error) {
			scan, err := repo.CreateScan(ctx, uploadID)
			if err != nil {
				return id.Nil, fmt.Errorf("command failed to create new scan: %w", err)
			}

			err = scansAPI.ExecuteScan(ctx, scan, func(entry scansmodel.FSEntry) error {
				log.Debugf("Processing entry: %s", entry.Path())
				_, isDirectory := entry.(*scansmodel.Directory)
				return fsEntryCb(entry.ID(), isDirectory)
			})

			if err != nil {
				return id.Nil, fmt.Errorf("command failed to execute scan: %w", err)
			}

			if scan.State() != scansmodel.ScanStateCompleted {
				return id.Nil, fmt.Errorf("scan did not complete successfully, state: %s, error: %w", scan.State(), scan.Error())
			}

			if !scan.HasRootID() {
				return id.Nil, errors.New("completed scan did not have a root ID")
			}

			return scan.RootID(), nil
		},
		RestartDagScansForUpload: dagsAPI.RestartDagScansForUpload,
		RunDagScansForUpload:     dagsAPI.RunDagScansForUpload,
		AddNodeToUploadShards:    shardsAPI.AddNodeToUploadShards,
		CloseUploadShards:        shardsAPI.CloseUploadShards,
	}

	return API{
		Spaces: spacesAPI,
		Uploads:        uploadsAPI,
		Sources:        sourcesAPI,
		DAGs:           dagsAPI,
		Scans:          scansAPI,
	}
}

func WithGetLocalFSForPathFn(getLocalFSForPathFn func(path string) (fs.FS, error)) Option {
	return func(cfg *config) error {
		cfg.getLocalFSForPathFn = getLocalFSForPathFn
		return nil
	}
}

func (a API) CreateSpace(ctx context.Context, name string, options ...spacesmodel.SpaceOption) (*spacesmodel.Space, error) {
	return a.Spaces.CreateSpace(ctx, name, options...)
}

func (a API) CreateSource(ctx context.Context, name string, path string, options ...sourcesmodel.SourceOption) (*sourcesmodel.Source, error) {
	return a.Sources.CreateSource(ctx, name, path, options...)
}

func (a API) CreateUploads(ctx context.Context, spaceID id.SpaceID) ([]*uploadsmodel.Upload, error) {
	return a.Uploads.CreateUploads(ctx, spaceID)
}

func (a API) ExecuteUpload(ctx context.Context, upload *uploadsmodel.Upload) error {
	return a.Uploads.ExecuteUpload(ctx, upload)
}
