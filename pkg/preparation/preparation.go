package preparation

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/dags"
	"github.com/storacha/guppy/pkg/preparation/scans"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/sources"
	sourcesmodel "github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/spaces"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var log = logging.Logger("preparation")

// By back-of-the-napkin math, this represents about 1MB max per index
const defaultMaxNodesPerIndex = 26000

type Repo interface {
	spaces.Repo
	uploads.Repo
	sources.Repo
	scans.Repo
	dags.Repo
	shards.Repo
}

type StorachaClient = storacha.Client

type API struct {
	Spaces  spaces.API
	Uploads uploads.API
	Sources sources.API
	DAGs    dags.API
	Scans   scans.API
}

// Option is an option configuring the API.
type Option func(cfg *config) error

type config struct {
	getLocalFSForPathFn func(path string) (fs.FS, error)
	maxNodesPerIndex    int
}

func NewAPI(repo Repo, client StorachaClient, space did.DID, options ...Option) API {
	cfg := &config{
		getLocalFSForPathFn: func(path string) (fs.FS, error) { return os.DirFS(path), nil },
		maxNodesPerIndex:    defaultMaxNodesPerIndex,
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
		Repo:           repo,
		SourceAccessor: sourcesAPI.AccessByID,
		WalkerFn:       walker.WalkDir,
	}

	dagsAPI := dags.API{
		Repo:         repo,
		FileAccessor: scansAPI.OpenFileByID,
	}

	nr, err := dags.NewNodeReader(repo, func(ctx context.Context, sourceID id.SourceID, path string) (fs.File, error) {
		source, err := repo.GetSourceByID(ctx, sourceID)
		if err != nil {
			return nil, fmt.Errorf("failed to get source by ID %s: %w", sourceID, err)
		}

		fs, err := sourcesAPI.Access(source)
		if err != nil {
			return nil, fmt.Errorf("failed to access source %s: %w", sourceID, err)
		}

		f, err := fs.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s in source %s: %w", path, sourceID, err)
		}
		return f, nil
	}, false)
	if err != nil {
		panic(fmt.Sprintf("failed to create node reader: %v", err))
	}

	shardsAPI := shards.API{
		Repo:             repo,
		NodeReader:       nr,
		MaxNodesPerIndex: cfg.maxNodesPerIndex,
	}

	storachaAPI := storacha.API{
		Repo:             repo,
		Client:           client,
		CarForShard:      shardsAPI.CarForShard,
		IndexesForUpload: shardsAPI.IndexesForUpload,
	}

	uploadsAPI = uploads.API{
		Repo:                        repo,
		ExecuteScan:                 scansAPI.ExecuteScan,
		ExecuteDagScansForUpload:    dagsAPI.ExecuteDagScansForUpload,
		AddNodeToUploadShards:       shardsAPI.AddNodeToUploadShards,
		CloseUploadShards:           shardsAPI.CloseUploadShards,
		SpaceBlobAddShardsForUpload: storachaAPI.SpaceBlobAddShardsForUpload,
		AddIndexesForUpload:         storachaAPI.AddIndexesForUpload,
		AddStorachaUploadForUpload:  storachaAPI.AddStorachaUploadForUpload,
	}

	return API{
		Spaces:  spacesAPI,
		Uploads: uploadsAPI,
		Sources: sourcesAPI,
		DAGs:    dagsAPI,
		Scans:   scansAPI,
	}
}

func WithGetLocalFSForPathFn(getLocalFSForPathFn func(path string) (fs.FS, error)) Option {
	return func(cfg *config) error {
		cfg.getLocalFSForPathFn = getLocalFSForPathFn
		return nil
	}
}

func WithMaxNodesPerIndex(maxNodesPerIndex int) Option {
	return func(cfg *config) error {
		cfg.maxNodesPerIndex = maxNodesPerIndex
		return nil
	}
}

func (a API) FindOrCreateSpace(ctx context.Context, spaceDID did.DID, name string, options ...spacesmodel.SpaceOption) (*spacesmodel.Space, error) {
	return a.Spaces.FindOrCreateSpace(ctx, spaceDID, name, options...)
}

func (a API) CreateSource(ctx context.Context, name string, path string, options ...sourcesmodel.SourceOption) (*sourcesmodel.Source, error) {
	return a.Sources.CreateSource(ctx, name, path, options...)
}

func (a API) CreateUploads(ctx context.Context, spaceDID did.DID) ([]*uploadsmodel.Upload, error) {
	return a.Uploads.CreateUploads(ctx, spaceDID)
}

func (a API) ExecuteUpload(ctx context.Context, upload *uploadsmodel.Upload) (cid.Cid, error) {
	return a.Uploads.ExecuteUpload(ctx, upload.ID(), upload.SpaceDID())
}
