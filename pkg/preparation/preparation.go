package preparation

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/pkg/preparation/blobs"
	"github.com/storacha/guppy/pkg/preparation/dags"
	"github.com/storacha/guppy/pkg/preparation/dags/nodereader"
	"github.com/storacha/guppy/pkg/preparation/scans"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/storacha/guppy/pkg/preparation/sources"
	sourcesmodel "github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/spaces"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/storacha"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

// By back-of-the-napkin math, this represents about 1MB max per index
const defaultMaxNodesPerIndex = 26000

type Repo interface {
	spaces.Repo
	uploads.Repo
	sources.Repo
	scans.Repo
	dags.Repo
	blobs.Repo
}

type StorachaClient = storacha.Client

type API struct {
	Repo    Repo
	Spaces  spaces.API
	Uploads uploads.API
	Sources sources.API
	DAGs    dags.API
	Scans   scans.API
}

// Option is an option configuring the API.
type Option func(cfg *config) error

type config struct {
	getLocalFSForPathFn   func(path string) (fs.FS, error)
	maxNodesPerIndex      int
	blobUploadParallelism int
}

const defaultBlobUploadParallelism = 6

func NewAPI(repo Repo, client StorachaClient, options ...Option) API {
	cfg := &config{
		blobUploadParallelism: defaultBlobUploadParallelism,
		getLocalFSForPathFn: func(path string) (fs.FS, error) {
			// A bit odd, but `fs.Sub()` happens to be okay with referring directly to
			// a single file, where opening `"."` gives you the file. `os.DirFS()`
			// gets grumpy if it's pointing at a file, using `fs.Sub()` over it works
			// just fine. So by being a little roundabout here, we can support both
			// single files and directories transparently.
			path, err := filepath.Rel("/", path)
			if err != nil {
				return nil, fmt.Errorf("getting relative path: %w", err)
			}
			fsys, err := fs.Sub(os.DirFS("/"), path)
			if err != nil {
				return nil, fmt.Errorf("getting sub fs: %w", err)
			}
			return fsys, nil
		},
		maxNodesPerIndex: defaultMaxNodesPerIndex,
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

	nr, err := nodereader.NewNodeReaderOpener(repo.LinksForCID, func(ctx context.Context, sourceID id.SourceID, path string) (fs.File, error) {
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
	}, true)
	if err != nil {
		panic(fmt.Sprintf("failed to create node reader: %v", err))
	}

	blobsAPI := &blobs.API{
		Repo:             repo,
		OpenNodeReader:   nr.OpenNodeReader,
		MaxNodesPerIndex: cfg.maxNodesPerIndex,
		ShardEncoder:     blobs.NewCAREncoder(),
	}

	storachaAPI := storacha.API{
		Repo:                  repo,
		Client:                client,
		ReaderForShard:        blobsAPI.ReaderForShard,
		ReaderForIndex:        blobsAPI.ReaderForIndex,
		BlobUploadParallelism: cfg.blobUploadParallelism,
	}

	uploadsAPI = uploads.API{
		Repo:                       repo,
		ExecuteScan:                scansAPI.ExecuteScan,
		ExecuteDagScansForUpload:   dagsAPI.ExecuteDagScansForUpload,
		AddNodesToUploadShards:     blobsAPI.AddNodesToUploadShards,
		AddShardToUploadIndexes:    blobsAPI.AddShardToUploadIndexes,
		CloseUploadShards:          blobsAPI.CloseUploadShards,
		CloseUploadIndexes:         blobsAPI.CloseUploadIndexes,
		AddShardsForUpload:         storachaAPI.AddShardsForUpload,
		PostProcessUploadedShards:  storachaAPI.PostProcessUploadedShards,
		PostProcessUploadedIndexes: storachaAPI.PostProcessUploadedIndexes,
		AddIndexesForUpload:        storachaAPI.AddIndexesForUpload,
		AddStorachaUploadForUpload: storachaAPI.AddStorachaUploadForUpload,
		RemoveBadFSEntry:           scansAPI.RemoveBadFSEntry,
		RemoveBadNodes:             dagsAPI.RemoveBadNodes,
		RemoveShard:                blobsAPI.RemoveShard,
	}

	return API{
		Repo:    repo,
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

func WithBlobUploadParallelism(blobUploadParallelism int) Option {
	return func(cfg *config) error {
		if blobUploadParallelism <= 0 {
			return fmt.Errorf("parallelism must be greater than 0")
		}
		cfg.blobUploadParallelism = blobUploadParallelism
		return nil
	}
}

func (a API) FindOrCreateSpace(ctx context.Context, spaceDID did.DID, name string, options ...spacesmodel.SpaceOption) (*spacesmodel.Space, error) {
	return a.Spaces.FindOrCreateSpace(ctx, spaceDID, name, options...)
}

func (a API) CreateSource(ctx context.Context, name string, path string, options ...sourcesmodel.SourceOption) (*sourcesmodel.Source, error) {
	return a.Sources.CreateSource(ctx, name, path, options...)
}

func (a API) FindOrCreateUploads(ctx context.Context, spaceDID did.DID) ([]*uploadsmodel.Upload, error) {
	return a.Uploads.FindOrCreateUploads(ctx, spaceDID)
}

func (a API) GetUploadByID(ctx context.Context, uploadID id.UploadID) (*uploadsmodel.Upload, error) {
	return a.Uploads.GetUploadByID(ctx, uploadID)
}

func (a API) AddSourceToSpace(ctx context.Context, spaceDID did.DID, sourceID id.SourceID) error {
	return a.Repo.AddSourceToSpace(ctx, spaceDID, sourceID)
}

func (a API) ExecuteUpload(ctx context.Context, upload *uploadsmodel.Upload) (cid.Cid, error) {
	return a.Uploads.ExecuteUpload(ctx, upload.ID(), upload.SpaceDID())
}
