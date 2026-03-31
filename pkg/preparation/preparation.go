package preparation

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/pkg/bus"
	clientpkg "github.com/storacha/guppy/pkg/client"
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
	Bus     bus.Bus
}

// Option is an option configuring the API.
type Option func(cfg *config) error

type config struct {
	getLocalFSForPathFn    func(path string) (fs.FS, error)
	maxNodesPerIndex       int
	blobUploadParallelism  int
	assumeUnchangedSources bool
	bus                    bus.Bus
	replicas               uint
	putHTTPClient          *http.Client
}

const (
	defaultBlobUploadParallelism = 6
	defaultReplicas              = 3
)

func NewAPI(repo Repo, client StorachaClient, options ...Option) API {
	cfg := &config{
		blobUploadParallelism: defaultBlobUploadParallelism,
		getLocalFSForPathFn: func(path string) (fs.FS, error) {
			absPath, err := filepath.Abs(path)
			if err != nil {
				return nil, fmt.Errorf("resolving absolute path: %w", err)
			}
			info, err := os.Stat(absPath)
			if err != nil {
				return nil, fmt.Errorf("statting path: %w", err)
			}
			// `os.DirFS()` only accepts directories, so fall back to a sub-FS when
			// the target is a single file.
			if info.IsDir() {
				return os.DirFS(absPath), nil
			}
			dir := filepath.Dir(absPath)
			base := filepath.Base(absPath)
			fsys, err := fs.Sub(os.DirFS(dir), base)
			if err != nil {
				return nil, fmt.Errorf("getting sub fs: %w", err)
			}
			return fsys, nil
		},
		maxNodesPerIndex: defaultMaxNodesPerIndex,
		bus:              &bus.NoopBus{},
		replicas:         defaultReplicas,
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
		Repo:                   repo,
		SourceAccessor:         sourcesAPI.AccessByID,
		WalkerFn:               walker.WalkDir,
		AssumeUnchangedSources: cfg.assumeUnchangedSources,
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

	var blobAddOptions []clientpkg.SpaceBlobAddOption
	if cfg.putHTTPClient != nil {
		blobAddOptions = append(blobAddOptions, clientpkg.WithPutClient(cfg.putHTTPClient))
	}
	storachaAPI := storacha.API{
		Repo:           repo,
		Client:         client,
		ReaderForShard: blobsAPI.ReaderForShard,
		ReaderForIndex: blobsAPI.ReaderForIndex,
		Bus:            cfg.bus,
		Replicas:       cfg.replicas,
		BlobAddOptions: blobAddOptions,
	}

	uploadsAPI = uploads.API{
		Repo:                       repo,
		AssumeUnchangedSources:     cfg.assumeUnchangedSources,
		ExecuteScan:                scansAPI.ExecuteScan,
		ExecuteDagScansForUpload:   dagsAPI.ExecuteDagScansForUpload,
		AddNodesToUploadShards:     blobsAPI.AddNodesToUploadShards,
		AddShardsToUploadIndexes:   blobsAPI.AddShardsToUploadIndexes,
		CloseUploadShards:          blobsAPI.CloseUploadShards,
		CloseUploadIndexes:         blobsAPI.CloseUploadIndexes,
		FindShardAddTasksForUpload: storachaAPI.FindShardAddTasksForUpload,
		FindIndexAddTasksForUpload: storachaAPI.FindIndexAddTasksForUpload,
		BlobUploadParallelism:      cfg.blobUploadParallelism,
		FindShardPostProcessTasksForUpload: storachaAPI.FindShardPostProcessTasksForUpload,
		FindIndexPostProcessTasksForUpload: storachaAPI.FindIndexPostProcessTasksForUpload,
		AddStorachaUploadForUpload: storachaAPI.AddStorachaUploadForUpload,
		RemoveBadFSEntry:           scansAPI.RemoveBadFSEntry,
		RemoveBadNodes:             dagsAPI.RemoveBadNodes,
		RemoveShard:                blobsAPI.RemoveShard,
		Publisher:                  cfg.bus,
	}

	return API{
		Repo:    repo,
		Spaces:  spacesAPI,
		Uploads: uploadsAPI,
		Sources: sourcesAPI,
		DAGs:    dagsAPI,
		Scans:   scansAPI,
		Bus:     cfg.bus,
	}
}

func WithEventBus(bus bus.Bus) Option {
	return func(cfg *config) error {
		cfg.bus = bus
		return nil
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

func WithAssumeUnchangedSources(assume bool) Option {
	return func(cfg *config) error {
		cfg.assumeUnchangedSources = assume
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

// WithReplicas sets the number of replicas to use when uploading blobs,
// including the original. The default is 3 replicas.
func WithReplicas(replicas uint) Option {
	return func(cfg *config) error {
		if replicas == 0 {
			return errors.New("replica count must be greater than 0")
		}
		cfg.replicas = replicas
		return nil
	}
}

// WithPutHTTPClient sets the HTTP client used for blob PUT uploads.
func WithPutHTTPClient(c *http.Client) Option {
	return func(cfg *config) error {
		cfg.putHTTPClient = c
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
