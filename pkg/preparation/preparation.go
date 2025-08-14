package preparation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	ipldcar "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/configurations"
	configurationsmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/dags"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/scans"
	scansmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/storacha/guppy/pkg/preparation/shards"
	shardsmodel "github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sources"
	sourcesmodel "github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var log = logging.Logger("preparation")

type Repo interface {
	configurations.Repo
	uploads.Repo
	sources.Repo
	scans.Repo
	dags.Repo
	shards.Repo
}

type API struct {
	Configurations configurations.API
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

func NewAPI(repo Repo, client shards.SpaceBlobAdder, space did.DID, options ...Option) API {
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

	configurationsAPI := configurations.API{
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
		Repo:   repo,
		Client: client,
		Space:  space,
		CarForShard: func(ctx context.Context, shard *shardsmodel.Shard) (io.Reader, error) {
			var buf bytes.Buffer

			header, err := cbor.DumpObject(
				ipldcar.CarHeader{
					Roots:   nil,
					Version: 1,
				},
			)
			if err != nil {
				return nil, fmt.Errorf("dumping CAR header: %w", err)
			}

			err = util.LdWrite(&buf, header)
			if err != nil {
				return nil, fmt.Errorf("writing CAR header: %w", err)
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

			err = repo.ForEachNode(ctx, shard.ID(), func(node dagsmodel.Node) error {
				data, err := nr.GetData(ctx, node)
				if err != nil {
					return fmt.Errorf("getting data for node %s: %w", node.CID(), err)
				}

				err = util.LdWrite(&buf, []byte(node.CID().KeyString()), data)
				if err != nil {
					return fmt.Errorf("writing CAR block for CID %s: %w", node.CID(), err)
				}

				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("failed to iterate over nodes in shard %s: %w", shard.ID(), err)
			}

			return bytes.NewReader(buf.Bytes()), nil
		},
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
		RestartDagScansForUpload:    dagsAPI.RestartDagScansForUpload,
		RunDagScansForUpload:        dagsAPI.RunDagScansForUpload,
		AddNodeToUploadShards:       shardsAPI.AddNodeToUploadShards,
		CloseUploadShards:           shardsAPI.CloseUploadShards,
		SpaceBlobAddShardsForUpload: shardsAPI.SpaceBlobAddShardsForUpload,
	}

	return API{
		Configurations: configurationsAPI,
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

func (a API) CreateConfiguration(ctx context.Context, name string, options ...configurationsmodel.ConfigurationOption) (*configurationsmodel.Configuration, error) {
	return a.Configurations.CreateConfiguration(ctx, name, options...)
}

func (a API) CreateSource(ctx context.Context, name string, path string, options ...sourcesmodel.SourceOption) (*sourcesmodel.Source, error) {
	return a.Sources.CreateSource(ctx, name, path, options...)
}

func (a API) CreateUploads(ctx context.Context, configurationID id.ConfigurationID) ([]*uploadsmodel.Upload, error) {
	return a.Uploads.CreateUploads(ctx, configurationID)
}

func (a API) ExecuteUpload(ctx context.Context, upload *uploadsmodel.Upload) (cid.Cid, error) {
	return a.Uploads.ExecuteUpload(ctx, upload)
}
