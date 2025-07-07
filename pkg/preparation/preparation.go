package preparation

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/configurations"
	configurationsmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	dags "github.com/storacha/guppy/pkg/preparation/dag"
	"github.com/storacha/guppy/pkg/preparation/scans"
	scansmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	sourcesmodel "github.com/storacha/guppy/pkg/preparation/sources/model"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"

	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/storacha/guppy/pkg/preparation/sources"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/uploads"
)

type Repo interface {
	configurations.Repo
	uploads.Repo
	sources.Repo
	scans.Repo
	dags.Repo
}

type API struct {
	Configurations configurations.ConfigurationsAPI
	Uploads        uploads.Uploads
	Sources        sources.SourcesAPI
	DAGs           dags.DAGAPI
	Scans          scans.Scans
}

func NewAPI(repo Repo) API {
	// The dependencies of the APIs involve a cycle, so we need to declare one
	// first and initialize it last.
	var uploadsAPI uploads.Uploads

	configurationsAPI := configurations.ConfigurationsAPI{
		Repo: repo,
	}

	sourcesAPI := sources.SourcesAPI{
		Repo: repo,
	}

	scansAPI := scans.Scans{
		Repo: repo,
		// Lazy-evaluate `uploadsAPI`, which isn't initialized yet, but will be.
		UploadSourceLookup: func(ctx context.Context, uploadID types.UploadID) (types.SourceID, error) {
			return uploadsAPI.GetSourceIDForUploadID(ctx, uploadID)
		},
		SourceAccessor: sourcesAPI.AccessByID,
		WalkerFn:       walker.WalkDir,
	}

	dagsAPI := dags.DAGAPI{
		Repo:         repo,
		FileAccessor: scansAPI.OpenFileByID,
	}

	uploadsAPI = uploads.Uploads{
		Repo: repo,
		RunNewScan: func(ctx context.Context, uploadID types.UploadID, fsEntryCb func(id types.FSEntryID, isDirectory bool) error) (types.FSEntryID, error) {
			scan, err := repo.CreateScan(ctx, uploadID)
			if err != nil {
				return uuid.Nil, fmt.Errorf("command failed to create new scan: %w", err)
			}

			err = scansAPI.ExecuteScan(ctx, scan, func(entry scansmodel.FSEntry) error {
				fmt.Println("Processing entry:", entry.Path())
				// Process each file system entry here
				return nil
			})

			if err != nil {
				return uuid.Nil, fmt.Errorf("command failed to execute scan: %w", err)
			}

			if scan.State() != scansmodel.ScanStateCompleted {
				return uuid.Nil, fmt.Errorf("scan did not complete successfully, state: %s, error: %w", scan.State(), scan.Error())
			}

			if !scan.HasRootID() {
				return uuid.Nil, errors.New("completed scan did not have a root ID")
			}

			return scan.RootID(), nil
		},
		UploadDAGScanWorker: dagsAPI.UploadDAGScanWorker,
	}

	return API{
		Configurations: configurationsAPI,
		Uploads:        uploadsAPI,
		Sources:        sourcesAPI,
		DAGs:           dagsAPI,
		Scans:          scansAPI,
	}
}

func (a API) CreateConfiguration(ctx context.Context, name string, options ...configurationsmodel.ConfigurationOption) (*configurationsmodel.Configuration, error) {
	return a.Configurations.CreateConfiguration(ctx, name, options...)
}

func (a API) CreateSource(ctx context.Context, name string, path string, options ...sourcesmodel.SourceOption) (*sourcesmodel.Source, error) {
	return a.Sources.CreateSource(ctx, name, path, options...)
}

func (a API) CreateUploads(ctx context.Context, configurationID types.ConfigurationID) ([]*uploadsmodel.Upload, error) {
	return a.Uploads.CreateUploads(ctx, configurationID)
}

func (a API) ExecuteUpload(ctx context.Context, upload *uploadsmodel.Upload, opts ...uploads.ExecutionOption) error {
	return a.Uploads.ExecuteUpload(ctx, upload, opts...)
}
