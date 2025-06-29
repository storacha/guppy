package preparation

/*import (
	"github.com/storacha/guppy/pkg/preparation/configurations"
	dags "github.com/storacha/guppy/pkg/preparation/dag"
	"github.com/storacha/guppy/pkg/preparation/sources"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	"github.com/storacha/guppy/pkg/preparation/uploads/scans"
)

type Repo interface {
	configurations.Repo
	uploads.Repo
	sources.Repo
	scans.Repo
	dags.Repo
}

type API struct {
	Configurations configurations.API
	Uploads        uploads.API
	Sources        sources.API
	DAGs           dags.API
	Scans          scans.API
}

func NewAPI(repo Repo) API {
	configurations := configurations.API{
		Repo: repo,
	}
	uploads := uploads.API{
		Repo:                       repo,
		ConfigurationSourcesLookup: configurations.GetSourceIDsForConfiguration,

		Uploads: uploads.API{
			Repo:                       repo,
			ConfigurationSourcesLookup: configurations.GetSourceIDsForConfiguration,
		},
		Sources: sources.NewAPI(repo),
		DAGs:    dags.NewAPI(repo),
		Scans:   scans.NewAPI(repo, uploads.GetSourceIDForUploadID, sources.GetSourceByID),
	}
}*/

// HACKME: This package is a placeholder for the preparation package.

// Intended stack:
// initialize a DB
// initialize a sqlrepo.Repo
// initialize configurations.Configurations
// initialize sources.Sources
// initialize uploads.Uploads - ConfigurationSourcesLookup = Configurations.GetSourceIDsForConfiguration
// initialize scans.Scans - SourceAccessor = Sources.GetSourceByID, UploadSourceLookup = Uploads.GetSourceIDForUploadID
// initialize dagscans.DAGScans - FileAccesor = Scans.OpenFileByID, UnixFSParams = Configurations.GetConfigurationByID (TBD)

// put execution code below...
