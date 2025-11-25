package repo

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"

	"github.com/storacha/guppy/pkg/agentdata"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
)

func Open(cfg config.RepoConfig) (*FsRepo, error) {
	stat, err := os.Stat(cfg.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			// initialize path if DNE
			if err := os.MkdirAll(cfg.Dir, os.ModePerm); err != nil {
				return nil, err
			}
		}
		return nil, err
	}
	// must be a dir
	if !stat.IsDir() {
		return nil, fmt.Errorf("repo '%s' is not a directory", cfg.Dir)
	}

	return &FsRepo{path: cfg.Dir}, nil
}

type FsRepo struct {
	path string
}

const agentDataFileName = "store.json"

func (r *FsRepo) ReadAgentData() (agentdata.AgentData, error) {
	return agentdata.ReadFromFile(r.join(agentDataFileName))
}

func (r *FsRepo) WriteAgentData(data agentdata.AgentData) error {
	return data.WriteToFile(r.join(agentDataFileName))
}

const uploadDBFileName = "preparation.db"

func (r *FsRepo) OpenSQLRepo(ctx context.Context) (*sqlrepo.Repo, error) {
	dbPath := r.join(filepath.Join(r.path, uploadDBFileName))
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database at %s: %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)

	_, err = db.ExecContext(ctx, sqlrepo.Schema)
	if err != nil {
		return nil, fmt.Errorf("command failed to execute schema: %w", err)
	}

	repo := sqlrepo.New(db)
	return repo, nil
}

func (r *FsRepo) join(paths ...string) string {
	return filepath.Join(append([]string{r.path}, paths...)...)
}

const spaceStoreFileName = "spaces.json"

func (r *FsRepo) SpaceStore() (*SpaceStore, error) {
	spaceStore := &SpaceStore{path: r.join(spaceStoreFileName)}
	storePath := r.join(spaceStoreFileName)
	if _, err := os.Stat(storePath); os.IsNotExist(err) {
		return spaceStore, spaceStore.init()
	}
	return spaceStore, nil
}
