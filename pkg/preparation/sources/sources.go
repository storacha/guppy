package sources

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// ErrUnrecognizedSourceKind is returned when a source kind is not recognized.
type ErrUnrecognizedSourceKind struct {
	Kind model.SourceKind
}

// Error implements the error interface for ErrUnrecognizedSourceKind.
func (e ErrUnrecognizedSourceKind) Error() string {
	return fmt.Sprintf("unrecognized source kind: %s", e.Kind)
}

// API is the API for accessing and managing sources.
type API struct {
	Repo Repo
}

// Access returns an fs.FS for the given source, or an error if the source kind is not supported.
func (a API) Access(source *model.Source) (fs.FS, error) {
	switch source.Kind() {
	case model.LocalSourceKind:
		return os.DirFS(source.Path()), nil
	default:
		return nil, ErrUnrecognizedSourceKind{Kind: source.Kind()}
	}
}

// AccessByID retrieves a source by its ID and returns an fs.FS for it, or an error if the source kind is not supported.
func (a API) AccessByID(ctx context.Context, sourceID id.SourceID) (fs.FS, error) {
	source, err := a.Repo.GetSourceByID(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get source by ID %s: %w", sourceID, err)
	}
	if source == nil {
		return nil, fmt.Errorf("source with ID %s not found", sourceID)
	}
	return a.Access(source)
}

// AccessByName retrieves a source by its name and returns an fs.FS for it, or an error if the source kind is not supported.
func (a API) AccessByName(ctx context.Context, name string) (fs.FS, error) {
	source, err := a.Repo.GetSourceByName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get source by name %s: %w", name, err)
	}
	if source == nil {
		return nil, fmt.Errorf("source with name %s not found", name)
	}
	return a.Access(source)
}

// CreateSource creates a new source with the given name, path, and options.
func (a API) CreateSource(ctx context.Context, name string, path string, options ...model.SourceOption) (*model.Source, error) {
	return a.Repo.CreateSource(ctx, name, path, options...)
}

// UpdateSource updates the given source in the repository.
func (a API) UpdateSource(ctx context.Context, src *model.Source) error {
	return a.Repo.UpdateSource(ctx, src)
}

// AccessOrCreateByName retrieves a source by its name or creates it if it does not exist.
func (a API) AccessOrCreateByName(ctx context.Context, name string, path string, options ...model.SourceOption) (fs.FS, error) {
	source, err := a.Repo.GetSourceByName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get source by name %s: %w", name, err)
	}
	if source != nil {
		return a.Access(source)
	}

	// Source does not exist, create it
	source, err = a.CreateSource(ctx, name, path, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create source %s: %w", name, err)
	}
	return a.Access(source)
}
