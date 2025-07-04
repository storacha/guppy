package scans_test

import (
	"context"
	"fmt"
	"io/fs"
	"testing"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/scans"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/testutil"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/stretchr/testify/require"
)

type repoErrOnUpdateScan struct {
	scans.Repo
}

func (m repoErrOnUpdateScan) UpdateScan(ctx context.Context, scan *model.Scan) error {
	// Simulate an error when updating the scan
	return fmt.Errorf("error updating scan with ID %s", scan.ID())
}

var _ scans.Repo = (*repoErrOnUpdateScan)(nil)

func newScanAndProcess(t *testing.T) (*model.Scan, scans.Scans) {
	uploadID := uuid.New()
	sourceID := uuid.New()
	scan, err := model.NewScan(uploadID)
	if err != nil {
		panic(fmt.Sprintf("failed to create new scan: %v", err))
	}

	scansProcess := scans.Scans{
		Repo: sqlrepo.New(testutil.CreateTestDB(t)),
		UploadSourceLookup: func(ctx context.Context, uploadID types.UploadID) (types.SourceID, error) {
			return sourceID, nil
		},
		SourceAccessor: func(ctx context.Context, sourceID types.SourceID) (fs.FS, error) {
			return nil, nil
		},
		WalkerFn: func(fsys fs.FS, root string, visitor walker.FSVisitor) (model.FSEntry, error) {
			return nil, nil
		},
	}

	return scan, scansProcess
}

func TestExecuteScan(t *testing.T) {
	t.Run("with an error updating the scan", func(t *testing.T) {
		scan, scansProcess := newScanAndProcess(t)
		scansProcess.Repo = repoErrOnUpdateScan{}

		err := scansProcess.ExecuteScan(t.Context(), scan, func(entry model.FSEntry) error {
			return nil
		})

		require.ErrorContains(t, err, "updating scan: error updating scan with ID")
	})

	t.Run("with an error looking up the source ID", func(t *testing.T) {
		scan, scansProcess := newScanAndProcess(t)
		scansProcess.UploadSourceLookup = func(ctx context.Context, uploadID types.UploadID) (types.SourceID, error) {
			return types.SourceID{}, fmt.Errorf("couldn't look up source ID for upload ID %s", uploadID)
		}

		err := scansProcess.ExecuteScan(t.Context(), scan, func(entry model.FSEntry) error {
			return nil
		})

		require.NoError(t, err, "should not return an error when scan itself is failed")
		require.Equal(t, model.ScanStateFailed, scan.State())
		require.ErrorContains(t, scan.Error(), "looking up source ID: couldn't look up source ID for upload ID")
	})

	t.Run("with an error accessing the source", func(t *testing.T) {
		scan, scansProcess := newScanAndProcess(t)
		scansProcess.SourceAccessor = func(ctx context.Context, sourceID types.SourceID) (fs.FS, error) {
			return nil, fmt.Errorf("couldn't access source for source ID %s", sourceID)
		}

		err := scansProcess.ExecuteScan(t.Context(), scan, func(entry model.FSEntry) error {
			return nil
		})

		require.NoError(t, err, "should not return an error when scan itself is failed")
		require.Equal(t, model.ScanStateFailed, scan.State())
		require.ErrorContains(t, scan.Error(), "accessing source: couldn't access source for source ID")
	})

	t.Run("with an error walking the source", func(t *testing.T) {
		scan, scansProcess := newScanAndProcess(t)
		scansProcess.WalkerFn = func(fsys fs.FS, root string, visitor walker.FSVisitor) (model.FSEntry, error) {
			return nil, fmt.Errorf("error walking the source at root %s", root)
		}

		err := scansProcess.ExecuteScan(t.Context(), scan, func(entry model.FSEntry) error {
			return nil
		})

		require.NoError(t, err, "should not return an error when scan itself is failed")
		require.Equal(t, model.ScanStateFailed, scan.State())
		require.ErrorContains(t, scan.Error(), "recursively creating directories: error walking the source at root")
	})

	t.Run("when the context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		scan, scansProcess := newScanAndProcess(t)
		scansProcess.WalkerFn = func(fsys fs.FS, root string, visitor walker.FSVisitor) (model.FSEntry, error) {
			cancel() // Cancel the context to simulate a cancelation
			return nil, ctx.Err()
		}

		err := scansProcess.ExecuteScan(ctx, scan, func(entry model.FSEntry) error {
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, model.ScanStateCanceled, scan.State())
	})
}
