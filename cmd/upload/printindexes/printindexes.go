package printindexes

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/preparation"
	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var Cmd = &cobra.Command{
	Use:   "print-indexes <space> [source-path-or-name]",
	Short: "Print index details for uploads",
	Args:  cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return err
		}

		repo, err := preparation.OpenRepo(ctx, cfg.Repo)
		if err != nil {
			return err
		}
		defer repo.Close()

		client := cmdutil.MustGetClient(cfg.Repo.Dir, cfg.Network)
		spaceDID, err := cmdutil.ResolveSpace(client, args[0])
		if err != nil {
			return err
		}

		var uploads []uploadInfo
		switch len(args) {
		case 1:
			uploads, err = getUploadsForSpace(ctx, repo, spaceDID)
			if err != nil {
				return fmt.Errorf("getting uploads for space %s: %w", spaceDID, err)
			}
		case 2:
			u, err := getUploadForSpaceAndSource(ctx, repo, spaceDID, args[1])
			if err != nil {
				return fmt.Errorf("getting upload for space %s and source %s: %w", spaceDID, args[1], err)
			}
			uploads = []uploadInfo{u}
		}

		if len(uploads) == 0 {
			cmd.Printf("No uploads found for space %s.\n", spaceDID)
			return nil
		}

		for _, u := range uploads {
			cmd.Printf("Upload: %s\n", u.displayName)

			indexes, err := repo.IndexesForUpload(ctx, u.uploadID)
			if err != nil {
				return fmt.Errorf("getting indexes for upload %s: %w", u.displayName, err)
			}

			if len(indexes) == 0 {
				cmd.Println("  No indexes found.")
				continue
			}

			for _, idx := range indexes {
				printIndex(cmd, idx)
			}
		}

		return nil
	},
}

func printIndex(cmd *cobra.Command, idx *model.Index) {
	cmd.Printf("  Index: %s\n", idx.ID())

	if len(idx.Digest()) > 0 {
		cmd.Printf("    Digest:      %x\n", []byte(idx.Digest()))
	} else {
		cmd.Printf("    Digest:      (none)\n")
	}

	if idx.PieceCID() != cid.Undef {
		cmd.Printf("    Piece CID:   %s\n", idx.PieceCID())
	} else {
		cmd.Printf("    Piece CID:   (none)\n")
	}

	cmd.Printf("    Size:        %d\n", idx.Size())
	cmd.Printf("    Slice Count: %d\n", idx.SliceCount())
	cmd.Printf("    State:       %s\n", idx.State())

	loc := idx.Location()
	if loc == nil {
		cmd.Printf("    Location:    (none)\n")
		return
	}

	cmd.Printf("    Location:\n")
	cmd.Printf("      Issuer:    %s\n", loc.Issuer().DID().String())
	cmd.Printf("      Audience:  %s\n", loc.Audience().DID().String())

	caps := loc.Capabilities()
	if len(caps) == 0 {
		cmd.Printf("      (no capabilities)\n")
		return
	}

	cap := caps[0]
	cmd.Printf("      Subject:   %s\n", cap.With())
	cmd.Printf("      Ability:   %s\n", cap.Can())
	cmd.Printf("      Caveats:   %v\n", cap.Nb())
}

type uploadInfo struct {
	uploadID    id.UploadID
	displayName string
}

func getUploadsForSpace(ctx context.Context, repo preparation.Repo, spaceDID did.DID) ([]uploadInfo, error) {
	sourceIDs, err := repo.ListSpaceSources(ctx, spaceDID)
	if err != nil {
		return nil, fmt.Errorf("listing space sources: %w", err)
	}

	if len(sourceIDs) == 0 {
		return nil, nil
	}

	uploads, err := repo.FindOrCreateUploads(ctx, spaceDID, sourceIDs)
	if err != nil {
		return nil, fmt.Errorf("finding uploads: %w", err)
	}

	result := make([]uploadInfo, 0, len(uploads))
	for _, upload := range uploads {
		source, err := repo.GetSourceByID(ctx, upload.SourceID())
		if err != nil {
			return nil, fmt.Errorf("getting source %s: %w", upload.SourceID(), err)
		}

		spaceName := spaceDID.String()
		if len(spaceName) > 20 {
			spaceName = spaceName[:20] + "..."
		}

		result = append(result, uploadInfo{
			uploadID:    upload.ID(),
			displayName: fmt.Sprintf("%s / %s", spaceName, source.Path()),
		})
	}

	return result, nil
}

func getUploadForSpaceAndSource(ctx context.Context, repo preparation.Repo, spaceDID did.DID, sourceArg string) (uploadInfo, error) {
	sourceIDs, err := repo.ListSpaceSources(ctx, spaceDID)
	if err != nil {
		return uploadInfo{}, fmt.Errorf("listing space sources: %w", err)
	}

	if len(sourceIDs) == 0 {
		return uploadInfo{}, fmt.Errorf("no sources found for space %s", spaceDID)
	}

	uploads, err := repo.FindOrCreateUploads(ctx, spaceDID, sourceIDs)
	if err != nil {
		return uploadInfo{}, fmt.Errorf("finding uploads: %w", err)
	}

	cleanSourceArg := filepath.Clean(sourceArg)

	for _, upload := range uploads {
		source, err := repo.GetSourceByID(ctx, upload.SourceID())
		if err != nil {
			continue
		}

		cleanPath := filepath.Clean(source.Path())
		if cleanPath == cleanSourceArg || source.Name() == sourceArg {
			spaceName := spaceDID.String()
			if len(spaceName) > 20 {
				spaceName = spaceName[:20] + "..."
			}

			return uploadInfo{
				uploadID:    upload.ID(),
				displayName: fmt.Sprintf("%s / %s", spaceName, source.Path()),
			}, nil
		}
	}

	return uploadInfo{}, fmt.Errorf("source %q not found in space %s", sourceArg, spaceDID)
}
