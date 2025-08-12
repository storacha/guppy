package indexes

import (
	"context"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/guppy/pkg/preparation/indexes/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var log = logging.Logger("preparation/indexes")

// API provides index generation functionality
type API struct {
	Repo Repo
}

// GenerateIndex creates an index manifest for all shards in an upload
func (a *API) GenerateIndex(ctx context.Context, uploadID id.UploadID) error {
	log.Debugf("Generating index for upload %s", uploadID)

	// Get all shards for this upload
	shards, err := a.Repo.GetShardsForUpload(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("getting shards: %w", err)
	}

	if len(shards) == 0 {
		log.Debugf("No shards found for upload %s", uploadID)
		return nil
	}

	// Create index manifest
	manifest, err := model.NewIndexManifest(uploadID)
	if err != nil {
		return fmt.Errorf("creating manifest: %w", err)
	}

	// Process each shard
	for _, shard := range shards {
		blocks, err := a.Repo.GetShardBlocks(ctx, shard.ID)
		if err != nil {
			return fmt.Errorf("getting blocks for shard %s: %w", shard.ID, err)
		}

		// Convert to JSON-serializable format
		jsonBlocks := make([]model.ShardBlockJSON, 0, len(blocks))
		for _, block := range blocks {
			jsonBlocks = append(jsonBlocks, block.ToJSON())
		}

		shardIndex := model.ShardIndex{
			ShardCID: shard.CID.String(),
			Blocks:   jsonBlocks,
		}
		manifest.AddShard(shardIndex)
	}

	// Save manifest as JSON
	manifestJSON, err := manifest.ToJSON()
	if err != nil {
		return fmt.Errorf("serializing manifest: %w", err)
	}

	if err := a.Repo.SaveIndexManifest(ctx, uploadID, manifestJSON); err != nil {
		return fmt.Errorf("saving manifest: %w", err)
	}

	log.Debugf("Generated index for upload %s with %d shards", uploadID, len(shards))
	return nil
}

// GetIndex retrieves the index manifest for an upload
func (a *API) GetIndex(ctx context.Context, uploadID id.UploadID) (*model.IndexManifest, error) {
	return a.Repo.GetIndexManifest(ctx, uploadID)
}
