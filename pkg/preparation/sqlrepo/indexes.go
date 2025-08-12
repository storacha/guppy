package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/indexes"
	"github.com/storacha/guppy/pkg/preparation/indexes/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Implement the indexes.Repo interface
var _ indexes.Repo = (*repo)(nil)

// AddShardBlock records a block's position within a shard
func (r *repo) AddShardBlock(ctx context.Context, shardID id.ShardID, blockCID cid.Cid, offset, size uint64) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO shard_blocks (shard_id, block_cid, offset, size) 
		VALUES (?, ?, ?, ?)`,
		shardID, blockCID.Bytes(), offset, size)
	return err
}

// GetShardBlocks retrieves all blocks for a shard with their positions
func (r *repo) GetShardBlocks(ctx context.Context, shardID id.ShardID) ([]*model.ShardBlock, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT block_cid, offset, size 
		FROM shard_blocks 
		WHERE shard_id = ? 
		ORDER BY offset`,
		shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var blocks []*model.ShardBlock
	for rows.Next() {
		var cidBytes []byte
		var offset, size uint64

		if err := rows.Scan(&cidBytes, &offset, &size); err != nil {
			return nil, err
		}

		blockCID, err := cid.Cast(cidBytes)
		if err != nil {
			return nil, err
		}

		block, err := model.NewShardBlock(blockCID, offset, size)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, block)
	}

	return blocks, rows.Err()
}

// SaveIndexManifest stores a JSON index manifest for an upload
func (r *repo) SaveIndexManifest(ctx context.Context, uploadID id.UploadID, manifestJSON []byte) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO index_manifests (upload_id, manifest_data, created_at) 
		VALUES (?, ?, ?)`,
		uploadID, string(manifestJSON), time.Now().Unix())
	return err
}

// GetIndexManifest retrieves the index manifest for an upload
func (r *repo) GetIndexManifest(ctx context.Context, uploadID id.UploadID) (*model.IndexManifest, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT manifest_data 
		FROM index_manifests 
		WHERE upload_id = ?`,
		uploadID)

	var manifestJSON string
	err := row.Scan(&manifestJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return model.FromJSON([]byte(manifestJSON))
}

// GetShardsForUpload retrieves all shards associated with an upload
func (r *repo) GetShardsForUpload(ctx context.Context, uploadID id.UploadID) ([]indexes.ShardInfo, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, cid 
		FROM shards 
		WHERE upload_id = ? AND state = 'closed'`,
		uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var shards []indexes.ShardInfo
	for rows.Next() {
		var shardID id.ShardID
		var cidBytes []byte

		if err := rows.Scan(&shardID, &cidBytes); err != nil {
			return nil, err
		}

		var shardCID cid.Cid
		if len(cidBytes) > 0 {
			shardCID, err = cid.Cast(cidBytes)
			if err != nil {
				return nil, err
			}
		}

		shards = append(shards, indexes.ShardInfo{
			ID:  shardID,
			CID: shardCID,
		})
	}

	return shards, rows.Err()
}
