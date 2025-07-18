package sqlrepo

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var _ shards.Repo = (*repo)(nil)

func (r *repo) AddNodeToUploadShard(ctx context.Context, uploadID id.UploadID, nodeCID cid.Cid) error {
	shard, err := r.createShard(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to add node %s to shards for upload %s: %w", nodeCID, uploadID, err)
	}

	err = r.addNodeToShard(ctx, shard.ID(), nodeCID)
	if err != nil {
		return fmt.Errorf("failed to add node %s to shard %s for upload %s: %w", nodeCID, shard.ID(), uploadID, err)
	}
	return nil
}

func (r *repo) createShard(ctx context.Context, uploadID id.UploadID) (*model.Shard, error) {
	shard, err := model.NewShard(uploadID)
	if err != nil {
		return nil, err
	}

	err = model.WriteShardToDatabase(shard, func(id id.ShardID, uploadID id.UploadID, cid *cid.Cid, state model.ShardState) error {
		_, err := r.db.ExecContext(ctx, `
			INSERT INTO shards (
				id,
				upload_id,
				cid,
				state
			) VALUES (?, ?, ?, ?)`,
			id,
			uploadID,
			Null(cid),
			state,
		)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to write shard for upload %s: %w", uploadID, err)
	}

	return shard, nil
}

func (r *repo) ShardsForUploadByStatus(ctx context.Context, uploadID id.UploadID, state model.ShardState) ([]*model.Shard, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			id,
			upload_id,
			cid,
			state
		FROM shards WHERE upload_id = ?`, uploadID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var shards []*model.Shard
	for rows.Next() {
		shard, err := model.ReadShardFromDatabase(func(
			id *id.ShardID,
			uploadID *id.UploadID,
			cid **cid.Cid,
			state *model.ShardState,
		) error {
			return rows.Scan(id, uploadID, cid, state)
		})
		if err != nil {
			return nil, err
		}
		if shard == nil {
			continue
		}
		shards = append(shards, shard)
	}
	return shards, nil
}

func (r *repo) addNodeToShard(ctx context.Context, shardID id.ShardID, nodeCID cid.Cid) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO nodes_in_shards (
			node_cid,
			shard_id
		) VALUES (?, ?)`,
		nodeCID.Bytes(),
		shardID,
	)
	if err != nil {
		return fmt.Errorf("failed to add node %s to shard %s: %w", nodeCID, shardID, err)
	}
	return nil
}
