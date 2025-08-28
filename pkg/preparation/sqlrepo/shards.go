package sqlrepo

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/did"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var _ shards.Repo = (*repo)(nil)

func (r *repo) CreateShard(ctx context.Context, uploadID id.UploadID) (*model.Shard, error) {
	shard, err := model.NewShard(uploadID)
	if err != nil {
		return nil, err
	}

	err = model.WriteShardToDatabase(shard, func(id id.ShardID, uploadID id.UploadID, digest multihash.Multihash, state model.ShardState) error {
		_, err := r.db.ExecContext(ctx, `
			INSERT INTO shards (
				id,
				upload_id,
				digest,
				state
			) VALUES (?, ?, ?, ?)`,
			id,
			uploadID,
			digest,
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
			digest,
			state
		FROM shards
		WHERE upload_id = ?
		  AND state = ?`,
		uploadID,
		state,
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
			digest *multihash.Multihash,
			state *model.ShardState,
		) error {
			return rows.Scan(id, uploadID, util.DbBytes(digest), state)
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

func (r *repo) AddNodeToShard(ctx context.Context, shardID id.ShardID, nodeCID cid.Cid, spaceDID did.DID) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO nodes_in_shards (
			node_cid,
			space_did,
			shard_id
		) VALUES (?, ?, ?)`,
		nodeCID.Bytes(),
		util.DbDID(&spaceDID),
		shardID,
	)
	if err != nil {
		return fmt.Errorf("failed to add node %s to shard %s: %w", nodeCID, shardID, err)
	}
	return nil
}

func (r *repo) FindNodeByCidAndSpaceDID(ctx context.Context, c cid.Cid, spaceDID did.DID) (dagsmodel.Node, error) {
	findQuery := `
		SELECT
			cid,
			size,
			space_did,
			ufsdata,
			path,
			source_id,
			offset
		FROM nodes
		WHERE cid = ? AND space_did = ?
	`
	row := r.db.QueryRowContext(
		ctx,
		findQuery,
		c.Bytes(),
		util.DbDID(&spaceDID),
	)
	return r.getNodeFromRow(row)
}

func (r *repo) ForEachNode(ctx context.Context, shardID id.ShardID, yield func(dagsmodel.Node) error) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			nodes.cid,
			nodes.size,
			nodes.space_did,
			nodes.ufsdata,
			nodes.path,
			nodes.source_id,
			nodes.offset
		FROM nodes_in_shards
		JOIN nodes ON nodes.cid = nodes_in_shards.node_cid AND nodes.space_did = nodes_in_shards.space_did
		WHERE shard_id = ?`,
		shardID,
	)
	if err != nil {
		return fmt.Errorf("failed to get sizes of blocks in shard %s: %w", shardID, err)
	}
	defer rows.Close()

	for rows.Next() {
		nd, err := r.getNodeFromRow(rows)
		if err != nil {
			return fmt.Errorf("failed to get node from row for shard %s: %w", shardID, err)
		}
		if err := yield(nd); err != nil {
			return fmt.Errorf("failed to yield node CID %s for shard %s: %w", nd.CID(), shardID, err)
		}
	}

	return nil
}

// UpdateShard updates a DAG scan in the repository.
func (r *repo) UpdateShard(ctx context.Context, shard *model.Shard) error {
	return model.WriteShardToDatabase(shard, func(id id.ShardID, uploadID id.UploadID, digest multihash.Multihash, state model.ShardState) error {
		_, err := r.db.ExecContext(ctx,
			`UPDATE shards
			SET id = ?,
			    upload_id = ?,
			    digest = ?,
			    state = ?
			WHERE id = ?`,
			id,
			uploadID,
			digest,
			state,
			id,
		)
		return err
	})
}
