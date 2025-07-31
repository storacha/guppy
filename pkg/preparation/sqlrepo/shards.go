package sqlrepo

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-varint"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Byte length of a CBOR encoded CAR header with zero roots.
const noRootsHeaderLen = 17

var _ shards.Repo = (*repo)(nil)

func (r *repo) CreateShard(ctx context.Context, uploadID id.UploadID) (*model.Shard, error) {
	shard, err := model.NewShard(uploadID)
	if err != nil {
		return nil, err
	}

	err = model.WriteShardToDatabase(shard, func(id id.ShardID, uploadID id.UploadID, cid cid.Cid, state model.ShardState) error {
		_, err := r.db.ExecContext(ctx, `
			INSERT INTO shards (
				id,
				upload_id,
				cid,
				state
			) VALUES (?, ?, ?, ?)`,
			id,
			uploadID,
			util.DbCid(&cid),
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
			cid *cid.Cid,
			state *model.ShardState,
		) error {
			return rows.Scan(id, uploadID, util.DbCid(cid), state)
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

func (r *repo) AddNodeToShard(ctx context.Context, shardID id.ShardID, nodeCID cid.Cid) error {
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

func (r *repo) FindNodeByCid(ctx context.Context, c cid.Cid) (dagsmodel.Node, error) {
	findQuery := `
		SELECT
			cid,
			size,
			ufsdata,
			path,
			source_id,
			offset
		FROM nodes
		WHERE cid = ?
	`
	row := r.db.QueryRowContext(
		ctx,
		findQuery,
		c.Bytes(),
	)
	return r.getNodeFromRow(row)
}

func (r *repo) CurrentSizeOfShard(ctx context.Context, shardID id.ShardID) (uint64, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT nodes.cid, nodes.size
		FROM nodes_in_shards
		JOIN nodes ON nodes.cid = nodes_in_shards.node_cid
		WHERE shard_id = ?`,
		shardID,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to get sizes of blocks in shard %s: %w", shardID, err)
	}
	defer rows.Close()

	var totalSize uint64 = noRootsHeaderLen
	for rows.Next() {
		var nodeCID cid.Cid
		var size uint64
		if err := rows.Scan(util.DbCid(&nodeCID), &size); err != nil {
			return 0, fmt.Errorf("failed to scan node for shard %s: %w", shardID, err)
		}
		totalSize += nodeEncodingLength(nodeCID, size)
	}

	return totalSize, nil
}

func nodeEncodingLength(cid cid.Cid, blockSize uint64) uint64 {
	pllen := uint64(len(cidlink.Link{Cid: cid}.Binary())) + blockSize
	vilen := uint64(varint.UvarintSize(uint64(pllen)))
	return pllen + vilen
}

// UpdateShard updates a DAG scan in the repository.
func (r *repo) UpdateShard(ctx context.Context, shard *model.Shard) error {
	return model.WriteShardToDatabase(shard, func(id id.ShardID, uploadID id.UploadID, cid cid.Cid, state model.ShardState) error {
		_, err := r.db.ExecContext(ctx,
			`UPDATE shards
			SET id = ?,
			    upload_id = ?,
			    cid = ?,
			    state = ?
			WHERE id = ?`,
			id,
			uploadID,
			util.DbCid(&cid),
			state,
			id,
		)
		return err
	})
}
