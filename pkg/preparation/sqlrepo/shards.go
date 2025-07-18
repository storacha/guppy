package sqlrepo

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-varint"
	configmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/shards"
	"github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Byte length of a CBOR encoded CAR header with zero roots.
const noRootsHeaderLen = 17

var _ shards.Repo = (*repo)(nil)

func (r *repo) AddNodeToUploadShards(ctx context.Context, uploadID id.UploadID, nodeCID cid.Cid) error {
	config, err := r.GetConfigurationByUploadID(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to get configuration for upload %s: %w", uploadID, err)
	}
	openShards, err := r.ShardsForUploadByStatus(ctx, uploadID, model.ShardStateOpen)
	if err != nil {
		return fmt.Errorf("failed to get open shards for upload %s: %w", uploadID, err)
	}

	var shard *model.Shard

	// Look for an open shard that has room for the node, and close any that don't
	// have room. (There should only be at most one open shard, but there's no
	// harm handling multiple if they exist.)
	for _, s := range openShards {
		hasRoom, err := r.roomInShard(ctx, s, nodeCID, config)
		if err != nil {
			return fmt.Errorf("failed to check room in shard %s for node %s: %w", s.ID(), nodeCID, err)
		}
		if hasRoom {
			shard = s
			break
		}
		s.Close()
		if err := r.UpdateShard(ctx, s); err != nil {
			return fmt.Errorf("updating scan: %w", err)
		}
	}

	// If no such shard exists, create a new one
	if shard == nil {
		shard, err = r.createShard(ctx, uploadID)
		if err != nil {
			return fmt.Errorf("failed to add node %s to shards for upload %s: %w", nodeCID, uploadID, err)
		}
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

func (r *repo) roomInShard(ctx context.Context, shard *model.Shard, nodeCID cid.Cid, config *configmodel.Configuration) (bool, error) {
	node, err := r.findNodeByCid(ctx, nodeCID)
	if err != nil {
		return false, fmt.Errorf("failed to find node %s: %w", nodeCID, err)
	}
	if node == nil {
		return false, fmt.Errorf("node %s not found", nodeCID)
	}
	nodeSize := nodeEncodingLength(nodeCID, node.Size())

	currentSize, err := r.currentSizeOfShard(ctx, shard.ID())
	if err != nil {
		return false, fmt.Errorf("failed to get current size of shard %s: %w", shard.ID(), err)
	}

	if currentSize+nodeSize > config.ShardSize() {
		return false, nil // No room in the shard
	}

	return true, nil
}

func (r *repo) currentSizeOfShard(ctx context.Context, shardID id.ShardID) (uint64, error) {
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
		if err := rows.Scan(util.CidScanner{Dst: &nodeCID}, &size); err != nil {
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
	return model.WriteShardToDatabase(shard, func(id id.ShardID, uploadID id.UploadID, cid *cid.Cid, state model.ShardState) error {
		_, err := r.db.ExecContext(ctx,
			`UPDATE shards
			SET id = ?,
			    upload_id = ?,
			    cid = ?,
			    state = ?
			WHERE id = ?`,
			id,
			uploadID,
			Null(cid),
			state,
			id,
		)
		return err
	})
}
