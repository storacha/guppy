package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/blobs"
	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var _ blobs.Repo = (*Repo)(nil)

func (r *Repo) CreateShard(ctx context.Context, uploadID id.UploadID, size uint64, digestState, pieceCidState []byte) (*model.Shard, error) {
	shard, err := model.NewShard(uploadID, size, digestState, pieceCidState)
	if err != nil {
		return nil, err
	}

	err = model.WriteShardToDatabase(shard, func(
		id id.ShardID,
		uploadID id.UploadID,
		size uint64,
		sliceCount int,
		digest multihash.Multihash,
		pieceCID cid.Cid,
		digestStateUpTo uint64,
		digestState []byte,
		pieceCIDState []byte,
		state model.BlobState,
		location invocation.Invocation,
		pdpAccept invocation.Invocation,
	) error {
		stmt, err := r.prepareStmt(ctx, `
			INSERT INTO shards (
				id,
				upload_id,
				size,
				slice_count,
				digest,
				piece_cid,
				digest_state_up_to,
				digest_state,
				piece_cid_state,
				state,
				location_inv,
				pdp_accept_inv
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		_, err = stmt.ExecContext(ctx,
			id,
			uploadID,
			size,
			sliceCount,
			util.DbBytes(&digest),
			util.DbCID(&pieceCID),
			digestStateUpTo,
			util.DbBytes(&digestState),
			util.DbBytes(&pieceCIDState),
			state,
			util.DbInvocation(&location),
			util.DbInvocation(&pdpAccept),
		)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to write shard for upload %s: %w", uploadID, err)
	}

	return shard, nil
}

func (r *Repo) ShardsForUploadByState(ctx context.Context, uploadID id.UploadID, state model.BlobState) ([]*model.Shard, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT
			id,
			upload_id,
			size,
			slice_count,
			digest,
			piece_cid,
			digest_state_up_to,
			digest_state,
			piece_cid_state,
			state,
			location_inv,
			pdp_accept_inv
		FROM shards
		WHERE upload_id = ?
		  AND state = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, uploadID, state)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var shards []*model.Shard
	for rows.Next() {
		shard, err := model.ReadShardFromDatabase(func(
			id *id.ShardID,
			uploadID *id.UploadID,
			size *uint64,
			sliceCount *int,
			digest *multihash.Multihash,
			pieceCID *cid.Cid,
			digestStateUpTo *uint64,
			digestState *[]byte,
			pieceCIDState *[]byte,
			state *model.BlobState,
			location *invocation.Invocation,
			pdpAccept *invocation.Invocation,
		) error {
			return rows.Scan(id, uploadID, size, sliceCount, util.DbBytes(digest), util.DbCID(pieceCID), digestStateUpTo, util.DbBytes(digestState), util.DbBytes(pieceCIDState), state, util.DbInvocation(location), util.DbInvocation(pdpAccept))
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

func (r *Repo) GetShardByID(ctx context.Context, shardID id.ShardID) (*model.Shard, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT
			id,
			upload_id,
			size,
			slice_count,
			digest,
			piece_cid,
			digest_state_up_to,
			digest_state,
			piece_cid_state,
			state,
			location_inv,
			pdp_accept_inv
		FROM shards
		WHERE id = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, shardID)

	shard, err := model.ReadShardFromDatabase(func(
		id *id.ShardID,
		uploadID *id.UploadID,
		size *uint64,
		sliceCount *int,
		digest *multihash.Multihash,
		pieceCID *cid.Cid,
		digestStateUpTo *uint64,
		digestState *[]byte,
		pieceCIDState *[]byte,
		state *model.BlobState,
		location *invocation.Invocation,
		pdpAccept *invocation.Invocation,
	) error {
		return row.Scan(id, uploadID, size, sliceCount, util.DbBytes(digest), util.DbCID(pieceCID), digestStateUpTo, util.DbBytes(digestState), util.DbBytes(pieceCIDState), state, util.DbInvocation(location), util.DbInvocation(pdpAccept))
	})
	if err != nil {
		return nil, err
	}

	return shard, nil
}

// AddNodeToShard adds a node to a shard in the repository. Note: the [offset]
// is NOT the absolute offset within the shard, but the offset into the *new*
// bytes in the shard CAR where the node data begins--in other words, the length
// of the length varint + the length of the CID bytes. The node's block will be
// indexed as appearing at `shard.size + offset`, running for `node.size` bytes,
// and then the shard size will be increased by `offset + node.size`.
func (r *Repo) AddNodeToShard(ctx context.Context, shardID id.ShardID, nodeCID cid.Cid, spaceDID did.DID, offset uint64, options ...blobs.AddNodeToShardOption) error {
	nodesInShardsStmt, err := r.prepareStmt(ctx, `
		INSERT INTO nodes_in_shards (
			node_cid,
			space_did,
			shard_id,
			shard_offset
		)
		SELECT ?, ?, s.id, s.size + ?
		FROM shards s
		WHERE s.id = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	updateShardStmt, err := r.prepareStmt(ctx, `
    UPDATE shards
    SET size = size + ? + (SELECT size FROM nodes WHERE cid = ?),
        slice_count = slice_count + 1
    WHERE id = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	digestStateUpdateStmt, err := r.prepareStmt(ctx, `
			UPDATE shards
			SET digest_state_up_to = ?,
			    digest_state = ?,
			    piece_cid_state = ?
			WHERE id = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	nodesInShardsStmt = tx.StmtContext(ctx, nodesInShardsStmt)
	updateShardStmt = tx.StmtContext(ctx, updateShardStmt)
	digestStateUpdateStmt = tx.StmtContext(ctx, digestStateUpdateStmt)

	config := blobs.NewAddNodeConfig(options...)

	_, err = nodesInShardsStmt.ExecContext(ctx,
		util.DbCID(&nodeCID),
		util.DbDID(&spaceDID),
		offset,
		shardID,
	)
	if err != nil {
		return fmt.Errorf("failed to add node %s to shard %s: %w", nodeCID, shardID, err)
	}

	_, err = updateShardStmt.ExecContext(ctx,
		offset, util.DbCID(&nodeCID), shardID)
	if err != nil {
		return err
	}

	if config.HasDigestStateUpdate() {
		digestState := config.DigestStateUpdate().DigestState()
		pieceCIDState := config.DigestStateUpdate().PieceCIDState()
		_, err = digestStateUpdateStmt.ExecContext(ctx,
			config.DigestStateUpdate().DigestStateUpTo(),
			util.DbBytes(&digestState),
			util.DbBytes(&pieceCIDState),
			shardID,
		)
		if err != nil {
			return fmt.Errorf("failed to update digest state for shard %s: %w", shardID, err)
		}
	}

	return tx.Commit()
}

func (r *Repo) FindNodeByCIDAndSpaceDID(ctx context.Context, c cid.Cid, spaceDID did.DID) (dagsmodel.Node, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT cid, size, space_did, ufsdata, path, source_id, offset
		FROM nodes
		WHERE cid = ?
		  AND space_did = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, util.DbCID(&c), util.DbDID(&spaceDID))
	return r.getNodeFromRow(row)
}

func (r *Repo) NodesByShard(ctx context.Context, shardID id.ShardID, startOffset uint64) ([]dagsmodel.Node, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT
			nodes.cid,
			nodes.size,
			nodes.space_did,
			nodes.ufsdata,
			nodes.path,
			nodes.source_id,
			nodes.offset
		FROM nodes_in_shards
		JOIN nodes ON nodes.cid = nodes_in_shards.node_cid
		         AND nodes.space_did = nodes_in_shards.space_did
		WHERE shard_id = ?
		  AND nodes_in_shards.shard_offset >= ?
		ORDER BY nodes_in_shards.shard_offset ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, shardID, startOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes in shard %s: %w", shardID, err)
	}
	defer rows.Close()

	var nodes []dagsmodel.Node
	for rows.Next() {
		node, err := dagsmodel.ReadNodeFromDatabase(func(cid *cid.Cid, size *uint64, spaceDID *did.DID, ufsdata *[]byte, path **string, sourceID *id.SourceID, offset **uint64) error {
			return rows.Scan(util.DbCID(cid), size, util.DbDID(spaceDID), util.DbBytes(ufsdata), path, util.DbID(sourceID), offset)
		})
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get node from row for shard %s: %w", shardID, err)
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (r *Repo) ForEachNode(ctx context.Context, shardID id.ShardID, yield func(node dagsmodel.Node, shardOffset uint64) error) error {
	stmt, err := r.prepareStmt(ctx, `
		SELECT
			nodes.cid,
			nodes.size,
			nodes.space_did,
			nodes.ufsdata,
			nodes.path,
			nodes.source_id,
			nodes.offset,
			nodes_in_shards.shard_offset
		FROM nodes_in_shards
		JOIN nodes ON nodes.cid = nodes_in_shards.node_cid
		         AND nodes.space_did = nodes_in_shards.space_did
		WHERE shard_id = ?
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, shardID)
	if err != nil {
		return fmt.Errorf("failed to get sizes of blocks in shard %s: %w", shardID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var shardOffset uint64
		node, err := dagsmodel.ReadNodeFromDatabase(func(cid *cid.Cid, size *uint64, spaceDID *did.DID, ufsdata *[]byte, path **string, sourceID *id.SourceID, offset **uint64) error {
			return rows.Scan(util.DbCID(cid), size, util.DbDID(spaceDID), util.DbBytes(ufsdata), path, sourceID, offset, &shardOffset)
		})
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get node from row for shard %s: %w", shardID, err)
		}
		if err := yield(node, shardOffset); err != nil {
			return fmt.Errorf("failed to yield node CID %s for shard %s: %w", node.CID(), shardID, err)
		}
	}

	return nil
}

// UpdateShard updates a DAG scan in the repository.
func (r *Repo) UpdateShard(ctx context.Context, shard *model.Shard) error {
	return model.WriteShardToDatabase(shard, func(
		id id.ShardID,
		uploadID id.UploadID,
		size uint64,
		sliceCount int,
		digest multihash.Multihash,
		pieceCID cid.Cid,
		digestStateUpTo uint64,
		digestState []byte,
		pieceCIDState []byte,
		state model.BlobState,
		location invocation.Invocation,
		pdpAccept invocation.Invocation,
	) error {
		stmt, err := r.prepareStmt(ctx, `
			UPDATE shards
			SET id = ?,
			    upload_id = ?,
			    size = ?,
			    slice_count = ?,
			    digest = ?,
			    piece_cid = ?,
			    digest_state_up_to = ?,
			    digest_state = ?,
			    piece_cid_state = ?,
			    state = ?,
			    location_inv = ?,
			    pdp_accept_inv = ?
			WHERE id = ?
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		_, err = stmt.ExecContext(ctx,
			id,
			uploadID,
			size,
			sliceCount,
			util.DbBytes(&digest),
			util.DbCID(&pieceCID),
			digestStateUpTo,
			util.DbBytes(&digestState),
			util.DbBytes(&pieceCIDState),
			state,
			util.DbInvocation(&location),
			util.DbInvocation(&pdpAccept),
			id,
		)
		return err
	})
}

func (r *Repo) DeleteShard(ctx context.Context, shardID id.ShardID) error {
	stmt, err := r.prepareStmt(ctx, `
		DELETE FROM shards
		WHERE id = ?
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	_, err = stmt.ExecContext(ctx, shardID)
	if err != nil {
		return fmt.Errorf("failed to delete shard %s: %w", shardID, err)
	}
	return nil
}
