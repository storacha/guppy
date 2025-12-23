package sqlrepo

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/guppy/pkg/preparation/blobs/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

func (r *Repo) CreateIndex(ctx context.Context, uploadID id.UploadID) (*model.Index, error) {
	index, err := model.NewIndex(uploadID)
	if err != nil {
		return nil, err
	}

	err = model.WriteIndexToDatabase(index, func(
		id id.IndexID,
		uploadID id.UploadID,
		size uint64,
		sliceCount int,
		digest multihash.Multihash,
		pieceCID cid.Cid,
		state model.BlobState,
		location invocation.Invocation,
		pdpAccept invocation.Invocation,
	) error {
		_, err := r.db.ExecContext(ctx, `
			INSERT INTO indexes (
				id,
				upload_id,
				size,
				slice_count,
				digest,
				piece_cid,
				state,
				location_inv,
				pdp_accept_inv
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id,
			uploadID,
			size,
			sliceCount,
			digest,
			util.DbCID(&pieceCID),
			state,
			util.DbInvocation(&location),
			util.DbInvocation(&pdpAccept),
		)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to write index for upload %s: %w", uploadID, err)
	}

	return index, nil
}

func (r *Repo) IndexesForUploadByState(ctx context.Context, uploadID id.UploadID, state model.BlobState) ([]*model.Index, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			id,
			upload_id,
			size,
			slice_count,
			digest,
			piece_cid,
			state,
			location_inv,
			pdp_accept_inv
		FROM indexes
		WHERE upload_id = ?
		  AND state = ?`,
		uploadID,
		state,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes for upload %s in state %s: %w", uploadID, state, err)
	}
	defer rows.Close()

	var indexes []*model.Index
	for rows.Next() {
		index, err := model.ReadIndexFromDatabase(func(
			id *id.IndexID,
			uploadID *id.UploadID,
			size *uint64,
			sliceCount *int,
			digest *multihash.Multihash,
			pieceCID *cid.Cid,
			state *model.BlobState,
			location *invocation.Invocation,
			pdpAccept *invocation.Invocation,
		) error {
			return rows.Scan(id, uploadID, size, sliceCount, util.DbBytes(digest), util.DbCID(pieceCID), state, util.DbInvocation(location), util.DbInvocation(pdpAccept))
		})
		if err != nil {
			return nil, err
		}
		if index == nil {
			continue
		}

		indexes = append(indexes, index)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating index rows: %w", err)
	}

	return indexes, nil
}

func (r *Repo) GetIndexByID(ctx context.Context, indexID id.IndexID) (*model.Index, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT
			id,
			upload_id,
			digest,
			piece_cid,
			size,
			slice_count,
			state,
			location_inv,
			pdp_accept_inv
		FROM indexes
		WHERE id = ?`,
		indexID,
	)

	index, err := model.ReadIndexFromDatabase(func(
		id *id.IndexID,
		uploadID *id.UploadID,
		size *uint64,
		sliceCount *int,
		digest *multihash.Multihash,
		pieceCID *cid.Cid,
		state *model.BlobState,
		location *invocation.Invocation,
		pdpAccept *invocation.Invocation,
	) error {
		return row.Scan(id, uploadID, util.DbBytes(digest), util.DbCID(pieceCID), size, sliceCount, state, util.DbInvocation(location), util.DbInvocation(pdpAccept))
	})
	if err != nil {
		return nil, err
	}

	return index, nil
}

func (r *Repo) UpdateIndex(ctx context.Context, index *model.Index) error {
	return model.WriteIndexToDatabase(index, func(
		id id.IndexID,
		uploadID id.UploadID,
		size uint64,
		sliceCount int,
		digest multihash.Multihash,
		pieceCID cid.Cid,
		state model.BlobState,
		location invocation.Invocation,
		pdpAccept invocation.Invocation,
	) error {
		_, err := r.db.ExecContext(ctx, `
			UPDATE indexes
			SET id = ?,
			    upload_id = ?,
			    size = ?,
			    slice_count = ?,
			    digest = ?,
			    piece_cid = ?,
			    state = ?,
			    location_inv = ?,
			    pdp_accept_inv = ?
			WHERE id = ?`,
			id,
			uploadID,
			size,
			sliceCount,
			digest,
			util.DbCID(&pieceCID),
			state,
			util.DbInvocation(&location),
			util.DbInvocation(&pdpAccept),
			id,
		)
		return err
	})
}

func (r *Repo) AddShardToIndex(ctx context.Context, indexID id.IndexID, shardID id.ShardID) error {
	// Add the shard to the index
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO shards_in_indexes (shard_id, index_id)
		VALUES (?, ?)`,
		shardID,
		indexID,
	)
	if err != nil {
		return fmt.Errorf("failed to add shard %s to index %s: %w", shardID, indexID, err)
	}

	// Update the index's slice count using a subquery to avoid a separate read
	_, err = r.db.ExecContext(ctx, `
		UPDATE indexes
		SET slice_count = slice_count + (SELECT slice_count FROM shards WHERE id = ?)
		WHERE id = ?`,
		shardID,
		indexID,
	)
	if err != nil {
		return fmt.Errorf("failed to update index %s slice count: %w", indexID, err)
	}

	return nil
}

func (r *Repo) ShardsForIndex(ctx context.Context, indexID id.IndexID) ([]*model.Shard, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			s.id,
			s.upload_id,
			s.size,
			s.slice_count,
			s.digest,
			s.piece_cid,
			s.digest_state_up_to,
			s.digest_state,
			s.piece_cid_state,
			s.state,
			s.location_inv,
			s.pdp_accept_inv
		FROM shards s
		INNER JOIN shards_in_indexes si ON s.id = si.shard_id
		WHERE si.index_id = ?
		ORDER BY s.id`,
		indexID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query shards for index %s: %w", indexID, err)
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
			return rows.Scan(
				id,
				uploadID,
				size,
				sliceCount,
				digest,
				util.DbCID(pieceCID),
				digestStateUpTo,
				util.DbBytes(digestState),
				util.DbBytes(pieceCIDState),
				state,
				util.DbInvocation(location),
				util.DbInvocation(pdpAccept),
			)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to read shard from database: %w", err)
		}

		shards = append(shards, shard)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating shard rows: %w", err)
	}

	return shards, nil
}
