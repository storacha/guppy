package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/invocation"
	indexesmodel "github.com/storacha/guppy/pkg/preparation/indexes/model"
	shardsmodel "github.com/storacha/guppy/pkg/preparation/shards/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

func (r *Repo) CreateIndex(ctx context.Context, uploadID id.UploadID) (*indexesmodel.Index, error) {
	index, err := indexesmodel.NewIndex(uploadID)
	if err != nil {
		return nil, err
	}

	err = indexesmodel.WriteIndexToDatabase(index, func(
		id id.IndexID,
		uploadID id.UploadID,
		digest multihash.Multihash,
		pieceCID cid.Cid,
		size uint64,
		sliceCount uint64,
		state indexesmodel.IndexState,
		location invocation.Invocation,
		pdpAccept invocation.Invocation,
	) error {
		_, err := r.db.ExecContext(ctx, `
			INSERT INTO indexes (
				id,
				upload_id,
				digest,
				piece_cid,
				size,
				slice_count,
				state,
				location_inv,
				pdp_accept_inv
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id,
			uploadID,
			digest,
			util.DbCID(&pieceCID),
			size,
			sliceCount,
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

func (r *Repo) IndexesForUploadByState(ctx context.Context, uploadID id.UploadID, state indexesmodel.IndexState) ([]*indexesmodel.Index, error) {
	rows, err := r.db.QueryContext(ctx, `
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
		WHERE upload_id = ?
		  AND state = ?`,
		uploadID,
		state,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes for upload %s in state %s: %w", uploadID, state, err)
	}
	defer rows.Close()

	var indexes []*indexesmodel.Index
	for rows.Next() {
		var indexID id.IndexID
		var uploadID id.UploadID
		var digest multihash.Multihash
		var pieceCID cid.Cid
		var size uint64
		var sliceCount uint64
		var state indexesmodel.IndexState
		var location invocation.Invocation
		var pdpAccept invocation.Invocation

		err := rows.Scan(
			&indexID,
			&uploadID,
			&digest,
			util.DbCID(&pieceCID),
			&size,
			&sliceCount,
			&state,
			util.DbInvocation(&location),
			util.DbInvocation(&pdpAccept),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan index row: %w", err)
		}

		index, err := indexesmodel.ReadIndexFromDatabase(
			indexID,
			uploadID,
			digest,
			pieceCID,
			size,
			sliceCount,
			state,
			location,
			pdpAccept,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to read index from database: %w", err)
		}

		indexes = append(indexes, index)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating index rows: %w", err)
	}

	return indexes, nil
}

func (r *Repo) GetIndexByID(ctx context.Context, indexID id.IndexID) (*indexesmodel.Index, error) {
	var uploadID id.UploadID
	var digest multihash.Multihash
	var pieceCID cid.Cid
	var size uint64
	var sliceCount uint64
	var state indexesmodel.IndexState
	var location invocation.Invocation
	var pdpAccept invocation.Invocation

	err := r.db.QueryRowContext(ctx, `
		SELECT
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
	).Scan(
		&uploadID,
		&digest,
		util.DbCID(&pieceCID),
		&size,
		&sliceCount,
		&state,
		util.DbInvocation(&location),
		util.DbInvocation(&pdpAccept),
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query index %s: %w", indexID, err)
	}

	return indexesmodel.ReadIndexFromDatabase(
		indexID,
		uploadID,
		digest,
		pieceCID,
		size,
		sliceCount,
		state,
		location,
		pdpAccept,
	)
}

func (r *Repo) UpdateIndex(ctx context.Context, index *indexesmodel.Index) error {
	return indexesmodel.WriteIndexToDatabase(index, func(
		id id.IndexID,
		uploadID id.UploadID,
		digest multihash.Multihash,
		pieceCID cid.Cid,
		size uint64,
		sliceCount uint64,
		state indexesmodel.IndexState,
		location invocation.Invocation,
		pdpAccept invocation.Invocation,
	) error {
		_, err := r.db.ExecContext(ctx, `
			UPDATE indexes
			SET
				digest = ?,
				piece_cid = ?,
				size = ?,
				slice_count = ?,
				state = ?,
				location_inv = ?,
				pdp_accept_inv = ?
			WHERE id = ?`,
			digest,
			util.DbCID(&pieceCID),
			size,
			sliceCount,
			state,
			util.DbInvocation(&location),
			util.DbInvocation(&pdpAccept),
			id,
		)
		return err
	})
}

func (r *Repo) AddShardToIndex(ctx context.Context, indexID id.IndexID, shardID id.ShardID) error {
	// First, get the shard's slice count
	var sliceCount uint64
	err := r.db.QueryRowContext(ctx, `
		SELECT slice_count
		FROM shards
		WHERE id = ?`,
		shardID,
	).Scan(&sliceCount)
	if err != nil {
		return fmt.Errorf("failed to get shard slice count: %w", err)
	}

	// Add the shard to the index
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO shards_in_indexes (shard_id, index_id)
		VALUES (?, ?)`,
		shardID,
		indexID,
	)
	if err != nil {
		return fmt.Errorf("failed to add shard %s to index %s: %w", shardID, indexID, err)
	}

	// Update the index's slice count
	_, err = r.db.ExecContext(ctx, `
		UPDATE indexes
		SET slice_count = slice_count + ?
		WHERE id = ?`,
		sliceCount,
		indexID,
	)
	if err != nil {
		return fmt.Errorf("failed to update index %s slice count: %w", indexID, err)
	}

	return nil
}

func (r *Repo) ShardsForIndex(ctx context.Context, indexID id.IndexID) ([]*shardsmodel.Shard, error) {
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

	var shards []*shardsmodel.Shard
	for rows.Next() {
		shard, err := shardsmodel.ReadShardFromDatabase(func(
			id *id.ShardID,
			uploadID *id.UploadID,
			size *uint64,
			sliceCount *uint64,
			digest *multihash.Multihash,
			pieceCID *cid.Cid,
			digestStateUpTo *uint64,
			digestState *[]byte,
			pieceCIDState *[]byte,
			state *shardsmodel.ShardState,
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
