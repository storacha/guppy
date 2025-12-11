package sqlrepo

import (
	"context"

	"github.com/storacha/guppy/pkg/preparation/types/id"
)

func (r *Repo) TotalBytesToScan(ctx context.Context, uploadID id.UploadID) (uint64, error) {
	row := r.db.QueryRowContext(
		ctx, `
			SELECT COALESCE(SUM(fs_entries.size), 0)
		 	FROM dag_scans
			JOIN fs_entries ON dag_scans.fs_entry_id = fs_entries.id
		 	WHERE dag_scans.upload_id = $1
			AND dag_scans.cid IS NULL
		`,
		uploadID,
	)

	var totalBytes uint64
	err := row.Scan(&totalBytes)
	return totalBytes, err
}

type FileInfo struct {
	Path string
	Size uint64
}

func (r *Repo) FilesToDAGScan(ctx context.Context, uploadID id.UploadID, count int) ([]FileInfo, error) {
	rows, err := r.db.QueryContext(
		ctx, `
			SELECT fs_entries.path, fs_entries.size
		 	FROM fs_entries
			JOIN dag_scans ON dag_scans.fs_entry_id = fs_entries.id
		 	WHERE dag_scans.upload_id = $1
			AND dag_scans.cid IS NULL
			ORDER BY dag_scans.created_at DESC
			LIMIT $2
		`,
		uploadID,
		count,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fileInfos []FileInfo
	for rows.Next() {
		var path string
		var size uint64
		if err := rows.Scan(&path, &size); err != nil {
			return nil, err
		}
		fileInfos = append(fileInfos, FileInfo{Path: path, Size: size})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return fileInfos, nil
}

func (r *Repo) ShardedFiles(ctx context.Context, uploadID id.UploadID, count int) ([]FileInfo, error) {
	rows, err := r.db.QueryContext(
		ctx, `
			SELECT fs_entries.path, fs_entries.size
		 	FROM fs_entries
			JOIN dag_scans ON dag_scans.fs_entry_id = fs_entries.id
      JOIN nodes_in_shards ON nodes_in_shards.node_cid = dag_scans.cid
      JOIN shards ON shards.id = nodes_in_shards.shard_id
			WHERE shards.state = 'open'
			ORDER BY dag_scans.created_at DESC
 			LIMIT $2
		`,
		uploadID,
		count,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fileInfos []FileInfo
	for rows.Next() {
		var path string
		var size uint64
		if err := rows.Scan(&path, &size); err != nil {
			return nil, err
		}
		fileInfos = append(fileInfos, FileInfo{Path: path, Size: size})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return fileInfos, nil
}
