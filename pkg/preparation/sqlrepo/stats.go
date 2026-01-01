package sqlrepo

import (
	"context"
	"fmt"

	"github.com/storacha/guppy/pkg/preparation/types/id"
)

func (r *Repo) TotalBytesToScan(ctx context.Context, uploadID id.UploadID) (uint64, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT COALESCE(SUM(fs_entries.size), 0)
		FROM dag_scans
		JOIN fs_entries ON dag_scans.fs_entry_id = fs_entries.id
		WHERE dag_scans.upload_id = ?
		AND dag_scans.cid IS NULL
	`)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare total bytes to scan statement: %w", err)
	}
	row := stmt.QueryRowContext(ctx, uploadID)

	var totalBytes uint64
	err = row.Scan(&totalBytes)
	return totalBytes, err
}

type FileInfo struct {
	Path string
	Size uint64
}

func (r *Repo) FilesToDAGScan(ctx context.Context, uploadID id.UploadID, count int) ([]FileInfo, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT fs_entries.path, fs_entries.size
		FROM fs_entries
		JOIN dag_scans ON dag_scans.fs_entry_id = fs_entries.id
		WHERE dag_scans.upload_id = ?
		AND dag_scans.cid IS NULL
		ORDER BY dag_scans.created_at ASC
		LIMIT ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare files to DAG scan statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, uploadID, count)
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
	stmt, err := r.prepareStmt(ctx, `
		SELECT fs_entries.path, fs_entries.size
		FROM fs_entries
		JOIN dag_scans ON dag_scans.fs_entry_id = fs_entries.id
		JOIN node_uploads ON node_uploads.node_cid = dag_scans.cid
      		JOIN shards ON shards.id = node_uploads.shard_id
		WHERE shards.state = 'open'
		ORDER BY dag_scans.created_at ASC
 		LIMIT 6
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare sharded files statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, uploadID, count)
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
