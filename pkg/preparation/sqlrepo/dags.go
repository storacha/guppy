package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/guppy/pkg/preparation/dags"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var _ dags.Repo = (*repo)(nil)

// CreateLinks creates links in the repository for the given parent CID and link parameters.
func (r *repo) CreateLinks(ctx context.Context, parent cid.Cid, linkParams []model.LinkParams) error {
	links := make([]*model.Link, 0, len(linkParams))
	for i, p := range linkParams {
		link, err := model.NewLink(p, parent, uint64(i))
		if err != nil {
			return err
		}
		links = append(links, link)
	}
	insertQuery := `INSERT INTO links (
			name,
  		t_size,
  	  hash,
  		parent_id,
  	  ordering,
		) VALUES`
	for _, link := range links {
		_, err := r.db.ExecContext(
			ctx,
			insertQuery,
			link.Name(),
			link.TSize(),
			link.Hash().Bytes(),
			link.Parent().Bytes(),
			link.Order(),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

type sqlScanner interface {
	Scan(dest ...any) error
}

func (r *repo) dagScanScanner(sqlScanner sqlScanner) model.DAGScanScanner {
	return func(kind *string, fsEntryID *id.FSEntryID, uploadID *id.UploadID, createdAt *time.Time, updatedAt *time.Time, errorMessage **string, state *model.DAGScanState, cidPointer **cid.Cid) error {
		var nullErrorMessage sql.NullString
		var cidTarget cid.Cid
		err := sqlScanner.Scan(fsEntryID, uploadID, createdAt, updatedAt, &nullErrorMessage, state, cidScanner{dst: &cidTarget}, kind)
		if err != nil {
			return err
		}
		if nullErrorMessage.Valid {
			*errorMessage = &nullErrorMessage.String
		} else {
			*errorMessage = nil
		}
		if cidTarget != cid.Undef {
			*cidPointer = &cidTarget
		} else {
			*cidPointer = nil
		}
		return nil
	}
}

// DAGScansForUploadByStatus retrieves all DAG scans for a given upload ID and optional states.
func (r *repo) DAGScansForUploadByStatus(ctx context.Context, uploadID id.UploadID, states ...model.DAGScanState) ([]model.DAGScan, error) {

	query := `SELECT fs_entry_id, upload_id, created_at, updated_at, state, error_message, cid, kind FROM dag_scans WHERE upload_id = $1`
	if len(states) > 0 {
		query += " AND state IN ("
		for i, state := range states {
			if i > 0 {
				query += ", "
			}
			query += "'" + string(state) + "'"
		}
		query += ")"
	}

	rows, err := r.db.QueryContext(ctx, query, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dagScans []model.DAGScan
	for rows.Next() {
		ds, err := model.ReadDAGScanFromDatabase(r.dagScanScanner(rows))
		if err != nil {
			return nil, err
		}
		dagScans = append(dagScans, ds)
	}

	return dagScans, rows.Err()
}

// DirectoryLinks retrieves link parameters for a given directory scan.
func (r *repo) DirectoryLinks(ctx context.Context, dirScan *model.DirectoryDAGScan) ([]model.LinkParams, error) {
	query := `SELECT fs_entries.path, nodes.size, nodes.cid FROM directory_children JOINS fs_entries ON directory_children.child_id = fs_entries.id JOINS dag_scans ON directory_children.child_id = dag_scans.fs_entry_id JOINS nodes ON dag_scans.cid = nodes.cid WHERE directory_children.parent_id = ?`
	rows, err := r.db.QueryContext(ctx, query, dirScan.FsEntryID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var links []model.LinkParams
	for rows.Next() {
		var path string
		var size uint64
		var cid cid.Cid
		if err := rows.Scan(&path, &size, cidScanner{dst: &cid}); err != nil {
			return nil, err
		}
		link := model.LinkParams{
			Name:  filepath.Base(path),
			TSize: size,
			Hash:  cid,
		}
		links = append(links, link)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return links, nil
}

func (r *repo) findNode(ctx context.Context, c cid.Cid, size uint64, ufsData []byte, path string, sourceID id.SourceID, offset uint64) (model.Node, error) {
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
		  AND size = ?
			AND ((ufsdata = ?) OR (? IS NULL AND ufsdata IS NULL))
		  AND path = ?
		  AND source_id = ?
		  AND offset = ?
	`
	row := r.db.QueryRowContext(
		ctx,
		findQuery,
		c.Bytes(),
		size,
		// Twice for NULL check
		ufsData, ufsData,
		path,
		sourceID,
		offset,
	)
	node, err := model.ReadNodeFromDatabase(func(cid *cid.Cid, size *uint64, ufsdata *[]byte, path *string, sourceID *id.SourceID, offset *uint64) error {
		return row.Scan(cidScanner{dst: cid}, size, ufsdata, path, sourceID, offset)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return node, err
}

func (r *repo) createNode(ctx context.Context, node model.Node) error {
	insertQuery := `INSERT INTO nodes (cid, size, ufsdata, path, source_id, offset) VALUES ($1, $2, $3, $4, $5, $6)`
	return model.WriteNodeToDatabase(func(cid cid.Cid, size uint64, ufsdata []byte, path string, sourceID id.SourceID, offset uint64) error {
		_, err := r.db.ExecContext(ctx, insertQuery, cid.Bytes(), size, ufsdata, path, sourceID, offset)
		return err
	}, node)
}

// FindOrCreateRawNode finds or creates a raw node in the repository.
// If a node with the same CID, size, path, source ID, and offset already exists, it returns that node.
// If not, it creates a new raw node with the provided parameters.
func (r *repo) FindOrCreateRawNode(ctx context.Context, cid cid.Cid, size uint64, path string, sourceID id.SourceID, offset uint64) (*model.RawNode, bool, error) {
	node, err := r.findNode(ctx, cid, size, nil, path, sourceID, offset)
	if err != nil {
		return nil, false, err
	}
	if node != nil {
		// File already exists, return it
		if rawNode, ok := node.(*model.RawNode); ok {
			return rawNode, false, nil
		}
		return nil, false, errors.New("found entry is not a raw node")
	}

	newNode, err := model.NewRawNode(cid, size, path, sourceID, offset)
	if err != nil {
		return nil, false, err
	}

	err = r.createNode(ctx, newNode)

	if err != nil {
		return nil, false, err
	}

	return newNode, true, nil
}

// FindOrCreateUnixFSNode finds or creates a UnixFS node in the repository.
// If a node with the same CID, size, and ufsdata already exists, it returns that node.
// If not, it creates a new UnixFS node with the provided parameters.
func (r *repo) FindOrCreateUnixFSNode(ctx context.Context, cid cid.Cid, size uint64, ufsdata []byte) (*model.UnixFSNode, bool, error) {
	node, err := r.findNode(ctx, cid, size, ufsdata, "", id.SourceID{}, 0)
	if err != nil {
		return nil, false, err
	}
	if node != nil {
		// File already exists, return it
		if unixFSNode, ok := node.(*model.UnixFSNode); ok {
			return unixFSNode, false, nil
		}
		return nil, false, errors.New("found entry is not a UnixFS node")
	}

	newNode, err := model.NewUnixFSNode(cid, size, ufsdata)
	if err != nil {
		return nil, false, err
	}

	err = r.createNode(ctx, newNode)

	if err != nil {
		return nil, false, err
	}

	return newNode, true, nil
}

// GetChildScans finds scans for child nodes of a given directory scan's file system entry.
func (r *repo) GetChildScans(ctx context.Context, directoryScans *model.DirectoryDAGScan) ([]model.DAGScan, error) {
	query := `SELECT fs_entry_id, upload_id, created_at, updated_at, state, error_message, cid, kind FROM dag_scans JOIN directory_children ON directory_children.child_id = dag_scans.fs_entry_id WHERE directory_children.parent_id = ?`
	rows, err := r.db.QueryContext(ctx, query, directoryScans.FsEntryID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	scanner := r.dagScanScanner(rows)
	var dagScans []model.DAGScan
	for rows.Next() {
		ds, err := model.ReadDAGScanFromDatabase(scanner)
		if err != nil {
			return nil, err
		}
		dagScans = append(dagScans, ds)
	}

	return dagScans, rows.Err()
}

// UpdateDAGScan updates a DAG scan in the repository.
func (r *repo) UpdateDAGScan(ctx context.Context, dagScan model.DAGScan) error {
	return model.WriteDAGScanToDatabase(dagScan, func(kind string, fsEntryID id.FSEntryID, uploadID id.UploadID, createdAt time.Time, updatedAt time.Time, errorMessage *string, state model.DAGScanState, cid *cid.Cid) error {
		_, err := r.db.ExecContext(ctx,
			`UPDATE dag_scans SET kind = ?, fs_entry_id = ?, upload_id = ?, created_at = ?, updated_at = ?, error_message = ?, state = ?, cid = ? WHERE fs_entry_id = ?`,
			kind,
			fsEntryID,
			uploadID,
			createdAt,
			updatedAt,
			errorMessage,
			state,
			cid.Bytes(),
			fsEntryID,
		)
		return err
	})
}
