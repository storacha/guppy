package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/preparation/dags"
	"github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var _ dags.Repo = (*Repo)(nil)

// CreateLinks creates links in the repository for the given parent CID and link parameters.
func (r *Repo) CreateLinks(ctx context.Context, parent cid.Cid, spaceDID did.DID, linkParams []model.LinkParams) error {
	links := make([]*model.Link, 0, len(linkParams))
	for i, p := range linkParams {
		link, err := model.NewLink(p, parent, spaceDID, uint64(i))
		if err != nil {
			return err
		}
		links = append(links, link)
	}
	insertQuery := `
		INSERT INTO links (
			name,
  		t_size,
  	  hash,
  		parent_id,
  		space_did,
  	  ordering
		) VALUES (?, ?, ?, ?, ?, ?)`
	for _, link := range links {
		_, err := r.db.ExecContext(
			ctx,
			insertQuery,
			link.Name(),
			link.TSize(),
			link.Hash().Bytes(),
			link.Parent().Bytes(),
			util.DbDID(&spaceDID),
			link.Order(),
		)
		if err != nil {
			return fmt.Errorf("failed to insert link: %w", err)
		}
	}
	return nil
}

func (r *Repo) LinksForCID(ctx context.Context, c cid.Cid, sd did.DID) ([]*model.Link, error) {
	query := `SELECT name, t_size, hash, parent_id, space_did, ordering FROM links WHERE parent_id = ? AND space_did = ?`
	rows, err := r.db.QueryContext(ctx, query, c.Bytes(), util.DbDID(&sd))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var links []*model.Link
	for rows.Next() {
		link, err := model.ReadLinkFromDatabase(func(name *string, tSize *uint64, hash, parent *cid.Cid, spaceDID *did.DID, order *uint64) error {
			return rows.Scan(name, tSize, util.DbCid(hash), util.DbCid(parent), util.DbDID(spaceDID), order)
		})
		if err != nil {
			return nil, err
		}
		links = append(links, link)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(links, func(a, b *model.Link) int {
		return int(a.Order() - b.Order())
	})
	return links, nil
}

type sqlScanner interface {
	Scan(dest ...any) error
}

func (r *Repo) dagScanScanner(sqlScanner sqlScanner) model.DAGScanScanner {
	return func(kind *string, fsEntryID *id.FSEntryID, uploadID *id.UploadID, spaceDID *did.DID, createdAt *time.Time, updatedAt *time.Time, errorMessage **string, state *model.DAGScanState, cid *cid.Cid) error {
		var nullErrorMessage sql.NullString
		err := sqlScanner.Scan(fsEntryID, uploadID, util.DbDID(spaceDID), util.TimestampScanner(createdAt), util.TimestampScanner(updatedAt), state, &nullErrorMessage, util.DbCid(cid), kind)
		if err != nil {
			return fmt.Errorf("scanning dag scan: %w", err)
		}
		if nullErrorMessage.Valid {
			*errorMessage = &nullErrorMessage.String
		} else {
			*errorMessage = nil
		}
		return nil
	}
}

// DAGScansForUploadByStatus retrieves all DAG scans for a given upload ID
// matching the given states, if given, or all if no states are provided.
func (r *Repo) DAGScansForUploadByStatus(ctx context.Context, uploadID id.UploadID, states ...model.DAGScanState) ([]model.DAGScan, error) {
	query := `SELECT fs_entry_id, upload_id, space_did, created_at, updated_at, state, error_message, cid, kind FROM dag_scans WHERE upload_id = $1`
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
func (r *Repo) DirectoryLinks(ctx context.Context, dirScan *model.DirectoryDAGScan) ([]model.LinkParams, error) {
	query := `
		SELECT
			fs_entries.path,
			nodes.size,
			nodes.cid
		FROM directory_children
		JOIN fs_entries ON directory_children.child_id = fs_entries.id
		JOIN dag_scans ON directory_children.child_id = dag_scans.fs_entry_id
		JOIN nodes ON dag_scans.cid = nodes.cid
		WHERE directory_children.directory_id = ?`
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
		if err := rows.Scan(&path, &size, util.DbCid(&cid)); err != nil {
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

func (r *Repo) findNode(ctx context.Context, c cid.Cid, size uint64, spaceDID did.DID, ufsData []byte, path *string, sourceID id.SourceID, offset *uint64) (model.Node, error) {
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
		WHERE cid = ?
		  AND size = ?
		  AND space_did = ?
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
		util.DbDID(&spaceDID),
		// Twice for NULL check
		ufsData, ufsData,
		path,
		sourceID,
		offset,
	)
	return r.getNodeFromRow(row)
}

// RowScanner can scan a row into a set of destinations. It should not be
// confused with [sql.Scanner], which is used to scan a single value.
type RowScanner interface {
	Scan(dest ...any) error
}

func (r *Repo) getNodeFromRow(scanner RowScanner) (model.Node, error) {
	node, err := model.ReadNodeFromDatabase(func(cid *cid.Cid, size *uint64, spaceDID *did.DID, ufsdata *[]byte, path **string, sourceID *id.SourceID, offset **uint64) error {
		return scanner.Scan(util.DbCid(cid), size, util.DbDID(spaceDID), ufsdata, path, sourceID, offset)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return node, err
}

func (r *Repo) createNode(ctx context.Context, node model.Node) error {
	insertQuery := `INSERT INTO nodes (cid, size, space_did, ufsdata, path, source_id, offset) VALUES ($1, $2, $3, $4, $5, $6, $7)`
	return model.WriteNodeToDatabase(func(cid cid.Cid, size uint64, spaceDID did.DID, ufsdata []byte, path *string, sourceID id.SourceID, offset *uint64) error {
		_, err := r.db.ExecContext(ctx, insertQuery, cid.Bytes(), size, util.DbDID(&spaceDID), ufsdata, path, sourceID, offset)
		return err
	}, node)
}

// FindOrCreateRawNode finds or creates a raw node in the repository.
// If a node with the same CID, size, path, source ID, and offset already exists, it returns that node.
// If not, it creates a new raw node with the provided parameters.
func (r *Repo) FindOrCreateRawNode(ctx context.Context, cid cid.Cid, size uint64, spaceDID did.DID, path string, sourceID id.SourceID, offset uint64) (*model.RawNode, bool, error) {
	node, err := r.findNode(ctx, cid, size, spaceDID, nil, &path, sourceID, &offset)
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

	newNode, err := model.NewRawNode(cid, size, spaceDID, path, sourceID, offset)
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
func (r *Repo) FindOrCreateUnixFSNode(ctx context.Context, cid cid.Cid, size uint64, spaceDID did.DID, ufsdata []byte) (*model.UnixFSNode, bool, error) {
	node, err := r.findNode(ctx, cid, size, spaceDID, ufsdata, nil, id.Nil, nil)
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

	newNode, err := model.NewUnixFSNode(cid, size, spaceDID, ufsdata)
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
func (r *Repo) GetChildScans(ctx context.Context, directoryScans *model.DirectoryDAGScan) ([]model.DAGScan, error) {
	query := `SELECT fs_entry_id, upload_id, space_did, created_at, updated_at, state, error_message, cid, kind FROM dag_scans JOIN directory_children ON directory_children.child_id = dag_scans.fs_entry_id WHERE directory_children.directory_id = ?`
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
func (r *Repo) UpdateDAGScan(ctx context.Context, dagScan model.DAGScan) error {
	return model.WriteDAGScanToDatabase(dagScan, func(kind string, fsEntryID id.FSEntryID, uploadID id.UploadID, spaceDID did.DID, createdAt time.Time, updatedAt time.Time, errorMessage *string, state model.DAGScanState, cidValue cid.Cid) error {
		if cidValue == cid.Undef {
			log.Debugf("Updating DAG scan: fs_entry_id: %s, cid: <cid.Undef>\n", fsEntryID)
		} else {
			log.Debugf("Updating DAG scan: fs_entry_id: %s, cid: %v\n", fsEntryID, cidValue)
		}
		_, err := r.db.ExecContext(ctx,
			`UPDATE dag_scans SET kind = ?, fs_entry_id = ?, upload_id = ?, space_did = ?, created_at = ?, updated_at = ?, error_message = ?, state = ?, cid = ? WHERE fs_entry_id = ?`,
			kind,
			fsEntryID,
			uploadID,
			util.DbDID(&spaceDID),
			createdAt.Unix(),
			updatedAt.Unix(),
			errorMessage,
			state,
			util.DbCid(&cidValue),
			fsEntryID,
		)
		return err
	})
}

func (r *Repo) DeleteNodes(ctx context.Context, spaceDID did.DID, nodeCIDs []cid.Cid) error {
	if len(nodeCIDs) == 0 {
		return nil
	}

	var dbCids []any
	for _, c := range nodeCIDs {
		dbCids = append(dbCids, util.DbCid(&c))
	}

	placeholders := strings.Repeat("?,", len(nodeCIDs)-1) + "?"

	// Clear node from uploads and dag scans first due to foreign key constraints.
	// Note that `ON DELETE SET NULL` won't work for us here, as it'll also
	// attempt to set the `space_did` to `NULL`, since it's part of the foreign
	// key.

	_, err := r.db.ExecContext(ctx,
		fmt.Sprintf(`
		UPDATE dag_scans
		SET cid = NULL
		WHERE cid IN (%s)
		  AND space_did = ?`, placeholders),
		append(dbCids, util.DbDID(&spaceDID))...,
	)
	if err != nil {
		return fmt.Errorf("failed to clear nodes from dag_scans for space %s: %w", spaceDID, err)
	}

	_, err = r.db.ExecContext(ctx,
		fmt.Sprintf(`
		UPDATE uploads
		SET root_cid = NULL
		WHERE root_cid IN (%s)
		  AND space_did = ?`, placeholders),
		append(dbCids, util.DbDID(&spaceDID))...,
	)
	if err != nil {
		return fmt.Errorf("failed to clear nodes from uploads for space %s: %w", spaceDID, err)
	}

	_, err = r.db.ExecContext(ctx,
		fmt.Sprintf(`
		DELETE FROM nodes
		WHERE cid IN (%s)
		  AND space_did = ?`, placeholders),
		append(dbCids, util.DbDID(&spaceDID))...,
	)
	if err != nil {
		return fmt.Errorf("failed to delete node for space %s: %w", spaceDID, err)
	}
	return nil
}
