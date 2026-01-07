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

func (r *Repo) LinksForCID(ctx context.Context, c cid.Cid, sd did.DID) ([]*model.Link, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT name, t_size, hash, parent_id, space_did, ordering
		FROM links
		WHERE parent_id = ?
		AND space_did = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, util.DbCID(&c), util.DbDID(&sd))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var links []*model.Link
	for rows.Next() {
		link, err := model.ReadLinkFromDatabase(func(name *string, tSize *uint64, hash, parent *cid.Cid, spaceDID *did.DID, order *uint64) error {
			return rows.Scan(name, tSize, util.DbCID(hash), util.DbCID(parent), util.DbDID(spaceDID), order)
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
	return func(
		kind *string,
		fsEntryID *id.FSEntryID,
		uploadID *id.UploadID,
		spaceDID *did.DID,
		createdAt *time.Time,
		updatedAt *time.Time,
		cid *cid.Cid,
	) error {
		err := sqlScanner.Scan(
			fsEntryID,
			uploadID,
			util.DbDID(spaceDID),
			util.TimestampScanner(createdAt),
			util.TimestampScanner(updatedAt),
			util.DbCID(cid),
			kind,
		)
		if err != nil {
			return fmt.Errorf("scanning dag scan: %w", err)
		}
		return nil
	}
}

func (r *Repo) IncompleteDAGScansForUpload(ctx context.Context, uploadID id.UploadID) ([]model.DAGScan, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT fs_entry_id, upload_id, space_did, created_at, updated_at, cid, kind
		FROM dag_scans
		WHERE upload_id = ?
		AND cid IS NULL
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, uploadID)
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

func (r *Repo) CompleteDAGScansForUpload(ctx context.Context, uploadID id.UploadID) ([]model.DAGScan, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT fs_entry_id, upload_id, space_did, created_at, updated_at, cid, kind
		FROM dag_scans
		WHERE upload_id = ?
		AND cid IS NOT NULL
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, uploadID)
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
	stmt, err := r.prepareStmt(ctx, `
		SELECT
			fs_entries.path,
			nodes.size,
			nodes.cid
		FROM directory_children
		JOIN fs_entries ON directory_children.child_id = fs_entries.id
		JOIN dag_scans ON directory_children.child_id = dag_scans.fs_entry_id
		JOIN nodes ON dag_scans.cid = nodes.cid AND nodes.space_did = fs_entries.space_did
		WHERE directory_children.directory_id = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	rows, err := stmt.QueryContext(ctx, dirScan.FsEntryID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var links []model.LinkParams
	for rows.Next() {
		var path string
		var size uint64
		var cid cid.Cid
		if err := rows.Scan(&path, &size, util.DbCID(&cid)); err != nil {
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

type nodeFinder struct {
	r    *Repo
	stmt *sql.Stmt
	ctx  context.Context
}

func (n nodeFinder) Find(spaceDID did.DID, c cid.Cid, tx *sql.Tx) (model.Node, error) {
	stmt := n.stmt
	if tx != nil {
		stmt = tx.StmtContext(n.ctx, stmt)
	}
	row := stmt.QueryRowContext(n.ctx, util.DbCID(&c), util.DbDID(&spaceDID))
	return n.r.getNodeFromRow(row)
}

func (r *Repo) nodeFinder(ctx context.Context) (nodeFinder, error) {
	stmt, err := r.prepareStmt(ctx, `
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
		AND space_did = ?
	`)
	if err != nil {
		return nodeFinder{}, fmt.Errorf("failed to prepare statement: %w", err)
	}
	return nodeFinder{r: r, stmt: stmt, ctx: ctx}, nil
}

// RowScanner can scan a row into a set of destinations. It should not be
// confused with [sql.Scanner], which is used to scan a single value.
type RowScanner interface {
	Scan(dest ...any) error
}

func (r *Repo) getNodeFromRow(scanner RowScanner) (model.Node, error) {
	node, err := model.ReadNodeFromDatabase(func(cid *cid.Cid, size *uint64, spaceDID *did.DID, ufsdata *[]byte, path **string, sourceID *id.SourceID, offset **uint64) error {
		return scanner.Scan(util.DbCID(cid), size, util.DbDID(spaceDID), util.DbBytes(ufsdata), path, util.DbID(sourceID), offset)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return node, err
}

type nodeCreator struct {
	r                    *Repo
	stmt                 *sql.Stmt
	createNodeUploadStmt *sql.Stmt
	findExistingQuery    *sql.Stmt
	ctx                  context.Context
}

func (r *Repo) nodeCreator(ctx context.Context) (nodeCreator, error) {
	stmt, err := r.prepareStmt(ctx, `
			INSERT INTO nodes (cid, size, space_did, ufsdata, path, source_id, offset)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`)

	if err != nil {
		return nodeCreator{}, fmt.Errorf("failed to prepare statement: %w", err)
	}

	createNodeUploadStmt, err := r.prepareStmt(ctx, `
		INSERT INTO node_uploads (node_cid, space_did, upload_id, shard_id, shard_offset)
		VALUES (?, ?, ?, NULL, NULL)`)

	if err != nil {
		return nodeCreator{}, fmt.Errorf("failed to prepare statement: %w", err)
	}

	findExistingQuery, err := r.prepareStmt(ctx, `
		SELECT 1
		FROM node_uploads
		WHERE node_cid = ? AND space_did = ? AND upload_id = ?`)
	if err != nil {
		return nodeCreator{}, fmt.Errorf("failed to prepare find existing query: %w", err)
	}
	return nodeCreator{
		r:                    r,
		stmt:                 stmt,
		createNodeUploadStmt: createNodeUploadStmt,
		findExistingQuery:    findExistingQuery,
		ctx:                  ctx,
	}, nil
}

func (n nodeCreator) Create(node model.Node, uploadID id.UploadID, tx *sql.Tx) error {
	err := model.WriteNodeToDatabase(func(cid cid.Cid, size uint64, spaceDID did.DID, ufsdata []byte, path *string, sourceID id.SourceID, offset *uint64) error {
		stmt := n.stmt
		if tx != nil {
			stmt = tx.StmtContext(n.ctx, stmt)
		}
		_, err := stmt.ExecContext(n.ctx, util.DbCID(&cid), size, util.DbDID(&spaceDID), util.DbBytes(&ufsdata), path, util.DbID(&sourceID), offset)
		return err
	}, node)
	if err != nil {
		return err
	}

	nodeCID := node.CID()
	spaceDID := node.SpaceDID()
	findExistingQuery := n.findExistingQuery
	if tx != nil {
		findExistingQuery = tx.StmtContext(n.ctx, findExistingQuery)
	}

	// Try to find existing record first
	row := findExistingQuery.QueryRowContext(n.ctx,
		util.DbCID(&nodeCID),
		util.DbDID(&spaceDID),
		uploadID,
	)
	var dummy int
	err = row.Scan(&dummy)
	if err == nil {
		return nil // Record already exists, nothing to do
	}
	createNodeUploadStmt := n.createNodeUploadStmt
	if tx != nil {
		createNodeUploadStmt = tx.StmtContext(n.ctx, createNodeUploadStmt)
	}
	_, err = createNodeUploadStmt.ExecContext(n.ctx, util.DbCID(&nodeCID), util.DbDID(&spaceDID), util.DbID(&uploadID))
	if err != nil {
		return fmt.Errorf("failed to create node upload entry: %w", err)
	}
	return nil
}

// FindOrCreateRawNode finds or creates a raw node in the repository.
// If a node with the same CID, size, path, source ID, and offset already exists, it returns that node.
// If not, it creates a new raw node with the provided parameters.
func (r *Repo) FindOrCreateRawNode(ctx context.Context, cid cid.Cid, size uint64, spaceDID did.DID, uploadID id.UploadID, path string, sourceID id.SourceID, offset uint64) (*model.RawNode, bool, error) {
	nf, err := r.nodeFinder(ctx)
	if err != nil {
		return nil, false, err
	}
	nc, err := r.nodeCreator(ctx)
	if err != nil {
		return nil, false, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	node, err := nf.Find(spaceDID, cid, tx)
	if err != nil {
		return nil, false, err
	}
	if node != nil {
		// File already exists, return it
		if rawNode, ok := node.(*model.RawNode); ok {
			return rawNode, false, tx.Commit()
		}
		return nil, false, errors.New("found entry is not a raw node")
	}

	newNode, err := model.NewRawNode(cid, size, spaceDID, path, sourceID, offset)
	if err != nil {
		return nil, false, err
	}

	err = nc.Create(newNode, uploadID, tx)
	if err != nil {
		return nil, false, err
	}

	return newNode, true, tx.Commit()
}

// FindOrCreateUnixFSNode finds or creates a UnixFS node in the repository.
// If a node with the same CID, size, and ufsdata already exists, it returns that node.
// If not, it creates a new UnixFS node with the provided parameters.
func (r *Repo) FindOrCreateUnixFSNode(ctx context.Context, cid cid.Cid, size uint64, spaceDID did.DID, uploadID id.UploadID, ufsdata []byte, linkParams []model.LinkParams) (*model.UnixFSNode, bool, error) {
	insertQuery, err := r.prepareStmt(ctx, `
		INSERT INTO links (
			name,
  		t_size,
  	  hash,
  		parent_id,
  		space_did,
  	  ordering
		) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return nil, false, fmt.Errorf("failed to prepare statement: %w", err)
	}

	nf, err := r.nodeFinder(ctx)
	if err != nil {
		return nil, false, err
	}
	nc, err := r.nodeCreator(ctx)
	if err != nil {
		return nil, false, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	node, err := nf.Find(spaceDID, cid, tx)
	if err != nil {
		return nil, false, err
	}
	if node != nil {
		// File already exists, return it
		if unixFSNode, ok := node.(*model.UnixFSNode); ok {
			return unixFSNode, false, tx.Commit()
		}
		return nil, false, errors.New("found entry is not a UnixFS node")
	}

	newNode, err := model.NewUnixFSNode(cid, size, spaceDID, ufsdata)
	if err != nil {
		return nil, false, err
	}

	err = nc.Create(newNode, uploadID, tx)

	if err != nil {
		return nil, false, err
	}

	if len(linkParams) == 0 {
		return newNode, true, tx.Commit()
	}

	links := make([]*model.Link, 0, len(linkParams))
	for i, p := range linkParams {
		link, err := model.NewLink(p, cid, spaceDID, uint64(i))
		if err != nil {
			return nil, false, err
		}
		links = append(links, link)
	}

	insertQuery = tx.StmtContext(ctx, insertQuery)

	for _, link := range links {
		hash := link.Hash()
		parent := link.Parent()

		_, err := insertQuery.ExecContext(
			ctx,
			link.Name(),
			link.TSize(),
			util.DbCID(&hash),
			util.DbCID(&parent),
			util.DbDID(&spaceDID),
			link.Order(),
		)
		if err != nil {
			return nil, false, fmt.Errorf("failed to insert link %s for parent %s: %w", link.Name(), parent, err)
		}
	}
	return newNode, true, tx.Commit()
}

// HasIncompleteChildren returns whether the given directory scan has at least
// one child scan that is not completed.
func (r *Repo) HasIncompleteChildren(ctx context.Context, directoryScans *model.DirectoryDAGScan) (bool, error) {
	stmt, err := r.prepareStmt(ctx, `
		SELECT 1
		FROM dag_scans
		JOIN directory_children ON directory_children.child_id = dag_scans.fs_entry_id
		WHERE directory_children.directory_id = ?
		  AND dag_scans.cid IS NULL
		LIMIT 1
	`)
	if err != nil {
		return false, fmt.Errorf("failed to prepare statement: %w", err)
	}
	var dummy int
	err = stmt.QueryRowContext(ctx, directoryScans.FsEntryID()).Scan(&dummy)

	if err != nil {
		if err == sql.ErrNoRows {
			// No incomplete children found
			return false, nil
		}
		// Actual error
		return false, err
	}
	// Found at least one incomplete child
	return true, nil
}

// UpdateDAGScan updates a DAG scan in the repository.
func (r *Repo) UpdateDAGScan(ctx context.Context, dagScan model.DAGScan) error {
	return model.WriteDAGScanToDatabase(dagScan, func(
		kind string,
		fsEntryID id.FSEntryID,
		uploadID id.UploadID,
		spaceDID did.DID,
		createdAt time.Time,
		updatedAt time.Time,
		cidValue cid.Cid,
	) error {
		if cidValue == cid.Undef {
			log.Debugf("Updating DAG scan: fs_entry_id: %s, cid: <cid.Undef>\n", fsEntryID)
		} else {
			log.Debugf("Updating DAG scan: fs_entry_id: %s, cid: %v\n", fsEntryID, cidValue)
		}
		stmt, err := r.prepareStmt(ctx, `
			UPDATE dag_scans
			SET kind = ?, fs_entry_id = ?, upload_id = ?, space_did = ?, created_at = ?, updated_at = ?, cid = ?
			WHERE fs_entry_id = ?
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		_, err = stmt.ExecContext(ctx, kind,
			util.DbID(&fsEntryID),
			util.DbID(&uploadID),
			util.DbDID(&spaceDID),
			createdAt.Unix(),
			updatedAt.Unix(),
			util.DbCID(&cidValue),
			util.DbID(&fsEntryID),
		)
		return err
	})
}

func (r *Repo) DeleteNodes(ctx context.Context, spaceDID did.DID, nodeCIDs []cid.Cid) error {
	if len(nodeCIDs) == 0 {
		return nil
	}

	var dbCIDs []any
	for _, c := range nodeCIDs {
		dbCIDs = append(dbCIDs, util.DbCID(&c))
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
		append(dbCIDs, util.DbDID(&spaceDID))...,
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
		append(dbCIDs, util.DbDID(&spaceDID))...,
	)
	if err != nil {
		return fmt.Errorf("failed to clear nodes from uploads for space %s: %w", spaceDID, err)
	}

	_, err = r.db.ExecContext(ctx,
		fmt.Sprintf(`
		DELETE FROM nodes
		WHERE cid IN (%s)
		  AND space_did = ?`, placeholders),
		append(dbCIDs, util.DbDID(&spaceDID))...,
	)
	if err != nil {
		return fmt.Errorf("failed to delete node for space %s: %w", spaceDID, err)
	}
	return nil
}
