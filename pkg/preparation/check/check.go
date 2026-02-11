package check

import (
	"context"
	"fmt"

	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/go-cid"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/pkg/preparation"
	blobsmodel "github.com/storacha/guppy/pkg/preparation/blobs/model"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// Checker provides methods for checking upload integrity and completeness.
type Checker struct {
	Repo preparation.Repo
}

// Option configures the check behavior.
type Option func(*config)

type config struct {
	applyRepairs bool
}

// WithRepairs enables automatic repair of issues found during checks.
// Without this option, checks run in dry-run mode (report only).
func WithRepairs() Option {
	return func(c *config) {
		c.applyRepairs = true
	}
}

// CheckUpload performs a comprehensive integrity and completeness check on an upload.
// It runs the following checks in order:
//  1. Upload Scanned Check - Verify FS and DAG scans completed
//  2. File System Integrity Check - Verify FS structure and DAG validity
//  3. Node Integrity Check - Verify all nodes have upload records
//  4. Node Completeness Check - Verify all nodes are in shards
//  5. Shard Completeness Check - Verify all shards are uploaded and indexed
//  6. Index Completeness Check - Verify all indexes are uploaded
//
// Returns a CheckReport containing results of all checks and any repairs applied.
func (c *Checker) CheckUpload(ctx context.Context, uploadID id.UploadID, opts ...Option) (*CheckReport, error) {
	cfg := &config{
		applyRepairs: false, // Default: dry-run mode
	}
	for _, opt := range opts {
		opt(cfg)
	}

	report := &CheckReport{
		UploadID: uploadID,
	}

	// 1. Upload Scanned Check
	// Ensures upload has both root_fs_entry_id and root_cid set,
	// and that they point to existing records in the database.
	result, err := c.checkUploadScanned(ctx, uploadID, cfg)
	if err != nil {
		return nil, fmt.Errorf("upload scanned check: %w", err)
	}
	report.Checks = append(report.Checks, result)

	// 2. File System Integrity Check
	// Starting from the root FSEntry, recursively validates:
	// - For files: DAGScan exists, and if complete, DAG structure is valid
	// - For directories: all children are valid, DAGScan exists and matches structure
	result, err = c.checkFileSystemIntegrity(ctx, uploadID, cfg)
	if err != nil {
		return nil, fmt.Errorf("file system integrity check: %w", err)
	}
	report.Checks = append(report.Checks, result)

	// 3. Node Integrity Check
	// Ensures every node in the DAG has a corresponding node_uploads record.
	result, err = c.checkNodeIntegrity(ctx, uploadID, cfg)
	if err != nil {
		return nil, fmt.Errorf("node integrity check: %w", err)
	}
	report.Checks = append(report.Checks, result)

	// 4. Node Completeness Check
	// Verifies that all nodes have been assigned to shards (no orphaned nodes).
	result, err = c.checkNodeCompleteness(ctx, uploadID, cfg)
	if err != nil {
		return nil, fmt.Errorf("node completeness check: %w", err)
	}
	report.Checks = append(report.Checks, result)

	// 5. Shard Completeness Check
	// Verifies all shards are in BlobStateAdded and assigned to indexes.
	result, err = c.checkShardCompleteness(ctx, uploadID, cfg)
	if err != nil {
		return nil, fmt.Errorf("shard completeness check: %w", err)
	}
	report.Checks = append(report.Checks, result)

	// 6. Index Completeness Check
	// Verifies all indexes are in BlobStateAdded (fully uploaded).
	result, err = c.checkIndexCompleteness(ctx, uploadID, cfg)
	if err != nil {
		return nil, fmt.Errorf("index completeness check: %w", err)
	}
	report.Checks = append(report.Checks, result)

	// Compute overall status
	report.OverallPass = true
	for _, check := range report.Checks {
		if !check.Passed {
			report.OverallPass = false
		}
		for _, repair := range check.Repairs {
			if repair.Applied {
				report.RepairsApplied++
			}
		}
	}

	return report, nil
}

// checkUploadScanned verifies that the upload has completed its scanning phases.
// Checks:
// - root_fs_entry_id is not NULL and points to existing FSEntry
// - root_cid is not NULL and points to existing node
//
// Repairs:
// - If root_fs_entry_id points to non-existent entry, set to NULL
// - If root_cid points to non-existent node, set to NULL
func (c *Checker) checkUploadScanned(ctx context.Context, uploadID id.UploadID, cfg *config) (CheckResult, error) {
	result := CheckResult{
		Name:   "Upload Scanned Check",
		Passed: true,
	}

	// Get the upload
	upload, err := c.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return result, fmt.Errorf("getting upload: %w", err)
	}

	// Track if upload has been started at all
	fsEntryIsNull := upload.RootFSEntryID() == id.Nil
	cidIsNull := upload.RootCID() == cid.Undef

	// Special case: upload never started (both NULL)
	if fsEntryIsNull && cidIsNull {
		result.Passed = false
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeWarning,
			Description: "Upload has not been started",
			Details:     "Both filesystem scan and DAG scan are incomplete. Run 'guppy upload' to begin.",
		})
		return result, nil
	}

	// Check root_fs_entry_id
	if fsEntryIsNull {
		result.Passed = false
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: "Filesystem scan incomplete",
			Details:     "root_fs_entry_id is NULL",
		})
	} else {
		// Verify FSEntry exists (could be file or directory)
		// GetFileByID returns (nil, nil) if entry doesn't exist,
		// or (nil, err) if it exists but is a directory (which is fine)
		file, err := c.Repo.GetFileByID(ctx, upload.RootFSEntryID())
		if file == nil && err == nil {
			// Entry doesn't exist
			result.Passed = false
			result.Issues = append(result.Issues, Issue{
				Type:        IssueTypeError,
				Description: "root_fs_entry_id points to non-existent entry",
				Details:     fmt.Sprintf("FSEntry ID: %s", upload.RootFSEntryID()),
			})

			// Attempt repair if enabled
			if cfg.applyRepairs {
				repair := Repair{
					Description: "Set root_fs_entry_id to NULL to trigger rescan",
				}

				if err := upload.SetRootFSEntryID(id.Nil); err != nil {
					repair.Applied = false
					repair.Error = fmt.Errorf("failed to set root_fs_entry_id: %w", err)
				} else if err := c.Repo.UpdateUpload(ctx, upload); err != nil {
					repair.Applied = false
					repair.Error = fmt.Errorf("failed to update upload: %w", err)
				} else {
					repair.Applied = true
				}

				result.Repairs = append(result.Repairs, repair)
			}
		}
		// If err != nil, it's likely "found entry is not a file" (i.e., it's a directory)
		// which is fine - the entry exists
	}

	// Check root_cid
	if cidIsNull {
		result.Passed = false
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: "DAG scan incomplete",
			Details:     "root_cid is NULL",
		})
	} else {
		// Verify node exists
		node, err := c.Repo.FindNodeByCIDAndSpaceDID(ctx, upload.RootCID(), upload.SpaceDID())
		if err != nil {
			return result, fmt.Errorf("checking if root node exists: %w", err)
		}

		if node == nil {
			result.Passed = false
			result.Issues = append(result.Issues, Issue{
				Type:        IssueTypeError,
				Description: "root_cid points to non-existent node",
				Details:     fmt.Sprintf("Node CID: %s", upload.RootCID()),
			})

			// Attempt repair if enabled
			if cfg.applyRepairs {
				repair := Repair{
					Description: "Set root_cid to NULL to trigger DAG rescan",
				}

				if err := upload.SetRootCID(cid.Undef); err != nil {
					repair.Applied = false
					repair.Error = fmt.Errorf("failed to set root_cid: %w", err)
				} else if err := c.Repo.UpdateUpload(ctx, upload); err != nil {
					repair.Applied = false
					repair.Error = fmt.Errorf("failed to update upload: %w", err)
				} else {
					repair.Applied = true
				}

				result.Repairs = append(result.Repairs, repair)
			}
		}
	}

	return result, nil
}

// checkFileSystemIntegrity validates the filesystem structure and DAG integrity.
// Starting from the root FSEntry, recursively checks:
//
// For files:
// - Ensure DAGScan exists (create if missing and cfg.applyRepairs)
// - If DAGScan complete (has CID), validate DAG structure
//
// For directories:
// - Recursively check all children first
// - Ensure DAGScan exists
// - If DAGScan complete and no child errors, validate directory DAG
// - If directory DAG invalid, clear the CID from DAGScan
//
// If root directory DAG is invalid, remove root_cid from upload.
func (c *Checker) checkFileSystemIntegrity(ctx context.Context, uploadID id.UploadID, cfg *config) (CheckResult, error) {
	result := CheckResult{
		Name:   "File System Integrity Check",
		Passed: true,
	}

	// Get the upload
	upload, err := c.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return result, fmt.Errorf("getting upload: %w", err)
	}

	// If root_fs_entry_id is not set, skip this check
	if upload.RootFSEntryID() == id.Nil {
		return result, nil
	}

	// Track issues
	var missingDAGScans []id.FSEntryID
	var invalidDAGs []id.FSEntryID
	rootInvalid := false

	// Recursively check filesystem structure
	// Returns: (hadErrors bool, error)
	var checkFSEntry func(fsEntryID id.FSEntryID) (bool, error)
	checkFSEntry = func(fsEntryID id.FSEntryID) (bool, error) {
		// Try to get as file first
		file, err := c.Repo.GetFileByID(ctx, fsEntryID)
		if err != nil {
			return false, fmt.Errorf("error checking FSEntry %s: %w", fsEntryID, err)
		}

		isDirectory := (file == nil)
		hadErrors := false

		if isDirectory {
			// Handle directory entry
			dir, err := c.Repo.GetDirectoryByID(ctx, fsEntryID)
			if err != nil {
				return false, fmt.Errorf("getting directory %s: %w", fsEntryID, err)
			}
			if dir == nil {
				return false, fmt.Errorf("FSEntry %s is neither file nor directory", fsEntryID)
			}

			// Recursively check all children first
			children, err := c.Repo.DirectoryChildren(ctx, dir)
			if err != nil {
				return false, fmt.Errorf("getting children of directory %s: %w", fsEntryID, err)
			}

			for _, child := range children {
				childHadErrors, err := checkFSEntry(child.ID())
				if err != nil {
					return false, err
				}
				if childHadErrors {
					hadErrors = true
				}
			}
		}

		// Check if DAGScan exists
		dagScan, err := c.Repo.GetDAGScanByFSEntryID(ctx, fsEntryID)
		if err != nil {
			return false, fmt.Errorf("checking DAGScan for FSEntry %s: %w", fsEntryID, err)
		}

		if dagScan == nil {
			// No DAGScan exists
			missingDAGScans = append(missingDAGScans, fsEntryID)
			hadErrors = true

			// Attempt repair if enabled
			if cfg.applyRepairs {
				_, err := c.Repo.CreateDAGScan(ctx, fsEntryID, isDirectory, uploadID, upload.SpaceDID())
				if err != nil {
					result.Repairs = append(result.Repairs, Repair{
						Description: fmt.Sprintf("Create DAGScan for FSEntry %s", fsEntryID),
						Applied:     false,
						Error:       err,
					})
				} else {
					result.Repairs = append(result.Repairs, Repair{
						Description: fmt.Sprintf("Create DAGScan for FSEntry %s", fsEntryID),
						Applied:     true,
					})
				}
			}
		} else if dagScan.HasCID() {
			// DAGScan exists and is complete - validate the DAG
			dagInvalid := false

			if isDirectory && hadErrors {
				// Directory with child errors - mark as invalid
				dagInvalid = true
			} else {
				// Validate DAG structure (check all nodes exist)
				dagInvalid, err = c.validateDAG(ctx, dagScan.CID(), upload.SpaceDID())
				if err != nil {
					return false, fmt.Errorf("validating DAG for FSEntry %s: %w", fsEntryID, err)
				}

				// For directories without child errors, also validate UnixFS entries
				if isDirectory && !dagInvalid {
					hasOrphans, err := c.validateDirectoryDAG(ctx, fsEntryID, dagScan.CID(), upload.SpaceDID())
					if err != nil {
						return false, fmt.Errorf("validating directory DAG for FSEntry %s: %w", fsEntryID, err)
					}
					if hasOrphans {
						dagInvalid = true
					}
				}
			}

			if dagInvalid {
				invalidDAGs = append(invalidDAGs, fsEntryID)
				hadErrors = true

				// Clear the CID from the DAGScan to trigger rescan
				if cfg.applyRepairs {
					if err := dagScan.ClearCID(); err != nil {
						result.Repairs = append(result.Repairs, Repair{
							Description: fmt.Sprintf("Clear invalid CID from DAGScan for FSEntry %s", fsEntryID),
							Applied:     false,
							Error:       fmt.Errorf("failed to clear CID: %w", err),
						})
					} else {
						// Update the DAGScan in the database
						if err := c.Repo.UpdateDAGScan(ctx, dagScan); err != nil {
							result.Repairs = append(result.Repairs, Repair{
								Description: fmt.Sprintf("Clear invalid CID from DAGScan for FSEntry %s", fsEntryID),
								Applied:     false,
								Error:       fmt.Errorf("failed to update DAGScan: %w", err),
							})
						} else {
							result.Repairs = append(result.Repairs, Repair{
								Description: fmt.Sprintf("Clear invalid CID from DAGScan for FSEntry %s", fsEntryID),
								Applied:     true,
							})
						}
					}
				}

				// Track if root is invalid
				if fsEntryID == upload.RootFSEntryID() {
					rootInvalid = true
				}
			}
		}

		return hadErrors, nil
	}

	// Check the root FSEntry
	_, err = checkFSEntry(upload.RootFSEntryID())
	if err != nil {
		return result, err
	}

	// Handle root directory DAG invalidity
	if rootInvalid && cfg.applyRepairs {
		// Clear the root CID from the upload to trigger re-DAG
		if err := upload.SetRootCID(cid.Undef); err != nil {
			result.Repairs = append(result.Repairs, Repair{
				Description: "Clear root CID from upload due to invalid root directory DAG",
				Applied:     false,
				Error:       err,
			})
		} else {
			// Update the upload in the database
			if err := c.Repo.UpdateUpload(ctx, upload); err != nil {
				result.Repairs = append(result.Repairs, Repair{
					Description: "Clear root CID from upload due to invalid root directory DAG",
					Applied:     false,
					Error:       fmt.Errorf("failed to update upload: %w", err),
				})
			} else {
				result.Repairs = append(result.Repairs, Repair{
					Description: "Clear root CID from upload due to invalid root directory DAG",
					Applied:     true,
				})
			}
		}
	}

	// Report issues
	if len(missingDAGScans) > 0 {
		result.Passed = false
		details := "Re-run the upload to create DAG scans for these entries."
		if !cfg.applyRepairs {
			details = "Use --repair to create missing DAGScans, or re-run the upload."
		}
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: fmt.Sprintf("Found %d FSEntry(s) without DAGScans", len(missingDAGScans)),
			Details:     details,
		})
	}

	if len(invalidDAGs) > 0 {
		result.Passed = false
		var details string
		if cfg.applyRepairs {
			details = "Invalid DAGs detected - CIDs have been cleared from DAGScans to trigger rescan. Re-run the upload to rebuild DAGs."
			if rootInvalid {
				details += " Root directory DAG was invalid - upload.root_cid has also been cleared."
			}
		} else {
			details = "Invalid DAGs detected. Use --repair to clear CIDs from DAGScans and trigger rescan."
			if rootInvalid {
				details += " Root directory DAG is invalid - use --repair to also clear upload.root_cid."
			}
		}
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: fmt.Sprintf("Found %d FSEntry(s) with invalid DAGs", len(invalidDAGs)),
			Details:     details,
		})
	}

	return result, nil
}

// validateDAG checks if a DAG structure is valid
// Returns true if invalid, false if valid
func (c *Checker) validateDAG(ctx context.Context, rootCID cid.Cid, spaceDID did.DID) (bool, error) {
	// Get the root node
	node, err := c.Repo.FindNodeByCIDAndSpaceDID(ctx, rootCID, spaceDID)
	if err != nil {
		return false, fmt.Errorf("finding root node %s: %w", rootCID, err)
	}
	if node == nil {
		// Node doesn't exist - DAG is invalid
		return true, nil
	}

	// Check node structure recursively
	visited := make(map[cid.Cid]struct{})
	var checkNode func(nodeCID cid.Cid) (bool, error)
	checkNode = func(nodeCID cid.Cid) (bool, error) {
		if _, ok := visited[nodeCID]; ok {
			return false, nil
		}
		visited[nodeCID] = struct{}{}

		node, err := c.Repo.FindNodeByCIDAndSpaceDID(ctx, nodeCID, spaceDID)
		if err != nil {
			return false, fmt.Errorf("finding node %s: %w", nodeCID, err)
		}
		if node == nil {
			// Node doesn't exist - invalid
			return true, nil
		}

		// Get links for this node
		links, err := c.Repo.LinksForCID(ctx, nodeCID, spaceDID)
		if err != nil {
			return false, fmt.Errorf("getting links for node %s: %w", nodeCID, err)
		}

		// For UnixFS nodes, validate that they have the expected number of children
		if unixfsNode, ok := node.(*dagsmodel.UnixFSNode); ok {
			// Decode UnixFS metadata
			fsNode, err := unixfs.FSNodeFromBytes(unixfsNode.UFSData())
			if err != nil {
				// If we can't decode the metadata, consider it invalid
				return true, nil
			}

			// Strict validation: child count must exactly match metadata
			expectedChildren := fsNode.NumChildren()
			actualChildren := len(links)

			// NumChildren() returns the number of block sizes recorded in metadata
			// For files, this should exactly match the number of links
			if expectedChildren != actualChildren {
				// Mismatch between metadata and actual links - invalid
				// This catches:
				// - Missing children (expected > actual)
				// - Extra unexpected links (expected < actual)
				// - Complete absence when expected (expected > 0, actual == 0)
				return true, nil
			}
		}

		// Check that all linked nodes exist
		// Note: Deeper UnixFS validation (matching directory entries to filesystem)
		// is done separately by validateDirectoryDAG
		for _, link := range links {
			invalid, err := checkNode(link.Hash())
			if err != nil {
				return false, err
			}
			if invalid {
				return true, nil
			}
		}

		return false, nil
	}

	return checkNode(rootCID)
}

// validateDirectoryDAG validates that a directory's UnixFS DAG entries match filesystem children.
// Checks for orphaned/mismatched entries:
// - DAG references non-existent children → Error
// - DAG has wrong CIDs for children → Error
// - DAG missing new children → OK (expected staleness)
// Returns (hasOrphans bool, error)
func (c *Checker) validateDirectoryDAG(ctx context.Context, dirFsEntryID id.FSEntryID, dagRootCID cid.Cid, spaceDID did.DID) (bool, error) {
	// Get filesystem children
	dir, err := c.Repo.GetDirectoryByID(ctx, dirFsEntryID)
	if err != nil {
		return false, fmt.Errorf("getting directory: %w", err)
	}
	if dir == nil {
		return false, fmt.Errorf("directory not found: %s", dirFsEntryID)
	}

	fsChildren, err := c.Repo.DirectoryChildren(ctx, dir)
	if err != nil {
		return false, fmt.Errorf("getting directory children: %w", err)
	}

	// Build map of filesystem children: name -> (fsEntryID, dagScanCID)
	fsChildMap := make(map[string]struct {
		fsEntryID id.FSEntryID
		dagCID    cid.Cid
	})
	for _, child := range fsChildren {
		dagScan, err := c.Repo.GetDAGScanByFSEntryID(ctx, child.ID())
		if err != nil {
			return false, fmt.Errorf("getting DAGScan for child %s: %w", child.ID(), err)
		}

		// Only include children that have completed DAGScans
		if dagScan != nil && dagScan.HasCID() {
			fsChildMap[child.Path()] = struct {
				fsEntryID id.FSEntryID
				dagCID    cid.Cid
			}{
				fsEntryID: child.ID(),
				dagCID:    dagScan.CID(),
			}
		}
	}

	// Get the directory's UnixFS node
	dirNode, err := c.Repo.FindNodeByCIDAndSpaceDID(ctx, dagRootCID, spaceDID)
	if err != nil {
		return false, fmt.Errorf("finding directory node: %w", err)
	}
	if dirNode == nil {
		// Node doesn't exist - this should have been caught by validateDAG
		return true, nil
	}

	// Check if it's a UnixFS node
	_, ok := dirNode.(*dagsmodel.UnixFSNode)
	if !ok {
		// Not a UnixFS node - might be a raw node directory, skip validation
		return false, nil
	}

	// Get the links for this directory
	links, err := c.Repo.LinksForCID(ctx, dagRootCID, spaceDID)
	if err != nil {
		return false, fmt.Errorf("getting links for directory: %w", err)
	}

	// Decode the UnixFS data to validate it's a directory
	// We'll use the links we already have from the database
	// The UFSData contains the UnixFS metadata, links are stored separately in our DB

	hasOrphans := false

	// Check each link in the DAG
	for _, link := range links {
		linkName := link.Name()
		linkCID := link.Hash()

		// Check if this link exists in filesystem
		if fsChild, exists := fsChildMap[linkName]; exists {
			// Check if CID matches
			if !fsChild.dagCID.Equals(linkCID) {
				// CID mismatch - this is an error
				hasOrphans = true
			}
		} else {
			// Link not in filesystem - this is an orphan
			hasOrphans = true
		}
	}

	return hasOrphans, nil
}

// checkNodeIntegrity ensures all nodes in the upload have node_uploads records.
// Traverses the DAG from root_cid and verifies each node has a corresponding
// entry in node_uploads table.
//
// Repairs:
// - Create missing node_uploads records with shard_id = NULL
func (c *Checker) checkNodeIntegrity(ctx context.Context, uploadID id.UploadID, cfg *config) (CheckResult, error) {
	result := CheckResult{
		Name:   "Node Integrity Check",
		Passed: true,
	}

	// Get the upload
	upload, err := c.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return result, fmt.Errorf("getting upload: %w", err)
	}

	// If root_cid is not set, skip this check
	if upload.RootCID() == cid.Undef {
		return result, nil
	}

	// Traverse DAG and collect all node CIDs
	visited := make(map[cid.Cid]struct{})
	var missingNodeUploads []cid.Cid

	var traverse func(nodeCID cid.Cid) error
	traverse = func(nodeCID cid.Cid) error {
		// Skip if already visited
		if _, ok := visited[nodeCID]; ok {
			return nil
		}
		visited[nodeCID] = struct{}{}

		// Check if node_uploads record exists
		hasRecord, err := c.Repo.NodeUploadExists(ctx, nodeCID, upload.SpaceDID(), uploadID)
		if err != nil {
			return fmt.Errorf("checking node_uploads for %s: %w", nodeCID, err)
		}

		if !hasRecord {
			missingNodeUploads = append(missingNodeUploads, nodeCID)

			// Attempt repair if enabled
			if cfg.applyRepairs {
				if err := c.Repo.CreateNodeUpload(ctx, nodeCID, upload.SpaceDID(), uploadID); err != nil {
					result.Repairs = append(result.Repairs, Repair{
						Description: fmt.Sprintf("Create node_uploads record for node %s", nodeCID),
						Applied:     false,
						Error:       err,
					})
				} else {
					result.Repairs = append(result.Repairs, Repair{
						Description: fmt.Sprintf("Create node_uploads record for node %s", nodeCID),
						Applied:     true,
					})
				}
			}
		}

		// Get links and traverse children
		links, err := c.Repo.LinksForCID(ctx, nodeCID, upload.SpaceDID())
		if err != nil {
			return fmt.Errorf("getting links for %s: %w", nodeCID, err)
		}

		for _, link := range links {
			if err := traverse(link.Hash()); err != nil {
				return err
			}
		}

		return nil
	}

	if err := traverse(upload.RootCID()); err != nil {
		return result, err
	}

	if len(missingNodeUploads) > 0 {
		result.Passed = false
		details := "This indicates database inconsistency."
		if !cfg.applyRepairs {
			details += " Use --repair to create missing records."
		}
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: fmt.Sprintf("Found %d node(s) without node_uploads records", len(missingNodeUploads)),
			Details:     details,
		})
	}

	return result, nil
}

// checkNodeCompleteness verifies all nodes have been assigned to shards.
// Queries for nodes in node_uploads where shard_id IS NULL.
func (c *Checker) checkNodeCompleteness(ctx context.Context, uploadID id.UploadID, cfg *config) (CheckResult, error) {
	result := CheckResult{
		Name:   "Node Completeness Check",
		Passed: true,
	}

	// Get the upload to get spaceDID
	upload, err := c.Repo.GetUploadByID(ctx, uploadID)
	if err != nil {
		return result, fmt.Errorf("getting upload: %w", err)
	}

	// Check for nodes not yet assigned to shards
	unshardedCIDs, err := c.Repo.NodesNotInShards(ctx, uploadID, upload.SpaceDID())
	if err != nil {
		return result, fmt.Errorf("checking for unsharded nodes: %w", err)
	}

	if len(unshardedCIDs) > 0 {
		result.Passed = false
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: fmt.Sprintf("Found %d node(s) not assigned to shards", len(unshardedCIDs)),
			Details:     "Re-run the upload to shard these nodes",
		})
		// No automatic repair - re-running upload will shard them
	}

	return result, nil
}

// checkShardCompleteness verifies all shards are uploaded and indexed.
// Checks:
// - All shards have state = BlobStateAdded
// - No shards are missing from indexes (ShardsNotInIndexes returns empty)
func (c *Checker) checkShardCompleteness(ctx context.Context, uploadID id.UploadID, cfg *config) (CheckResult, error) {
	result := CheckResult{
		Name:   "Shard Completeness Check",
		Passed: true,
	}

	// Get all shards for upload
	shards, err := c.Repo.ShardsForUpload(ctx, uploadID)
	if err != nil {
		return result, fmt.Errorf("getting shards for upload: %w", err)
	}

	// Check each shard's state
	incompleteCount := 0
	for _, shard := range shards {
		if shard.State() != blobsmodel.BlobStateAdded {
			incompleteCount++
		}
	}

	if incompleteCount > 0 {
		result.Passed = false
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: fmt.Sprintf("%d shard(s) not fully uploaded", incompleteCount),
			Details:     "Re-run the upload to complete shard uploads",
		})
	}

	// Check for shards not yet assigned to indexes
	unindexedShardIDs, err := c.Repo.ShardsNotInIndexes(ctx, uploadID)
	if err != nil {
		return result, fmt.Errorf("checking for unindexed shards: %w", err)
	}

	if len(unindexedShardIDs) > 0 {
		result.Passed = false
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: fmt.Sprintf("%d shard(s) not assigned to indexes", len(unindexedShardIDs)),
			Details:     "Re-run the upload to index these shards",
		})
	}

	return result, nil
}

// checkIndexCompleteness verifies all indexes are uploaded.
// Checks that all indexes have state = BlobStateAdded.
func (c *Checker) checkIndexCompleteness(ctx context.Context, uploadID id.UploadID, cfg *config) (CheckResult, error) {
	result := CheckResult{
		Name:   "Index Completeness Check",
		Passed: true,
	}

	// Get all indexes for upload
	indexes, err := c.Repo.IndexesForUpload(ctx, uploadID)
	if err != nil {
		return result, fmt.Errorf("getting indexes for upload: %w", err)
	}

	// Check each index's state
	incompleteCount := 0
	for _, index := range indexes {
		if index.State() != blobsmodel.BlobStateAdded {
			incompleteCount++
		}
	}

	if incompleteCount > 0 {
		result.Passed = false
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: fmt.Sprintf("%d index(es) not fully uploaded", incompleteCount),
			Details:     "Re-run the upload to complete index uploads",
		})
	}

	return result, nil
}
