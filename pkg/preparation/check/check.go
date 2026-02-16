package check

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/guppy/pkg/preparation"
	blobsmodel "github.com/storacha/guppy/pkg/preparation/blobs/model"
	dagsmodel "github.com/storacha/guppy/pkg/preparation/dags/model"
	"github.com/storacha/guppy/pkg/preparation/dags/nodereader"
	scansmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

type OpenNodeReaderFunc func() (nodereader.NodeReader, error)

// Checker provides methods for checking upload integrity and completeness.
type Checker struct {
	OpenNodeReader OpenNodeReaderFunc
	Repo           preparation.Repo
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

	rootFSEntryID := upload.RootFSEntryID()
	rootCID := upload.RootCID()

	// Check root_fs_entry_id
	if rootFSEntryID == id.Nil {
		result.Passed = false
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: "Filesystem scan not started or incomplete. Re-run the upload to start scanning.",
			Details:     "root_fs_entry_id is NULL",
		})
	} else {
		// Verify FSEntry exists (could be file or directory)
		entry, err := c.Repo.GetFSEntryByID(ctx, upload.RootFSEntryID())
		if entry == nil && err == nil {
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
	}

	// Check root_cid
	if rootCID == cid.Undef {
		result.Passed = false
		result.Issues = append(result.Issues, Issue{
			Type:        IssueTypeError,
			Description: "DAG scan incomplete. Re-run the upload to start scanning",
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
// If root FSEntry DAG is invalid, remove root_cid from upload.
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
		entry, err := c.Repo.GetFSEntryByID(ctx, fsEntryID)
		if err != nil {
			return false, fmt.Errorf("error checking FSEntry %s: %w", fsEntryID, err)
		}

		// Handle case where FSEntry doesn't exist (shouldn't happen with FK constraints,
		// but could occur with disabled FK constraints in tests or database corruption)
		if entry == nil {
			return true, nil // Entry doesn't exist - mark as having errors
		}

		_, isDirectory := entry.(*scansmodel.Directory)
		hadErrors := false

		if isDirectory {
			// Recursively check all children first
			children, err := c.Repo.DirectoryChildren(ctx, entry.ID())
			if err != nil {
				return false, fmt.Errorf("getting children of directory %s: %w", entry.ID(), err)
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
		dagScan, err := c.Repo.GetDAGScanByFSEntryID(ctx, entry.ID())
		if err != nil {
			return false, fmt.Errorf("checking DAGScan for FSEntry %s: %w", entry.ID(), err)
		}

		if dagScan == nil {
			// No DAGScan exists
			missingDAGScans = append(missingDAGScans, entry.ID())
			hadErrors = true

			// Attempt repair if enabled
			if cfg.applyRepairs {
				_, err := c.Repo.CreateDAGScan(ctx, entry.ID(), isDirectory, uploadID, upload.SpaceDID())
				if err != nil {
					result.Repairs = append(result.Repairs, Repair{
						Description: fmt.Sprintf("Create DAGScan for FSEntry %s", entry.ID()),
						Applied:     false,
						Error:       err,
					})
				} else {
					result.Repairs = append(result.Repairs, Repair{
						Description: fmt.Sprintf("Create DAGScan for FSEntry %s", entry.ID()),
						Applied:     true,
					})
				}
			}
		} else if dagScan.HasCID() {
			// DAGScan exists and is complete - validate the DAG
			dagInvalid := false

			if isDirectory {
				if hadErrors {
					// Directory with child errors - mark as invalid
					dagInvalid = true
				} else {
					// Validate DAG structure (check all nodes exist)
					dagInvalid, err = c.validateDAG(ctx, dagScan.CID(), upload.SpaceDID())
					if err != nil {
						return false, fmt.Errorf("validating DAG for FSEntry %s: %w", fsEntryID, err)
					}

					if !dagInvalid {
						// For directories without child errors, also validate UnixFS entries
						hasOrphans, err := c.validateDirectoryDAG(ctx, fsEntryID, dagScan.CID(), upload.SpaceDID())
						if err != nil {
							return false, fmt.Errorf("validating directory DAG for FSEntry %s: %w", fsEntryID, err)
						}
						if hasOrphans {
							dagInvalid = true
						}
					}
				}
			} else {
				// Validate DAG structure (check all nodes exist)
				dagInvalid, err = c.validateDAG(ctx, dagScan.CID(), upload.SpaceDID())
				if err != nil {
					return false, fmt.Errorf("validating DAG for FSEntry %s: %w", fsEntryID, err)
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

	// Handle root FSEntry DAG invalidity
	if rootInvalid && cfg.applyRepairs {
		// Clear the root CID from the upload to trigger re-DAG
		if err := upload.SetRootCID(cid.Undef); err != nil {
			result.Repairs = append(result.Repairs, Repair{
				Description: "Clear root CID from upload due to invalid root FSEntry DAG",
				Applied:     false,
				Error:       err,
			})
		} else {
			// Update the upload in the database
			if err := c.Repo.UpdateUpload(ctx, upload); err != nil {
				result.Repairs = append(result.Repairs, Repair{
					Description: "Clear root CID from upload due to invalid root FSEntry DAG",
					Applied:     false,
					Error:       fmt.Errorf("failed to update upload: %w", err),
				})
			} else {
				result.Repairs = append(result.Repairs, Repair{
					Description: "Clear root CID from upload due to invalid root FSEntry DAG",
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
				details += " Root FSEntry DAG was invalid - upload.root_cid has also been cleared."
			}
		} else {
			details = "Invalid DAGs detected. Use --repair to clear CIDs from DAGScans and trigger rescan."
			if rootInvalid {
				details += " Root FSEntry DAG is invalid - use --repair to also clear upload.root_cid."
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
	rootNode, err := c.Repo.FindNodeByCIDAndSpaceDID(ctx, rootCID, spaceDID)
	if err != nil {
		return false, fmt.Errorf("finding root node %s: %w", rootCID, err)
	}
	if rootNode == nil {
		// Node doesn't exist - DAG is invalid
		return true, nil
	}

	// Check node structure recursively
	visited := make(map[cid.Cid]struct{})
	var checkNode func(nodeCID cid.Cid, expectedSize uint64) (bool, error)
	checkNode = func(nodeCID cid.Cid, expectedSize uint64) (bool, error) {
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

		if _, ok := node.(*dagsmodel.UnixFSNode); ok {
			// Get links for this node
			links, err := c.Repo.LinksForCID(ctx, node.CID(), spaceDID)
			if err != nil {
				return false, fmt.Errorf("getting links for node %s: %w", node.CID(), err)
			}

			// Check links recursively
			actualSize := uint64(0)
			for _, link := range links {
				actualSize += link.TSize()
				invalid, err := checkNode(link.Hash(), link.TSize())
				if err != nil {
					return false, err
				}
				if invalid {
					return true, nil
				}
			}

			return actualSize != expectedSize, nil
		} else {
			// Raw node
			return node.Size() != expectedSize, nil
		}
	}

	return checkNode(rootNode.CID(), rootNode.Size())
}

// validateDirectoryDAG validates that a directory's UnixFS DAG entries match filesystem children.
// Returns true (invalid) if:
// - DAG has entries not in filesystem (orphans)
// - DAG has entries with wrong CIDs (mismatches)
// - DAG is missing filesystem children (stale - needs rebuild)
// Returns (isInvalid bool, error)
func (c *Checker) validateDirectoryDAG(ctx context.Context, dirFsEntryID id.FSEntryID, dagRootCID cid.Cid, spaceDID did.DID) (bool, error) {
	// Get filesystem children
	fsChildren, err := c.Repo.DirectoryChildren(ctx, dirFsEntryID)
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

	nodeReader, err := c.OpenNodeReader()
	if err != nil {
		return false, fmt.Errorf("opening node reader: %w", err)
	}

	// we'll use the database and the node reader to convert CIDs to their block representation
	// go-ipld-prime's link system + unixfsnode.Reify will then allow us to traverse the dag transparently, reading "blocks" as needed,
	// and presenting an abstracted view of the directory as a map of names -> links, transparently
	// handling simple directories vs HAMTS
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		ctx := lctx.Ctx
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("unexpected link type: %T", lnk)
		}
		dirNode, err := c.Repo.FindNodeByCIDAndSpaceDID(ctx, cidLink.Cid, spaceDID)
		if err != nil {
			return nil, fmt.Errorf("finding node for link %s: %w", cidLink.Cid, err)
		}
		if dirNode == nil {
			return nil, fmt.Errorf("linked node not found: %s", cidLink.Cid)
		}
		data, err := nodeReader.GetData(ctx, dirNode)
		if err != nil {
			return nil, fmt.Errorf("getting data for node %s: %w", cidLink.Cid, err)
		}
		return bytes.NewReader(data), nil
	}

	nd, err := lsys.Load(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: dagRootCID}, dagpb.Type.PBNode)
	if err != nil {
		return false, fmt.Errorf("loading root DAG node: %w", err)
	}
	// modify the interface to reify the node to a UnixFS node, which will enable us to treat the directory as a map of paths
	nd, err = unixfsnode.Reify(linking.LinkContext{Ctx: ctx}, nd, &lsys)
	if err != nil {
		return false, fmt.Errorf("reifying root DAG node to UnixFS: %w", err)
	}

	// hasOrphans tracks if there are DAG entries not in FS or with mismatched CIDs
	hasOrphans := false
	iter := nd.MapIterator()
	// need to verify there are no orphans from the FS children that are not represented in
	// in the dag -- which we do by insuring every single FS node is matched
	matched := map[string]struct{}{}
	// Check each link in the DAG
	for !iter.Done() {
		nameNode, lnkNode, err := iter.Next()
		if err != nil {
			return false, fmt.Errorf("iterating directory DAG: %w", err)
		}

		linkName, err := nameNode.AsString()
		if err != nil {
			return false, fmt.Errorf("getting link name as string: %w", err)
		}
		lnk, err := lnkNode.AsLink()
		if err != nil {
			return false, fmt.Errorf("getting link from node: %w", err)
		}
		linkCID := lnk.(cidlink.Link).Cid

		// Check if this link exists in filesystem
		if fsChild, exists := fsChildMap[linkName]; exists {
			// Check if CID matches
			if !fsChild.dagCID.Equals(linkCID) {
				// CID mismatch - this is an error
				hasOrphans = true
			} else {
				matched[linkName] = struct{}{}
			}
		} else {
			// Link not in filesystem - this is an orphan
			hasOrphans = true
		}
	}

	// Invalid if:
	// 1. DAG has orphans (entries not in FS or with wrong CIDs), OR
	// 2. DAG is missing FS children (stale - needs rebuild)
	return hasOrphans || len(matched) != len(fsChildMap), nil
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
