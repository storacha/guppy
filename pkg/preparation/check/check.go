package check

import (
	"context"
	"fmt"

	"github.com/storacha/guppy/pkg/preparation"
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
//   1. Upload Scanned Check - Verify FS and DAG scans completed
//   2. File System Integrity Check - Verify FS structure and DAG validity
//   3. Node Integrity Check - Verify all nodes have upload records
//   4. Node Completeness Check - Verify all nodes are in shards
//   5. Shard Completeness Check - Verify all shards are uploaded and indexed
//   6. Index Completeness Check - Verify all indexes are uploaded
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

	// TODO: Implement
	// 1. Get upload
	// 2. Check root_fs_entry_id is not NULL
	// 3. If not NULL, verify FSEntry exists (query fs_entries)
	// 4. If missing, add issue and optionally repair (set to NULL)
	// 5. Check root_cid is not NULL
	// 6. If not NULL, verify node exists (query nodes)
	// 7. If missing, add issue and optionally repair (set to NULL)

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

	// TODO: Implement
	// This is the most complex check - requires recursive traversal
	// 1. Get upload and root FSEntry
	// 2. Recursively traverse from root:
	//    - For files: check DAGScan, validate DAG if complete
	//    - For directories: check children first, then DAGScan
	// 3. Validate DAG structure (UnixFS nodes have links, RawNodes don't)
	// 4. Track any invalidated DAGScans
	// 5. If root DAG invalidated, clear upload.root_cid

	return result, nil
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

	// TODO: Implement
	// 1. Get upload.root_cid
	// 2. Traverse DAG from root (follow links recursively)
	// 3. For each node CID, check if node_uploads record exists
	// 4. If missing, add issue and optionally create record

	return result, nil
}

// checkNodeCompleteness verifies all nodes have been assigned to shards.
// Queries for nodes in node_uploads where shard_id IS NULL.
func (c *Checker) checkNodeCompleteness(ctx context.Context, uploadID id.UploadID, cfg *config) (CheckResult, error) {
	result := CheckResult{
		Name:   "Node Completeness Check",
		Passed: true,
	}

	// TODO: Implement
	// 1. Call repo.NodesNotInShards(uploadID)
	// 2. If any returned, add issues
	// 3. No automatic repair (re-running upload will shard them)

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

	// TODO: Implement
	// 1. Get all shards for upload
	// 2. Check each shard.state == BlobStateAdded
	// 3. If not, add issue (no auto-repair, re-running upload will retry)
	// 4. Call repo.ShardsNotInIndexes(uploadID)
	// 5. If any returned, add issues

	return result, nil
}

// checkIndexCompleteness verifies all indexes are uploaded.
// Checks that all indexes have state = BlobStateAdded.
func (c *Checker) checkIndexCompleteness(ctx context.Context, uploadID id.UploadID, cfg *config) (CheckResult, error) {
	result := CheckResult{
		Name:   "Index Completeness Check",
		Passed: true,
	}

	// TODO: Implement
	// 1. Get all indexes for upload
	// 2. Check each index.state == BlobStateAdded
	// 3. If not, add issue (no auto-repair, re-running upload will retry)

	return result, nil
}
