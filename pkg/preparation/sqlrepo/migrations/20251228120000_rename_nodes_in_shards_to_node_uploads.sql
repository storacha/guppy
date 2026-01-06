-- +goose Up
-- +goose StatementBegin

-- Create the new node_uploads table with updated schema
-- Primary key is now (node_cid, space_did, upload_id) since a node belongs to exactly one upload
-- shard_id and shard_offset are nullable for nodes not yet assigned to shards
CREATE TABLE IF NOT EXISTS node_uploads (
  -- Which node we're talking about
  node_cid BLOB NOT NULL,
  space_did BLOB NOT NULL,
  -- Which upload this node belongs to
  upload_id BLOB NOT NULL,
  -- Which shard this node is in (NULL if not yet assigned)
  shard_id BLOB,
  -- Offset of the node in the shard (NULL if not yet assigned)
  shard_offset INTEGER,
  FOREIGN KEY (node_cid, space_did) REFERENCES nodes(cid, space_did) ON DELETE CASCADE,
  FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
  FOREIGN KEY (shard_id) REFERENCES shards(id) ON DELETE SET NULL,
  PRIMARY KEY (node_cid, space_did, upload_id)
) STRICT;

-- Create partial index for efficient querying of nodes not yet in shards
CREATE INDEX IF NOT EXISTS idx_node_uploads_no_shard
  ON node_uploads(upload_id) WHERE shard_id IS NULL;

-- Create index for efficient querying by shard_id and shard_offset
CREATE INDEX IF NOT EXISTS idx_node_uploads_shard_id_shard_offset
  ON node_uploads (shard_id, shard_offset) WHERE shard_id IS NOT NULL;

-- Migrate existing data from nodes_in_shards to node_uploads
-- Join with shards table to get upload_id for each node
INSERT INTO node_uploads (node_cid, space_did, upload_id, shard_id, shard_offset)
SELECT
  nis.node_cid,
  nis.space_did,
  s.upload_id,
  nis.shard_id,
  nis.shard_offset
FROM nodes_in_shards nis
JOIN shards s ON s.id = nis.shard_id;

-- Drop the old table
DROP TABLE IF EXISTS nodes_in_shards;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Recreate the old nodes_in_shards table
CREATE TABLE IF NOT EXISTS nodes_in_shards (
  node_cid BLOB NOT NULL,
  space_did BLOB NOT NULL,
  shard_id BLOB NOT NULL,
  shard_offset INTEGER NOT NULL,
  FOREIGN KEY (node_cid, space_did) REFERENCES nodes(cid, space_did) ON DELETE CASCADE,
  FOREIGN KEY (shard_id) REFERENCES shards(id) ON DELETE CASCADE,
  PRIMARY KEY (node_cid, space_did, shard_id)
) STRICT;

-- Recreate the old index
CREATE INDEX IF NOT EXISTS idx_nodes_in_shards_shard_id_shard_offset
  ON nodes_in_shards (shard_id, shard_offset);

-- Migrate data back (only records that have shard assignments)
INSERT INTO nodes_in_shards (node_cid, space_did, shard_id, shard_offset)
SELECT node_cid, space_did, shard_id, shard_offset
FROM node_uploads
WHERE shard_id IS NOT NULL;

-- Drop the new table and index
DROP INDEX IF EXISTS idx_node_uploads_no_shard;
DROP INDEX IF EXISTS idx_node_uploads_shard_id_shard_offset;
DROP TABLE IF EXISTS node_uploads;

-- +goose StatementEnd
