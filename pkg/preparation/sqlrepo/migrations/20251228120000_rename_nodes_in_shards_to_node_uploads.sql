-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS node_uploads (
  node_cid BLOB NOT NULL,
  space_did BLOB NOT NULL,
  upload_id BLOB NOT NULL,
  shard_id BLOB,
  shard_offset INTEGER,
  FOREIGN KEY (node_cid, space_did) REFERENCES nodes(cid, space_did) ON DELETE CASCADE,
  FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
  FOREIGN KEY (shard_id) REFERENCES shards(id) ON DELETE SET NULL,
  PRIMARY KEY (node_cid, space_did, upload_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_node_uploads_no_shard
  ON node_uploads(upload_id) WHERE shard_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_node_uploads_shard_id_shard_offset
  ON node_uploads (shard_id, shard_offset) WHERE shard_id IS NOT NULL;

INSERT INTO node_uploads (node_cid, space_did, upload_id, shard_id, shard_offset)
SELECT
  nis.node_cid,
  nis.space_did,
  s.upload_id,
  nis.shard_id,
  nis.shard_offset
FROM nodes_in_shards nis
JOIN shards s ON s.id = nis.shard_id;

DROP TABLE IF EXISTS nodes_in_shards;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS nodes_in_shards (
  node_cid BLOB NOT NULL,
  space_did BLOB NOT NULL,
  shard_id BLOB NOT NULL,
  shard_offset INTEGER NOT NULL,
  FOREIGN KEY (node_cid, space_did) REFERENCES nodes(cid, space_did) ON DELETE CASCADE,
  FOREIGN KEY (shard_id) REFERENCES shards(id) ON DELETE CASCADE,
  PRIMARY KEY (node_cid, space_did, shard_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_nodes_in_shards_shard_id_shard_offset
  ON nodes_in_shards (shard_id, shard_offset);

INSERT INTO nodes_in_shards (node_cid, space_did, shard_id, shard_offset)
SELECT node_cid, space_did, shard_id, shard_offset
FROM node_uploads
WHERE shard_id IS NOT NULL;

DROP INDEX IF EXISTS idx_node_uploads_no_shard;
DROP INDEX IF EXISTS idx_node_uploads_shard_id_shard_offset;
DROP TABLE IF EXISTS node_uploads;
-- +goose StatementEnd
