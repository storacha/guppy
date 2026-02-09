-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS indexes (
  -- UUID identifying the index locally
  id BLOB PRIMARY KEY,
  -- The upload this index belongs to
  upload_id BLOB NOT NULL,
  -- The multihash digest of the completed index
  -- If NULL, has not yet been calculated (and maybe cannot be, if still
  -- accepting new shards)
  digest BLOB,
  -- The piece CID of the completed index
  piece_cid BLOB,
  -- The size of the completed index in bytes
  size INTEGER NOT NULL DEFAULT 0,
  -- The total number of slices (nodes) across all shards in this index
  slice_count INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL,
  -- The location commitment from space/blob/add
  location_inv BLOB,
  -- The PDP accept invocation from space/blob/add 1
  pdp_accept_inv BLOB
) STRICT;

-- The fact that a shard has been assigned to a index.
CREATE TABLE IF NOT EXISTS shards_in_indexes (
  -- Which shard we're talking about
  shard_id BLOB NOT NULL,
  -- Which index this shard is in
  index_id BLOB NOT NULL,
  FOREIGN KEY (shard_id) REFERENCES shards(id) ON DELETE CASCADE,
  FOREIGN KEY (index_id) REFERENCES indexes(id) ON DELETE CASCADE,
  PRIMARY KEY (shard_id, index_id)
) STRICT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS shards_in_indexes;
DROP TABLE IF EXISTS indexes;
-- +goose StatementEnd
