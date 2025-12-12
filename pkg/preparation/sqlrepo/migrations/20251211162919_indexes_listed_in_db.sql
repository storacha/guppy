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
  state TEXT NOT NULL
) STRICT;

-- The fact that a shard has been assigned to a index.
CREATE TABLE IF NOT EXISTS shards_in_indexes (
  -- Which shard we're talking about
  shard_cid BLOB NOT NULL,
  space_did BLOB NOT NULL,
  -- Which index this shard is in
  index_id BLOB NOT NULL,
  FOREIGN KEY (shard_cid, space_did) REFERENCES shards(cid, space_did) ON DELETE CASCADE,
  FOREIGN KEY (index_id) REFERENCES indexes(id) ON DELETE CASCADE,
  PRIMARY KEY (shard_cid, space_did, index_id)
) STRICT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS shards_in_indexes;
DROP TABLE IF EXISTS indexes;
-- +goose StatementEnd