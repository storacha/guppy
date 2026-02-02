-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS indexes (
  id BLOB PRIMARY KEY,
  upload_id BLOB NOT NULL,
  digest BLOB,
  piece_cid BLOB,
  size INTEGER NOT NULL DEFAULT 0,
  slice_count INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL,
  location_inv BLOB,
  pdp_accept_inv BLOB
) STRICT;

CREATE TABLE IF NOT EXISTS shards_in_indexes (
  shard_id BLOB NOT NULL,
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
