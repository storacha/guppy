-- +goose Up
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_links_parent_id_space_did
  ON links (parent_id, space_did);
CREATE INDEX IF NOT EXISTS idx_nodes_in_shards_shard_id_shard_offset
  ON nodes_in_shards (shard_id, shard_offset);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_links_parent_id_space_did;
DROP INDEX IF EXISTS idx_nodes_in_shards_shard_id_shard_offset;
-- +goose StatementEnd
