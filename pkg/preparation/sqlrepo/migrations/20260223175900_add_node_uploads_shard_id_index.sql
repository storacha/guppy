-- +goose Up
CREATE INDEX IF NOT EXISTS idx_node_uploads_shard_id
  ON node_uploads (shard_id);

-- +goose Down
DROP INDEX IF EXISTS idx_node_uploads_shard_id;
