-- +goose Up
CREATE INDEX IF NOT EXISTS idx_shards_upload_id_state
  ON shards (upload_id, state);

-- +goose Down
DROP INDEX IF EXISTS idx_shards_upload_id_state;
