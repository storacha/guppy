-- +goose Up
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_shards_upload_id_state
  ON shards (upload_id, state);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_shards_upload_id_state;
-- +goose StatementEnd
