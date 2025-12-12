-- +goose Up
-- +goose StatementBegin
CREATE UNIQUE INDEX IF NOT EXISTS idx_fs_entries_lookup
  ON fs_entries (space_did, source_id, path, last_modified, mode, size, checksum);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_fs_entries_lookup;
-- +goose StatementEnd
