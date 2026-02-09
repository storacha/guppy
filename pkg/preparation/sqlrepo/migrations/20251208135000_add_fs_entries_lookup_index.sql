-- +goose Up
CREATE UNIQUE INDEX IF NOT EXISTS idx_fs_entries_lookup
  ON fs_entries (space_did, source_id, path, last_modified, "mode", size, "checksum");

-- +goose Down
DROP INDEX IF EXISTS idx_fs_entries_lookup;
