-- +goose Up
-- Index needed to make the backfill UPDATE fast (avoids full table scans of
-- node_uploads for each shard).
CREATE INDEX IF NOT EXISTS idx_node_uploads_shard_id
  ON node_uploads (shard_id);

-- Backfill slice_count for shards created before the column was added.
-- The column was added in 20251211111007 with DEFAULT 0 and no backfill,
-- so any pre-existing shards have slice_count = 0 despite having nodes.
UPDATE shards
SET slice_count = (
  SELECT COUNT(*)
  FROM node_uploads
  WHERE node_uploads.shard_id = shards.id
)
WHERE slice_count = 0
  AND EXISTS (SELECT 1 FROM node_uploads WHERE node_uploads.shard_id = shards.id);

-- +goose Down
DROP INDEX IF EXISTS idx_node_uploads_shard_id;
