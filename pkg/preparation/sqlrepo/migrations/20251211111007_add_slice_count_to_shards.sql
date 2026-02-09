-- +goose Up
ALTER TABLE shards ADD COLUMN slice_count INTEGER DEFAULT 0 NOT NULL;

-- +goose Down
ALTER TABLE shards DROP COLUMN slice_count;
