-- +goose Up
-- +goose StatementBegin
ALTER TABLE shards ADD COLUMN slice_count INTEGER DEFAULT 0 NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE shards DROP COLUMN slice_count;
-- +goose StatementEnd
