-- +goose Up
-- +goose StatementBegin
ALTER TABLE shards
ADD COLUMN location_inv BLOB;
ALTER TABLE shards
ADD COLUMN pdp_accept_inv BLOB;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
ALTER TABLE shards DROP COLUMN pdp_accept_inv;
ALTER TABLE shards DROP COLUMN location_inv;
-- +goose StatementEnd