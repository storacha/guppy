-- +goose Up
-- +goose StatementBegin
ALTER TABLE shards ADD COLUMN piece_cid BLOB;
ALTER TABLE shards ADD COLUMN digest_state_up_to INTEGER DEFAULT 0 NOT NULL;
ALTER TABLE shards ADD COLUMN digest_state BLOB;
ALTER TABLE shards ADD COLUMN piece_cid_state BLOB;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE shards DROP COLUMN piece_cid;
ALTER TABLE shards DROP COLUMN digest_state_up_to;
ALTER TABLE shards DROP COLUMN digest_state;
ALTER TABLE shards DROP COLUMN piece_cid_state;
-- +goose StatementEnd
