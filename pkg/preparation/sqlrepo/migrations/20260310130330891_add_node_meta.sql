-- +goose Up
ALTER TABLE nodes ADD COLUMN meta BLOB;

-- +goose Down
ALTER TABLE nodes DROP COLUMN meta;
