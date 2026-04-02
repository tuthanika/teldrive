-- +goose Up
ALTER TABLE teldrive.files ADD COLUMN IF NOT EXISTS topic_id integer;
ALTER TABLE teldrive.channels ADD COLUMN IF NOT EXISTS topic_id integer;

-- +goose Down
ALTER TABLE teldrive.files DROP COLUMN IF EXISTS topic_id;
ALTER TABLE teldrive.channels DROP COLUMN IF EXISTS topic_id;
