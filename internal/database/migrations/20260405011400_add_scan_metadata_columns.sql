-- +goose Up
ALTER TABLE teldrive.scan_tasks ADD COLUMN IF NOT EXISTS channel_name TEXT;
ALTER TABLE teldrive.scan_tasks ADD COLUMN IF NOT EXISTS topic_name TEXT;

-- +goose Down
ALTER TABLE teldrive.scan_tasks DROP COLUMN IF EXISTS channel_name;
ALTER TABLE teldrive.scan_tasks DROP COLUMN IF EXISTS topic_name;
