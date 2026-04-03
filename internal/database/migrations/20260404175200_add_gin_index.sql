-- +goose Up
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_files_parts_gin ON teldrive.files USING GIN (parts);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS teldrive.idx_files_parts_gin;
-- +goose StatementEnd
