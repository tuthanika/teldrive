-- +goose Up
CREATE TABLE IF NOT EXISTS teldrive.scan_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id BIGINT NOT NULL,
    folder_id VARCHAR(255),
    channel_id BIGINT NOT NULL,
    topic_id BIGINT,
    split_mode BOOLEAN DEFAULT FALSE,
    rule_mode BOOLEAN DEFAULT FALSE,
    rule_string TEXT,
    repeat_enabled BOOLEAN DEFAULT FALSE,
    schedule TEXT,
    status VARCHAR(50) DEFAULT 'waiting',
    scanned_count INT DEFAULT 0,
    imported_count INT DEFAULT 0,
    logs JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES teldrive.users(user_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS scan_tasks_user_id_idx ON teldrive.scan_tasks(user_id);
CREATE INDEX IF NOT EXISTS scan_tasks_status_idx ON teldrive.scan_tasks(status);

-- +goose Down
DROP TABLE IF EXISTS teldrive.scan_tasks;
