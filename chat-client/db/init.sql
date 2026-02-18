CREATE TABLE messages (
    sequence_id TEXT PRIMARY KEY, -- Changed to TEXT to support UUIDs from server
    sender_id TEXT,
    content TEXT,
    created_at INTEGER
);

CREATE TABLE user_state (
    user_id TEXT PRIMARY KEY,
    email TEXT,
    user_name TEXT,
    last_sync_sequence_id TEXT -- Changed to TEXT to support UUIDs
);
