CREATE TABLE messages (
    client_msg_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    server_msg_id TEXT,
    conversation_id TEXT NOT NULL,
    sender_user_id TEXT NOT NULL,
    sender_email TEXT,
    recipient_user_id TEXT NOT NULL,
    recipient_email TEXT,
    content TEXT NOT NULL,
    sent_at_ms INTEGER NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(client_msg_id, direction)
);

CREATE INDEX idx_messages_conversation_time ON messages(conversation_id, sent_at_ms DESC);
CREATE INDEX idx_messages_server_msg_id ON messages(server_msg_id);

CREATE TABLE conversations (
    conversation_id TEXT PRIMARY KEY,
    peer_user_id TEXT NOT NULL,
    peer_email TEXT,
    last_message_at INTEGER NOT NULL,
    last_message_preview TEXT
);

CREATE INDEX idx_conversations_last_message_at ON conversations(last_message_at DESC);

CREATE TABLE user_state (
    user_id TEXT PRIMARY KEY,
    email TEXT,
    user_name TEXT,
    last_sync_sequence_id TEXT
);
