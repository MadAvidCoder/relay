CREATE TABLE conversations (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT,
    type TEXT NOT NULL CHECK (type IN ('dm', 'group')),
    last_message_at TEXT,
    last_synced_at TEXT
);

CREATE TABLE messages (
    id TEXT PRIMARY KEY NOT NULL,
    conversation_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT NOT NULL,

    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);

CREATE INDEX idx_msg_conv_at ON messages(conversation_id, created_at, id);
