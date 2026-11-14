CREATE TABLE contacts (
    user_id TEXT PRIMARY KEY NOT NULL,
    username TEXT NOT NULL,
    display_name TEXT,
    added_at TEXT NOT NULL
);

CREATE TABLE conversation_members (
    user_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'member', 'viewer')) DEFAULT 'member',
    PRIMARY KEY (user_id, conversation_id),
    FOREIGN KEY (user_id) REFERENCES contacts(user_id),
    FOREIGN KEY (conversation_id) REFERENCES conversations(id)
);
