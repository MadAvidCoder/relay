-- Add migration script here
CREATE TYPE conversation_type AS ENUM ('dm', 'group');

CREATE TABLE conversations (
    id UUID PRIMARY KEY,
    type conversation_type NOT NULL,
    name VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
