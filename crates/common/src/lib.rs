use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct User {
    pub id: Uuid,
    pub username: String,
    pub display_name: String,
}
impl User {
    pub fn new(username: String, display_name: String) -> Self {
        User {
            id: Uuid::now_v7(),
            username,
            display_name,
        }
    }
    pub fn new_with_id(id: Uuid, username: String, display_name: String) -> Self {
        User {
            id,
            username,
            display_name,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Connection {
    pub id: Uuid,
    pub user_id: Uuid,
}
impl Connection {
    pub fn new(user_id: Uuid) -> Self {
        Connection {
            id: Uuid::now_v7(),
            user_id,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, sqlx::Type)]
#[sqlx(type_name = "conversation_type", rename_all = "snake_case")]
pub enum ConversationType {
    DM,
    Group,
}

#[derive(Debug, Clone, PartialEq, sqlx::Type)]
#[sqlx(type_name = "conversation_role", rename_all = "snake_case")]
pub enum ConversationRoles {
    Admin,
    Member,
    Viewer,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Conversation {
    pub id: Uuid,
    pub users: Vec<User>,
    pub conversation_type: ConversationType,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub id: Uuid,
    pub sender_id: Uuid,
    pub conversation_id: Uuid,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub edited_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
}
impl Message {
    pub fn new(sender_id: Uuid, conversation_id: Uuid, content: String) -> Self {
        Message {
            id: Uuid::now_v7(),
            sender_id,
            conversation_id,
            content,
            created_at: chrono::Utc::now(),
            edited_at: None,
            deleted_at: None,
        }
    }
    pub fn new_with_timestamp(
        sender_id: Uuid,
        conversation_id: Uuid,
        content: String,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Message {
            id: Uuid::now_v7(),
            sender_id,
            conversation_id,
            content,
            created_at: timestamp,
            edited_at: None,
            deleted_at: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientProtocol {
    Auth {
        username: String,
    },
    SendMessage {
        message: String,
        conversation_id: Uuid,
    },
    CreateConversation {
        users: Vec<Uuid>,
        name: Option<String>,
    },
}
impl ClientProtocol {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(json_str: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json_str)
    }
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum AuthError {
    InvalidCredentials,
    UserNotFound,
    AlreadyAuthenticated,
    AlreadyConnected,
    Unauthorized,
    UnknownError,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ErrorCodes {
    NotAuthenticated,
    Unauthorised,
    InvalidRequest,
    RecipientNotFound,
    ConversationNotFound,
    InternalError,
    UnknownError,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerProtocol {
    AuthSuccess {
        user_id: Uuid,
        timestamp: DateTime<Utc>,
    },
    AuthFailure {
        user_id: Uuid,
        reason: AuthError,
        timestamp: DateTime<Utc>,
    },
    ConversationCreated {
        conversation_id: Uuid,
        timestamp: DateTime<Utc>,
    },
    NewMessage {
        message_id: Uuid,
        sender_id: Uuid,
        conversation_id: Uuid,
        message: String,
        timestamp: DateTime<Utc>,
    },
    Error {
        code: ErrorCodes,
        timestamp: DateTime<Utc>,
    },
}

impl ServerProtocol {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(json_str: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json_str)
    }
}
