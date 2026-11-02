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

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub id: Uuid,
    pub sender_id: Uuid,
    pub recipient_id: Uuid,
    pub content: String,
    pub timestamp: u64,
}
impl Message {
    pub fn new(sender_id: Uuid, recipient_id: Uuid, content: String) -> Self {
        Message {
            id: Uuid::now_v7(),
            sender_id,
            recipient_id,
            content,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
    pub fn new_with_timestamp(
        sender_id: Uuid,
        recipient_id: Uuid,
        content: String,
        timestamp: u64,
    ) -> Self {
        Message {
            id: Uuid::now_v7(),
            sender_id,
            recipient_id,
            content,
            timestamp,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientProtocol {
    Auth { username: String },
    SendMessage { message: String, recipient_id: Uuid },
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
    InvalidRequest,
    RecipientNotFound,
    InternalError,
    UnknownError,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerProtocol {
    AuthSuccess {
        user_id: Uuid,
        timestamp: u64,
    },
    AuthFailure {
        user_id: Uuid,
        reason: AuthError,
        timestamp: u64,
    },
    NewMessage {
        message_id: Uuid,
        sender_id: Uuid,
        recipient_id: Uuid,
        message: String,
        timestamp: u64,
    },
    Error {
        code: ErrorCodes,
        timestamp: u64,
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
