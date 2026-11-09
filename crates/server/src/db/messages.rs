use chrono::{DateTime, Utc};
use relay_common::Message;
use sqlx::{Error, PgPool};
use uuid::Uuid;

pub async fn create_message(
    pool: &PgPool,
    sender_id: Uuid,
    conversation_id: Uuid,
    content: String,
) -> Result<Message, Error> {
    let id = Uuid::now_v7();
    sqlx::query_as!(
        Message,
        "INSERT INTO messages (id, sender_id, conversation_id, content) VALUES ($1, $2, $3, $4) RETURNING *",
        id,
        sender_id,
        conversation_id,
        content,
    )
    .fetch_one(pool)
    .await
}

pub async fn get_message_by_id(pool: &PgPool, message_id: Uuid) -> Result<Option<Message>, Error> {
    let row = sqlx::query_as!(Message, "SELECT * FROM messages WHERE id = $1", message_id,)
        .fetch_optional(pool)
        .await?;

    Ok(row)
}

pub async fn get_messages_by_conversation_id(
    pool: &PgPool,
    conversation_id: Uuid,
    limit: u32,
) -> Result<Vec<Message>, Error> {
    sqlx::query_as!(
        Message,
        "SELECT * FROM ( SELECT * FROM messages WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT $2) ORDER BY created_at ASC",
        conversation_id,
        limit as i64
    )
    .fetch_all(pool)
    .await
}

pub async fn update_message(pool: &PgPool, message: Message) -> Result<(), Error> {
    sqlx::query!(
        "UPDATE messages SET content = $1, edited_at = NOW() WHERE id = $2",
        message.content,
        message.id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_message(pool: &PgPool, message_id: Uuid) -> Result<(), Error> {
    sqlx::query!(
        "UPDATE messages SET content = '<DELETED>', deleted_at = NOW() WHERE id = $1",
        message_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_messages_since(
    pool: &PgPool,
    conversation_id: Uuid,
    since: DateTime<Utc>,
) -> Result<Vec<Message>, Error> {
    sqlx::query_as!(
        Message,
        "SELECT * FROM messages WHERE conversation_id = $1 AND created_at > $2 ORDER BY created_at ASC",
        conversation_id,
        since,
    )
    .fetch_all(pool)
    .await
}

pub async fn get_latest_message(
    pool: &PgPool,
    conversation_id: Uuid,
) -> Result<Option<Message>, Error> {
    sqlx::query_as!(
        Message,
        "SELECT * FROM messages WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT 1",
        conversation_id
    )
    .fetch_optional(pool)
    .await
}
