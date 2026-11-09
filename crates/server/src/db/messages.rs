use chrono::{DateTime, Utc};
use relay_common::{Message, MessageCursor};
use sqlx::{Error, PgPool};
use uuid::Uuid;

pub async fn create_message(
    pool: &PgPool,
    sender_id: Uuid,
    conversation_id: Uuid,
    content: String,
) -> Result<Message, Error> {
    if !sqlx::query!("SELECT EXISTS (SELECT 1 FROM conversation_members WHERE conversation_id = $1 AND user_id = $2) AS is_member", conversation_id, sender_id).fetch_one(pool).await?.is_member.unwrap_or(false)
    {
        return Err(Error::RowNotFound);
    }
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
    since: Option<MessageCursor>,
    user_id: Uuid,
) -> Result<Vec<Message>, Error> {
    if !sqlx::query!("SELECT EXISTS (SELECT 1 FROM conversation_members WHERE conversation_id = $1 AND user_id = $2) AS is_member", conversation_id, user_id).fetch_one(pool).await?.is_member.unwrap_or(false)
    {
        return Err(Error::RowNotFound);
    }

    if let Some(since) = since {
        sqlx::query_as!(
        Message,
        "SELECT * FROM (SELECT * FROM messages WHERE conversation_id = $1 AND (created_at, id) > ($2, $3) ORDER BY created_at DESC, id DESC LIMIT $4) ORDER BY created_at ASC, id ASC",
        conversation_id,
        since.created_at,
        since.id,
        limit as i64,
    )
    .fetch_all(pool)
    .await
    } else {
        sqlx::query_as!(
        Message,
        "SELECT * FROM ( SELECT * FROM messages WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT $2) ORDER BY created_at ASC",
        conversation_id,
        limit as i64
    )
    .fetch_all(pool)
    .await
    }
}

pub async fn update_message(pool: &PgPool, message: Message, user_id: Uuid) -> Result<(), Error> {
    sqlx::query!(
        "UPDATE messages SET content = $1, edited_at = NOW() WHERE id = $2 AND sender_id = $3",
        message.content,
        message.id,
        user_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_message(pool: &PgPool, message_id: Uuid, user_id: Uuid) -> Result<(), Error> {
    sqlx::query!(
        "UPDATE messages SET content = '<DELETED>', deleted_at = NOW() WHERE id = $1 AND sender_id = $2",
        message_id,
        user_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_messages_since(
    pool: &PgPool,
    conversation_id: Uuid,
    since: DateTime<Utc>,
    user_id: Uuid,
) -> Result<Vec<Message>, Error> {
    if !sqlx::query!("SELECT EXISTS (SELECT 1 FROM conversation_members WHERE conversation_id = $1 AND user_id = $2) AS is_member", conversation_id, user_id).fetch_one(pool).await?.is_member.unwrap_or(false)
    {
        return Err(Error::RowNotFound);
    }
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
    user_id: Uuid,
) -> Result<Option<Message>, Error> {
    if !sqlx::query!("SELECT EXISTS (SELECT 1 FROM conversation_members WHERE conversation_id = $1 AND user_id = $2) AS is_member", conversation_id, user_id).fetch_one(pool).await?.is_member.unwrap_or(false)
    {
        return Err(Error::RowNotFound);
    }
    sqlx::query_as!(
        Message,
        "SELECT * FROM messages WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT 1",
        conversation_id
    )
    .fetch_optional(pool)
    .await
}
