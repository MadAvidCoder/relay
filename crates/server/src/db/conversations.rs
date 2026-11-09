use std::collections::HashMap;

use relay_common::{Conversation, Message, User};
use sqlx::{Error, PgPool};
use uuid::Uuid;

pub async fn create_conversation(
    pool: &PgPool,
    users: Vec<Uuid>,
    name: Option<String>,
) -> Result<Conversation, Error> {
    let id = Uuid::now_v7();
    let conv = sqlx::query!(
        "INSERT INTO conversations (id, type, name) VALUES ($1, $2, $3) RETURNING id, type AS \"conversation_type: relay_common::ConversationType\", name, created_at",
        id,
        if users.len() == 2 {
            relay_common::ConversationType::DM
        } else {
            relay_common::ConversationType::Group
        } as _,
        name,
    )
    .fetch_one(pool)
    .await?;

    let users = sqlx::query_as!(
        User,
        "WITH new_members AS (INSERT INTO conversation_members (conversation_id, user_id) SELECT $1, id FROM users WHERE id = ANY($2) RETURNING user_id) SELECT users.id, users.username, users.display_name FROM new_members JOIN users ON new_members.user_id = users.id",
        conv.id,
        &users,
    )
    .fetch_all(pool)
    .await?;

    Ok(Conversation {
        id: conv.id,
        users,
        name: conv.name,
        conversation_type: conv.conversation_type,
        latest_message: None,
        created_at: conv.created_at,
    })
}

pub async fn add_user_to_conversation(
    pool: &PgPool,
    conversation_id: Uuid,
    user: User,
) -> Result<(), Error> {
    sqlx::query!(
        "INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2)",
        conversation_id,
        user.id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_conversation_members(
    pool: &PgPool,
    conversation_id: Uuid,
    user_id: Uuid,
) -> Result<Vec<User>, Error> {
    if !sqlx::query!("SELECT EXISTS (SELECT 1 FROM conversation_members WHERE conversation_id = $1 AND user_id = $2) AS is_member", conversation_id, user_id).fetch_one(pool).await?.is_member.unwrap_or(false)
    {
        return Err(Error::RowNotFound);
    }

    let rows = sqlx::query!(
        "SELECT u.id, u.username, u.display_name FROM users u INNER JOIN conversation_members cm ON u.id = cm.user_id WHERE cm.conversation_id = $1",
        conversation_id,
    )
    .fetch_all(pool)
    .await?;

    let users = rows
        .into_iter()
        .map(|row| User::new_with_id(row.id, row.username, row.display_name))
        .collect();

    Ok(users)
}

pub async fn get_user_conversations(pool: &PgPool, user: Uuid) -> Result<Vec<Conversation>, Error> {
    let q = sqlx::query!(
        "SELECT c.id, c.type AS \"c_type: relay_common::ConversationType\", c.name, c.created_at, m.sender_id, m.content, m.edited_at, m.deleted_at, m.id AS message_id, m.created_at AS message_created_at FROM conversations c JOIN conversation_members cm ON c.id = cm.conversation_id LEFT JOIN LATERAL (SELECT m.content, m.id, m.sender_id, m.edited_at, m.deleted_at, m.created_at FROM messages m WHERE conversation_id = c.id ORDER BY created_at DESC LIMIT 1) m ON true WHERE cm.user_id = $1",
        user
    ).fetch_all(pool).await?;
    let a = sqlx::query!(
        "SELECT u.id, u.username, u.display_name, cm.conversation_id FROM users u INNER JOIN conversation_members cm ON u.id = cm.user_id WHERE cm.conversation_id = ANY($1)",
        &q.iter().map(|w| w.id).collect::<Vec<_>>()
    ).fetch_all(pool).await?;

    let mut map = HashMap::new();
    for row in a {
        map.entry(row.conversation_id)
            .or_insert_with(Vec::new)
            .push(User::new_with_id(row.id, row.username, row.display_name));
    }

    Ok(q.into_iter()
        .map(|w| Conversation {
            id: w.id,
            name: w.name,
            created_at: w.created_at,
            latest_message: if let (Some(m_id), Some(con), Some(c_at), Some(s_id)) =
                (w.message_id, w.content, w.message_created_at, w.sender_id)
            {
                Some(Message {
                    id: m_id,
                    sender_id: s_id,
                    conversation_id: w.id,
                    content: con,
                    created_at: c_at,
                    edited_at: w.edited_at,
                    deleted_at: w.deleted_at,
                })
            } else {
                None
            },
            conversation_type: w.c_type,
            users: map.get(&w.id).cloned().unwrap_or_default(),
        })
        .collect())
}
