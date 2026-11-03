use relay_common::{Conversation, User};
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
    for user in &users {
        sqlx::query!(
            "INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1, $2)",
            conv.id,
            user,
        )
        .execute(pool)
        .await?;
    }

    Ok(Conversation {
        id: conv.id,
        users,
        name: conv.name,
        conversation_type: conv.conversation_type,
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
) -> Result<Vec<User>, Error> {
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
