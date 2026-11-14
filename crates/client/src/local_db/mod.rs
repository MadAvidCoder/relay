use std::str::FromStr;

use anyhow::anyhow;
use directories::ProjectDirs;
use relay_common::{Conversation, ConversationType, Message, User};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    types::chrono::{DateTime, Utc},
};
use uuid::Uuid;

pub async fn init() -> anyhow::Result<SqlitePool> {
    let conf =
        ProjectDirs::from("", "", "relay-client").ok_or(anyhow!("Failed to get config dirs"))?;
    std::fs::create_dir_all(conf.data_dir())?;
    let options = SqliteConnectOptions::new()
        .filename(conf.data_dir().join("messages.db"))
        .create_if_missing(true)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    Ok(pool)
}

pub async fn update_contact_name(
    id: Uuid,
    name: &str,
    pool: &SqlitePool,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "UPDATE contacts SET display_name = $1 WHERE user_id = $2 RETURNING user_id;",
        name,
        &id.to_string()
    )
    .fetch_optional(pool)
    .await?
    .ok_or(sqlx::Error::RowNotFound)?;
    Ok(())
}

pub async fn save_contact(user: User, pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"INSERT INTO contacts
            (user_id, username, display_name, added_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username
        "#,
        &user.id.to_string(),
        user.username,
        Some(user.display_name).filter(|s| !s.is_empty()),
        Utc::now().to_rfc3339(),
    )
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn get_contact(userid: &Uuid, pool: &SqlitePool) -> anyhow::Result<Option<User>> {
    sqlx::query!(
        "SELECT user_id, username, display_name FROM contacts WHERE user_id = $1",
        &userid.to_string()
    )
    .fetch_optional(pool)
    .await?
    .map(|a| {
        Ok(User {
            id: Uuid::parse_str(&a.user_id)?,
            username: a.username,
            display_name: a.display_name.unwrap_or(String::new()),
        })
    })
    .transpose()
}

pub async fn get_contacts(pool: &SqlitePool) -> Result<Vec<User>, anyhow::Error> {
    sqlx::query!("SELECT user_id, username, display_name FROM contacts")
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|a| {
            Ok::<User, anyhow::Error>(User {
                id: Uuid::from_str(&a.user_id)?,
                username: a.username,
                display_name: a.display_name.unwrap_or(String::new()),
            })
        })
        .collect::<Result<Vec<User>, anyhow::Error>>()
}

pub async fn save_conversation(conv: &Conversation, pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"INSERT INTO conversations
            (id, name, type, last_message_at, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                type = excluded.type,
                last_message_at = excluded.last_message_at"#,
        &conv.id.to_string(),
        &conv.name,
        match conv.conversation_type {
            ConversationType::DM => "dm",
            ConversationType::Group => "group",
        },
        conv.latest_message
            .as_ref()
            .map(|f| f.created_at.to_rfc3339()),
        conv.created_at.to_rfc3339(),
    )
    .execute(pool)
    .await?;

    sqlx::QueryBuilder::new("INSERT INTO contacts (user_id, username, display_name, added_at)")
        .push_values(&conv.users, |mut b, user| {
            b.push_bind(conv.id.to_string())
                .push_bind(&user.username)
                .push_bind(Some(&user.display_name).filter(|a| !a.is_empty()))
                .push_bind(Utc::now().to_rfc3339());
        })
        .push("ON CONFLICT (user_id) DO UPDATE SET username = excluded.username")
        .build()
        .execute(pool)
        .await?;

    sqlx::QueryBuilder::new("INSERT INTO conversation_members (conversation_id, user_id) ")
        .push_values(&conv.users, |mut b, user| {
            b.push_bind(conv.id.to_string())
                .push_bind(user.id.to_string());
        })
        .push("ON CONFLICT (conversation_id, user_id) DO NOTHING")
        .build()
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn get_conversation(
    conv_id: &Uuid,
    pool: &SqlitePool,
) -> anyhow::Result<Option<Conversation>> {
    let conv_id_str = conv_id.to_string();
    let req = match sqlx::query!(
        r#"SELECT
            c.id,
            c.created_at,
            c.type AS conv_type,
            c.name,
            m.id AS mes_id,
            m.sender_id,
            m.content,
            m.created_at AS mes_cat,
            m.edited_at,
            m.deleted_at
        FROM conversations c
        LEFT JOIN messages m ON c.id = m.conversation_id
        WHERE c.id = $1
        ORDER BY m.created_at DESC, m.id DESC LIMIT 1"#,
        &conv_id_str
    )
    .fetch_optional(pool)
    .await?
    {
        Some(s) => s,
        None => return Ok(None),
    };

    let users = sqlx::query!(
        r#"
        SELECT u.user_id, u.username, u.display_name
        FROM conversation_members cm
        JOIN contacts u ON cm.user_id = u.user_id
        WHERE cm.conversation_id = $1"#,
        &conv_id_str
    )
    .fetch_all(pool)
    .await?;

    Ok(Some(Conversation {
        id: Uuid::from_str(&req.id)?,
        users: users
            .into_iter()
            .map(|u| {
                Ok(User {
                    id: Uuid::parse_str(&u.user_id)?,
                    username: u.username,
                    display_name: u.display_name.unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<User>, anyhow::Error>>()?,
        conversation_type: match req.conv_type.as_str() {
            "dm" => ConversationType::DM,
            "group" => ConversationType::Group,
            _ => unreachable!(),
        },
        created_at: DateTime::parse_from_rfc3339(&req.created_at)?.with_timezone(&Utc),
        name: req.name,
        latest_message: match req.mes_id {
            Some(id) => Some(Message {
                id: Uuid::from_str(&id)?,
                sender_id: Uuid::from_str(&req.sender_id.ok_or(anyhow!("Missing sender id"))?)?,
                conversation_id: Uuid::from_str(&req.id)?,
                content: req.content.ok_or(anyhow!("Missing message content"))?,
                created_at: DateTime::parse_from_rfc3339(
                    &req.mes_cat
                        .ok_or(anyhow!("Missing message created at date"))?,
                )?
                .with_timezone(&Utc),
                edited_at: req
                    .edited_at
                    .and_then(|f| Some(DateTime::parse_from_rfc3339(&f).ok()?.with_timezone(&Utc))),
                deleted_at: req
                    .deleted_at
                    .and_then(|a| Some(DateTime::parse_from_rfc3339(&a).ok()?.with_timezone(&Utc))),
            }),
            None => None,
        },
    }))
}

pub async fn delete_conversation(conv_id: &Uuid, pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "DELETE FROM conversations WHERE id = $1",
        &conv_id.to_string()
    )
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn save_message(msg: &Message, pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"INSERT INTO messages (
            id,
            conversation_id,
            sender_id,
            content,
            created_at,
            edited_at,
            deleted_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT(id) DO UPDATE SET
            content = excluded.content,
            edited_at = excluded.edited_at,
            deleted_at = excluded.deleted_at"#,
        msg.id.to_string(),
        msg.conversation_id.to_string(),
        msg.sender_id.to_string(),
        msg.content,
        msg.created_at.to_rfc3339(),
        msg.edited_at.as_ref().map(|x| x.to_rfc3339()),
        msg.deleted_at.as_ref().map(|a| a.to_rfc3339()),
    )
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn get_messages(
    conv_id: &Uuid,
    pool: &SqlitePool,
) -> Result<Vec<Message>, anyhow::Error> {
    let x = sqlx::query!(
        "SELECT id, sender_id, conversation_id, content, created_at, edited_at, deleted_at FROM messages WHERE conversation_id = $1 ORDER BY created_at ASC, id ASC",
        &conv_id.to_string()
    )
    .fetch_all(pool)
    .await?;
    x.into_iter()
        .map(|f| {
            Ok(Message {
                id: Uuid::from_str(&f.id)?,
                sender_id: Uuid::from_str(&f.sender_id)?,
                conversation_id: Uuid::from_str(&f.conversation_id)?,
                content: f.content,
                created_at: f.created_at.parse::<DateTime<Utc>>()?,
                edited_at: f
                    .edited_at
                    .map(|a| a.parse::<DateTime<Utc>>())
                    .transpose()?,
                deleted_at: f
                    .deleted_at
                    .map(|a| a.parse::<DateTime<Utc>>())
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<Message>, anyhow::Error>>()
}
