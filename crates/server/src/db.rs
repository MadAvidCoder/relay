use relay_common::User;
use sqlx::{Error, PgPool, postgres::PgPoolOptions};

pub async fn get_pool(url: String) -> Result<PgPool, Error> {
    PgPoolOptions::new().connect(&url).await
}

pub async fn create_user(
    pool: PgPool,
    username: String,
    display_name: String,
) -> Result<User, Error> {
    let id = uuid::Uuid::now_v7();
    sqlx::query!(
        "INSERT INTO users (id, username, display_name) VALUES ($1, $2, $3)",
        id,
        username,
        display_name,
    )
    .execute(&pool)
    .await?;
    Ok(User::new_with_id(id, username, display_name))
}

pub async fn get_user_by_id(pool: PgPool, user_id: uuid::Uuid) -> Result<Option<User>, Error> {
    let row = sqlx::query!(
        "SELECT id, username, display_name FROM users WHERE id = $1",
        user_id,
    )
    .fetch_optional(&pool)
    .await?;

    Ok(row.map(|r| User::new_with_id(r.id, r.username, r.display_name)))
}

pub async fn get_user_by_username(pool: PgPool, username: String) -> Result<Option<User>, Error> {
    let row = sqlx::query!(
        "SELECT id, username, display_name FROM users WHERE username = $1",
        username
    )
    .fetch_optional(&pool)
    .await?;

    Ok(row.map(|r| User::new_with_id(r.id, r.username, r.display_name)))
}

pub async fn update_user(pool: PgPool, user: User) -> Result<(), Error> {
    let before = sqlx::query!(
        "SELECT username, display_name FROM users WHERE id = $1",
        user.id
    )
    .fetch_optional(&pool)
    .await?;
    if let Some(before) = before {
        if before.username != user.username || before.display_name != user.display_name {
            sqlx::query!(
                "UPDATE users SET username = $1, display_name = $2, updated_at = NOW() WHERE id = $3",
                user.username,
                user.display_name,
                user.id
            )
            .execute(&pool)
            .await?;
            Ok(())
        } else {
            Ok(())
        }
    } else {
        Err(Error::RowNotFound)
    }
}
