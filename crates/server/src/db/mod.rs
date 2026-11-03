pub mod users;
pub mod messages;
pub mod conversations;

use sqlx::{Error, PgPool, postgres::PgPoolOptions};

pub async fn get_pool(url: String) -> Result<PgPool, Error> {
    PgPoolOptions::new().connect(&url).await
}
