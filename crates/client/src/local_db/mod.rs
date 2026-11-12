use std::str::FromStr;

use anyhow::Result;
use directories::ProjectDirs;
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

pub async fn init() -> Result<SqlitePool> {
    let conf = ProjectDirs::from("", "", "relay-client").expect("Failed to get config dirs");
    std::fs::create_dir_all(conf.data_dir())?;
    let options = SqliteConnectOptions::new()
        .filename(conf.data_dir().join("messages.db"))
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    Ok(pool)
}
