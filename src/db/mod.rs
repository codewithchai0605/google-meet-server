pub mod meetings;
pub mod models;
pub mod recordings;
pub mod users;

use sqlx::postgres::{PgPool, PgPoolOptions};

/// Connect with a small, explicit pool size rather than the library default: this server holds
/// most hot state (rooms, peers) in memory, so Postgres is only touched for auth and meeting
/// metadata — a handful of connections is plenty and avoids reserving idle backend memory on
/// the database side for connections we won't use.
pub async fn connect(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    Ok(pool)
}
