use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(20)
        .connect(database_url)
        .await
}

/// Run all migrations from the migrations/ directory.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(include_str!("../migrations/001_initial.sql"))
        .execute(pool)
        .await?;
    Ok(())
}
