use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(20)
        .connect(database_url)
        .await
}

/// Run all migrations. Uses raw_sql to support multi-statement migration files.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(include_str!("../migrations/001_initial.sql"))
        .execute(pool)
        .await?;
    sqlx::raw_sql(include_str!("../migrations/002_nullable_trade_order_ids.sql"))
        .execute(pool)
        .await?;
    sqlx::raw_sql(include_str!("../migrations/003_symbols.sql"))
        .execute(pool)
        .await?;
    sqlx::raw_sql(include_str!("../migrations/004_freeze_price.sql"))
        .execute(pool)
        .await?;
    Ok(())
}
