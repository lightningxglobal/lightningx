use sqlx::{postgres::PgPoolOptions, PgPool};

pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(20)
        .connect(database_url)
        .await
}

/// Run all migrations. Uses raw_sql to support multi-statement migration files.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
    .execute(pool)
    .await?;

    apply_migration(
        pool,
        "001_initial",
        include_str!("../../migrations/001_initial.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "002_nullable_trade_order_ids",
        include_str!("../../migrations/002_nullable_trade_order_ids.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "003_symbols",
        include_str!("../../migrations/003_symbols.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "004_freeze_price",
        include_str!("../../migrations/004_freeze_price.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "005_klines",
        include_str!("../../migrations/005_klines.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "006_client_order_id",
        include_str!("../../migrations/006_client_order_id.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "007_trades_composite_index",
        include_str!("../../migrations/007_trades_composite_index.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "008_user_registration_ip",
        include_str!("../../migrations/008_user_registration_ip.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "009_client_order_id_unique",
        include_str!("../../migrations/009_client_order_id_unique.sql"),
    )
    .await?;
    Ok(())
}

async fn apply_migration(pool: &PgPool, version: &str, sql: &str) -> Result<(), sqlx::Error> {
    let already_applied: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM _schema_migrations WHERE version=$1")
            .bind(version)
            .fetch_optional(pool)
            .await?;
    if already_applied.is_some() {
        return Ok(());
    }

    sqlx::raw_sql(sql).execute(pool).await?;
    sqlx::query("INSERT INTO _schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(version)
        .execute(pool)
        .await?;
    Ok(())
}
