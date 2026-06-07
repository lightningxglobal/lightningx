use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    create_pool_sized(database_url, 20).await
}

/// Create a PG pool that enforces durable commits.
///
/// Every pooled connection runs `SET synchronous_commit = on` on connect,
/// overriding any weaker server/database/role-level default (benchmark
/// setups commonly flip it off globally). Settled funds and trade records
/// must be in the WAL before COMMIT returns — a crash right after an
/// acknowledged settle must never lose it. The setting is then verified
/// once so a misconfigured server fails fast at startup instead of
/// silently running with reduced durability.
pub async fn create_pool_sized(database_url: &str, max_conns: u32) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(max_conns)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET synchronous_commit = on")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await?;

    let (value,): (String,) = sqlx::query_as("SHOW synchronous_commit")
        .fetch_one(&pool)
        .await?;
    if value != "on" {
        return Err(sqlx::Error::Configuration(
            format!("synchronous_commit must be 'on', server reports '{value}'").into(),
        ));
    }
    Ok(pool)
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
    apply_migration(
        pool,
        "010_api_keys",
        include_str!("../../migrations/010_api_keys.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "011_drop_trades_order_fk",
        include_str!("../../migrations/011_drop_trades_order_fk.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "012_account_amount_atoms",
        include_str!("../../migrations/012_account_amount_atoms.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "013_matching_events",
        include_str!("../../migrations/013_matching_events.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "014_order_trade_amount_atoms",
        include_str!("../../migrations/014_order_trade_amount_atoms.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "015_persist_checkpoints",
        include_str!("../../migrations/015_persist_checkpoints.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "016_api_key_secrets",
        include_str!("../../migrations/016_api_key_secrets.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "017_audit_log",
        include_str!("../../migrations/017_audit_log.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "018_drop_legacy_float_accounts",
        include_str!("../../migrations/018_drop_legacy_float_accounts.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "019_trade_atomicity_fund_audit",
        include_str!("../../migrations/019_trade_atomicity_fund_audit.sql"),
    )
    .await?;
    apply_migration(
        pool,
        "020_leader_lease",
        include_str!("../../migrations/020_leader_lease.sql"),
    )
    .await?;
    Ok(())
}

async fn apply_migration(pool: &PgPool, version: &str, sql: &str) -> Result<(), sqlx::Error> {
    let already_applied: Option<i32> =
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
