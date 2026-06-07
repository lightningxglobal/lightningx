//! S1.4 round-trip — the property that makes positions durable:
//!
//!   engine A (fills happen) → margin_state_frames → PgWriteBatch.flush
//!     → PG → hydrate_from_pg → engine B
//!
//! must leave engine B field-equivalent to engine A (modulo mark price,
//! which re-converges on the first tick: hydrate pins it to entry and
//! zeroes uPnL by design). Also covers the index structures on_fill
//! normally maintains (symbol_position_index, symbol_oi_lots) and the
//! insurance fund, including a NEGATIVE fund balance.

use lightning_exchange::db;
use lightning_exchange::desk::pg_store::PgWriteBatch;
use lightning_exchange::desk::risk::{PositionSide, RiskEngine, RiskStatus};
use lightning_exchange::desk::risk_persist::{hydrate_from_pg, margin_state_frames};
use sqlx::PgPool;
use std::sync::atomic::Ordering;
use uuid::Uuid;

const PUBLISHER: u16 = 63_002;
// Unique symbol so the full-table hydrate cannot pick up other suites'
// rows (fallback SymbolRules are identical to BTC_USDT: scale 1e6,
// 50 bps maintenance, 10x default).
const SYM: [u8; 16] = *b"HYD_TEST\0\0\0\0\0\0\0\0";

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 2).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_user(pg: &PgPool) -> i64 {
    sqlx::query_scalar("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(format!("pos_hyd_{}@lightning.test", Uuid::new_v4()))
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .expect("make user")
}

async fn cleanup(pg: &PgPool, users: &[i64]) {
    for sql in [
        "DELETE FROM positions WHERE user_id = ANY($1::bigint[])",
        "DELETE FROM risk_accounts WHERE user_id = ANY($1::bigint[])",
        "DELETE FROM users WHERE id = ANY($1::bigint[])",
    ] {
        let _ = sqlx::query(sql).bind(users).execute(pg).await;
    }
    let _ = sqlx::query("DELETE FROM persist_checkpoints WHERE publisher_id = $1")
        .bind(PUBLISHER as i32)
        .execute(pg)
        .await;
    let _ = sqlx::query("UPDATE insurance_fund SET balance_atoms = 0 WHERE id = 1")
        .execute(pg)
        .await;
}

#[tokio::test]
async fn crashed_engine_state_roundtrips_through_pg() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let long_user = make_user(&pg).await;
    let short_user = make_user(&pg).await;
    cleanup(&pg, &[]).await; // reset fund + stale checkpoints
    // Clear any residue rows for OUR symbol from a previous failed run.
    let _ = sqlx::query("DELETE FROM positions WHERE symbol = 'HYD_TEST'")
        .execute(&pg)
        .await;

    // ── Engine A: two users, opposite positions, fund debited ──────────
    // Realistic economics (rules: scale 1e6, maint 50bps, 10x):
    // entry $50,000 = 5_000_000 ticks; 0.1 BTC = 100_000 lots →
    // notional 500_000 cents, initial margin 50_000 cents at 10x.
    let a = RiskEngine::new();
    a.initialize_account(long_user, 60_000); // $600 — thin on purpose
    a.initialize_account(short_user, 200_000);
    a.check_and_reserve_margin(long_user, 50_000).expect("reserve long");
    a.on_fill(long_user, SYM, 0, 5_000_000, 100_000, 50_000, 1_000_000, 10, 50, 0);
    a.check_and_reserve_margin(short_user, 75_000).expect("reserve short");
    a.on_fill(short_user, SYM, 1, 5_000_000, 150_000, 75_000, 1_000_000, 10, 50, 0);
    // Socialised-loss debt: fund can be negative through the round-trip.
    a.insurance_fund_cents.store(-12_345, Ordering::Relaxed);

    // ── Crash boundary: frames → PG (what the desk publishes on fills) ─
    let mut batch = PgWriteBatch::new();
    let mut seq = 0u64;
    for user in [long_user, short_user] {
        for mut frame in margin_state_frames(&a, user, &SYM) {
            seq += 1;
            frame.publisher_id = PUBLISHER;
            frame.seq = seq;
            assert!(batch.push(&frame), "frame admitted");
        }
    }
    batch.flush(&pg).await.expect("flush");

    // ── Engine B: hydrate from PG only ──────────────────────────────────
    let b = RiskEngine::new();
    let stats = hydrate_from_pg(&b, &pg).await.expect("hydrate");
    assert!(stats.accounts >= 2, "both accounts restored");
    assert_eq!(stats.insurance_fund_cents, -12_345, "fund debt survives");

    // Position equivalence (the essentials persisted, the rest recomputed
    // through the SAME calc path on_fill uses).
    for user in [long_user, short_user] {
        let pa = a.positions.get(&(user, SYM)).expect("A position");
        let pb = b.positions.get(&(user, SYM)).expect("B position");
        assert_eq!(pa.side, pb.side, "side");
        assert_eq!(pa.qty_lots, pb.qty_lots, "qty");
        assert_eq!(pa.entry_price_ticks, pb.entry_price_ticks, "entry");
        assert_eq!(pa.leverage, pb.leverage, "leverage");
        assert_eq!(pa.initial_margin, pb.initial_margin, "margin");
        assert_eq!(pa.maintenance_margin, pb.maintenance_margin, "maint recomputed equal");
        assert_eq!(
            pa.liquidation_price_ticks, pb.liquidation_price_ticks,
            "liq price recomputed equal"
        );
        assert_eq!(
            pa.bankruptcy_price_ticks, pb.bankruptcy_price_ticks,
            "bankruptcy price recomputed equal"
        );
    }
    assert_eq!(
        b.positions.get(&(long_user, SYM)).unwrap().side,
        PositionSide::Long
    );
    assert_eq!(
        b.positions.get(&(short_user, SYM)).unwrap().side,
        PositionSide::Short
    );

    // Account equivalence: equity/used/order/status survive exactly.
    for user in [long_user, short_user] {
        let aa = a.accounts.get(&user).expect("A acct");
        let ab = b.accounts.get(&user).expect("B acct");
        assert_eq!(aa.equity, ab.equity, "equity");
        assert_eq!(aa.used_margin, ab.used_margin, "used margin");
        assert_eq!(
            aa.order_margin.load(Ordering::Relaxed),
            ab.order_margin.load(Ordering::Relaxed),
            "order margin"
        );
        assert_eq!(aa.status, ab.status, "status");
        assert_eq!(ab.status, RiskStatus::Normal);
        // available absorbs uPnL-at-snapshot (0 here: no mark move in A).
        assert_eq!(
            aa.available_margin.load(Ordering::Relaxed),
            ab.available_margin.load(Ordering::Relaxed),
            "available margin"
        );
    }

    // Index structures rebuilt: the risk tick and OI cap see the world.
    let idx = b.symbol_position_index.get(&SYM).expect("index");
    assert!(idx.contains(&long_user) && idx.contains(&short_user));
    assert_eq!(
        b.symbol_oi_lots.get(&SYM).unwrap().load(Ordering::Relaxed),
        100_000 + 150_000,
        "gross open interest restored"
    );

    // The hydrated engine is ALIVE: a mark-price move triggers its risk
    // tick exactly as a never-died engine. Dump to $44,240: the long's
    // uPnL = -576_000 ticks × 100_000 lots / 1e6 = -57_600 cents →
    // equity 2_400 cents, inside (0, maintenance=2_500] — the
    // LiquidationPending branch (deeper and it would be Bankruptcy,
    // which is a different, fund-absorbing path).
    b.update_mark_price(SYM, 4_424_000, 1_000_000);
    let events = b.run_risk_tick();
    assert!(
        events.iter().any(|e| e.user_id == long_user),
        "hydrated long position must be liquidatable by the live tick"
    );

    cleanup(&pg, &[long_user, short_user]).await;
}
