//! Leader-lease semantics against real PG: mutual exclusion, expiry
//! takeover with fencing-epoch bump, renewal keeping the epoch, clean
//! release, and the zombie property the epoch protects against.

use lightning_exchange::db;
use lightning_exchange::leader::{release, split_epoch, stamp_epoch, try_acquire};
use sqlx::PgPool;
use uuid::Uuid;

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 4).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

#[tokio::test]
async fn lease_mutual_exclusion_takeover_and_epochs() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    // Unique role per run so parallel test runs never interfere.
    let role = format!("test-engine-{}", Uuid::new_v4());

    // A acquires; epoch starts at 1.
    let a = try_acquire(&pg, &role, "node-a", 0.5).await.expect("a1");
    let epoch_a = a.expect("A must win a free lease").epoch;
    assert_eq!(epoch_a, 1);

    // B cannot acquire while A's lease is live.
    let b = try_acquire(&pg, &role, "node-b", 0.5).await.expect("b1");
    assert!(b.is_none(), "mutual exclusion while lease is live");

    // A renews: SAME epoch (renewal is not a change of ownership).
    let a2 = try_acquire(&pg, &role, "node-a", 0.5).await.expect("a2");
    assert_eq!(a2.expect("renewal").epoch, epoch_a);

    // A dies (no renewal); after expiry B takes over with epoch+1.
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let b2 = try_acquire(&pg, &role, "node-b", 5.0).await.expect("b2");
    let epoch_b = b2.expect("takeover after expiry").epoch;
    assert_eq!(epoch_b, epoch_a + 1, "takeover bumps the fencing token");

    // Zombie A wakes up: acquisition fails (B's lease is live)…
    let a3 = try_acquire(&pg, &role, "node-a", 1.0).await.expect("a3");
    assert!(a3.is_none(), "zombie cannot reacquire a held lease");
    // …and even if it still publishes, its stamped output orders BELOW
    // everything from epoch B — the consumer-side drop rule.
    assert!(
        stamp_epoch(epoch_b, 1) > stamp_epoch(epoch_a, u64::MAX >> 16),
        "fencing order property"
    );
    assert_eq!(split_epoch(stamp_epoch(epoch_b, 42)), (epoch_b as u16, 42));

    // Clean release lets the next holder in immediately, with a bump.
    release(&pg, &role, "node-b").await.expect("release");
    let c = try_acquire(&pg, &role, "node-c", 5.0).await.expect("c1");
    assert_eq!(c.expect("post-release acquire").epoch, epoch_b + 1);

    let _ = sqlx::query("DELETE FROM leader_lease WHERE role = $1")
        .bind(&role)
        .execute(&pg)
        .await;
}
