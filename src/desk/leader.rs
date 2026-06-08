//! PostgreSQL-backed leader election with fencing tokens.
//!
//! Why PG and not etcd: a lease is one atomic UPSERT on infrastructure the
//! exchange already runs and already trusts for funds; quorum consensus
//! adds operational surface without adding safety HERE because the fencing
//! token — not the lock — is what protects against zombies. The token
//! (`epoch`) increments on every ownership change and is stamped into the
//! leader's output stream; consumers drop anything from a lower epoch.
//!
//! Standby model (with the input-stream journal): a standby engine replays
//! the journal, then keeps silently applying the live input stream. When
//! it wins the lease it starts publishing under epoch+1 — its book is
//! bit-identical to the dead leader's by input-stream determinism.
//!
//! Liveness rules:
//! - acquire succeeds when the lease is free, expired, or already ours;
//! - renewal keeps the SAME epoch; a takeover increments it;
//! - a leader that cannot renew must STOP PUBLISHING immediately
//!   (the engine treats lost leadership as fatal and exits — restart
//!   rejoins as standby and catches up from the journal).
//!
//! # Fail-closed behaviour
//! When PG is unreachable, try_acquire returns Err. The caller MUST stop publishing.
//! The standby also cannot promote (also cannot reach PG). This is intentional:
//! split-brain is more dangerous than a brief outage.

use anyhow::Result;
use sqlx::PgPool;

/// Role name constant for the matching engine leader election.
/// Callers should use this constant instead of the literal "engine".
pub const LEADER_ROLE_ENGINE: &str = "engine";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Leadership {
    /// Fencing token to stamp into published output.
    pub epoch: i64,
}

/// Local Instant-based lease validity guard.
///
/// Tracks the last successful renewal time. When the elapsed time exceeds
/// `ttl_secs`, the lease is considered expired locally — even before the
/// next PG query fires. This prevents a leader with broken PG connectivity
/// from continuing to publish until the sqlx query timeout fires.
pub struct LeaseGuard {
    pub last_renew_success: std::time::Instant,
    pub ttl_secs: f64,
}

impl LeaseGuard {
    pub fn new(ttl_secs: f64) -> Self {
        Self {
            last_renew_success: std::time::Instant::now(),
            ttl_secs,
        }
    }

    pub fn record_renewal(&mut self) {
        self.last_renew_success = std::time::Instant::now();
    }

    /// Returns false if the local deadline has passed, meaning the caller
    /// MUST stop publishing regardless of PG reachability.
    pub fn is_lease_valid(&self) -> bool {
        self.last_renew_success.elapsed().as_secs_f64() < self.ttl_secs
    }
}

/// Try to acquire or renew the lease for `role`. `Ok(Some)` = we are the
/// leader for at least `ttl_secs`; `Ok(None)` = somebody else holds an
/// unexpired lease.
///
/// Returns Err when PG is unreachable. Caller MUST stop publishing on Err.
pub async fn try_acquire(
    pool: &PgPool,
    role: &str,
    holder: &str,
    ttl_secs: f64,
) -> Result<Option<Leadership>> {
    let row: Option<(i64,)> = sqlx::query_as(
        r#"
        INSERT INTO leader_lease (role, holder, epoch, expires_at)
        VALUES ($1, $2, 1, NOW() + make_interval(secs => $3))
        ON CONFLICT (role) DO UPDATE SET
            holder = EXCLUDED.holder,
            -- renewal keeps the epoch; takeover (expired lease) bumps it
            epoch = CASE
                WHEN leader_lease.holder = EXCLUDED.holder THEN leader_lease.epoch
                ELSE leader_lease.epoch + 1
            END,
            expires_at = EXCLUDED.expires_at
        WHERE leader_lease.holder = EXCLUDED.holder
           OR leader_lease.expires_at < NOW()
        RETURNING epoch
        "#,
    )
    .bind(role)
    .bind(holder)
    .bind(ttl_secs)
    .fetch_optional(pool)
    .await?;

    if let Some((epoch,)) = row {
        if epoch >= 60_000 {
            tracing::warn!(
                "leader epoch {} approaching u16 ceiling (65535); coordinate a reset",
                epoch
            );
        }
        Ok(Some(Leadership { epoch }))
    } else {
        Ok(None)
    }
}

/// Release the lease iff we hold it (clean shutdown — lets a standby take
/// over without waiting out the TTL; the epoch still bumps on takeover).
pub async fn release(pool: &PgPool, role: &str, holder: &str) -> Result<()> {
    sqlx::query("UPDATE leader_lease SET expires_at = NOW() WHERE role = $1 AND holder = $2")
        .bind(role)
        .bind(holder)
        .execute(pool)
        .await?;
    Ok(())
}

/// Stamp an epoch into the high 16 bits of a response-stream sequence.
/// 48 bits of sequence ≈ 281e12 messages per epoch — decades at any
/// realistic rate; epochs wrap at 65535 takeovers.
pub const EPOCH_SHIFT: u32 = 48;
pub const SEQ_MASK: u64 = (1 << EPOCH_SHIFT) - 1;

#[inline]
pub fn stamp_epoch(epoch: i64, seq: u64) -> u64 {
    debug_assert!(
        epoch >= 0 && epoch <= 65535,
        "epoch {} exceeds u16 range",
        epoch
    );
    ((epoch as u64 & 0xFFFF) << EPOCH_SHIFT) | (seq & SEQ_MASK)
}

#[inline]
pub fn split_epoch(stamped: u64) -> (u16, u64) {
    ((stamped >> EPOCH_SHIFT) as u16, stamped & SEQ_MASK)
}

// NOTE on atomic ordering for the epoch / lease-lost sentinel:
// If an AtomicI64 epoch is held in desk_server (or a caller), the store of -1
// (lease lost) MUST use Ordering::SeqCst, and the corresponding load in the
// matching/publish path MUST use Ordering::Acquire. Relaxed is insufficient:
// the publish path must observe the lease-lost signal before it publishes
// any output under the stale epoch.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_stamping_roundtrips_and_orders() {
        let s = stamp_epoch(3, 12345);
        assert_eq!(split_epoch(s), (3, 12345));
        // Higher epoch always compares above any sequence of a lower epoch
        // (the property zombie-fencing relies on).
        assert!(stamp_epoch(4, 0) > stamp_epoch(3, SEQ_MASK));
        // seq overflow cannot bleed into the epoch bits.
        assert_eq!(split_epoch(stamp_epoch(1, SEQ_MASK + 7)).0, 1);
    }

    #[test]
    fn test_stamp_epoch_u16_ceiling() {
        // epoch=65535 is valid (maximum u16 value)
        let s = stamp_epoch(65535, 42);
        assert_eq!(split_epoch(s), (65535, 42));
    }

    #[test]
    #[cfg(debug_assertions)]
    fn test_stamp_epoch_u16_overflow_panics() {
        // In debug builds, epoch=65536 exceeds the u16 range and must panic.
        let result = std::panic::catch_unwind(|| stamp_epoch(65536, 0));
        assert!(result.is_err(), "epoch 65536 must trigger debug_assert panic");
    }

    #[test]
    fn lease_guard_expires_after_ttl() {
        let guard = LeaseGuard::new(0.01);
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(!guard.is_lease_valid(), "lease must be invalid after TTL elapsed");
    }

    #[test]
    fn lease_guard_valid_before_ttl() {
        let guard = LeaseGuard::new(10.0);
        assert!(guard.is_lease_valid(), "lease must be valid immediately after creation");
    }

    #[test]
    fn lease_guard_record_renewal_resets_timer() {
        let mut guard = LeaseGuard::new(0.01);
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(!guard.is_lease_valid());
        guard.record_renewal();
        assert!(guard.is_lease_valid(), "lease must be valid after renewal");
    }
}
