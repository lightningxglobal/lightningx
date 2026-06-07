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

use anyhow::Result;
use sqlx::PgPool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Leadership {
    /// Fencing token to stamp into published output.
    pub epoch: i64,
}

/// Try to acquire or renew the lease for `role`. `Ok(Some)` = we are the
/// leader for at least `ttl_secs`; `Ok(None)` = somebody else holds an
/// unexpired lease.
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
    Ok(row.map(|(epoch,)| Leadership { epoch }))
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
    ((epoch as u64 & 0xFFFF) << EPOCH_SHIFT) | (seq & SEQ_MASK)
}

#[inline]
pub fn split_epoch(stamped: u64) -> (u16, u64) {
    ((stamped >> EPOCH_SHIFT) as u16, stamped & SEQ_MASK)
}

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
}
