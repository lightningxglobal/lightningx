//! Integration tests for P6 auth hardening: HMAC-signed API keys (legacy
//! bare keys gated by absence of a secret) and the append-only audit log.

use lightning_exchange::db;
use lightning_exchange::user_service::{
    audit, compute_api_signature, verify_api_key, verify_api_key_signed,
};
use sqlx::{PgPool, Row};
use uuid::Uuid;

async fn try_pg() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string());
    let pg = db::create_pool_sized(&url, 2).await.ok()?;
    db::run_migrations(&pg).await.ok()?;
    Some(pg)
}

async fn make_user(pg: &PgPool) -> Option<i64> {
    let email = format!("sec_{}@lightning.test", Uuid::new_v4());
    sqlx::query("INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id")
        .bind(email)
        .bind("$argon2id$v=19$test$abcdefghijklmnopqrstuvwxyz0123456789")
        .fetch_one(pg)
        .await
        .ok()?
        .try_get("id")
        .ok()
}

async fn cleanup(pg: &PgPool, user_ids: &[i64], api_keys: &[&str]) {
    for k in api_keys {
        let _ = sqlx::query("DELETE FROM api_keys WHERE api_key = $1")
            .bind(k)
            .execute(pg)
            .await;
    }
    let _ = sqlx::query("DELETE FROM users WHERE id = ANY($1::bigint[])")
        .bind(user_ids)
        .execute(pg)
        .await;
}

#[tokio::test]
async fn signed_keys_require_signature_and_legacy_keys_stay_bare() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };
    let Some(user) = make_user(&pg).await else {
        eprintln!("skip: cannot make user");
        return;
    };

    let legacy_key = format!("legacy_{}", Uuid::new_v4());
    let signed_key = format!("signed_{}", Uuid::new_v4());
    let secret = format!("s3cr3t_{}", Uuid::new_v4());

    sqlx::query("INSERT INTO api_keys (api_key, user_id, description) VALUES ($1, $2, 'legacy')")
        .bind(&legacy_key)
        .bind(user)
        .execute(&pg)
        .await
        .expect("insert legacy key");
    sqlx::query(
        "INSERT INTO api_keys (api_key, user_id, description, secret) VALUES ($1, $2, 'signed', $3)",
    )
    .bind(&signed_key)
    .bind(user)
    .bind(&secret)
    .execute(&pg)
    .await
    .expect("insert signed key");

    // Legacy key: bare auth works, signed auth errors (no secret on file).
    assert_eq!(verify_api_key(&pg, &legacy_key).await.expect("bare"), user);
    assert!(
        verify_api_key_signed(&pg, &legacy_key, "1", "00")
            .await
            .is_err()
    );

    // Signed key: bare auth REJECTED.
    let err = verify_api_key(&pg, &signed_key).await.unwrap_err();
    assert!(
        err.to_string().contains("signed"),
        "bare auth must be rejected for keys with a secret: {err}"
    );

    // Signed key + correct signature within window: accepted.
    let ts = chrono::Utc::now().timestamp().to_string();
    let sig = compute_api_signature(&secret, &ts);
    assert_eq!(
        verify_api_key_signed(&pg, &signed_key, &ts, &sig)
            .await
            .expect("signed auth"),
        user
    );

    // Stale timestamp (replay) rejected even with a valid signature shape.
    let old_ts = (chrono::Utc::now().timestamp() - 120).to_string();
    let old_sig = compute_api_signature(&secret, &old_ts);
    assert!(
        verify_api_key_signed(&pg, &signed_key, &old_ts, &old_sig)
            .await
            .is_err(),
        "replayed timestamp must be rejected"
    );

    // Wrong signature rejected.
    assert!(
        verify_api_key_signed(&pg, &signed_key, &ts, "deadbeef")
            .await
            .is_err()
    );

    cleanup(&pg, &[user], &[&legacy_key, &signed_key]).await;
}

#[tokio::test]
async fn audit_log_is_append_only() {
    let Some(pg) = try_pg().await else {
        eprintln!("skip: no PG");
        return;
    };

    let probe = format!("test_probe_{}", Uuid::new_v4());
    audit(&pg, Some(1), "login_ok", Some("127.0.0.1"), serde_json::json!({"probe": probe}))
        .await;

    let id: i64 = sqlx::query_scalar(
        "SELECT id FROM audit_log WHERE detail->>'probe' = $1",
    )
    .bind(&probe)
    .fetch_one(&pg)
    .await
    .expect("audit row written");

    // UPDATE and DELETE must both raise — the table can only grow.
    let upd = sqlx::query("UPDATE audit_log SET action = 'tampered' WHERE id = $1")
        .bind(id)
        .execute(&pg)
        .await;
    assert!(upd.is_err(), "UPDATE must be blocked");
    let del = sqlx::query("DELETE FROM audit_log WHERE id = $1")
        .bind(id)
        .execute(&pg)
        .await;
    assert!(del.is_err(), "DELETE must be blocked");
}

#[tokio::test]
async fn revocation_persists_and_refreshes_via_redis() {
    use lightning_exchange::user_service::{
        is_revoked, refresh_revocations, revoke_user, unrevoke_user,
    };
    let url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    let Ok(client) = redis::Client::open(url) else {
        eprintln!("skip: no Redis");
        return;
    };
    let Ok(mut conn) = client.get_multiplexed_async_connection().await else {
        eprintln!("skip: no Redis");
        return;
    };
    let uid: i64 = 880_000_777;

    revoke_user(Some(&mut conn), uid).await.expect("revoke");
    assert!(is_revoked(uid));

    // Simulate another desk: wipe local memory, refresh from Redis.
    unrevoke_user(None, uid).await.expect("local clear only");
    assert!(!is_revoked(uid), "local memory cleared");
    refresh_revocations(&mut conn).await.expect("refresh");
    assert!(is_revoked(uid), "revocation restored from Redis");

    unrevoke_user(Some(&mut conn), uid).await.expect("lift");
    refresh_revocations(&mut conn).await.expect("refresh 2");
    assert!(!is_revoked(uid));
}
