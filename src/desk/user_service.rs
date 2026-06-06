use crate::models::User;
use anyhow::{Result, anyhow};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::OnceLock;

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Dev-only fallback signing key. NEVER valid in production: startup
/// aborts if EXCHANGE_ENV=production and EXCHANGE_JWT_SECRET is missing
/// or weak (see resolve_jwt_secret).
const DEV_JWT_SECRET: &[u8] = b"exchange_jwt_secret_change_in_prod";

/// Minimum secret length: HS256 security degrades below the hash block
/// size; 32 bytes is the commonly enforced floor.
const MIN_JWT_SECRET_LEN: usize = 32;

/// Pure resolution rule, unit-testable without process-global env state.
///
/// - secret set and >= 32 bytes → use it.
/// - secret set but shorter     → hard error (mis-pasted/truncated key is
///   worse than no key: it LOOKS configured).
/// - secret unset, env=production → hard error (refuse to sign with the
///   public dev constant).
/// - secret unset otherwise     → dev fallback (local runs, tests, CI).
fn resolve_jwt_secret(
    env_secret: Option<String>,
    env_profile: Option<String>,
) -> Result<Vec<u8>> {
    let is_production = env_profile.as_deref() == Some("production");
    match env_secret {
        Some(s) if s.len() >= MIN_JWT_SECRET_LEN => Ok(s.into_bytes()),
        Some(s) => Err(anyhow!(
            "EXCHANGE_JWT_SECRET is too short ({} bytes, need >= {})",
            s.len(),
            MIN_JWT_SECRET_LEN
        )),
        None if is_production => Err(anyhow!(
            "EXCHANGE_ENV=production requires EXCHANGE_JWT_SECRET (>= {MIN_JWT_SECRET_LEN} bytes)"
        )),
        None => Ok(DEV_JWT_SECRET.to_vec()),
    }
}

/// Process-wide signing key. Resolved once on first use; a configuration
/// error is fatal by design — serving auth with a broken key set-up must
/// not silently degrade to the dev secret.
fn jwt_secret() -> &'static [u8] {
    static SECRET: OnceLock<Vec<u8>> = OnceLock::new();
    SECRET.get_or_init(|| {
        let secret = resolve_jwt_secret(
            std::env::var("EXCHANGE_JWT_SECRET").ok(),
            std::env::var("EXCHANGE_ENV").ok(),
        )
        .unwrap_or_else(|e| panic!("JWT secret configuration error: {e}"));
        if secret.as_slice() == DEV_JWT_SECRET {
            tracing::warn!(
                "EXCHANGE_JWT_SECRET not set — using the PUBLIC dev signing key. \
                 Set EXCHANGE_ENV=production to make this a startup error."
            );
        }
        secret
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: i64, // user id
    pub email: String,
    pub exp: usize, // expiry unix timestamp
}

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
    pub full_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {
    pub token: String,
    pub user: User,
    pub counter_shard_id: u16,
}

pub async fn register(
    pool: &PgPool,
    req: RegisterRequest,
    ip: Option<String>,
) -> Result<AuthResponse> {
    let hash = bcrypt::hash(&req.password, bcrypt::DEFAULT_COST)
        .map_err(|e| anyhow!("Hash error: {}", e))?;

    let user = sqlx::query_as::<_, User>(
        "INSERT INTO users (email, password_hash, full_name, registration_ip)
         VALUES ($1, $2, $3, $4)
         RETURNING *",
    )
    .bind(&req.email)
    .bind(&hash)
    .bind(&req.full_name)
    .bind(&ip)
    .fetch_one(pool)
    .await
    .map_err(|e| anyhow!("Register failed: {}", e))?;

    // Seed test funds: 10,000 USDT + 1 BTC for new users to trade immediately
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen, balance_atoms, frozen_atoms)
         VALUES
            ($1, 'USDT', 10000, 0, 1000000000000, 0),
            ($1, 'BTC', 1, 0, 100000000, 0)
         ON CONFLICT DO NOTHING",
    )
    .bind(user.id)
    .execute(pool)
    .await?;

    let token = make_token(user.id, &user.email)?;
    let counter_shard_id = crate::desk::counter_shard::owner_shard_for_user_id(user.id);
    Ok(AuthResponse {
        token,
        user,
        counter_shard_id,
    })
}

pub async fn login(pool: &PgPool, req: LoginRequest) -> Result<AuthResponse> {
    let user = sqlx::query_as::<_, User>("SELECT * FROM users WHERE email = $1")
        .bind(&req.email)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow!("Invalid email or password"))?;

    let valid = bcrypt::verify(&req.password, &user.password_hash)
        .map_err(|_| anyhow!("Invalid email or password"))?;
    if !valid {
        return Err(anyhow!("Invalid email or password"));
    }

    let token = make_token(user.id, &user.email)?;
    let counter_shard_id = crate::desk::counter_shard::owner_shard_for_user_id(user.id);
    Ok(AuthResponse {
        token,
        user,
        counter_shard_id,
    })
}

pub fn verify_token(token: &str) -> Result<Claims> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(jwt_secret()),
        &Validation::default(),
    )
    .map_err(|e| anyhow!("Invalid token: {}", e))?;
    Ok(data.claims)
}

/// Access-token TTL. Default keeps the historical 7 days so existing
/// clients/bots are unaffected; production should set
/// EXCHANGE_JWT_ACCESS_TTL_SECS=900 (15 min) and rely on /api/auth/refresh.
fn access_ttl_secs() -> i64 {
    static TTL: OnceLock<i64> = OnceLock::new();
    *TTL.get_or_init(|| {
        std::env::var("EXCHANGE_JWT_ACCESS_TTL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&t| t > 0)
            .unwrap_or(7 * 24 * 3600)
    })
}

/// Re-issue a token for already-verified claims (refresh endpoint).
/// Sliding expiry: each refresh grants a fresh TTL window. Revocation
/// (deny-list in Redis) is tracked in the roadmap.
pub fn refresh_token(claims: &Claims) -> Result<String> {
    make_token(claims.sub, &claims.email)
}

fn make_token(user_id: i64, email: &str) -> Result<String> {
    let exp = (chrono::Utc::now() + chrono::Duration::seconds(access_ttl_secs())).timestamp()
        as usize;
    let claims = Claims {
        sub: user_id,
        email: email.to_owned(),
        exp,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(jwt_secret()),
    )
    .map_err(|e| anyhow!("Token encode error: {}", e))
}

/// Look up user_id by bare API key. Allowed ONLY for legacy keys without
/// a signing secret; once a key has a secret, bare auth is rejected and
/// the signed flow (verify_api_key_signed) is mandatory.
pub async fn verify_api_key(pool: &PgPool, api_key: &str) -> Result<i64> {
    let row: Option<(i64, Option<String>)> =
        sqlx::query_as("SELECT user_id, secret FROM api_keys WHERE api_key = $1")
            .bind(api_key)
            .fetch_optional(pool)
            .await?;
    match row {
        None => Err(anyhow!("Invalid API key")),
        Some((_, Some(_))) => Err(anyhow!("API key requires signed authentication")),
        Some((user_id, None)) => Ok(user_id),
    }
}

/// Max allowed clock skew between client timestamp and server time.
pub const API_SIGNATURE_WINDOW_SECS: i64 = 30;

/// Compute the hex HMAC-SHA256 signature for a timestamp (the WS session
/// auth payload). Exposed so clients/tests share one definition.
pub fn compute_api_signature(secret: &str, timestamp: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(timestamp.as_bytes());
    let out = mac.finalize().into_bytes();
    let mut hex = String::with_capacity(out.len() * 2);
    for b in out {
        use std::fmt::Write;
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

/// Pure verification rule (unit-testable without DB or wall clock):
/// timestamp parses, |now − ts| ≤ window, constant-time signature match.
pub fn check_api_signature(
    secret: &str,
    timestamp: &str,
    signature_hex: &str,
    now_unix: i64,
) -> Result<()> {
    let ts: i64 = timestamp
        .parse()
        .map_err(|_| anyhow!("invalid timestamp"))?;
    if (now_unix - ts).abs() > API_SIGNATURE_WINDOW_SECS {
        return Err(anyhow!("timestamp outside allowed window"));
    }
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| anyhow!("bad secret"))?;
    mac.update(timestamp.as_bytes());
    let sig = decode_hex(signature_hex).ok_or_else(|| anyhow!("invalid signature encoding"))?;
    // Mac::verify_slice is constant-time.
    mac.verify_slice(&sig)
        .map_err(|_| anyhow!("signature mismatch"))
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Signed API-key session auth: requires the key to have a secret.
pub async fn verify_api_key_signed(
    pool: &PgPool,
    api_key: &str,
    timestamp: &str,
    signature_hex: &str,
) -> Result<i64> {
    let row: Option<(i64, Option<String>)> =
        sqlx::query_as("SELECT user_id, secret FROM api_keys WHERE api_key = $1")
            .bind(api_key)
            .fetch_optional(pool)
            .await?;
    let (user_id, secret) = row.ok_or_else(|| anyhow!("Invalid API key"))?;
    let secret = secret.ok_or_else(|| anyhow!("API key has no signing secret"))?;
    check_api_signature(
        &secret,
        timestamp,
        signature_hex,
        chrono::Utc::now().timestamp(),
    )?;
    Ok(user_id)
}

/// Best-effort append to the audit log. Failures are logged, never block
/// the authenticated action itself.
pub async fn audit(
    pool: &PgPool,
    actor_user_id: Option<i64>,
    action: &str,
    ip: Option<&str>,
    detail: serde_json::Value,
) {
    let res = sqlx::query(
        "INSERT INTO audit_log (actor_user_id, action, ip, detail) VALUES ($1, $2, $3, $4)",
    )
    .bind(actor_user_id)
    .bind(action)
    .bind(ip)
    .bind(detail)
    .execute(pool)
    .await;
    if let Err(e) = res {
        tracing::warn!("audit log write failed (action={action}): {e}");
    }
}

/// Ensure the robot account exists and has the given API key registered.
/// Called once at desk_server startup.
pub async fn ensure_robot_api_key(
    pool: &PgPool,
    email: &str,
    password: &str,
    api_key: &str,
    description: &str,
) -> Result<i64> {
    // Find or create the robot user.
    let existing: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
        .bind(email)
        .fetch_optional(pool)
        .await?;
    let user_id = if let Some(id) = existing {
        id
    } else {
        let req = RegisterRequest {
            email: email.to_owned(),
            password: password.to_owned(),
            full_name: None,
        };
        register(pool, req, None).await?.user.id
    };
    // Upsert API key.
    sqlx::query(
        "INSERT INTO api_keys (api_key, user_id, description)
         VALUES ($1, $2, $3)
         ON CONFLICT (api_key) DO NOTHING",
    )
    .bind(api_key)
    .bind(user_id)
    .bind(description)
    .execute(pool)
    .await?;
    Ok(user_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_secret_resolution_rules() {
        let strong = "s".repeat(MIN_JWT_SECRET_LEN);

        // Configured strong secret wins regardless of profile.
        assert_eq!(
            resolve_jwt_secret(Some(strong.clone()), Some("production".into())).unwrap(),
            strong.as_bytes()
        );
        assert_eq!(
            resolve_jwt_secret(Some(strong.clone()), None).unwrap(),
            strong.as_bytes()
        );

        // A configured-but-weak secret is a hard error, never a fallback.
        let err = resolve_jwt_secret(Some("short".into()), None).unwrap_err();
        assert!(err.to_string().contains("too short"), "{err}");

        // Production without a secret refuses to start.
        let err = resolve_jwt_secret(None, Some("production".into())).unwrap_err();
        assert!(err.to_string().contains("EXCHANGE_ENV=production"), "{err}");

        // Dev/CI without a secret falls back to the public dev key.
        assert_eq!(resolve_jwt_secret(None, None).unwrap(), DEV_JWT_SECRET);
        assert_eq!(
            resolve_jwt_secret(None, Some("dev".into())).unwrap(),
            DEV_JWT_SECRET
        );
    }

    #[test]
    fn api_signature_roundtrip_and_rejections() {
        let secret = "k".repeat(32);
        let ts = "1750000000";
        let sig = compute_api_signature(&secret, ts);
        let now: i64 = 1_750_000_000;

        // Valid signature within window.
        check_api_signature(&secret, ts, &sig, now).expect("valid");
        check_api_signature(&secret, ts, &sig, now + API_SIGNATURE_WINDOW_SECS).expect("edge ok");

        // Replay: timestamp outside the window — same signature, rejected.
        let err = check_api_signature(&secret, ts, &sig, now + API_SIGNATURE_WINDOW_SECS + 1)
            .unwrap_err();
        assert!(err.to_string().contains("window"), "{err}");

        // Wrong secret / tampered signature / tampered timestamp.
        assert!(check_api_signature("wrong-secret-wrong-secret-wrong!", ts, &sig, now).is_err());
        let mut bad = sig.clone();
        let flipped = if bad.ends_with('0') { '1' } else { '0' };
        bad.pop();
        bad.push(flipped);
        assert!(check_api_signature(&secret, ts, &bad, now).is_err());
        assert!(check_api_signature(&secret, "1750000001", &sig, now).is_err());

        // Hostile encodings must error, not panic.
        assert!(check_api_signature(&secret, ts, "zz", now).is_err());
        assert!(check_api_signature(&secret, ts, "abc", now).is_err()); // odd length
        assert!(check_api_signature(&secret, "not_a_number", &sig, now).is_err());
        assert!(check_api_signature(&secret, ts, "", now).is_err());
    }

    #[test]
    fn refresh_reissues_token_for_same_subject() {
        let token = make_token(42, "user@example.com").expect("sign");
        let claims = verify_token(&token).expect("verify");
        let refreshed = refresh_token(&claims).expect("refresh");
        let claims2 = verify_token(&refreshed).expect("verify refreshed");
        assert_eq!(claims2.sub, 42);
        assert_eq!(claims2.email, "user@example.com");
        assert!(claims2.exp >= claims.exp, "sliding expiry must not shrink");
    }

    #[test]
    fn token_roundtrip_with_resolved_secret() {
        let token = make_token(42, "user@example.com").expect("sign");
        let claims = verify_token(&token).expect("verify");
        assert_eq!(claims.sub, 42);
        assert_eq!(claims.email, "user@example.com");
    }
}
