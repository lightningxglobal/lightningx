use crate::models::User;
use anyhow::{anyhow, Result};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

const JWT_SECRET: &[u8] = b"exchange_jwt_secret_change_in_prod";

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: i64,   // user id
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
}

pub async fn register(pool: &PgPool, req: RegisterRequest, ip: Option<String>) -> Result<AuthResponse> {
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
        "INSERT INTO accounts (user_id, asset, balance, frozen)
         VALUES ($1, 'USDT', 10000, 0), ($1, 'BTC', 1, 0)
         ON CONFLICT DO NOTHING",
    )
    .bind(user.id)
    .execute(pool)
    .await?;

    let token = make_token(user.id, &user.email)?;
    Ok(AuthResponse { token, user })
}

pub async fn login(pool: &PgPool, req: LoginRequest) -> Result<AuthResponse> {
    let user = sqlx::query_as::<_, User>(
        "SELECT * FROM users WHERE email = $1",
    )
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
    Ok(AuthResponse { token, user })
}

pub fn verify_token(token: &str) -> Result<Claims> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(JWT_SECRET),
        &Validation::default(),
    )
    .map_err(|e| anyhow!("Invalid token: {}", e))?;
    Ok(data.claims)
}

fn make_token(user_id: i64, email: &str) -> Result<String> {
    let exp = (chrono::Utc::now() + chrono::Duration::days(7))
        .timestamp() as usize;
    let claims = Claims { sub: user_id, email: email.to_owned(), exp };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET),
    )
    .map_err(|e| anyhow!("Token encode error: {}", e))
}

/// Look up user_id by static API key.
pub async fn verify_api_key(pool: &PgPool, api_key: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>("SELECT user_id FROM api_keys WHERE api_key = $1")
        .bind(api_key)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow!("Invalid API key"))
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
        let req = RegisterRequest { email: email.to_owned(), password: password.to_owned(), full_name: None };
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
