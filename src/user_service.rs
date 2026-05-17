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

pub async fn register(pool: &PgPool, req: RegisterRequest) -> Result<AuthResponse> {
    let hash = bcrypt::hash(&req.password, bcrypt::DEFAULT_COST)
        .map_err(|e| anyhow!("Hash error: {}", e))?;

    let user = sqlx::query_as::<_, User>(
        "INSERT INTO users (email, password_hash, full_name)
         VALUES ($1, $2, $3)
         RETURNING *",
    )
    .bind(&req.email)
    .bind(&hash)
    .bind(&req.full_name)
    .fetch_one(pool)
    .await
    .map_err(|e| anyhow!("Register failed: {}", e))?;

    // Create default USDT account for new user
    sqlx::query(
        "INSERT INTO accounts (user_id, asset, balance, frozen) VALUES ($1, 'USDT', 0, 0)
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
