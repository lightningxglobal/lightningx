use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct User {
    pub id: i64,
    pub email: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub full_name: Option<String>,
    pub kyc_status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DbAccount {
    pub id: i64,
    pub user_id: i64,
    pub asset: String,
    pub balance: f64,
    pub frozen: f64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DbOrder {
    pub id: i64,
    pub user_id: i64,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: Option<f64>,
    pub quantity: f64,
    pub filled: f64,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DbTrade {
    pub id: i64,
    pub symbol: String,
    pub buy_order_id: i64,
    pub sell_order_id: i64,
    pub price: f64,
    pub quantity: f64,
    pub buy_fee: f64,
    pub sell_fee: f64,
    pub created_at: DateTime<Utc>,
}
