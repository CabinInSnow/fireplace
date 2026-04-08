use cabin_fireplace::FireplaceEntity;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct User {
    pub id: i64,
    pub display_id: String,
    pub nickname: String,
    pub device_id: String,
    pub login_type: i32,
    pub login_id: String,
    pub created_at: Option<String>, // stored as ISO-8601 string from Postgres timestamptz
}

impl FireplaceEntity for User {
    fn table_name() -> &'static str { "users" }
    fn primary_key() -> &'static str { "id" }
    fn pk_value(&self) -> Value { serde_json::json!(self.id) }
}
