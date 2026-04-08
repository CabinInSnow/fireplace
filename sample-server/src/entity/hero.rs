use cabin_fireplace::FireplaceEntity;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct Hero {
    pub id: i64,
    pub uid: i64,
    pub hero_type: String,
    pub level: i32,
    pub exp: i32,
}

impl FireplaceEntity for Hero {
    fn table_name() -> &'static str { "heroes" }
    fn primary_key() -> &'static str { "id" }
    fn pk_value(&self) -> Value { serde_json::json!(self.id) }
}
