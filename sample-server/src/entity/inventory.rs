use cabin_fireplace::FireplaceEntity;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct Inventory {
    pub id: i64,
    pub uid: i64,
    pub item_name: String,
    pub quantity: i32,
}

impl FireplaceEntity for Inventory {
    fn table_name() -> &'static str { "inventory" }
    fn primary_key() -> &'static str { "id" }
    fn pk_value(&self) -> Value { serde_json::json!(self.id) }
}
