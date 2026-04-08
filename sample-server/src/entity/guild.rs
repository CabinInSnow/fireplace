use cabin_fireplace::FireplaceEntity;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct Guild {
    pub id: i64,
    pub name: String,
    pub leader_uid: i64,
}

impl FireplaceEntity for Guild {
    fn table_name() -> &'static str { "guilds" }
    fn primary_key() -> &'static str { "id" }
    fn pk_value(&self) -> Value { serde_json::json!(self.id) }
}
