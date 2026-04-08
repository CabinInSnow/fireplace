use cabin_fireplace::FireplaceEntity;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct GuildMember {
    pub id: i64,
    pub guild_id: i64,
    pub uid: i64,
    pub role: String,
}

impl FireplaceEntity for GuildMember {
    fn table_name() -> &'static str { "guild_members" }
    fn primary_key() -> &'static str { "id" }
    fn pk_value(&self) -> Value { serde_json::json!(self.id) }
}
