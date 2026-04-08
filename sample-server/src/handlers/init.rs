use crate::entity::{hero, inventory, user};
use crate::state::AppState;
use cabin_fireplace::DbManager;
use serde_json::json;
use std::sync::Arc;

pub async fn init_handler(
    uid: u64,
    db: DbManager,
    _state: Arc<AppState>,
) -> cabin_fireplace::Result<serde_json::Value> {
    let user_info = db.find_by_id::<user::User>(uid as i64).await?;

    let heroes = db.find_by::<hero::Hero>("uid", uid as i64).all().await?;

    let inv = db
        .find_by::<inventory::Inventory>("uid", uid as i64)
        .all()
        .await?;

    Ok(json!({
        "profile": user_info,
        "heroes": heroes,
        "inventory": inv,
    }))
}
