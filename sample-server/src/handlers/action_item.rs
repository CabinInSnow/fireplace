use crate::entity::inventory::Inventory;
use crate::state::AppState;
use cabin_fireplace::{FireplaceEntity, GameContext, StateBundle, SyncEvent, FireplaceError};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct GiveItemParams {
    pub target_uid: u64,
    pub item_name: String,
    pub quantity: i32,
}

pub async fn give_item(
    uid: u64,
    bundle: StateBundle,
    params: Value,
    _state: Arc<AppState>,
) -> cabin_fireplace::Result<(Option<Value>, Option<GameContext>)> {
    let req: GiveItemParams = serde_json::from_value(params)?;

    if req.quantity <= 0 {
        return Err("Quantity must be greater than 0".into());
    }

    if req.target_uid == uid {
        return Err("Cannot give item to yourself".into());
    }

    let mut txn = bundle.db_manager.begin().await?;

    // 1. Deduct from sender
    let mut sender_item = txn
        .find_by::<Inventory>("uid", uid as i64)
        .and("item_name", req.item_name.clone())
        .one()
        .await?
        .ok_or_else(|| FireplaceError::Internal("Item not found in inventory".to_string()))?;

    if sender_item.quantity < req.quantity {
        return Err("Insufficient item quantity".into());
    }

    let remaining = sender_item.quantity - req.quantity;
    sender_item.quantity = remaining;
    txn.update(sender_item).await?;

    // 2. Add to target
    let target_sync_event = if let Some(mut t_item) = txn
        .find_by::<Inventory>("uid", req.target_uid as i64)
        .and("item_name", req.item_name.clone())
        .one()
        .await?
    {
        t_item.quantity += req.quantity;
        let saved = txn.update(t_item).await?;
        SyncEvent::upsert(
            Inventory::table_name(),
            saved.pk_value(),
            serde_json::to_value(&saved).unwrap(),
        )
    } else {
        let new_item = Inventory {
            id: cabin_fireplace::generate_snowflake() as i64,
            uid: req.target_uid as i64,
            item_name: req.item_name.clone(),
            quantity: req.quantity,
        };
        let saved = txn.insert(new_item).await?;
        SyncEvent::upsert(
            Inventory::table_name(),
            saved.pk_value(),
            serde_json::to_value(&saved).unwrap(),
        )
    };

    // 3. Publish to target (Testing Redis PubSub!)
    txn.publish_to(req.target_uid, vec![target_sync_event]);

    Ok((
        Some(serde_json::json!({ "success": true, "remaining": remaining })),
        Some(txn.into_ctx()),
    ))
}
