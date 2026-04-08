use crate::entity::{hero, inventory, user};
use crate::state::AppState;
use cabin_fireplace::{AuthRequest, DbManager, core::server::router::AuthInfo};
use std::sync::Arc;

pub async fn auth_handler(
    req: AuthRequest,
    db: DbManager,
    _state: Arc<AppState>,
) -> cabin_fireplace::Result<AuthInfo> {
    tracing::info!(
        "Auth: device_id={} login_type={} login_id={}",
        req.device_id, req.login_type, req.login_id
    );

    // Try to find existing user
    if let Some(existing) = db
        .find_by::<user::User>("login_type", req.login_type as i64)
        .and("login_id", req.login_id.clone())
        .one()
        .await?
    {
        return Ok(AuthInfo {
            uid: existing.id as u64,
            display_id: existing.display_id,
            nickname: existing.nickname,
        });
    }

    // New user — open a transaction for the multi-row insert
    let mut txn = db.begin().await?;

    let uid_val = cabin_fireplace::utils::generate_snowflake();
    let display_id = cabin_fireplace::utils::snowflake_to_display_id(uid_val);
    let nickname_val = format!("User_{}", req.login_id);

    let new_user = user::User {
        id: uid_val as i64,
        display_id: display_id.clone(),
        nickname: nickname_val.clone(),
        device_id: req.device_id.clone(),
        login_type: req.login_type as i32,
        login_id: req.login_id.clone(),
        created_at: None,
    };
    txn.insert(new_user).await?;

    let uid = uid_val as i64;

    txn.insert(hero::Hero {
        id: cabin_fireplace::utils::generate_snowflake() as i64,
        uid,
        hero_type: "warrior".to_string(),
        level: 1,
        exp: 0,
    })
    .await?;

    txn.insert(inventory::Inventory {
        id: cabin_fireplace::utils::generate_snowflake() as i64,
        uid,
        item_name: "gold".to_string(),
        quantity: 1000,
    })
    .await?;

    // Auth doesn't need a GameContext — commit manually via the raw txn
    // (TxnManager exposes into_ctx for action handlers; for auth we just commit)
    txn.commit().await?;

    tracing::info!("Created new user uid={} for login_id={}", uid_val, req.login_id);
    Ok(AuthInfo {
        uid: uid_val,
        display_id,
        nickname: nickname_val,
    })
}
