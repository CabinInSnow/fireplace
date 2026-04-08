use chrono::Utc;
use redis::AsyncCommands;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::{
    core::server::pubsub::USER_SOCKETS,
    core::server::ws::FirePlaceState,
    network::protocol::{
        ActionResponse, AuthResponse, InitResponse, WsErrorCode, WsResponse,
    },
    network::session::{ConnectionSessionData, ConnectionUserData},
    sync::SyncEvent,
};
use std::sync::atomic::{AtomicBool, Ordering};

// ── 내부 send_error ───────────────────────────────────────────────────────────

pub(crate) async fn send_error(tx: &mpsc::Sender<Arc<String>>, code: i32, msg: &str) {
    let err = WsResponse::Error {
        error: crate::network::protocol::ErrorDetail {
            code,
            message: msg.to_string(),
        },
    };
    let payload = Arc::new(serde_json::to_string(&err).unwrap());
    let _ = tx.send(payload).await;
}

// ── ProcessingGuard ───────────────────────────────────────────────────────────

pub(crate) struct ProcessingGuard(pub Arc<AtomicBool>);
impl Drop for ProcessingGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

// ── verify_session ────────────────────────────────────────────────────────────

/// 세션을 Redis에서 검증합니다.
/// 검증 성공 시 TTL 잔여 시간이 1시간 미만이면 조건부로 갱신합니다.
pub(crate) async fn verify_session(
    uid: u64,
    current_sid: &str,
    mut con: redis::aio::MultiplexedConnection,
    tx: &mpsc::Sender<Arc<String>>,
    kill_tx: &mpsc::Sender<()>,
) -> bool {
    let uid_key = crate::constants::redis::key_session_uid(uid);
    let sid_key = crate::constants::redis::key_session_sid(current_sid);

    if let Some(uid_json) = con.get::<_, Option<String>>(&uid_key).await.unwrap_or(None) {
        if let Ok(user_data) = serde_json::from_str::<ConnectionUserData>(&uid_json) {
            if user_data.sid != current_sid {
                send_error(tx, WsErrorCode::AuthFailed.code(), "Invalid session (logged in elsewhere).").await;
                let _ = kill_tx.try_send(());
                return false;
            }

            // 세션 TTL 조건부 갱신: 잔여 TTL이 1시간(3600초) 미만일 때만 갱신합니다.
            if let Ok(Some(ttl)) = con.ttl::<_, Option<i64>>(&uid_key).await {
                if ttl < 3600 {
                    let session_ttl = crate::constants::redis::SESSION_TTL as i64;
                    let _ = con.expire(&uid_key, session_ttl).await.unwrap_or(());
                    let _ = con.expire(&sid_key, session_ttl).await.unwrap_or(());
                }
            }

            return true;
        } else {
            send_error(tx, WsErrorCode::AuthFailed.code(), "Invalid session data.").await;
            let _ = kill_tx.try_send(());
            return false;
        }
    } else {
        send_error(tx, WsErrorCode::AuthFailed.code(), "Session expired.").await;
        let _ = kill_tx.try_send(());
        return false;
    }
}

// ── process_auth_request ──────────────────────────────────────────────────────

#[tracing::instrument(skip(state, addr, tx, kill_tx, seqnum))]
pub(crate) async fn process_auth_request<S: Clone + Send + Sync + 'static>(
    auth_req: crate::network::protocol::AuthRequest,
    state: &FirePlaceState<S>,
    addr: &SocketAddr,
    sid: &str,
    tx: &mpsc::Sender<Arc<String>>,
    kill_tx: &mpsc::Sender<()>,
    seqnum: &Arc<std::sync::atomic::AtomicU64>,
) -> Option<u64> {
    let handler = match &state.handler_set.auth_handler {
        Some(h) => h,
        None => {
            send_error(tx, WsErrorCode::InvalidJson.code(), "No auth handler registered.").await;
            return None;
        }
    };

    match handler
        .handle(
            auth_req,
            state.core_state.db_manager.clone(),
            state.user_state.clone(),
        )
        .await
    {
        Ok(auth_info) => {
            let uid = auth_info.uid;
            let mut con = state.core_state.redis_data.clone();
            let now = Utc::now().timestamp();
            let uid_key = crate::constants::redis::key_session_uid(uid);

            let existing_uid_data: Option<String> = con.get(&uid_key).await.unwrap_or(None);
            if let Some(uid_json) = existing_uid_data {
                if let Ok(old_user_data) =
                    serde_json::from_str::<ConnectionUserData>(&uid_json)
                {
                    let old_sid_key =
                        crate::constants::redis::key_session_sid(&old_user_data.sid);
                    let _: () = con.del(&old_sid_key).await.unwrap_or(());
                }
            }

            let promoted_session = ConnectionSessionData {
                uid: Some(uid),
                remote_ip: addr.ip().to_string(),
                remote_port: addr.port(),
                created_at: now,
                authed_at: Some(now),
            };
            let sid_key = crate::constants::redis::key_session_sid(sid);
            let session_ttl = crate::constants::redis::SESSION_TTL;
            let _: () = con
                .set_ex(
                    &sid_key,
                    serde_json::to_string(&promoted_session).unwrap(),
                    session_ttl,
                )
                .await
                .unwrap_or(());

            let user_data = ConnectionUserData {
                sid: sid.to_string(),
            };
            let _: () = con
                .set_ex(
                    &uid_key,
                    serde_json::to_string(&user_data).unwrap(),
                    session_ttl,
                )
                .await
                .unwrap_or(());

            let res = WsResponse::Auth(AuthResponse {
                uid,
                sid: sid.to_string(),
                display_id: auth_info.display_id,
                nickname: auth_info.nickname,
            });
            let _ = tx.send(Arc::new(serde_json::to_string(&res).unwrap())).await;

            USER_SOCKETS.insert(
                uid,
                crate::core::server::pubsub::UserConnection {
                    sid: sid.to_string(),
                    tx: tx.clone(),
                    kill_tx: kill_tx.clone(),
                    seqnum: seqnum.clone(),
                },
            );

            Some(uid)
        }
        Err(e) => {
            send_error(
                tx,
                WsErrorCode::AuthFailed.code(),
                &format!("Auth error: {}", e),
            )
            .await;
            None
        }
    }
}

// ── process_init_request ──────────────────────────────────────────────────────

#[tracing::instrument(skip(state, tx, kill_tx, seqnum))]
pub(crate) async fn process_init_request<S: Clone + Send + Sync + 'static>(
    current_uid: Option<u64>,
    current_sid: &str,
    state: &FirePlaceState<S>,
    tx: &mpsc::Sender<Arc<String>>,
    kill_tx: &mpsc::Sender<()>,
    seqnum: &Arc<std::sync::atomic::AtomicU64>,
) {
    let uid = match current_uid {
        Some(id) => id,
        None => {
            send_error(tx, WsErrorCode::AuthFailed.code(), "Not authenticated").await;
            return;
        }
    };

    if !verify_session(
        uid,
        current_sid,
        state.core_state.redis_data.clone(),
        tx,
        kill_tx,
    )
    .await
    {
        return;
    }

    let handler = match &state.handler_set.init_handler {
        Some(h) => h,
        None => {
            send_error(tx, WsErrorCode::InvalidJson.code(), "No init handler registered").await;
            return;
        }
    };

    match handler
        .handle(uid, state.core_state.db_manager.clone(), state.user_state.clone())
        .await
    {
        Ok(val) => {
            seqnum.store(0, std::sync::atomic::Ordering::SeqCst);
            let res = WsResponse::Init(InitResponse { data: val, seqnum: 0 });
            let _ = tx.send(Arc::new(serde_json::to_string(&res).unwrap())).await;
        }
        Err(e) => {
            send_error(
                tx,
                WsErrorCode::InvalidJson.code(),
                &format!("Init error: {}", e),
            )
            .await;
        }
    }
}

// ── process_action_request ────────────────────────────────────────────────────

#[tracing::instrument(skip(state, tx, kill_tx, seqnum, is_processing))]
pub(crate) async fn process_action_request<S: Clone + Send + Sync + 'static>(
    current_uid: Option<u64>,
    current_sid: String,
    action_req: crate::network::protocol::ActionRequest,
    state: FirePlaceState<S>,
    tx: mpsc::Sender<Arc<String>>,
    kill_tx: mpsc::Sender<()>,
    seqnum: Arc<std::sync::atomic::AtomicU64>,
    is_processing: Arc<AtomicBool>,
) {
    let _guard = ProcessingGuard(is_processing);

    let uid = match current_uid {
        Some(id) => id,
        None => {
            send_error(&tx, WsErrorCode::AuthFailed.code(), "Not authenticated.").await;
            return;
        }
    };

    if !verify_session(
        uid,
        &current_sid,
        state.core_state.redis_data.clone(),
        &tx,
        &kill_tx,
    )
    .await
    {
        return;
    }

    let action_name = action_req.action.clone();

    let current_seqnum: u64 = seqnum.load(std::sync::atomic::Ordering::SeqCst);
    if action_req.seqnum < current_seqnum {
        let res = WsResponse::Action(ActionResponse {
            action: action_name,
            success: false,
            error: Some(crate::network::protocol::ErrorDetail {
                code: WsErrorCode::StaleSeqnum.code(),
                message: format!(
                    "Stale seqnum: sent {}, server is at {}. Re-sync with Init or retry with the latest seqnum.",
                    action_req.seqnum, current_seqnum
                ),
            }),
            sync_events: vec![],
            seqnum: current_seqnum,
            data: None,
        });
        let _ = tx
            .send(Arc::new(serde_json::to_string(&res).unwrap()))
            .await;
        return;
    }

    let mut response_data = None;
    let mut error_detail = None;
    let mut sync_events: Vec<SyncEvent> = vec![];

    if let Some(handler) = state.handler_set.action_handlers.get(&action_req.action) {
        match handler
            .handle(
                uid,
                state.core_state.clone(),
                action_req.params.clone(),
                state.user_state.clone(),
            )
            .await
        {
            Ok((data, optional_ctx)) => {
                response_data = data;

                if let Some(mut ctx) = optional_ctx {
                    sync_events = ctx.flush_sync_events();
                    let outbound = ctx.flush_outbound();

                    match ctx.commit().await {
                        Ok(_) => {
                            if !outbound.is_empty() {
                                let mut pubsub_con = state.core_state.redis_pubsub.clone();
                                for (target_uid, events) in outbound {
                                    let chan = crate::constants::redis::channel_message_user(
                                        target_uid,
                                    );
                                    let payload = serde_json::to_string(&events).unwrap();
                                    let _: () = pubsub_con
                                        .publish(&chan, payload)
                                        .await
                                        .unwrap_or(());
                                }
                            }
                        }
                        Err(e) => {
                            error_detail = Some(crate::network::protocol::ErrorDetail {
                                code: WsErrorCode::InvalidJson.code(),
                                message: format!("Commit failed: {}", e),
                            });
                        }
                    }
                }
            }
            Err(e) => {
                error_detail = Some(crate::network::protocol::ErrorDetail {
                    code: WsErrorCode::InvalidJson.code(),
                    message: e.to_string(), // FireplaceError::to_string()
                });
            }
        }
    } else {
        error_detail = Some(crate::network::protocol::ErrorDetail {
            code: WsErrorCode::InvalidJson.code(),
            message: format!("Unknown action: {}", action_req.action),
        });
    }

    let new_seqnum: u64 = seqnum.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

    let success = error_detail.is_none();
    let res = WsResponse::Action(ActionResponse {
        action: action_name,
        success,
        error: error_detail,
        sync_events,
        seqnum: new_seqnum,
        data: response_data,
    });
    let _ = tx
        .send(Arc::new(serde_json::to_string(&res).unwrap()))
        .await;
}
