use axum::{
    extract::{
        ws::{Message, WebSocket},
        ConnectInfo, State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use redis::AsyncCommands;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    core::state::StateBundle,
    core::server::engine::FirePlaceConfig,
    core::server::router::HandlerSet,
    core::server::pubsub::{USER_SOCKETS, PubSubCommand},
    network::protocol::{WsErrorCode, WsRequest},
    network::session::ConnectionSessionData,
};
use std::sync::atomic::{AtomicUsize, Ordering};

pub static ACTIVE_SESSIONS: AtomicUsize = AtomicUsize::new(0);


#[derive(Clone)]
pub struct FirePlaceState<S> {
    pub config: FirePlaceConfig,
    pub core_state: StateBundle,
    pub user_state: Arc<S>,
    pub handler_set: Arc<HandlerSet<S>>,
    pub pubsub_cmd_tx: mpsc::Sender<PubSubCommand>,
}

pub async fn ws_central_handler<S: Clone + Send + Sync + 'static>(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<FirePlaceState<S>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_unified_socket(socket, state, addr))
}

pub mod handlers;

use handlers::{process_action_request, process_auth_request, process_init_request, send_error};

// ── 토큰 버킷 레이트 리미터 ──────────────────────────────────────────────────────

/// 슬라이딩 윈도우 VecDeque 방식을 대체하는 토큰 버킷 구현.
/// 연결당 메모리 사용이 O(1)으로 고정됩니다.
struct TokenBucket {
    tokens: f64,
    max_tokens: f64,
    /// 밀리초당 보충 속도 (rate_limit_per_sec / 1000.0)
    refill_rate: f64,
    last_refill_ms: i64,
}

impl TokenBucket {
    fn new(rate_per_sec: usize) -> Self {
        Self {
            tokens: rate_per_sec as f64,
            max_tokens: rate_per_sec as f64,
            refill_rate: rate_per_sec as f64 / 1000.0,
            last_refill_ms: Utc::now().timestamp_millis(),
        }
    }

    /// 토큰 소비를 시도합니다. 성공 시 true, 리밋 초과 시 false.
    fn try_consume(&mut self, now_ms: i64) -> bool {
        let elapsed_ms = (now_ms - self.last_refill_ms).max(0) as f64;
        self.tokens = (self.tokens + elapsed_ms * self.refill_rate).min(self.max_tokens);
        self.last_refill_ms = now_ms;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────

pub async fn handle_unified_socket<S: Clone + Send + Sync + 'static>(
    socket: WebSocket,
    state: FirePlaceState<S>,
    addr: SocketAddr,
) {
    ACTIVE_SESSIONS.fetch_add(1, Ordering::SeqCst);
    tracing::info!("New WS connection from {}", addr);
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Bounded channel — 백프레셔. 슬로우 클라이언트가 메모리를 무한히 소모하지 않도록 합니다.
    let (tx, mut rx) = mpsc::channel::<Arc<String>>(100);
    let (kill_tx, mut kill_rx) = mpsc::channel::<()>(1);
    let seqnum = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let forward_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender
                .send(Message::Text((*msg).clone()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let sid = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();

    let initial_session = ConnectionSessionData {
        uid: None,
        remote_ip: addr.ip().to_string(),
        remote_port: addr.port(),
        created_at: now,
        authed_at: None,
    };

    let mut con = state.core_state.redis_data.clone();
    let sid_key = crate::constants::redis::key_session_sid(&sid);
    let _: () = con
        .set_ex(
            &sid_key,
            serde_json::to_string(&initial_session).unwrap(),
            crate::constants::redis::SESSION_TTL,
        )
        .await
        .unwrap_or(());

    let mut current_uid: Option<u64> = None;

    let mut rate_limiter = TokenBucket::new(state.config.rate_limit_per_sec);
    let is_processing = Arc::new(std::sync::atomic::AtomicBool::new(false));

    loop {
        tokio::select! {
            msg_opt = ws_receiver.next() => {
                let msg = match msg_opt {
                    Some(Ok(m)) => m,
                    _ => break,
                };

                // 토큰 버킷 레이트 리밋
                let now_ms = Utc::now().timestamp_millis();
                if !rate_limiter.try_consume(now_ms) {
                    tracing::warn!(
                        "Force disconnecting {} (sid={}, uid={:?}): Rate limit exceeded",
                        addr, sid, current_uid
                    );
                    send_error(&tx, WsErrorCode::RateLimitExceeded.code(), "Too many requests. Rate limit exceeded.").await;
                    break;
                }

                // 동시 처리 방지 (Action 전용)
                if is_processing.load(std::sync::atomic::Ordering::SeqCst) {
                    tracing::warn!(
                        "Force disconnecting {} (sid={}, uid={:?}): Concurrent request processing",
                        addr, sid, current_uid
                    );
                    send_error(
                        &tx,
                        WsErrorCode::ConcurrentRequest.code(),
                        "Concurrent request processing. Please wait for the previous action to complete.",
                    )
                    .await;
                    break;
                }

                match msg {
                    Message::Close(_) => {
                        tracing::info!("WS close frame received: {} (sid={})", addr, sid);
                        break;
                    }
                    Message::Text(text) => {
                        let req: WsRequest = match serde_json::from_str(&text) {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::warn!("Invalid JSON from {}: {} — {}", addr, text, e);
                                send_error(&tx, WsErrorCode::InvalidJson.code(), "Invalid JSON or unknown message type").await;
                                continue;
                            }
                        };

                        match req {
                            WsRequest::Auth(auth_req) => {
                                let start = std::time::Instant::now();
                                if let Some(uid) =
                                    process_auth_request(auth_req, &state, &addr, &sid, &tx, &kill_tx, &seqnum).await
                                {
                                    current_uid = Some(uid);
                                }
                                let elapsed = start.elapsed().as_micros() as u64;
                                crate::core::metrics::PERF_METRICS.ws_msg_latency_us.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);
                                crate::core::metrics::PERF_METRICS.ws_msg_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            WsRequest::Init => {
                                let start = std::time::Instant::now();
                                process_init_request(current_uid, &sid, &state, &tx, &kill_tx, &seqnum).await;
                                let elapsed = start.elapsed().as_micros() as u64;
                                crate::core::metrics::PERF_METRICS.ws_msg_latency_us.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);
                                crate::core::metrics::PERF_METRICS.ws_msg_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            WsRequest::Action(action_req) => {
                                is_processing.store(true, std::sync::atomic::Ordering::SeqCst);
                                let state_clone = state.clone();
                                let tx_clone = tx.clone();
                                let is_proc_clone = is_processing.clone();
                                let sid_clone = sid.clone();
                                let kill_tx_clone = kill_tx.clone();
                                let seqnum_clone = seqnum.clone();
                                tokio::spawn(async move {
                                    let start = std::time::Instant::now();
                                    process_action_request(
                                        current_uid,
                                        sid_clone,
                                        action_req,
                                        state_clone,
                                        tx_clone,
                                        kill_tx_clone,
                                        seqnum_clone,
                                        is_proc_clone,
                                    )
                                    .await;
                                    let elapsed = start.elapsed().as_micros() as u64;
                                    crate::core::metrics::PERF_METRICS.ws_msg_latency_us.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);
                                    crate::core::metrics::PERF_METRICS.ws_msg_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                });
                            }
                        }
                    }
                    _ => {}
                }
            } // end select msg
            _ = kill_rx.recv() => {
                tracing::info!("Kill signal received for sid={}", sid);
                break;
            }
        } // end select!
    }

    forward_task.abort();
    if let Some(uid) = current_uid {
        let mut should_remove = false;
        if let Some(entry) = USER_SOCKETS.get(&uid) {
            if entry.sid == sid {
                should_remove = true;
            }
        }
        if should_remove {
            USER_SOCKETS.remove(&uid);
            // PSUBSCRIBE 전환으로 unsubscribe 왕복이 불필요합니다.
            // (PubSubCommand는 시그니처 호환용으로만 유지)
        }
    }
    ACTIVE_SESSIONS.fetch_sub(1, Ordering::SeqCst);
    tracing::info!("WS disconnected: {} (sid={})", addr, sid);
}
