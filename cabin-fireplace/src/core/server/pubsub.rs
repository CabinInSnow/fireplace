use dashmap::DashMap;
use futures::StreamExt;
use lazy_static::lazy_static;
use tokio::sync::mpsc;

use crate::core::server::ws::FirePlaceState;
use crate::network::protocol::SyncPush;
use crate::sync::SyncEvent;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct UserConnection {
    pub sid: String,
    pub tx: mpsc::Sender<Arc<String>>,
    pub kill_tx: mpsc::Sender<()>,
    pub seqnum: Arc<AtomicU64>,
}

lazy_static! {
    /// 현재 이 프로세스(인스턴스)에 WebSocket으로 연결된 유저들의 송신 채널 맵.
    ///
    /// # 다중 인스턴스 동작
    /// Redis PubSub은 모든 구독 인스턴스에 브로드캐스트하므로,
    /// 유저가 연결되지 않은 인스턴스에서는 `USER_SOCKETS.get(&uid)` → None으로 자연히 무시됩니다.
    /// 이는 의도된 정상 동작입니다.
    pub static ref USER_SOCKETS: DashMap<u64, UserConnection> = DashMap::new();
}

/// `message:user:*` 패턴을 단일 PSUBSCRIBE로 구독하여 모든 유저 메시지를 처리합니다.
///
/// 기존의 uid별 개별 Subscribe/Unsubscribe 왕복이 완전히 제거되었습니다.
/// 수신된 메시지의 채널명에서 uid를 파싱하여 해당 유저에게 전달합니다.
pub async fn spawn_global_pubsub_multiplexer<S: Clone + Send + Sync + 'static>(
    state: FirePlaceState<S>,
    // 이전 PubSubCommand 수신자 — PSUBSCRIBE 전환으로 더 이상 사용하지 않습니다.
    // 시그니처 호환성을 위해 파라미터는 유지하되 consume만 합니다.
    mut _cmd_rx: mpsc::Receiver<crate::core::server::pubsub::PubSubCommand>,
) {
    let redis_url = state.config.redis_url.clone();

    tokio::spawn(async move {
        let client = match redis::Client::open(redis_url) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Failed to open redis client for global pubsub: {}", e);
                return;
            }
        };

        loop {
            let mut pubsub_conn = match client.get_async_pubsub().await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::error!(
                        "Failed to get async pubsub connection: {}. Retrying in 5s...",
                        e
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            // 단일 PSUBSCRIBE 로 모든 유저 채널 커버.
            // 접속/종료 시 subscribe/unsubscribe 왕복이 불필요합니다.
            let pattern = crate::constants::redis::CHANNEL_PATTERN_MESSAGE_USER;
            if let Err(e) = pubsub_conn.psubscribe(pattern).await {
                tracing::error!(
                    "Failed to psubscribe to '{}': {}. Retrying in 5s...",
                    pattern,
                    e
                );
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }

            tracing::info!("PSUBSCRIBE active on pattern: {}", pattern);

            let mut stream = pubsub_conn.on_message();

            loop {
                let msg = match stream.next().await {
                    Some(m) => m,
                    None => {
                        tracing::warn!(
                            "Redis PubSub stream closed. Attempting to reconnect..."
                        );
                        break;
                    }
                };

                let channel_name = msg.get_channel_name();
                if let Some(uid_str) = channel_name
                    .strip_prefix(crate::constants::redis::CHANNEL_PREFIX_MESSAGE_USER)
                {
                    if let Ok(uid) = uid_str.parse::<u64>() {
                        if let Ok(payload) = msg.get_payload::<String>() {
                            if uid == 0 {
                                // ── Global Broadcast ──────────────────────────────
                                if let Ok(events) =
                                    serde_json::from_str::<Vec<SyncEvent>>(&payload)
                                {
                                    // 직렬화는 단 한 번만 수행하고 Arc로 공유합니다.
                                    for entry in USER_SOCKETS.iter() {
                                        let conn = entry.value();
                                        let new_seqnum =
                                            conn.seqnum.fetch_add(1, Ordering::SeqCst) + 1;
                                        let push = SyncPush {
                                            sync_events: events.clone(),
                                            seqnum: new_seqnum,
                                        };
                                        let res =
                                            crate::network::protocol::WsResponse::Sync(push);
                                        let payload_str =
                                            Arc::new(serde_json::to_string(&res).unwrap());
                                        let _ = conn.tx.try_send(payload_str);
                                    }
                                }
                            } else {
                                // ── Targeted Message ──────────────────────────────
                                let result = if let Some(entry) = USER_SOCKETS.get(&uid) {
                                    if let Ok(events) =
                                        serde_json::from_str::<Vec<SyncEvent>>(&payload)
                                    {
                                        let conn = entry.value();
                                        let new_seqnum =
                                            conn.seqnum.fetch_add(1, Ordering::SeqCst) + 1;
                                        let push = SyncPush {
                                            sync_events: events,
                                            seqnum: new_seqnum,
                                        };
                                        let res =
                                            crate::network::protocol::WsResponse::Sync(push);
                                        let payload_str =
                                            Arc::new(serde_json::to_string(&res).unwrap());
                                        Some((payload_str, conn.kill_tx.clone(), conn.tx.clone()))
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };

                                if let Some((payload_str, kill_tx, tx)) = result {
                                    if tx.try_send(payload_str).is_err() {
                                        tracing::warn!(
                                            "Force disconnecting uid={}: tx channel full",
                                            uid
                                        );
                                        USER_SOCKETS.remove(&uid);
                                        let _ = kill_tx.try_send(());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });
}

/// PubSubCommand는 이전 개별 subscribe/unsubscribe 방식에서 사용하던 타입입니다.
/// PSUBSCRIBE 전환으로 실제로 사용되지 않지만, 엔진 파이프라인 시그니처 호환을 위해 유지합니다.
pub enum PubSubCommand {
    Subscribe(u64),
    Unsubscribe(u64),
}
