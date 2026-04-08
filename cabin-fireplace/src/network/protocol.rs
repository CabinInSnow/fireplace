use serde::{Deserialize, Serialize};

/// WebSocket 에러 코드 정의. `send_error` 호출 시 매직 넘버 대신 사용합니다.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum WsErrorCode {
    /// 잘못된 JSON 또는 알 수 없는 메시지 타입
    InvalidJson = 4000,
    /// 인증 실패 / 세션 만료 / 중복 로그인
    AuthFailed = 4001,
    /// 클라이언트 seqnum이 서버보다 오래됨
    StaleSeqnum = 4002,
    /// 이전 요청이 아직 처리 중
    ConcurrentRequest = 4009,
    /// 레이트 리밋 초과
    RateLimitExceeded = 4029,
}

impl WsErrorCode {
    pub fn code(self) -> i32 {
        self as i32
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetail {
    pub code: i32,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthRequest {
    /// Unique identifier for the device (always required).
    pub device_id: String,
    /// Login provider type; interpreted by the auth handler (e.g. 0=guest, 1=google, 2=apple).
    pub login_type: u32,
    /// Provider-specific credential (e.g. Firebase ID token, guest UUID).
    pub login_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthResponse {
    pub uid: u64,
    pub sid: String,
    pub display_id: String,
    pub nickname: String,
}

use crate::sync::SyncEvent;

#[derive(Serialize, Deserialize, Debug)]
pub struct ActionRequest {
    pub action: String,
    pub params: serde_json::Value,
    /// Client's current seqnum (last value received from server).
    /// Server validates this >= Redis seqnum, then responds with seqnum + 1.
    pub seqnum: u64,
}

/// Response to an Action request.
///
/// `seqnum` is a unified, Redis-backed counter shared between Action responses
/// and cross-user SyncPush events. Incremented on every processed action
/// (success or failure). Client always sends its latest received seqnum.
/// If `request.seqnum < redis_seqnum`, the server rejects with the current
/// seqnum so the client can catch up (or call Init to reset).
#[derive(Serialize, Deserialize, Debug)]
pub struct ActionResponse {
    pub action: String,
    pub success: bool,
    pub error: Option<ErrorDetail>,
    pub sync_events: Vec<SyncEvent>,
    pub seqnum: u64,
    pub data: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InitResponse {
    pub data: serde_json::Value,
    pub seqnum: u64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "payload")]
pub enum WsRequest {
    Auth(AuthRequest),
    Init,
    Action(ActionRequest),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "payload")]
pub enum WsResponse {
    Auth(AuthResponse),
    Init(InitResponse),
    Action(ActionResponse),
    /// Server-initiated push from another user's action affecting this user.
    /// Carries events + updated seqnum (subscriber-side INCR on receipt).
    Sync(SyncPush),
    Error {
        error: ErrorDetail,
    },
}

/// Payload published to `message:user:{uid}` via Redis PubSub.
/// The subscriber task wraps received events in this struct after INCR.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SyncPush {
    pub sync_events: Vec<SyncEvent>,
    pub seqnum: u64,
}
