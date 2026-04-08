use serde::{Deserialize, Serialize};

/// Stores the active session ID associated with a given uid.
/// Created when auth succeeds. Keyed by `session:uid:{uid}` in Redis.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectionUserData {
    pub sid: String,
}

/// Tracks metadata about an established WebSocket connection.
/// Created immediately on WS connect; uid is populated after successful auth.
/// Keyed by `session:sid:{sid}` in Redis.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectionSessionData {
    pub uid: Option<u64>,       // None until auth succeeds
    pub remote_ip: String,
    pub remote_port: u16,
    pub created_at: i64,
    pub authed_at: Option<i64>, // None until auth succeeds
}
