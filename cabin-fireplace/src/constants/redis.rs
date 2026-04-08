//! Redis key formatters and constants

pub const SESSION_TTL: u64 = 21600; // 6 hours

pub const CHANNEL_PATTERN_MESSAGE_USER: &str = "message:user:*";
pub const CHANNEL_PREFIX_MESSAGE_USER: &str = "message:user:";

#[inline]
pub fn key_session_uid(uid: u64) -> String {
    format!("session:uid:{}", uid)
}

#[inline]
pub fn key_session_sid(sid: &str) -> String {
    format!("session:sid:{}", sid)
}

#[inline]
pub fn channel_message_user(uid: u64) -> String {
    format!("message:user:{}", uid)
}
