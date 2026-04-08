pub mod core;
pub mod db;
pub mod network;
pub mod sync;
pub mod utils;
pub mod constants;
pub mod error;

// ── Public API Re-exports ───────────────────────────────────────────────────

pub use error::{FireplaceError, Result};

// Core framework types
pub use core::server::engine::{FirePlace, FirePlaceConfig};
pub use core::server::router::{HandlerSet, ServerMetrics, AuthInfo, ServerMode, AuthHandler, InitHandler, ActionHandler, WorkerHandler, MonitorHandler};
pub use core::state::StateBundle;

// Database
pub use db::manager::{DbManager, TxnManager};
pub use db::entity::FireplaceEntity;

// Network & Protocol
pub use network::protocol::{WsRequest, WsResponse, WsErrorCode, ActionRequest, ActionResponse, AuthRequest, AuthResponse, InitResponse, ErrorDetail, SyncPush};

// Sync & Game Logic
pub use sync::{GameContext, SyncAction, SyncEvent};

// Utilities
pub use utils::{generate_snowflake, snowflake_to_display_id, snowflake_to_string};

// ── Type Aliases ─────────────────────────────────────────────────────────────

/// 편의를 위한 서버 상태 타입 별칭입니다. HTTP 핸들러 등에서 `State<AppState<S>>` 형태로 사용합니다.
pub type AppState<S> = crate::core::server::ws::FirePlaceState<S>;
