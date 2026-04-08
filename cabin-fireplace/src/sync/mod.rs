use sqlx::{Postgres, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ── Sync event types ──────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SyncAction {
    Upsert,
    Delete,
    Patch,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SyncEvent {
    pub domain: String,
    pub id: Value,
    pub action: SyncAction,
    pub payload: Value,
}

impl SyncEvent {
    pub fn upsert(domain: impl Into<String>, id: Value, payload: Value) -> Self {
        Self { domain: domain.into(), id, action: SyncAction::Upsert, payload }
    }
    pub fn patch(domain: impl Into<String>, id: Value, payload: Value) -> Self {
        Self { domain: domain.into(), id, action: SyncAction::Patch, payload }
    }
    pub fn delete(domain: impl Into<String>, id: Value) -> Self {
        Self { domain: domain.into(), id, action: SyncAction::Delete, payload: serde_json::json!({}) }
    }
}

// ── GameContext ───────────────────────────────────────────────────────────────

/// Returned by action handlers. Holds the sqlx transaction and accumulated
/// sync events. The engine commits and publishes atomically after the handler
/// returns.
pub struct GameContext {
    pub(crate) txn: Transaction<'static, Postgres>,
    events: Vec<SyncEvent>,
    outbound: Vec<(u64, Vec<SyncEvent>)>,
}

impl GameContext {
    pub(crate) fn from_txn_manager(mgr: crate::db::manager::TxnManager) -> Self {
        Self {
            txn: mgr.txn,
            events: mgr.sync_events,
            outbound: mgr.outbound,
        }
    }

    pub fn publish_to(&mut self, target_uid: u64, events: Vec<SyncEvent>) {
        if !events.is_empty() {
            self.outbound.push((target_uid, events));
        }
    }

    // ── Called by the engine (ws/handlers.rs) ────────────────────────────────

    pub fn flush_sync_events(&mut self) -> Vec<SyncEvent> {
        std::mem::take(&mut self.events)
    }

    pub fn flush_outbound(&mut self) -> Vec<(u64, Vec<SyncEvent>)> {
        std::mem::take(&mut self.outbound)
    }

    /// Commit the transaction. Called by the engine after flushing events.
    pub async fn commit(self) -> crate::error::Result<()> {
        self.txn.commit().await.map_err(|e| e.into())
    }
}
