use axum::routing::MethodRouter;
use crate::db::manager::DbManager;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use serde::Serialize;
use crate::error::Result;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum ServerMode {
    Frontend,
    Worker,
    All,
}

impl std::fmt::Display for ServerMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerMode::Frontend => write!(f, "FRONTEND"),
            ServerMode::Worker => write!(f, "WORKER"),
            ServerMode::All => write!(f, "ALL"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerMetrics {
    pub mode: ServerMode,
    pub connected_users: usize,
    pub connected_sessions: usize,
    pub redis_status: String,
    pub redis_clients: usize,
    pub redis_mem_mb: f64,
    pub db_pool_active: u32,
    pub db_pool_idle: u32,
    pub cpu_usage: f32,
    pub used_mem_mb: f64,
    pub peak_mem_mb: f64,
    
    // Performance
    pub frontend_throughput_sec: f64,
    pub frontend_latency_1m_ms: f64,
    pub worker_latency_1m_ms: f64,
}

use crate::core::state::StateBundle;
use crate::sync::GameContext;

pub struct AuthInfo {
    pub uid: u64,
    pub display_id: String,
    pub nickname: String,
}

#[async_trait::async_trait]
pub trait AuthHandler<S>: Send + Sync + 'static {
    async fn handle(
        &self,
        req: crate::network::protocol::AuthRequest,
        db: DbManager,
        state: Arc<S>,
    ) -> Result<AuthInfo>;
}

#[async_trait::async_trait]
impl<S, F, Fut> AuthHandler<S> for F
where
    S: Send + Sync + 'static,
    F: Fn(crate::network::protocol::AuthRequest, DbManager, Arc<S>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<AuthInfo>> + Send + 'static,
{
    async fn handle(
        &self,
        req: crate::network::protocol::AuthRequest,
        db: DbManager,
        state: Arc<S>,
    ) -> Result<AuthInfo> {
        (self)(req, db, state).await
    }
}

#[async_trait::async_trait]
pub trait InitHandler<S>: Send + Sync + 'static {
    async fn handle(
        &self,
        uid: u64,
        db: DbManager,
        state: Arc<S>,
    ) -> Result<Value>;
}

#[async_trait::async_trait]
impl<S, F, Fut> InitHandler<S> for F
where
    S: Send + Sync + 'static,
    F: Fn(u64, DbManager, Arc<S>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<Value>> + Send + 'static,
{
    async fn handle(
        &self,
        uid: u64,
        db: DbManager,
        state: Arc<S>,
    ) -> Result<Value> {
        (self)(uid, db, state).await
    }
}

#[async_trait::async_trait]
pub trait ActionHandler<S>: Send + Sync + 'static {
    async fn handle(
        &self,
        uid: u64,
        bundle: StateBundle,
        params: Value,
        state: Arc<S>,
    ) -> Result<(Option<Value>, Option<GameContext>)>;
}

#[async_trait::async_trait]
impl<S, F, Fut> ActionHandler<S> for F
where
    S: Send + Sync + 'static,
    F: Fn(u64, StateBundle, Value, Arc<S>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(Option<Value>, Option<GameContext>)>> + Send + 'static,
{
    async fn handle(
        &self,
        uid: u64,
        bundle: StateBundle,
        params: Value,
        state: Arc<S>,
    ) -> Result<(Option<Value>, Option<GameContext>)> {
        (self)(uid, bundle, params, state).await
    }
}

#[async_trait::async_trait]
pub trait WorkerHandler<S>: Send + Sync + 'static {
    async fn handle(&self, bundle: StateBundle, state: Arc<S>);
}

#[async_trait::async_trait]
impl<S, F, Fut> WorkerHandler<S> for F
where
    S: Send + Sync + 'static,
    F: Fn(StateBundle, Arc<S>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    async fn handle(&self, bundle: StateBundle, state: Arc<S>) {
        (self)(bundle, state).await
    }
}

pub struct WorkerEntry<S> {
    pub name: String,
    pub tick_ms: u64,
    pub handler: Arc<dyn WorkerHandler<S>>,
}

impl<S> Clone for WorkerEntry<S> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            tick_ms: self.tick_ms,
            handler: self.handler.clone(),
        }
    }
}

#[async_trait::async_trait]
pub trait MonitorHandler<S>: Send + Sync + 'static {
    async fn handle(&self, metrics: ServerMetrics, state: Arc<S>);
}

#[async_trait::async_trait]
impl<S, F, Fut> MonitorHandler<S> for F
where
    S: Send + Sync + 'static,
    F: Fn(ServerMetrics, Arc<S>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    async fn handle(&self, metrics: ServerMetrics, state: Arc<S>) {
        (self)(metrics, state).await
    }
}

pub struct HandlerSet<S> {
    pub auth_handler: Option<Arc<dyn AuthHandler<S>>>,
    pub init_handler: Option<Arc<dyn InitHandler<S>>>,
    pub action_handlers: HashMap<String, Arc<dyn ActionHandler<S>>>,
    pub worker_handlers: Vec<WorkerEntry<S>>,
    pub monitor_handler: Option<Arc<dyn MonitorHandler<S>>>,
    pub http_routers: Vec<(String, MethodRouter<super::FirePlaceState<S>>)>,
}

impl<S> Clone for HandlerSet<S> {
    fn clone(&self) -> Self {
        Self {
            auth_handler: self.auth_handler.clone(),
            init_handler: self.init_handler.clone(),
            action_handlers: self.action_handlers.clone(),
            worker_handlers: self.worker_handlers.clone(),
            monitor_handler: self.monitor_handler.clone(),
            http_routers: self.http_routers.clone(),
        }
    }
}

impl<S> HandlerSet<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn default_action_handlers() -> HashMap<String, Arc<dyn ActionHandler<S>>> {
        let mut handlers: HashMap<String, Arc<dyn ActionHandler<S>>> = HashMap::new();
        handlers.insert(
            "ping".to_string(),
            Arc::new(|_uid: u64, _bundle: StateBundle, _params: Value, _state: Arc<S>| async move {
                Ok((Some(serde_json::json!({ "response": "pong" })), None))
            }),
        );
        handlers.insert(
            "echo".to_string(),
            Arc::new(|_uid: u64, _bundle: StateBundle, params: Value, _state: Arc<S>| async move {
                Ok((Some(params), None))
            }),
        );
        handlers
    }

    pub fn new() -> Self {
        Self {
            auth_handler: None,
            init_handler: None,
            action_handlers: Self::default_action_handlers(),
            worker_handlers: Vec::new(),
            monitor_handler: None,
            http_routers: Vec::new(),
        }
    }

    pub fn register_auth_handler<H>(mut self, handler: H) -> Self
    where
        H: AuthHandler<S>,
    {
        self.auth_handler = Some(Arc::new(handler));
        self
    }

    pub fn register_init_handler<H>(mut self, handler: H) -> Self
    where
        H: InitHandler<S>,
    {
        self.init_handler = Some(Arc::new(handler));
        self
    }

    pub fn register_action_handlers<H>(mut self, action_name: &str, handler: H) -> Self
    where
        H: ActionHandler<S>,
    {
        self.action_handlers
            .insert(action_name.to_string(), Arc::new(handler));
        self
    }

    /// Register a worker with a specific name and tick interval.
    pub fn register_worker<H>(mut self, name: &str, tick_ms: u64, handler: H) -> Self
    where
        H: WorkerHandler<S>,
    {
        self.worker_handlers.push(WorkerEntry {
            name: name.to_string(),
            tick_ms,
            handler: Arc::new(handler),
        });
        self
    }

    /// Legacy method for registering a single worker handler. 
    /// Internally uses config.worker_tick_ms and "default" as name.
    pub fn register_worker_handler<H>(self, handler: H) -> Self
    where
        H: WorkerHandler<S>,
    {
        // We don't have config here, so we'll use a placeholder and 
        // the engine will use config.worker_tick_ms for this one if it finds it.
        // Actually, it's better to just use a default tick here.
        self.register_worker("default", 1000, handler)
    }

    pub fn register_monitor_handler<H>(mut self, handler: H) -> Self
    where
        H: MonitorHandler<S>,
    {
        self.monitor_handler = Some(Arc::new(handler));
        self
    }

    pub fn register_http_handlers(
        mut self,
        path: &str,
        router: MethodRouter<super::FirePlaceState<S>>,
    ) -> Self {
        self.http_routers.push((path.to_string(), router));
        self
    }
}
