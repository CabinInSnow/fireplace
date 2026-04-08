use axum::Router;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::core::server::router::HandlerSet;
use crate::core::server::ws::FirePlaceState;
use crate::core::state::StateBundle;

#[derive(Clone)]
pub struct FirePlaceConfig {
    pub port: u16,
    pub redis_url: String,
    pub db_url: String,
    pub websocket_enable: bool,
    pub websocket_endpoint: Option<String>,
    pub rate_limit_per_sec: usize,
    pub db_max_connections: u32,
    pub worker_tick_ms: u64,
    /// Monitor task polling interval in milliseconds. Defaults to 1000ms.
    pub monitor_tick_ms: u64,
}

pub struct FirePlace<S> {
    pub config: FirePlaceConfig,
    core_state: StateBundle,
    user_state: Arc<S>,
    handler_set: HandlerSet<S>,
    router_builder: Router<FirePlaceState<S>>,
}

fn format_critical_error(title: &str, url: &str, err_details: &str, tip: &str) -> String {
    format!(
        "\n\n======================================================\n\
         [CRITICAL] {}!\n\
         ------------------------------------------------------\n\
         Target URL: {}\n\
         Error Details: {}\n\
         \n\
         {}\n\
         ======================================================\n",
        title, url, err_details, tip
    )
}

impl<S> FirePlace<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub async fn new(config: FirePlaceConfig, user_state: S) -> Result<Self, String> {
        tracing::info!(
            "Connecting to Redis data connection at {}",
            config.redis_url
        );
        let redis_client = redis::Client::open(config.redis_url.clone()).map_err(|e| {
            format!(
                "Invalid Redis URL configuration ({}): {}",
                config.redis_url, e
            )
        })?;

        let redis_con = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            redis_client.get_multiplexed_tokio_connection(),
        )
        .await
        {
            Ok(Ok(con)) => con,
            Ok(Err(e)) => {
                return Err(format_critical_error(
                    "Redis Connection Failed",
                    &config.redis_url,
                    &e.to_string(),
                    "Please ensure that your Redis server is running and accessible.",
                ))
            }
            Err(_) => {
                return Err(format_critical_error(
                    "Redis Connection Failed",
                    &config.redis_url,
                    "Connection Timeout (5s)",
                    "Please ensure that your Redis server is running and accessible.",
                ))
            }
        };

        tracing::info!("Connecting to Database at {}", config.db_url);
        let pool = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            PgPoolOptions::new()
                .max_connections(config.db_max_connections)
                .connect(&config.db_url),
        )
        .await
        {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => {
                return Err(format_critical_error(
                    "Database Connection Failed",
                    &config.db_url,
                    &e.to_string(),
                    "Please ensure that your Postgres server is running and accessible.",
                ))
            }
            Err(_) => {
                return Err(format_critical_error(
                    "Database Connection Failed",
                    &config.db_url,
                    "Connection Timeout (5s)",
                    "Please ensure that your Postgres server is running and accessible.",
                ))
            }
        };

        tracing::info!("Database connection established");
        let core_state = StateBundle::new(pool, redis_con.clone(), redis_con);

        Ok(Self {
            config,
            core_state,
            user_state: Arc::new(user_state),
            handler_set: HandlerSet::new(),
            router_builder: Router::new(),
        })
    }

    pub fn register_handlers(mut self, handler_set: HandlerSet<S>) -> Self {
        for (path, router) in handler_set.http_routers.clone() {
            self.router_builder = self.router_builder.route(&path, router);
        }
        self.handler_set = handler_set;
        self
    }

    fn build_state(&self, pubsub_cmd_tx: tokio::sync::mpsc::Sender<crate::core::server::pubsub::PubSubCommand>) -> FirePlaceState<S> {
        FirePlaceState {
            config: self.config.clone(),
            core_state: self.core_state.clone(),
            user_state: self.user_state.clone(),
            handler_set: Arc::new(self.handler_set.clone()),
            pubsub_cmd_tx,
        }
    }

    async fn serve_frontend(self, final_state: FirePlaceState<S>, pubsub_cmd_rx: tokio::sync::mpsc::Receiver<crate::core::server::pubsub::PubSubCommand>) -> Result<(), std::io::Error> {
        let bind_addr = format!("0.0.0.0:{}", self.config.port);
        tracing::info!("FirePlace Server starting on {}", bind_addr);

        crate::core::server::pubsub::spawn_global_pubsub_multiplexer(final_state.clone(), pubsub_cmd_rx).await;

        let mut app = self.router_builder;

        if self.config.websocket_enable {
            let mut ws_endpoint = self
                .config
                .websocket_endpoint
                .unwrap_or_else(|| "/ws".to_string());
            if !ws_endpoint.starts_with('/') {
                ws_endpoint.insert(0, '/');
            }
            tracing::info!("Mounting WebSocket endpoint at {}", ws_endpoint);
            app = app.route(
                &ws_endpoint,
                axum::routing::get(crate::core::server::ws::ws_central_handler::<S>),
            );
        }

        let app = app
            .route("/health", axum::routing::get(|| async { "OK" }))
            .with_state(final_state);

        let listener = TcpListener::bind(bind_addr).await?;
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
    }

    pub async fn run_worker(self) {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let final_state = self.build_state(tx);
        spawn_monitor_task(
            crate::core::server::router::ServerMode::Worker,
            final_state.clone(),
        );
        crate::core::server::worker::run_worker_loop(final_state).await;
    }

    pub async fn run_frontend(self) -> Result<(), std::io::Error> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let final_state = self.build_state(tx);
        spawn_monitor_task(
            crate::core::server::router::ServerMode::Frontend,
            final_state.clone(),
        );
        self.serve_frontend(final_state, rx).await
    }

    pub async fn run_all(self) -> Result<(), std::io::Error> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let final_state = self.build_state(tx);
        spawn_monitor_task(
            crate::core::server::router::ServerMode::All,
            final_state.clone(),
        );

        let worker_state = final_state.clone();
        tokio::spawn(async move {
            crate::core::server::worker::run_worker_loop(worker_state).await;
        });

        self.serve_frontend(final_state, rx).await
    }
}

fn spawn_monitor_task<S: Clone + Send + Sync + 'static>(
    mode: crate::core::server::router::ServerMode,
    state: FirePlaceState<S>,
) {
    let monitor_tick_ms = state.config.monitor_tick_ms;
    tokio::spawn(async move {
        let mut sys = sysinfo::System::new_all();
        let pid = sysinfo::get_current_pid().expect("Failed to get current process PID");
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(monitor_tick_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut frontend_history: std::collections::VecDeque<(u64, u64)> =
            std::collections::VecDeque::new();
        let mut worker_history: std::collections::VecDeque<(u64, u64)> =
            std::collections::VecDeque::new();
        let mut peak_mem_mb: f64 = 0.0;

        loop {
            interval.tick().await;

            let ws_count = crate::core::metrics::PERF_METRICS
                .ws_msg_count
                .swap(0, std::sync::atomic::Ordering::Relaxed);
            let ws_lat_us = crate::core::metrics::PERF_METRICS
                .ws_msg_latency_us
                .swap(0, std::sync::atomic::Ordering::Relaxed);
            let worker_count = crate::core::metrics::PERF_METRICS
                .worker_loop_count
                .swap(0, std::sync::atomic::Ordering::Relaxed);
            let worker_lat_us = crate::core::metrics::PERF_METRICS
                .worker_loop_latency_us
                .swap(0, std::sync::atomic::Ordering::Relaxed);

            let ws_throughput = ws_count as f64; // interval is 1s

            // Frontend 1-minute sliding window
            frontend_history.push_back((ws_lat_us, ws_count));
            if frontend_history.len() > 60 {
                frontend_history.pop_front();
            }
            let (total_lat_1m, total_count_1m) = frontend_history
                .iter()
                .fold((0, 0), |acc, x| (acc.0 + x.0, acc.1 + x.1));
            let avg_lat_1m_ms = if total_count_1m > 0 {
                (total_lat_1m as f64 / total_count_1m as f64) / 1000.0
            } else {
                0.0
            };

            // Worker 1-minute sliding window
            worker_history.push_back((worker_lat_us, worker_count));
            if worker_history.len() > 60 {
                worker_history.pop_front();
            }
            let (total_worker_lat_1m, total_worker_count_1m) = worker_history
                .iter()
                .fold((0, 0), |acc, x| (acc.0 + x.0, acc.1 + x.1));
            let avg_worker_lat_1m_ms = if total_worker_count_1m > 0 {
                (total_worker_lat_1m as f64 / total_worker_count_1m as f64) / 1000.0
            } else {
                0.0
            };

            // Refresh system info for current process (blocking call)
            let (result, new_sys) = tokio::task::spawn_blocking(move || {
                sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
                if let Some(process) = sys.process(pid) {
                    let cpu = process.cpu_usage();
                    let used_mem = process.memory() as f64 / 1048576.0;
                    ((cpu, used_mem), sys)
                } else {
                    ((0.0, 0.0), sys)
                }
            }).await.unwrap_or(((0.0, 0.0), sysinfo::System::new_all()));
            
            let (cpu_usage, used_mem_mb) = result;
            sys = new_sys;

            peak_mem_mb = peak_mem_mb.max(used_mem_mb);

            // Connection metrics
            let connected_users = crate::core::server::pubsub::USER_SOCKETS.len();
            let connected_sessions =
                crate::core::server::ws::ACTIVE_SESSIONS.load(std::sync::atomic::Ordering::SeqCst);

            // DB pool metrics
            let (db_pool_active, db_pool_idle) = {
                let pool = &state.core_state.pool;
                let idle = pool.num_idle() as u32;
                let size = pool.size();
                (size.saturating_sub(idle), idle)
            };

            // Redis status (parse INFO)
            let (redis_status, redis_clients, redis_mem_mb) = {
                let mut con = state.core_state.redis_data.clone();
                match redis::cmd("INFO").query_async::<_, String>(&mut con).await {
                    Ok(info_str) => {
                        let mut clients = 0;
                        let mut mem_bytes: u64 = 0;
                        for line in info_str.lines() {
                            if let Some(val) = line.strip_prefix("connected_clients:") {
                                clients = val.trim().parse().unwrap_or(0);
                            } else if let Some(val) = line.strip_prefix("used_memory:") {
                                mem_bytes = val.trim().parse().unwrap_or(0);
                            }
                        }
                        (
                            "Connected".to_string(),
                            clients,
                            mem_bytes as f64 / 1048576.0,
                        )
                    }
                    Err(_) => ("Error".to_string(), 0, 0.0),
                }
            };

            let metrics = crate::core::server::router::ServerMetrics {
                mode: mode.clone(),
                connected_users,
                connected_sessions,
                redis_status,
                redis_clients,
                redis_mem_mb,
                db_pool_active,
                db_pool_idle,
                cpu_usage,
                used_mem_mb,
                peak_mem_mb,
                frontend_throughput_sec: ws_throughput,
                frontend_latency_1m_ms: avg_lat_1m_ms,
                worker_latency_1m_ms: avg_worker_lat_1m_ms,
            };

            if let Some(handler) = &state.handler_set.monitor_handler {
                handler.handle(metrics, state.user_state.clone()).await;
            }
        }
    });
}
