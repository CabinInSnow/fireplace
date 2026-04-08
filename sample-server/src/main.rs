pub mod entity;
pub mod handlers;
pub mod state;

use cabin_fireplace::{FirePlace, FirePlaceConfig, HandlerSet};
use state::AppState;

#[tokio::main]
async fn main() {
    cabin_fireplace::utils::init_logger(tracing::Level::INFO);
    tracing::info!("sample-server start");

    let config = FirePlaceConfig {
        port: 80,
        redis_url: "redis://127.0.0.1/".to_string(),
        db_url: "postgres://user:pass@localhost/db".to_string(), // Development DB
        websocket_enable: true,
        websocket_endpoint: Some("/ws".to_string()),
        rate_limit_per_sec: 10,
        db_max_connections: 10,
        worker_tick_ms: 1000,
        monitor_tick_ms: 1000,
    };

    let user_state = AppState {
        dummy_db_flag: true,
    };

    let game_server = match FirePlace::new(config, user_state).await {
        Ok(server) => server,
        Err(e) => {
            tracing::error!("Failed to create FirePlace Server: {}", e);
            return;
        }
    };

    let handler_set = HandlerSet::new()
        .register_auth_handler(handlers::auth::auth_handler)
        .register_init_handler(handlers::init::init_handler)
        .register_action_handlers("give_item", handlers::action_item::give_item)
        .register_worker_handler(|_state_bundle, _app_state| async {
            // Dummy work
            // tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        })
        .register_monitor_handler(
            |metrics: cabin_fireplace::core::server::router::ServerMetrics, _app_state| async move {
                tracing::info!(
                    "\n\n======================================================\n\
                 [MONITOR] Server Health & Metrics\n\
                 ------------------------------------------------------\n\
                 • Mode:      {}\n\
                 • Sessions:  {} (Authenticated Users: {})\n\
                 • Redis:     {} (Clients: {}, Mem: {:.2} MB)\n\
                 • DB Pool:   Active: {}, Idle: {}\n\
                 • Process:   CPU: {:.1}%\n\
                 • Memory:    Resident: {:.2} MB, Peak: {:.2} MB\n\
                 • Frontend Throughput: {:.1} req/s\n\
                 • Frontend Latency:   WS Msg: {:.2}ms (avg last 1m)\n\
                 • Worker Latency:    Loop: {:.2}ms (avg last 1m)\n\
                 ======================================================\n",
                    metrics.mode,
                    metrics.connected_sessions,
                    metrics.connected_users,
                    metrics.redis_status,
                    metrics.redis_clients,
                    metrics.redis_mem_mb,
                    metrics.db_pool_active,
                    metrics.db_pool_idle,
                    metrics.cpu_usage,
                    metrics.used_mem_mb,
                    metrics.peak_mem_mb,
                    metrics.frontend_throughput_sec,
                    metrics.frontend_latency_1m_ms,
                    metrics.worker_latency_1m_ms
                );
            },
        )
        .register_http_handlers(
            "/api/hello",
            axum::routing::get(handlers::http::hello_world),
        );

    let run_mode = std::env::var("RUN_MODE").unwrap_or_else(|_| "ALL".to_string());

    match run_mode.as_str() {
        "FRONTEND" => {
            tracing::info!("Starting in FRONTEND mode...");
            let server_future = game_server.register_handlers(handler_set).run_frontend();
            if let Err(e) = server_future.await {
                tracing::error!("Server error: {}", e);
            }
        }
        "WORKER" => {
            tracing::info!("Starting in WORKER mode (Scheduler only)...");
            game_server
                .register_handlers(handler_set)
                .run_worker()
                .await;
        }
        _ => {
            tracing::info!("Starting in ALL mode (Frontend + Worker)...");

            // Run the all
            let server_future = game_server.register_handlers(handler_set).run_all();
            if let Err(e) = server_future.await {
                tracing::error!("Server error: {}", e);
            }
        }
    }

    tracing::info!("sample-server exit");
}
