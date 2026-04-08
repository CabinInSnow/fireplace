use crate::core::server::ws::FirePlaceState;

pub async fn run_worker_loop<S: Clone + Send + Sync + 'static>(state: FirePlaceState<S>) {
    let worker_handlers = state.handler_set.worker_handlers.clone();
    
    if worker_handlers.is_empty() {
        tracing::warn!("No worker handlers registered. Worker loop will exit.");
        return;
    }

    let mut handles = Vec::new();

    for entry in worker_handlers {
        let state_clone = state.clone();
        let name = entry.name.clone();
        let tick_ms = entry.tick_ms;
        let handler = entry.handler.clone();

        tracing::info!("Worker [{}] starting with tick {} ms.", name, tick_ms);

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(tick_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                let start = std::time::Instant::now();
                handler.handle(state_clone.core_state.clone(), state_clone.user_state.clone()).await;
                let elapsed = start.elapsed().as_micros() as u64;

                // Metrics are shared across all workers for simplicity, or we could add per-worker metrics
                crate::core::metrics::PERF_METRICS.worker_loop_latency_us.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);
                crate::core::metrics::PERF_METRICS.worker_loop_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                crate::core::metrics::PERF_METRICS.last_worker_loop_us.store(elapsed, std::sync::atomic::Ordering::Relaxed);
            }
        });

        handles.push(handle);
    }

    // Wait for all workers (though they loop forever)
    for h in handles {
        let _ = h.await;
    }
}
