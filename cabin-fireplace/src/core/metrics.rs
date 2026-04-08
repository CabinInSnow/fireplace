use std::sync::atomic::AtomicU64;

pub struct PerformanceMetrics {
    pub ws_msg_count: AtomicU64,
    pub ws_msg_latency_us: AtomicU64,
    pub worker_loop_count: AtomicU64,
    pub worker_loop_latency_us: AtomicU64,
    pub last_worker_loop_us: AtomicU64,
}

lazy_static::lazy_static! {
    pub static ref PERF_METRICS: PerformanceMetrics = PerformanceMetrics {
        ws_msg_count: AtomicU64::new(0),
        ws_msg_latency_us: AtomicU64::new(0),
        worker_loop_count: AtomicU64::new(0),
        worker_loop_latency_us: AtomicU64::new(0),
        last_worker_loop_us: AtomicU64::new(0),
    };
}
