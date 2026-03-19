use std::sync::OnceLock;
use std::time::Instant;

use tokio::task::JoinHandle;

use crate::metrics::{BlockingTaskStats, BlockingTaskStatsSnapshot};

fn blocking_task_stats() -> &'static BlockingTaskStats {
    static STATS: OnceLock<BlockingTaskStats> = OnceLock::new();
    STATS.get_or_init(BlockingTaskStats::default)
}

pub fn spawn_blocking_profiled<F, T>(task_kind: &'static str, operation: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let queued_at = Instant::now();
    blocking_task_stats().record_submit();
    tokio::task::spawn_blocking(move || {
        let queue_wait = queued_at.elapsed();
        let queue_wait_ns = queue_wait.as_nanos().min(u64::MAX as u128) as u64;
        let _guard = blocking_task_stats().record_start(queue_wait);
        tracing::trace!(
            target: "studiofs.cache.profile",
            task_kind,
            queue_wait_ns,
            "cache blocking task started"
        );
        operation()
    })
}

pub fn blocking_task_stats_snapshot() -> BlockingTaskStatsSnapshot {
    blocking_task_stats().snapshot()
}

pub fn reset_blocking_task_stats() {
    blocking_task_stats().reset();
}
