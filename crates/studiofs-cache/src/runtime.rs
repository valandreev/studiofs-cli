use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use dashmap::DashMap;
use tokio::task::JoinHandle;

use crate::metrics::{
    BlockingTaskKindStatsSnapshot, BlockingTaskMetricsSnapshot, BlockingTaskStats,
};

struct BlockingTaskRegistry {
    total: BlockingTaskStats,
    by_kind: DashMap<&'static str, Arc<BlockingTaskStats>>,
}

impl Default for BlockingTaskRegistry {
    fn default() -> Self {
        Self {
            total: BlockingTaskStats::default(),
            by_kind: DashMap::new(),
        }
    }
}

fn blocking_task_registry() -> &'static BlockingTaskRegistry {
    static REGISTRY: OnceLock<BlockingTaskRegistry> = OnceLock::new();
    REGISTRY.get_or_init(BlockingTaskRegistry::default)
}

pub fn spawn_blocking_profiled<F, T>(task_kind: &'static str, operation: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let queued_at = Instant::now();
    blocking_task_registry().total.record_submit();
    let kind_stats = blocking_task_registry()
        .by_kind
        .entry(task_kind)
        .or_insert_with(|| Arc::new(BlockingTaskStats::default()))
        .clone();
    kind_stats.record_submit();
    tokio::task::spawn_blocking(move || {
        let queue_wait = queued_at.elapsed();
        let queue_wait_ns = queue_wait.as_nanos().min(u64::MAX as u128) as u64;
        let _guard_total = blocking_task_registry().total.record_start(queue_wait);
        let kind_stats = blocking_task_registry()
            .by_kind
            .entry(task_kind)
            .or_insert_with(|| Arc::new(BlockingTaskStats::default()))
            .clone();
        let _guard_kind = kind_stats.record_start(queue_wait);
        if tracing::enabled!(target: "studiofs.cache.profile", tracing::Level::TRACE) {
            tracing::trace!(
                target: "studiofs.cache.profile",
                task_kind,
                queue_wait_ns,
                "cache blocking task started"
            );
        }
        operation()
    })
}

pub fn blocking_task_stats_snapshot() -> BlockingTaskMetricsSnapshot {
    let mut by_kind: Vec<_> = blocking_task_registry()
        .by_kind
        .iter()
        .map(|entry| BlockingTaskKindStatsSnapshot {
            task_kind: (*entry.key()).to_string(),
            stats: entry.value().as_ref().snapshot(),
        })
        .collect();
    by_kind.sort_by(|a, b| match b.stats.submitted.cmp(&a.stats.submitted) {
        Ordering::Equal => a.task_kind.cmp(&b.task_kind),
        other => other,
    });

    BlockingTaskMetricsSnapshot {
        total: blocking_task_registry().total.snapshot(),
        by_kind,
    }
}

pub fn reset_blocking_task_stats() {
    blocking_task_registry().total.reset();
    for entry in blocking_task_registry().by_kind.iter() {
        entry.value().as_ref().reset();
    }
}
