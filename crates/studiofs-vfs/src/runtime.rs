use std::sync::Arc;
use std::time::Duration;
use studiofs_meta::{AsyncMetaEngine, MetaEngine, MetaResult, execute_sync_meta};
use tokio::task;
use tokio_util::sync::CancellationToken;

use super::{VfsError, VfsResult};

pub enum TimeoutOrCancel<T> {
    Completed(T),
    TimedOut,
    Cancelled,
}

/// Single execution model for local cache I/O.
///
/// Local cache operations run inline on the active runtime worker after the
/// bridge boundary (WinFSP `spawn_wait` / FUSE `block_on`) has entered Tokio.
/// This avoids per-operation task handoffs for hot cache calls, while
/// heavyweight blocking calls (for example `sync_data`) stay isolated on the
/// blocking pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalCacheIoExecutionModel {
    InlineRuntimeWorkerWithBlockingIsolation,
}

impl LocalCacheIoExecutionModel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InlineRuntimeWorkerWithBlockingIsolation => {
                "inline-runtime-worker-with-blocking-isolation"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalCacheIoTiming {
    pub handoff_ns: u64,
    pub backend_io_ns: u64,
}

pub const fn local_cache_io_execution_model() -> LocalCacheIoExecutionModel {
    LocalCacheIoExecutionModel::InlineRuntimeWorkerWithBlockingIsolation
}

pub fn run_local_cache_io<T>(operation: impl FnOnce() -> T) -> (T, LocalCacheIoTiming) {
    let io_started = std::time::Instant::now();
    let result = operation();
    let backend_io_ns = io_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    (
        result,
        LocalCacheIoTiming {
            handoff_ns: 0,
            backend_io_ns,
        },
    )
}

/// Run a potentially blocking closure on Tokio's dedicated blocking pool and
/// propagate `JoinError` as `VfsError::Join`.
pub async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> VfsResult<T> + Send + 'static,
) -> VfsResult<T> {
    task::spawn_blocking(operation)
        .await
        .map_err(VfsError::Join)?
}

/// Run local cache I/O on the blocking pool when the operation is
/// unavoidably blocking (for example fsync/maintenance), while still exposing
/// handoff-vs-IO timing split for observability.
pub async fn run_local_cache_io_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> VfsResult<T> + Send + 'static,
) -> VfsResult<(T, LocalCacheIoTiming)> {
    let submitted_at = std::time::Instant::now();
    run_blocking(move || {
        let io_started_at = std::time::Instant::now();
        let handoff_ns = io_started_at
            .checked_duration_since(submitted_at)
            .unwrap_or_default()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let result = operation();
        let backend_io_ns = io_started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        result.map(|value| {
            (
                value,
                LocalCacheIoTiming {
                    handoff_ns,
                    backend_io_ns,
                },
            )
        })
    })
    .await
}

/// Run a metadata-engine closure on the blocking pool.
pub async fn run_meta<T: Send + 'static>(
    engine: Arc<dyn AsyncMetaEngine>,
    operation: impl FnOnce(Arc<dyn MetaEngine>) -> MetaResult<T> + Send + 'static,
) -> VfsResult<T> {
    // Keep runtime/task failures distinct from metadata-engine domain failures.
    // A JoinError generally indicates panic/cancellation at the execution layer.
    execute_sync_meta(engine, operation)
        .await
        .map_err(VfsError::Join)?
        .map_err(VfsError::Meta)
}

/// Race an async operation against timeout and cancellation.
pub async fn run_with_timeout_or_cancel<T>(
    cancel_token: &CancellationToken,
    timeout: Duration,
    operation: impl std::future::Future<Output = T>,
) -> TimeoutOrCancel<T> {
    tokio::select! {
        _ = cancel_token.cancelled() => TimeoutOrCancel::Cancelled,
        result = tokio::time::timeout(timeout, operation) => {
            match result {
                Ok(value) => TimeoutOrCancel::Completed(value),
                Err(_) => TimeoutOrCancel::TimedOut,
            }
        }
    }
}

/// Sleep unless cancellation is requested first.
pub async fn sleep_or_cancel(cancel_token: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        _ = cancel_token.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{local_cache_io_execution_model, run_local_cache_io};

    #[test]
    fn local_cache_io_execution_model_is_inline_runtime_worker() {
        assert_eq!(
            local_cache_io_execution_model().as_str(),
            "inline-runtime-worker-with-blocking-isolation"
        );
    }

    #[test]
    fn run_local_cache_io_reports_zero_handoff() {
        let (value, timing) = run_local_cache_io(|| 42usize);
        assert_eq!(value, 42);
        assert_eq!(timing.handoff_ns, 0);
    }
}
