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

/// Run a potentially blocking closure on Tokio's dedicated blocking pool and
/// propagate `JoinError` as `VfsError::Join`.
pub async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> VfsResult<T> + Send + 'static,
) -> VfsResult<T> {
    task::spawn_blocking(operation)
        .await
        .map_err(VfsError::Join)?
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
