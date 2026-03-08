use std::sync::Arc;
use studiofs_meta::{MetaEngine, MetaResult};
use tokio::task;

use super::{VfsError, VfsResult};

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
    engine: Arc<dyn MetaEngine>,
    operation: impl FnOnce(Arc<dyn MetaEngine>) -> MetaResult<T> + Send + 'static,
) -> VfsResult<T> {
    task::spawn_blocking(move || operation(engine).map_err(VfsError::Meta))
        .await
        .map_err(VfsError::Join)?
}
