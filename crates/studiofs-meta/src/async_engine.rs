use std::sync::Arc;

use tokio::task;

use crate::{MetaEngine, MetaError, MetaResult};

/// Async boundary for metadata operations.
///
/// Implementations may forward to a native async backend or bridge a
/// synchronous engine using a blocking task pool.
pub trait AsyncMetaEngine: Send + Sync {
    fn sync_engine(&self) -> Arc<dyn MetaEngine>;
}

/// Adapter that exposes a synchronous [`MetaEngine`] through an async API.
#[derive(Clone)]
pub struct SyncMetaEngineAdapter {
    engine: Arc<dyn MetaEngine>,
}

impl SyncMetaEngineAdapter {
    pub fn new(engine: Arc<dyn MetaEngine>) -> Self {
        Self { engine }
    }
}

impl AsyncMetaEngine for SyncMetaEngineAdapter {
    fn sync_engine(&self) -> Arc<dyn MetaEngine> {
        self.engine.clone()
    }
}

/// Execute a synchronous metadata closure behind an async boundary.
///
/// This keeps the object-safe async boundary (`AsyncMetaEngine`) while allowing
/// callers to pass typed closures without boxing the return value.
pub async fn execute_sync_meta<T: Send + 'static>(
    engine: Arc<dyn AsyncMetaEngine>,
    operation: impl FnOnce(Arc<dyn MetaEngine>) -> MetaResult<T> + Send + 'static,
) -> MetaResult<T> {
    let sync_engine = engine.sync_engine();
    task::spawn_blocking(move || operation(sync_engine))
        .await
        .map_err(|err| MetaError::Storage(format!("metadata task join error: {err}")))?
}
