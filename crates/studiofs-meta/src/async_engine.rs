use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::task;

use crate::{MetaEngine, MetaError, MetaResult};

pub type ErasedMetaValue = Box<dyn Any + Send>;
pub type ErasedMetaOperation =
    Box<dyn FnOnce(Arc<dyn MetaEngine>) -> MetaResult<ErasedMetaValue> + Send + 'static>;
pub type ErasedMetaFuture =
    Pin<Box<dyn Future<Output = Result<MetaResult<ErasedMetaValue>, task::JoinError>> + Send>>;

/// Async boundary for metadata operations.
///
/// Implementations may forward to a native async backend or bridge a
/// synchronous engine using a blocking task pool.
pub trait AsyncMetaEngine: Send + Sync {
    fn sync_engine(&self) -> Arc<dyn MetaEngine>;

    /// Execute a metadata operation across the async boundary.
    ///
    /// Implementations may use `spawn_blocking` (sync backends) or run natively
    /// async (remote backends) as long as they preserve operation semantics.
    fn execute_sync_boxed(&self, operation: ErasedMetaOperation) -> ErasedMetaFuture;
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

    fn execute_sync_boxed(&self, operation: ErasedMetaOperation) -> ErasedMetaFuture {
        let sync_engine = self.engine.clone();
        Box::pin(task::spawn_blocking(move || operation(sync_engine)))
    }
}

/// Execute a synchronous metadata closure behind an async boundary.
///
/// This keeps the object-safe async boundary (`AsyncMetaEngine`) while allowing
/// callers to pass typed closures without boxing the return value.
pub async fn execute_sync_meta<T: Send + 'static>(
    engine: Arc<dyn AsyncMetaEngine>,
    operation: impl FnOnce(Arc<dyn MetaEngine>) -> MetaResult<T> + Send + 'static,
) -> Result<MetaResult<T>, task::JoinError> {
    let erased = engine
        .execute_sync_boxed(Box::new(move |sync_engine| {
            operation(sync_engine).map(|value| Box::new(value) as ErasedMetaValue)
        }))
        .await?;
    Ok(erased.and_then(|boxed| {
        boxed
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| MetaError::Storage("execute_sync_meta type mismatch".to_string()))
    }))
}
