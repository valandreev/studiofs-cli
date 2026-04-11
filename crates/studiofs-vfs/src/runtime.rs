use std::sync::Arc;
use std::time::Duration;
use studiofs_meta::{AsyncMetaEngine, MetaEngine, MetaResult, execute_sync_meta};
use tokio::task;
use tokio_util::sync::CancellationToken;

use super::{VfsError, VfsResult};

#[derive(Clone)]
pub struct MetaExecutor {
    backend: Arc<dyn AsyncMetaEngine>,
}

impl MetaExecutor {
    pub fn new(backend: Arc<dyn AsyncMetaEngine>) -> Self {
        Self { backend }
    }

    pub fn sync_view(&self) -> Arc<dyn MetaEngine> {
        self.backend.sync_engine()
    }

    pub fn run_sync<T>(
        &self,
        operation: impl FnOnce(Arc<dyn MetaEngine>) -> MetaResult<T>,
    ) -> VfsResult<T> {
        operation(self.sync_view()).map_err(VfsError::Meta)
    }

    pub async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Arc<dyn MetaEngine>) -> MetaResult<T> + Send + 'static,
    ) -> VfsResult<T> {
        execute_sync_meta(self.backend.clone(), operation)
            .await
            .map_err(VfsError::Join)?
            .map_err(VfsError::Meta)
    }
}

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

pub const fn cache_io_execution_model() -> LocalCacheIoExecutionModel {
    LocalCacheIoExecutionModel::InlineRuntimeWorkerWithBlockingIsolation
}

pub fn run_local_cache_io<T>(operation: impl FnOnce() -> T) -> (T, LocalCacheIoTiming) {
    let io_started = std::time::Instant::now();
    let result = operation();
    let backend_io_ns = duration_to_nanos_u64(io_started.elapsed());
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
            .into_nanos_saturating_u64();
        let result = operation();
        let backend_io_ns = io_started_at.elapsed().into_nanos_saturating_u64();
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

trait DurationNanosExt {
    fn into_nanos_saturating_u64(self) -> u64;
}

impl DurationNanosExt for std::time::Duration {
    fn into_nanos_saturating_u64(self) -> u64 {
        u64::try_from(self.as_nanos()).unwrap_or(u64::MAX)
    }
}

fn duration_to_nanos_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Run a metadata-engine closure on the blocking pool.
pub async fn run_meta<T: Send + 'static>(
    engine: Arc<dyn AsyncMetaEngine>,
    operation: impl FnOnce(Arc<dyn MetaEngine>) -> MetaResult<T> + Send + 'static,
) -> VfsResult<T> {
    MetaExecutor::new(engine).run(operation).await
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
    use super::{MetaExecutor, cache_io_execution_model, run_local_cache_io};
    use std::sync::Arc;
    use studiofs_meta::{FormatInfo, MetaError, MetaFormat, RedbMetaEngine, SyncMetaEngineAdapter};

    #[test]
    fn cache_io_execution_model_is_inline_runtime_worker() {
        assert_eq!(
            cache_io_execution_model().as_str(),
            "inline-runtime-worker-with-blocking-isolation"
        );
    }

    #[test]
    fn run_local_cache_io_reports_zero_handoff() {
        let (value, timing) = run_local_cache_io(|| 42usize);
        assert_eq!(value, 42);
        assert_eq!(timing.handoff_ns, 0);
    }

    #[test]
    fn meta_executor_run_sync_reads_from_sync_engine() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("meta.redb");
        let engine = Arc::new(RedbMetaEngine::open(&path).expect("open redb"));
        let info = FormatInfo::new(
            "runtime-meta".to_string(),
            "runtime-meta-uuid".to_string(),
            "http://127.0.0.1:9000".to_string(),
            "runtime-meta-bucket".to_string(),
            "runtime-access-key".to_string(),
            "runtime-secret-key".to_string(),
            String::new(),
            16,
        );
        engine.set_format_info(&info).expect("set format info");

        let executor = MetaExecutor::new(Arc::new(SyncMetaEngineAdapter::new(engine)));
        let observed = executor
            .run_sync(|meta| meta.get_format_info().map(|format| format.name))
            .expect("sync metadata call");

        assert_eq!(observed, "runtime-meta");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn meta_executor_run_maps_meta_errors() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("meta.redb");
        let engine = Arc::new(RedbMetaEngine::open(&path).expect("open redb"));
        let info = FormatInfo::new(
            "runtime-meta".to_string(),
            "runtime-meta-uuid".to_string(),
            "http://127.0.0.1:9000".to_string(),
            "runtime-meta-bucket".to_string(),
            "runtime-access-key".to_string(),
            "runtime-secret-key".to_string(),
            String::new(),
            16,
        );
        engine.set_format_info(&info).expect("set format info");

        let executor = MetaExecutor::new(Arc::new(SyncMetaEngineAdapter::new(engine)));
        let err = executor
            .run::<()>(|_| Err(MetaError::Storage("boom".to_string())))
            .await
            .expect_err("metadata error must bubble");

        match err {
            super::super::VfsError::Meta(MetaError::Storage(message)) => {
                assert_eq!(message, "boom")
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
