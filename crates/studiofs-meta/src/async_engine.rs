use std::sync::Arc;

use tokio::task;

use crate::MetaResult;

/// Adapter that exposes a synchronous metadata principal through an async API.
pub struct SyncMetaEngineAdapter<M: ?Sized> {
    inner: Arc<M>,
}

impl<M: ?Sized> Clone for SyncMetaEngineAdapter<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<M: ?Sized> SyncMetaEngineAdapter<M> {
    /// Wrap a synchronous metadata capability in an async-friendly adapter.
    pub fn new(engine: Arc<M>) -> Self {
        Self { inner: engine }
    }

    /// Return the underlying synchronous capability principal.
    pub fn sync_engine(&self) -> Arc<M> {
        self.inner.clone()
    }
}

/// Execute a synchronous metadata closure behind an async boundary.
pub async fn execute_sync_meta<M: ?Sized + Send + Sync + 'static, T: Send + 'static>(
    engine: SyncMetaEngineAdapter<M>,
    operation: impl FnOnce(Arc<M>) -> MetaResult<T> + Send + 'static,
) -> Result<MetaResult<T>, task::JoinError> {
    let sync_engine = engine.sync_engine();
    task::spawn_blocking(move || operation(sync_engine)).await
}

#[cfg(test)]
mod tests {
    use super::{SyncMetaEngineAdapter, execute_sync_meta};
    use crate::{FormatInfo, FormatInfoDataStore, MetaError, MetaFormat, RedbMetaEngine};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn seed_format_engine() -> (TempDir, Arc<RedbMetaEngine>) {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("meta.redb");
        let engine = Arc::new(RedbMetaEngine::open(&path).expect("open redb"));
        let info = FormatInfo::new(
            "async-meta".to_string(),
            "async-meta-uuid".to_string(),
            FormatInfoDataStore {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "async-meta-bucket".to_string(),
                access_key: "async-meta-access".to_string(),
                secret_key: "async-meta-secret".to_string(),
                region: String::new(),
            },
            16,
        );
        engine.set_format_info(&info).expect("set format info");
        (temp, engine)
    }

    #[test]
    fn sync_meta_engine_adapter_accepts_narrow_capability_principal() {
        let (_temp, engine) = seed_format_engine();
        let narrow: Arc<dyn MetaFormat> = engine;
        let adapter = SyncMetaEngineAdapter::new(narrow.clone());
        let observed = adapter
            .sync_engine()
            .get_format_info()
            .expect("get format info");
        assert_eq!(observed.name, "async-meta");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execute_sync_meta_supports_typed_narrow_capability_calls() {
        let (_temp, engine) = seed_format_engine();
        let narrow: Arc<dyn MetaFormat> = engine;
        let adapter = SyncMetaEngineAdapter::new(narrow);
        let observed = execute_sync_meta(adapter, |meta| {
            meta.get_format_info().map(|format| format.uuid)
        })
        .await
        .expect("join")
        .expect("metadata result");
        assert_eq!(observed, "async-meta-uuid");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execute_sync_meta_preserves_meta_errors_for_narrow_principals() {
        let (_temp, engine) = seed_format_engine();
        let narrow: Arc<dyn MetaFormat> = engine;
        let adapter = SyncMetaEngineAdapter::new(narrow);
        let err = execute_sync_meta(adapter, |_| {
            Err::<(), _>(MetaError::Storage("boom".to_string()))
        })
        .await
        .expect("join")
        .expect_err("metadata error");
        match err {
            MetaError::Storage(message) => assert_eq!(message, "boom"),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
