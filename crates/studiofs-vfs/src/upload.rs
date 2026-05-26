//! UploadScheduler — adaptive object uploader with AIMD concurrency and retries.
//!
//! This mirrors the fetch-side adaptive control, but for PUT requests.

use crate::aimd::{AimdConfig, AimdController};
use crate::runtime::{TimeoutOrCancel, run_with_timeout_or_cancel, sleep_or_cancel};
use crate::vfs::VfsConfig;
use opendal::{Buffer, ErrorKind};
use std::convert::TryFrom;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use studiofs_data::store::{DataError, DataStore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

/// Lock-free counters for upload observability.
pub struct UploadMetrics {
    /// Total number of upload attempts made since process start.
    pub attempts: AtomicU64,
    /// Number of uploads that completed successfully.
    pub uploads_ok: AtomicU64,
    /// Number of upload attempts that failed.
    pub uploads_err: AtomicU64,
    /// Number of times an upload request was retried.
    pub retries: AtomicU64,
    /// Number of uploads that exhausted all allowed attempts.
    pub exhausted: AtomicU64,
    /// Number of upload attempts that failed due to request timeouts.
    pub timeout_err: AtomicU64,
    /// Number of errors categorized as temporary.
    pub temporary_err: AtomicU64,
    /// Number of 5xx HTTP/storage responses observed.
    pub status_5xx_err: AtomicU64,
    /// Number of failures classified as network-related.
    pub network_err: AtomicU64,
    /// Total number of bytes successfully uploaded.
    pub bytes_uploaded: AtomicU64,
}

impl UploadMetrics {
    fn new() -> Self {
        Self {
            attempts: AtomicU64::new(0),
            uploads_ok: AtomicU64::new(0),
            uploads_err: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            exhausted: AtomicU64::new(0),
            timeout_err: AtomicU64::new(0),
            temporary_err: AtomicU64::new(0),
            status_5xx_err: AtomicU64::new(0),
            network_err: AtomicU64::new(0),
            bytes_uploaded: AtomicU64::new(0),
        }
    }
}

impl std::fmt::Display for UploadMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "attempts={} ok={} err={} retries={} exhausted={} timeout={} temporary={} http_5xx={} network={} bytes={}",
            self.attempts.load(Ordering::Relaxed),
            self.uploads_ok.load(Ordering::Relaxed),
            self.uploads_err.load(Ordering::Relaxed),
            self.retries.load(Ordering::Relaxed),
            self.exhausted.load(Ordering::Relaxed),
            self.timeout_err.load(Ordering::Relaxed),
            self.temporary_err.load(Ordering::Relaxed),
            self.status_5xx_err.load(Ordering::Relaxed),
            self.network_err.load(Ordering::Relaxed),
            self.bytes_uploaded.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug, thiserror::Error)]
/// Errors that can occur while uploading data through the scheduler.
pub enum UploadError {
    /// Upload failed after the configured retry budget was exhausted.
    #[error("upload failed after {attempts} attempts: {source}")]
    Exhausted {
        /// Number of attempts performed before giving up.
        attempts: u32,
        /// Last underlying data layer error observed during upload attempts.
        source: DataError,
    },
    /// Upload operation was cancelled.
    #[error("upload cancelled")]
    Cancelled,
}

/// Remote object write port used by the upload scheduler.
pub trait UploadObjectStore: Send + Sync + 'static {
    /// Upload already-serialized STFS object bytes to a remote storage key.
    fn put_by_key<'a>(
        &'a self,
        key: &'a str,
        payload: Buffer,
    ) -> impl Future<Output = Result<(), DataError>> + Send + 'a;
}

impl UploadObjectStore for DataStore {
    #[allow(clippy::manual_async_fn)]
    fn put_by_key<'a>(
        &'a self,
        key: &'a str,
        payload: Buffer,
    ) -> impl Future<Output = Result<(), DataError>> + Send + 'a {
        async move { DataStore::put_by_key(self, key, payload).await }
    }
}

/// Adaptive uploader with AIMD concurrency and per-request retry.
pub struct UploadScheduler<S = DataStore>
where
    S: UploadObjectStore,
{
    aimd: AimdController,
    data_store: Arc<S>,
    max_concurrency_hint: usize,
    max_attempts: u32,
    request_timeout: Duration,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
    adaptive_penalty_ms: AtomicU64,
    /// Runtime metrics for upload requests and outcomes.
    pub metrics: UploadMetrics,
}

impl<S> UploadScheduler<S>
where
    S: UploadObjectStore,
{
    /// Creates a new scheduler bound to the supplied data store and configuration.
    pub fn new(data_store: Arc<S>, aimd_config: AimdConfig, config: &VfsConfig) -> Self {
        let max_attempts = config.upload_max_attempts.max(1);
        let request_timeout_s = config.upload_request_timeout_secs.max(1);
        let retry_base_ms = config.upload_retry_base_ms.max(1);
        let retry_max_ms = config.upload_retry_max_ms.max(retry_base_ms);
        let max_concurrency_hint = if aimd_config.max_window.is_finite() {
            aimd_config.max_window.clamp(1.0, usize::MAX as f64).floor() as usize
        } else {
            1
        };

        debug!(
            initial_window = aimd_config.initial_window,
            max_window = aimd_config.max_window,
            max_attempts,
            request_timeout_s,
            retry_base_ms,
            retry_max_ms,
            "UploadScheduler created"
        );

        Self {
            aimd: AimdController::new(aimd_config),
            data_store,
            max_concurrency_hint,
            max_attempts,
            request_timeout: Duration::from_secs(request_timeout_s),
            retry_base_delay: Duration::from_millis(retry_base_ms),
            retry_max_delay: Duration::from_millis(retry_max_ms),
            adaptive_penalty_ms: AtomicU64::new(0),
            metrics: UploadMetrics::new(),
        }
    }

    /// Sets the current adaptive penalty delay, in milliseconds, for future window calculations.
    pub fn set_adaptive_penalty(&self, delay: Duration) {
        let penalty_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
        self.adaptive_penalty_ms
            .store(penalty_ms, Ordering::Relaxed);
    }

    /// Returns the currently configured adaptive penalty in milliseconds.
    pub fn adaptive_penalty_ms(&self) -> u64 {
        self.adaptive_penalty_ms.load(Ordering::Relaxed)
    }

    /// Upload object bytes to `key` with adaptive retries.
    pub async fn put(
        &self,
        key: &str,
        payload: Buffer,
        cancel_token: &CancellationToken,
    ) -> Result<(), UploadError> {
        let mut last_err: Option<DataError> = None;
        let overall_start = Instant::now();

        for attempt in 0..self.max_attempts {
            if cancel_token.is_cancelled() {
                return Err(UploadError::Cancelled);
            }
            self.metrics.attempts.fetch_add(1, Ordering::Relaxed);
            let permit = self.aimd.acquire().await;
            let req_start = Instant::now();
            let put_result = run_with_timeout_or_cancel(
                cancel_token,
                self.request_timeout,
                self.data_store.put_by_key(key, payload.clone()),
            )
            .await;
            let req_ms = req_start.elapsed().as_millis();

            match put_result {
                TimeoutOrCancel::Completed(Ok(())) => {
                    permit.success();
                    self.metrics.uploads_ok.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .bytes_uploaded
                        .fetch_add(payload.len() as u64, Ordering::Relaxed);
                    debug!(
                        key,
                        bytes = payload.len(),
                        req_ms,
                        total_ms = overall_start.elapsed().as_millis() as u64,
                        attempt,
                        "S3 PUT ok"
                    );
                    return Ok(());
                }
                TimeoutOrCancel::Completed(Err(e)) => {
                    self.metrics.uploads_err.fetch_add(1, Ordering::Relaxed);
                    classify_data_error(&self.metrics, &e);
                    let retryable = is_retryable(&e);

                    if retryable {
                        permit.failure();
                    } else {
                        drop(permit);
                    }

                    if retryable && attempt + 1 < self.max_attempts {
                        let delay = self
                            .retry_base_delay
                            .saturating_mul(2u32.saturating_pow(attempt))
                            .min(self.retry_max_delay);
                        self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            attempt = attempt + 1,
                            max = self.max_attempts,
                            key,
                            req_ms,
                            delay_ms = delay.as_millis() as u64,
                            error = %e,
                            "upload failed, retrying"
                        );
                        if !sleep_or_cancel(cancel_token, delay).await {
                            return Err(UploadError::Cancelled);
                        }
                    } else if retryable {
                        debug!(
                            attempts = self.max_attempts,
                            key,
                            req_ms,
                            total_ms = overall_start.elapsed().as_millis() as u64,
                            error = %e,
                            "upload exhausted all retries"
                        );
                    } else {
                        error!(
                            key,
                            req_ms,
                            total_ms = overall_start.elapsed().as_millis() as u64,
                            error = %e,
                            "upload failed with non-retryable error"
                        );
                    }

                    last_err = Some(e);
                    if !retryable {
                        break;
                    }
                }
                TimeoutOrCancel::TimedOut => {
                    permit.failure();
                    self.metrics.uploads_err.fetch_add(1, Ordering::Relaxed);
                    self.metrics.timeout_err.fetch_add(1, Ordering::Relaxed);

                    let timeout_err = DataError::Storage(format!(
                        "request timeout after {}s",
                        self.request_timeout.as_secs()
                    ));

                    if attempt + 1 < self.max_attempts {
                        let delay = self
                            .retry_base_delay
                            .saturating_mul(2u32.saturating_pow(attempt))
                            .min(self.retry_max_delay);
                        self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            attempt = attempt + 1,
                            max = self.max_attempts,
                            key,
                            timeout_s = self.request_timeout.as_secs(),
                            delay_ms = delay.as_millis() as u64,
                            "upload timed out, retrying"
                        );
                        if !sleep_or_cancel(cancel_token, delay).await {
                            return Err(UploadError::Cancelled);
                        }
                    } else {
                        debug!(
                            attempts = self.max_attempts,
                            key,
                            total_ms = overall_start.elapsed().as_millis() as u64,
                            "upload timed out, all retries exhausted"
                        );
                    }

                    last_err = Some(timeout_err);
                }
                TimeoutOrCancel::Cancelled => {
                    return Err(UploadError::Cancelled);
                }
            }
        }

        self.metrics.exhausted.fetch_add(1, Ordering::Relaxed);
        let source = last_err.unwrap_or_else(|| {
            DataError::Storage("upload exhausted without any captured error".to_string())
        });
        Err(UploadError::Exhausted {
            attempts: self.max_attempts,
            source,
        })
    }

    /// Returns the current AIMD window size as a floating-point control value.
    pub fn window(&self) -> f64 {
        self.aimd.window()
    }

    /// Returns the number of requests currently in flight.
    pub fn in_flight(&self) -> usize {
        self.aimd.in_flight()
    }

    /// Returns the effective concurrency cap for upload scheduling.
    pub fn max_concurrency_hint(&self) -> usize {
        self.max_concurrency_hint.max(1)
    }

    /// Log a periodic summary line for upload pipeline health.
    pub fn log_stats(&self, pending: usize, pending_age_p95_ms: u64, pending_age_max_ms: u64) {
        if upload_log_is_idle(pending, self.aimd.in_flight()) {
            return;
        }
        debug!(
            window = format!("{:.1}", self.aimd.window()),
            inflight = self.aimd.in_flight(),
            pending,
            pending_age_p95_ms,
            pending_age_max_ms,
            "upload: {}",
            self.metrics,
        );
    }
}

/// Indicates whether upload logging is considered idle.
///
/// The scheduler treats pending and in-flight work as idle only when both are zero.
pub(crate) fn upload_log_is_idle(pending: usize, inflight: usize) -> bool {
    pending == 0 && inflight == 0
}

fn is_retryable(err: &DataError) -> bool {
    match err {
        DataError::OpenDal(oe) => {
            oe.kind() == ErrorKind::RateLimited
                || oe.kind() == ErrorKind::Unexpected
                || oe.is_temporary()
        }
        DataError::Storage(_) => true,
        DataError::InvalidCryptoConfig(_) | DataError::Crypto(_) => false,
    }
}

fn classify_data_error(metrics: &UploadMetrics, err: &DataError) {
    if let DataError::OpenDal(oe) = err {
        if oe.is_temporary() {
            metrics.temporary_err.fetch_add(1, Ordering::Relaxed);
        }
        let kind = oe.kind();
        if kind == ErrorKind::Unexpected {
            let msg = oe.to_string();
            if msg.contains("status: 5")
                || msg.contains(" 500")
                || msg.contains(" 502")
                || msg.contains(" 503")
                || msg.contains(" 504")
            {
                metrics.status_5xx_err.fetch_add(1, Ordering::Relaxed);
            }
            if msg.contains("send http request")
                || msg.contains("error sending request")
                || msg.contains("connection")
                || msg.contains("timed out")
            {
                metrics.network_err.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{UploadObjectStore, UploadScheduler, is_retryable, upload_log_is_idle};
    use crate::AimdConfig;
    use crate::vfs::VfsConfig;
    use bytes::Bytes;
    use opendal::{Buffer, Operator, services::Memory};
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use studiofs_data::envelope::SFEC_MAGIC;
    use studiofs_data::object::{MAGIC as STFS_MAGIC, create_object_buffer};
    use studiofs_data::{
        crypto::CryptoError,
        store::{DataError, DataStore, DataStoreConfig, DataStoreCryptoConfig},
    };
    use studiofs_meta::{EncryptionConfig, EncryptionContractError};
    use tokio_util::sync::CancellationToken;

    fn encrypted_test_config() -> DataStoreConfig {
        DataStoreConfig {
            endpoint: "memory://".into(),
            bucket: "unit-test".into(),
            access_key: "access".into(),
            secret_key: "secret".into(),
            region: "us-east-1".into(),
            volume_name: "unit-volume".into(),
            volume_uuid: "scheduler-upload-test-volume".into(),
            crypto: DataStoreCryptoConfig::from(EncryptionConfig::new([0x51; 32])),
        }
    }

    fn upload_test_config() -> VfsConfig {
        VfsConfig {
            upload_max_attempts: 2,
            upload_retry_base_ms: 1,
            upload_retry_max_ms: 1,
            upload_request_timeout_secs: 5,
            ..VfsConfig::default()
        }
    }

    #[test]
    fn idle_upload_info_log_is_compact() {
        assert!(upload_log_is_idle(0, 0));
        assert!(!upload_log_is_idle(1, 0));
        assert!(!upload_log_is_idle(0, 1));
    }

    #[test]
    fn crypto_failures_are_not_upload_retryable() {
        assert!(!is_retryable(&DataError::InvalidCryptoConfig(
            EncryptionContractError::Disabled
        )));
        assert!(!is_retryable(&DataError::Crypto(
            CryptoError::AuthenticationFailed
        )));
    }

    #[tokio::test]
    async fn scheduler_writes_sfec_remote_object_from_plaintext_stfs_payload() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store = Arc::new(
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap(),
        );
        let scheduler = UploadScheduler::new(
            store.clone(),
            AimdConfig::upload_default(),
            &upload_test_config(),
        );
        let plaintext_object =
            create_object_buffer(Bytes::from_static(b"upload plaintext")).unwrap();
        assert!(plaintext_object.to_bytes().starts_with(STFS_MAGIC));

        let key = "unit-volume/objects/aa/bb/scheduler-upload";
        scheduler
            .put(key, plaintext_object.clone(), &CancellationToken::new())
            .await
            .unwrap();

        let remote_bytes = raw.read(key).await.unwrap().to_bytes();
        assert!(remote_bytes.starts_with(SFEC_MAGIC));
        assert!(!remote_bytes.starts_with(STFS_MAGIC));
        assert!(remote_bytes.len() > plaintext_object.len());
        assert_eq!(
            store.get_by_key(key).await.unwrap().to_bytes(),
            plaintext_object.to_bytes()
        );
    }

    #[derive(Default)]
    struct RetryOnceUploadStore {
        attempts: AtomicU64,
        payloads: Mutex<Vec<(String, Bytes)>>,
    }

    impl UploadObjectStore for RetryOnceUploadStore {
        #[allow(clippy::manual_async_fn)]
        fn put_by_key<'a>(
            &'a self,
            key: &'a str,
            data: Buffer,
        ) -> impl Future<Output = Result<(), DataError>> + Send + 'a {
            async move {
                self.payloads
                    .lock()
                    .push((key.to_string(), data.to_bytes()));
                let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
                if attempt == 0 {
                    return Err(DataError::Storage("temporary encrypted put failure".into()));
                }
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn scheduler_retries_after_retryable_encrypted_put_failure() {
        let store = Arc::new(RetryOnceUploadStore::default());
        let scheduler = UploadScheduler::new(
            store.clone(),
            AimdConfig::upload_default(),
            &upload_test_config(),
        );
        let plaintext_object =
            create_object_buffer(Bytes::from_static(b"retry plaintext")).unwrap();

        scheduler
            .put(
                "unit-volume/objects/aa/bb/retry-upload",
                plaintext_object.clone(),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(store.attempts.load(Ordering::Relaxed), 2);
        let payloads = store.payloads.lock();
        assert_eq!(payloads.len(), 2);
        assert!(
            payloads
                .iter()
                .all(|(_, bytes)| bytes.starts_with(STFS_MAGIC))
        );
        assert!(
            payloads
                .iter()
                .all(|(_, bytes)| bytes == &plaintext_object.to_bytes())
        );
        assert_eq!(scheduler.metrics.retries.load(Ordering::Relaxed), 1);
        assert_eq!(scheduler.metrics.uploads_ok.load(Ordering::Relaxed), 1);
    }
}
