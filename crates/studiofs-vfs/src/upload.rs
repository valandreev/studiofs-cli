//! UploadScheduler — adaptive object uploader with AIMD concurrency and retries.
//!
//! This mirrors the fetch-side adaptive control, but for PUT requests.

use crate::aimd::{AimdConfig, AimdController};
use bytes::Bytes;
use opendal::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use studiofs_data::store::{DataError, DataStore};
use tracing::{debug, error, info, warn};

/// Lock-free counters for upload observability.
pub struct UploadMetrics {
    pub uploads_ok: AtomicU64,
    pub uploads_err: AtomicU64,
    pub retries: AtomicU64,
    pub bytes_uploaded: AtomicU64,
}

impl UploadMetrics {
    fn new() -> Self {
        Self {
            uploads_ok: AtomicU64::new(0),
            uploads_err: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            bytes_uploaded: AtomicU64::new(0),
        }
    }
}

impl std::fmt::Display for UploadMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ok={} err={} retries={} bytes={}",
            self.uploads_ok.load(Ordering::Relaxed),
            self.uploads_err.load(Ordering::Relaxed),
            self.retries.load(Ordering::Relaxed),
            self.bytes_uploaded.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("upload failed after {attempts} attempts: {source}")]
    Exhausted { attempts: u32, source: DataError },
}

/// Adaptive uploader with AIMD concurrency and per-request retry.
pub struct UploadScheduler {
    aimd: AimdController,
    data_store: Arc<DataStore>,
    max_attempts: u32,
    request_timeout: Duration,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
    pub metrics: UploadMetrics,
}

impl UploadScheduler {
    pub fn new(data_store: Arc<DataStore>, aimd_config: AimdConfig) -> Self {
        fn env_u32(key: &str, default: u32) -> u32 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default)
        }

        fn env_u64(key: &str, default: u64) -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default)
        }

        let max_attempts = env_u32("STUDIOFS_UPLOAD_MAX_ATTEMPTS", 10);
        let request_timeout_s = env_u64("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", 120);
        let retry_base_ms = env_u64("STUDIOFS_UPLOAD_RETRY_BASE_MS", 200);
        let retry_max_ms = env_u64("STUDIOFS_UPLOAD_RETRY_MAX_MS", 5_000).max(retry_base_ms);

        info!(
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
            max_attempts,
            request_timeout: Duration::from_secs(request_timeout_s),
            retry_base_delay: Duration::from_millis(retry_base_ms),
            retry_max_delay: Duration::from_millis(retry_max_ms),
            metrics: UploadMetrics::new(),
        }
    }

    /// Upload object bytes to `key` with adaptive concurrency + retries.
    pub async fn put(&self, key: &str, payload: Bytes) -> Result<(), UploadError> {
        let mut last_err: Option<DataError> = None;
        let overall_start = Instant::now();

        for attempt in 0..self.max_attempts {
            let permit = self.aimd.acquire().await;
            let req_start = Instant::now();
            let put_result = tokio::time::timeout(
                self.request_timeout,
                self.data_store.put_by_key(key, payload.clone()),
            )
            .await;
            let req_ms = req_start.elapsed().as_millis();

            match put_result {
                Ok(Ok(())) => {
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
                Ok(Err(e)) => {
                    self.metrics.uploads_err.fetch_add(1, Ordering::Relaxed);
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
                        warn!(
                            attempt = attempt + 1,
                            max = self.max_attempts,
                            key,
                            req_ms,
                            delay_ms = delay.as_millis() as u64,
                            error = %e,
                            "upload failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    } else if retryable {
                        error!(
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
                Err(_) => {
                    permit.failure();
                    self.metrics.uploads_err.fetch_add(1, Ordering::Relaxed);

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
                        warn!(
                            attempt = attempt + 1,
                            max = self.max_attempts,
                            key,
                            timeout_s = self.request_timeout.as_secs(),
                            delay_ms = delay.as_millis() as u64,
                            "upload timed out, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    } else {
                        error!(
                            attempts = self.max_attempts,
                            key,
                            total_ms = overall_start.elapsed().as_millis() as u64,
                            "upload timed out, all retries exhausted"
                        );
                    }

                    last_err = Some(timeout_err);
                }
            }
        }

        Err(UploadError::Exhausted {
            attempts: self.max_attempts,
            source: last_err.expect("at least one error"),
        })
    }

    pub fn window(&self) -> f64 {
        self.aimd.window()
    }

    pub fn in_flight(&self) -> usize {
        self.aimd.in_flight()
    }
}

fn is_retryable(err: &DataError) -> bool {
    match err {
        DataError::OpenDal(oe) => {
            oe.kind() == ErrorKind::RateLimited
                || oe.kind() == ErrorKind::Unexpected
                || oe.is_temporary()
        }
        DataError::Storage(_) => true,
    }
}
