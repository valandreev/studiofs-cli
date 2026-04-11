//! UploadScheduler — adaptive object uploader with AIMD concurrency and retries.
//!
//! This mirrors the fetch-side adaptive control, but for PUT requests.

use crate::aimd::{AimdConfig, AimdController};
use crate::runtime::{TimeoutOrCancel, run_with_timeout_or_cancel, sleep_or_cancel};
use crate::vfs::VfsConfig;
use opendal::{Buffer, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use studiofs_data::store::{DataError, DataStore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Lock-free counters for upload observability.
pub struct UploadMetrics {
    pub attempts: AtomicU64,
    pub uploads_ok: AtomicU64,
    pub uploads_err: AtomicU64,
    pub retries: AtomicU64,
    pub exhausted: AtomicU64,
    pub timeout_err: AtomicU64,
    pub temporary_err: AtomicU64,
    pub status_5xx_err: AtomicU64,
    pub network_err: AtomicU64,
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
pub enum UploadError {
    #[error("upload failed after {attempts} attempts: {source}")]
    Exhausted { attempts: u32, source: DataError },
    #[error("upload cancelled")]
    Cancelled,
}

/// Adaptive uploader with AIMD concurrency and per-request retry.
pub struct UploadScheduler {
    aimd: AimdController,
    data_store: Arc<DataStore>,
    max_attempts: u32,
    request_timeout: Duration,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
    adaptive_penalty_ms: AtomicU64,
    pub metrics: UploadMetrics,
}

impl UploadScheduler {
    pub fn new(data_store: Arc<DataStore>, aimd_config: AimdConfig, config: &VfsConfig) -> Self {
        let max_attempts = config.upload_max_attempts.max(1);
        let request_timeout_s = config.upload_request_timeout_secs.max(1);
        let retry_base_ms = config.upload_retry_base_ms.max(1);
        let retry_max_ms = config.upload_retry_max_ms.max(retry_base_ms);

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
            max_attempts,
            request_timeout: Duration::from_secs(request_timeout_s),
            retry_base_delay: Duration::from_millis(retry_base_ms),
            retry_max_delay: Duration::from_millis(retry_max_ms),
            adaptive_penalty_ms: AtomicU64::new(0),
            metrics: UploadMetrics::new(),
        }
    }

    pub fn set_adaptive_penalty(&self, delay: Duration) {
        let penalty_ms = delay.as_millis().clamp(0, u64::MAX as u128) as u64;
        self.adaptive_penalty_ms
            .store(penalty_ms, Ordering::Relaxed);
    }

    pub fn adaptive_penalty_ms(&self) -> u64 {
        self.adaptive_penalty_ms.load(Ordering::Relaxed)
    }

    /// Upload object bytes to `key` with adaptive concurrency + retries.
    ///
    /// Note: foreground-aware adaptive cool-down is intentionally applied at
    /// the VFS worker level before local L2 disk reads. Keep this method
    /// focused on network/S3 request scheduling and retry behavior.
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
                        warn!(
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
                        warn!(
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
                        error!(
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

    pub fn window(&self) -> f64 {
        self.aimd.window()
    }

    pub fn in_flight(&self) -> usize {
        self.aimd.in_flight()
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
    use super::upload_log_is_idle;

    #[test]
    fn idle_upload_info_log_is_compact() {
        assert!(upload_log_is_idle(0, 0));
        assert!(!upload_log_is_idle(1, 0));
        assert!(!upload_log_is_idle(0, 1));
    }
}
