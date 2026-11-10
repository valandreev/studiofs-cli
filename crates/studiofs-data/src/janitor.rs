//! OpenDAL operations used by the standalone janitor.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use futures::TryStreamExt as _;
use opendal::layers::{HttpClientLayer, RetryInterceptor, RetryLayer};
use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use opendal::services::S3;
use opendal::{Buffer, EntryMode, ErrorKind, Operator, Result};
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Retries after the first attempt of one idempotent LIST page, HEAD or DELETE.
pub const JANITOR_S3_MAX_RETRIES: usize = 3;
const RETRY_MIN_DELAY: Duration = Duration::from_millis(100);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(2);
/// Longest service-level pause a throttle response (or its `Retry-After`) can impose.
pub const JANITOR_S3_MAX_COOLDOWN: Duration = Duration::from_secs(30);
/// Slowest request spacing the adaptive gate backs off to under sustained throttling.
pub const JANITOR_S3_MAX_REQUEST_INTERVAL: Duration = Duration::from_secs(8);

/// OpenDAL adapter for listing and mutating payload objects.
#[derive(Clone)]
pub struct JanitorObjectStore {
    operator: Operator,
    transport: Arc<TransportCounters>,
}

/// Result category for deleting one fragment.
#[derive(Debug, thiserror::Error)]
pub enum JanitorDeleteError {
    /// The fragment was already absent.
    #[error("fragment not found")]
    NotFound,
    /// The provider rejected the request in a way retrying cannot fix (e.g. access denied).
    #[error("{0}")]
    Terminal(String),
    /// The request failed transiently (throttle, 5xx, network, timeout) after bounded retries.
    #[error("{0}")]
    Other(String),
}

/// How the janitor must treat a failed object-store request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JanitorFailureClass {
    /// The key no longer exists; nothing is left to retry.
    Vanished,
    /// Throttling, server, network or timeout failure; a later attempt may succeed.
    Transient,
    /// Authorization, configuration or malformed-request failure; retrying cannot help.
    Terminal,
}

/// Classify one OpenDAL error by kind: OpenDAL's retry layer marks every final error
/// persistent, so its retryable flag is gone by now. Unrecognized failures (5xx,
/// network, unknown status) are transient and bounded by the caller's overload breaker.
pub fn classify_object_error(error: &opendal::Error) -> JanitorFailureClass {
    match error.kind() {
        ErrorKind::NotFound => JanitorFailureClass::Vanished,
        ErrorKind::PermissionDenied
        | ErrorKind::ConfigInvalid
        | ErrorKind::Unsupported
        | ErrorKind::IsADirectory
        | ErrorKind::NotADirectory => JanitorFailureClass::Terminal,
        _ => JanitorFailureClass::Transient,
    }
}

/// Provider pressure observed by one store since it was created.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JanitorTransportStats {
    /// HTTP 429/503 responses and transport-level failures.
    pub throttled: u64,
    /// Request retries issued by the bounded retry policy.
    pub retries: u64,
    /// Current spacing between request starts; grows on throttle, shrinks on success.
    pub request_interval: Duration,
}

#[derive(Debug, Default)]
struct TransportCounters {
    throttled: AtomicU64,
    retries: AtomicU64,
    interval_micros: AtomicU64,
    batch_unsupported: AtomicBool,
}

struct RetryCounter(Arc<TransportCounters>);

impl RetryInterceptor for RetryCounter {
    fn intercept(&self, err: &opendal::Error, dur: Duration) {
        self.0.retries.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(kind = ?err.kind(), delay_ms = dur.as_millis(), "janitor S3 request retry");
    }
}

impl JanitorObjectStore {
    /// Wrap a configured object-storage operator.
    pub fn new(operator: Operator) -> Self {
        Self {
            operator,
            transport: Arc::default(),
        }
    }

    /// Throttle, retry and pacing counters of the request gate.
    pub fn transport_stats(&self) -> JanitorTransportStats {
        JanitorTransportStats {
            throttled: self.transport.throttled.load(Ordering::Relaxed),
            retries: self.transport.retries.load(Ordering::Relaxed),
            request_interval: Duration::from_micros(
                self.transport.interval_micros.load(Ordering::Relaxed),
            ),
        }
    }

    /// Configure a provider-neutral S3-compatible endpoint without loading user credentials.
    pub fn from_s3(
        endpoint: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
        region: &str,
        requests_per_second: u32,
    ) -> Result<Self> {
        if !(1..=32).contains(&requests_per_second) {
            return Err(opendal::Error::new(
                ErrorKind::ConfigInvalid,
                "janitor S3 request rate must be between 1 and 32 per second",
            ));
        }
        let transport = Arc::<TransportCounters>::default();
        let base_interval = Duration::from_secs_f64(1.0 / f64::from(requests_per_second));
        transport.interval_micros.store(
            u64::try_from(base_interval.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let request_rate = AdaptiveRateLimitedHttpClient {
            inner: HttpClient::new()?,
            base_interval,
            pace: Mutex::new(Pace {
                next_request_at: Instant::now(),
                interval: base_interval,
            }),
            counters: transport.clone(),
        };
        let operator = Operator::new(
            S3::default()
                .endpoint(endpoint)
                .bucket(bucket)
                .access_key_id(access_key_id)
                .secret_access_key(secret_access_key)
                .region(if region.is_empty() {
                    "us-east-1"
                } else {
                    region
                })
                .disable_config_load()
                .disable_ec2_metadata(),
        )?
        .layer(HttpClientLayer::new(HttpClient::with(request_rate)))
        // Outermost, so every retry attempt passes the request gate again.
        // The janitor issues only idempotent requests: LIST pages, HEAD and DELETE.
        .layer(
            RetryLayer::new()
                .with_jitter()
                .with_min_delay(RETRY_MIN_DELAY)
                .with_max_delay(RETRY_MAX_DELAY)
                .with_max_times(JANITOR_S3_MAX_RETRIES)
                .with_notify(RetryCounter(transport.clone())),
        )
        .finish();
        Ok(Self {
            operator,
            transport,
        })
    }

    /// Start a lazy recursive listing below `prefix`.
    pub async fn list(&self, prefix: &str) -> Result<JanitorLister> {
        self.list_after(prefix, None).await
    }

    /// Start a lazy recursive listing below `prefix`, optionally after one key.
    pub async fn list_after(&self, prefix: &str, after_key: Option<&str>) -> Result<JanitorLister> {
        let mut lister = self.operator.lister_with(prefix);
        if let Some(after_key) = after_key {
            lister = lister.start_after(after_key);
        }
        Ok(JanitorLister(lister.recursive(true).await?))
    }

    /// Return the remote object's last-modified time when the provider supplies one.
    pub async fn last_modified(&self, key: &str) -> Result<Option<SystemTime>> {
        Ok(self
            .operator
            .stat(key)
            .await?
            .last_modified()
            .map(Into::into))
    }

    /// Return whether an exact canonical key exists and, when available, its age.
    pub async fn stat_key(&self, key: &str) -> Result<JanitorKeyStat> {
        match self.operator.stat(key).await {
            Ok(metadata) => Ok(JanitorKeyStat::Found(
                metadata.last_modified().map(Into::into),
            )),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(JanitorKeyStat::Missing),
            Err(error) => Err(error),
        }
    }

    /// Delete one object fragment.
    pub async fn delete(&self, key: &str) -> std::result::Result<(), JanitorDeleteError> {
        self.operator
            .delete(key)
            .await
            .map_err(|error| match classify_object_error(&error) {
                JanitorFailureClass::Vanished => JanitorDeleteError::NotFound,
                JanitorFailureClass::Terminal => JanitorDeleteError::Terminal(error.to_string()),
                JanitorFailureClass::Transient => JanitorDeleteError::Other(error.to_string()),
            })
    }

    /// Delete keys in input order, confirming ambiguous batch outcomes with single deletes.
    /// The bounded recovery leaves time to settle the PostgreSQL claims before their lease ends.
    pub async fn delete_batch(
        &self,
        keys: &[String],
    ) -> Vec<std::result::Result<(), JanitorDeleteError>> {
        let deadline = Instant::now() + Duration::from_secs(35);
        let capacity = self
            .operator
            .info()
            .full_capability()
            .delete_max_size
            .unwrap_or(1);
        let mut outcomes = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(capacity.clamp(1, 1000)) {
            if chunk.len() > 1 && !self.transport.batch_unsupported.load(Ordering::Relaxed) {
                let attempt = async {
                    let mut deleter = self.operator.deleter().await?;
                    deleter.delete_iter(chunk.iter().cloned()).await?;
                    deleter.flush().await
                };
                match tokio::time::timeout_at(
                    deadline.min(Instant::now() + Duration::from_secs(20)),
                    attempt,
                )
                .await
                {
                    Ok(Ok(deleted)) if deleted == chunk.len() => {
                        outcomes.extend(chunk.iter().map(|_| Ok(())));
                        continue;
                    }
                    Ok(Err(error)) if error.kind() == ErrorKind::Unsupported => {
                        self.transport
                            .batch_unsupported
                            .store(true, Ordering::Relaxed);
                    }
                    Err(_) => {
                        outcomes.extend(chunk.iter().map(|_| {
                            Err(JanitorDeleteError::Other(
                                "batch delete timed out".to_owned(),
                            ))
                        }));
                        continue;
                    }
                    // OpenDAL exposes only a success count, not the keys in a partial result.
                    // Repeating an idempotent DELETE independently confirms each key.
                    _ => {}
                }
            }
            for key in chunk {
                if Instant::now() >= deadline {
                    outcomes.push(Err(JanitorDeleteError::Other(
                        "delete confirmation timed out".to_owned(),
                    )));
                    continue;
                }
                let result = tokio::time::timeout_at(
                    deadline.min(Instant::now() + Duration::from_secs(20)),
                    self.delete(key),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(JanitorDeleteError::Other(
                        "delete confirmation timed out".to_owned(),
                    ))
                });
                outcomes.push(result);
            }
        }
        outcomes
    }
}

/// Spreads provider HTTP request starts across an adaptive rate window.
///
/// A 429/503 response or a transport failure doubles the spacing (up to
/// [`JANITOR_S3_MAX_REQUEST_INTERVAL`]) and pauses every request for the provider's
/// `Retry-After` or the new spacing, capped at [`JANITOR_S3_MAX_COOLDOWN`]. Each other
/// response shrinks the spacing by 1/8 back toward the configured rate.
struct AdaptiveRateLimitedHttpClient {
    inner: HttpClient,
    base_interval: Duration,
    pace: Mutex<Pace>,
    counters: Arc<TransportCounters>,
}

struct Pace {
    next_request_at: Instant,
    interval: Duration,
}

impl HttpFetch for AdaptiveRateLimitedHttpClient {
    async fn fetch(&self, request: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let batch_delete = request.method() == http::Method::POST
            && request.uri().query().is_some_and(|query| {
                query
                    .split('&')
                    .any(|part| part == "delete" || part == "delete=")
            });
        let mut pace = self.pace.lock().await;
        tokio::time::sleep_until(pace.next_request_at).await;
        pace.next_request_at = Instant::now() + pace.interval;
        drop(pace);

        let result = self.inner.fetch(request).await;
        if batch_delete
            && result
                .as_ref()
                .is_ok_and(|response| matches!(response.status().as_u16(), 405 | 501))
        {
            self.counters
                .batch_unsupported
                .store(true, Ordering::Relaxed);
        }
        let throttle = match &result {
            Ok(response) => matches!(response.status().as_u16(), 429 | 503)
                .then(|| retry_after(response.headers())),
            Err(error) => error.is_temporary().then_some(None),
        };
        let mut pace = self.pace.lock().await;
        match throttle {
            Some(retry_after) => {
                self.counters.throttled.fetch_add(1, Ordering::Relaxed);
                pace.interval = pace
                    .interval
                    .saturating_mul(2)
                    .min(JANITOR_S3_MAX_REQUEST_INTERVAL);
                let cooldown = retry_after
                    .unwrap_or(pace.interval)
                    .min(JANITOR_S3_MAX_COOLDOWN);
                pace.next_request_at = pace.next_request_at.max(Instant::now() + cooldown);
            }
            None => pace.interval = (pace.interval - pace.interval / 8).max(self.base_interval),
        }
        self.counters.interval_micros.store(
            u64::try_from(pace.interval.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        result
    }
}

/// `Retry-After` in delta-seconds form.
// ponytail: the HTTP-date form falls back to the doubled spacing; parse it if a provider sends one.
fn retry_after(headers: &http::HeaderMap) -> Option<Duration> {
    let seconds = headers.get(http::header::RETRY_AFTER)?.to_str().ok()?;
    seconds.trim().parse().ok().map(Duration::from_secs)
}

/// Result of statting one exact object key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JanitorKeyStat {
    /// The object is absent.
    Missing,
    /// The object exists; its timestamp may be unavailable.
    Found(Option<SystemTime>),
}

/// Streaming listing that holds at most one provider page in memory.
pub struct JanitorLister(opendal::Lister);

impl JanitorLister {
    /// Return the next listed entry, `None` once the listing is exhausted.
    pub async fn next(&mut self) -> Result<Option<JanitorEntry>> {
        Ok(self.0.try_next().await?.map(|entry| JanitorEntry {
            key: entry.path().to_string(),
            is_file: entry.metadata().mode() == EntryMode::FILE,
            last_modified: entry.metadata().last_modified().map(Into::into),
        }))
    }
}

/// One listed key and whether OpenDAL reports it as a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JanitorEntry {
    /// Object key relative to the operator root.
    pub key: String,
    /// Whether this entry represents a file rather than a directory.
    pub is_file: bool,
    /// `LastModified` from the listing page, when the provider returned one.
    pub last_modified: Option<SystemTime>,
}

/// Grace-period verdict for one orphan candidate; a greater verdict is stricter,
/// so `max` combines two observations conservatively.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JanitorAge {
    /// Older than the grace period; deletion may proceed.
    Eligible,
    /// Inside the grace period.
    Young,
    /// Missing or future-dated; never infer an age from it.
    Unknown,
}

/// Classify an object's age against `grace`; a timestamp after `now` is `Unknown`.
pub fn janitor_age(now: SystemTime, observed: Option<SystemTime>, grace: Duration) -> JanitorAge {
    match observed.map(|observed| now.duration_since(observed)) {
        Some(Ok(age)) if age >= grace => JanitorAge::Eligible,
        Some(Ok(_)) => JanitorAge::Young,
        None | Some(Err(_)) => JanitorAge::Unknown,
    }
}

/// [`janitor_age`] for a PostgreSQL pending-deletion timestamp in Unix seconds.
pub fn pending_deletion_age(now_unix: i64, deleted_at_unix: i64, grace: Duration) -> JanitorAge {
    match u64::try_from(now_unix.saturating_sub(deleted_at_unix)) {
        Ok(age) if age >= grace.as_secs() => JanitorAge::Eligible,
        Ok(_) => JanitorAge::Young,
        Err(_) => JanitorAge::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_transport_stats_start_at_the_configured_request_interval() {
        let store = JanitorObjectStore::from_s3(
            "http://127.0.0.1:9000",
            "test",
            "test",
            "test",
            "us-east-1",
            4,
        )
        .unwrap();

        let stats = store.transport_stats();
        assert_eq!(stats.request_interval, Duration::from_millis(250));
        assert_eq!((stats.throttled, stats.retries), (0, 0));
    }

    #[test]
    fn janitor_age_skips_missing_future_and_young_timestamps() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let grace = Duration::from_secs(100);
        let at = |secs| Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs));
        assert_eq!(janitor_age(now, None, grace), JanitorAge::Unknown);
        assert_eq!(janitor_age(now, at(1_001), grace), JanitorAge::Unknown);
        assert_eq!(janitor_age(now, at(950), grace), JanitorAge::Young);
        assert_eq!(janitor_age(now, at(900), grace), JanitorAge::Eligible);
        assert_eq!(
            pending_deletion_age(1_000, 1_001, grace),
            JanitorAge::Unknown
        );
        assert_eq!(pending_deletion_age(1_000, 950, grace), JanitorAge::Young);
        assert_eq!(
            pending_deletion_age(1_000, 900, grace),
            JanitorAge::Eligible
        );
    }
}
