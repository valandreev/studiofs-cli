//! FetchScheduler — adaptive block fetcher with singleflight request coalescing.
//!
//! Replaces `FetchThrottle`, `InFlightGuard`, `inflight_fetches`, and the VFS-level
//! retry loop with a clean, self-contained component that:
//!
//! 1. Uses an [`AimdController`] for elastic concurrency (TCP-cwnd–like).
//! 2. Coalesces concurrent requests for the same block via singleflight (DashMap +
//!    tokio broadcast), so only one S3 GET is issued per block.
//! 3. Applies its own retry with exponential backoff per fetch, independent of
//!    OpenDAL's internal `RetryLayer`.

use crate::aimd::{AimdConfig, AimdController};
use bytes::Bytes;
use dashmap::DashMap;
use opendal::ErrorKind;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use studiofs_cache::block_cache::BlockCache;
use studiofs_data::object::read_object;
use studiofs_data::store::{DataError, DataStore};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Maximum retries *within* the FetchScheduler (includes the initial attempt).
/// With io_timeout=5s per attempt and 50ms base delay, worst case is
/// 5 × (5s + ~0.1s) ≈ 25s total.  In practice the connection pool warms up
/// after 1–2 failures, so typical cold-start stall is ~5–10s.
const MAX_FETCH_ATTEMPTS: u32 = 5;

/// Per-request hard timeout (independent of OpenDAL layers).
/// Must be > io_timeout (5s) to let OpenDAL finish normally.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);

/// Base delay between retries.  Kept very short because the purpose of
/// the retry is to get a fresh TCP connection, not to wait for the server
/// to recover.
const RETRY_BASE_DELAY: Duration = Duration::from_millis(50);

/// Maximum per-attempt delay cap.
const RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

/// Capacity of the broadcast channel used for singleflight notifications.
const BROADCAST_CAPACITY: usize = 2;

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

/// Error returned by a fetch operation.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("object not found: {key}")]
    NotFound { key: String },

    #[error("fetch failed after {attempts} attempts: {source}")]
    Exhausted {
        attempts: u32,
        source: DataError,
    },

    #[error("object decode error: {0}")]
    Decode(#[from] studiofs_data::object::ObjectError),

    #[error("cache write error: {0}")]
    CacheWrite(std::io::Error),
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics
// ─────────────────────────────────────────────────────────────────────────────

/// Lock-free counters for observability (logged periodically by the VFS).
pub struct FetchMetrics {
    pub cache_hits: AtomicU64,
    pub s3_fetches: AtomicU64,
    pub s3_errors: AtomicU64,
    pub singleflight_joins: AtomicU64,
    pub retries: AtomicU64,
    pub bytes_downloaded: AtomicU64,
}

impl FetchMetrics {
    fn new() -> Self {
        Self {
            cache_hits: AtomicU64::new(0),
            s3_fetches: AtomicU64::new(0),
            s3_errors: AtomicU64::new(0),
            singleflight_joins: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            bytes_downloaded: AtomicU64::new(0),
        }
    }
}

impl std::fmt::Display for FetchMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cache_hits={} s3={} errors={} singleflight={} retries={} bytes={}",
            self.cache_hits.load(Ordering::Relaxed),
            self.s3_fetches.load(Ordering::Relaxed),
            self.s3_errors.load(Ordering::Relaxed),
            self.singleflight_joins.load(Ordering::Relaxed),
            self.retries.load(Ordering::Relaxed),
            self.bytes_downloaded.load(Ordering::Relaxed),
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FetchScheduler
// ─────────────────────────────────────────────────────────────────────────────

/// Result broadcast to singleflight waiters: `true` = success, `false` = failure.
type BroadcastResult = bool;

/// Adaptive block fetcher with singleflight dedup and AIMD concurrency.
pub struct FetchScheduler {
    /// AIMD-based adaptive concurrency controller.
    aimd: AimdController,
    /// In-flight fetch map for singleflight dedup.
    /// Key: `object_id`.  Value: broadcast sender that signals completion.
    inflight: DashMap<u64, broadcast::Sender<BroadcastResult>>,
    /// S3 data store (read-optimised operator).
    data_store: Arc<DataStore>,
    /// Unified block cache (L1 RAM + L2 NVMe).
    cache: Arc<BlockCache>,
    /// Metrics.
    pub metrics: FetchMetrics,
}

impl FetchScheduler {
    /// Create a new fetch scheduler.
    ///
    /// # Arguments
    /// - `data_store`: read-optimised `DataStore` (no `ConcurrentLimitLayer` — AIMD manages concurrency).
    /// - `cache`: shared block cache.
    /// - `aimd_config`: AIMD parameters (use [`AimdConfig::fetch_default()`] for sane defaults).
    pub fn new(
        data_store: Arc<DataStore>,
        cache: Arc<BlockCache>,
        aimd_config: AimdConfig,
    ) -> Self {
        info!(
            initial_window = aimd_config.initial_window,
            max_window = aimd_config.max_window,
            "FetchScheduler created"
        );
        Self {
            aimd: AimdController::new(aimd_config),
            inflight: DashMap::new(),
            data_store,
            cache,
            metrics: FetchMetrics::new(),
        }
    }

    // ── Public API ──────────────────────────────────────────────────────

    /// Fetch a block, returning decompressed data.
    ///
    /// This is the **single entry-point** for all remote reads.  It handles:
    /// 1. L1/L2 cache check (fast path).
    /// 2. Singleflight dedup (only one S3 GET per `object_id`).
    /// 3. AIMD-governed concurrency for the actual HTTP request.
    /// 4. Retry with exponential backoff on transient errors.
    /// 5. Cache population on success.
    pub async fn fetch_block(
        &self,
        object_id: u64,
        key: &str,
    ) -> Result<Bytes, FetchError> {
        // ── Fast path: cache ────────────────────────────────────────
        if let Some(cached) = self.cache.get(object_id).await {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached);
        }

        // ── Singleflight ───────────────────────────────────────────
        //
        // Try to become the "fetcher" for this object_id.  If another task
        // is already fetching, we subscribe to its broadcast and wait.
        loop {
            // We need to handle the DashMap entry carefully:
            // - Occupied → subscribe, drop entry ref, await.
            // - Vacant → insert sender, drop entry ref, proceed as fetcher.
            let action = {
                match self.inflight.entry(object_id) {
                    dashmap::Entry::Occupied(e) => {
                        let rx = e.get().subscribe();
                        self.metrics.singleflight_joins.fetch_add(1, Ordering::Relaxed);
                        Action::Wait(rx)
                    }
                    dashmap::Entry::Vacant(e) => {
                        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
                        e.insert(tx);
                        Action::Fetch
                    }
                }
            };

            match action {
                Action::Fetch => break,
                Action::Wait(mut rx) => {
                    // Wait for the fetcher to finish (with a generous timeout
                    // to avoid infinite hangs).
                    let recv = tokio::time::timeout(Duration::from_secs(300), rx.recv()).await;
                    match recv {
                        // Fetcher succeeded — grab from cache.
                        Ok(Ok(true)) => {
                            if let Some(cached) = self.cache.get(object_id).await {
                                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                                return Ok(cached);
                            }
                            // Rare: cache evicted between notify and our read.
                            // Fall through to re-enter singleflight as fetcher.
                            debug!(object_id, "singleflight: cache miss after notify, retrying");
                        }
                        // Fetcher failed, channel closed, or timeout.
                        // Become a new fetcher ourselves.
                        Ok(Ok(false)) | Ok(Err(_)) | Err(_) => {
                            debug!(object_id, "singleflight: fetcher failed, becoming new fetcher");
                        }
                    }
                    // Loop back to try to become the fetcher.
                    continue;
                }
            }
        }

        // ── We are the fetcher ──────────────────────────────────────

        // Guard: if we panic or return early, clean up the inflight entry
        // so waiters receive `Closed` and can retry.
        let _cleanup = InFlightCleanup {
            object_id,
            inflight: &self.inflight,
            active: std::cell::Cell::new(true),
        };

        let result = self.do_fetch_with_retry(object_id, key).await;

        // Notify waiters before cleanup removes the entry.
        let success = result.is_ok();
        if let Some((_, tx)) = self.inflight.remove(&object_id) {
            let _ = tx.send(success);
        }
        // Defuse cleanup guard — we already removed the entry.
        _cleanup.defuse();

        result
    }

    /// Warm up the HTTP connection pool by issuing real GET requests.
    ///
    /// `check()` only does HEAD/stat which primes a different part of the
    /// reqwest connection pool.  We need actual GET requests to warm up
    /// the read path.  Issuing a few parallel GETs to a non-existent key
    /// forces TCP connection establishment — the resulting 404/NotFound
    /// errors are expected and ignored.
    pub async fn warmup(&self, timeout: Duration) -> Result<(), String> {
        use futures::future::join_all;

        // Fire N parallel GETs to prime N connections in the pool.
        // Keys that don't exist will return NotFound quickly, but the
        // TCP handshake (the slow part) still happens.
        const WARMUP_CONCURRENCY: usize = 4;
        let futs: Vec<_> = (0..WARMUP_CONCURRENCY)
            .map(|i| {
                let ds = &self.data_store;
                async move {
                    let key = format!("__studiofs_warmup_{i}__");
                    ds.get_by_key(&key).await
                }
            })
            .collect();

        match tokio::time::timeout(timeout, join_all(futs)).await {
            Ok(results) => {
                let ok_count = results.iter().filter(|r| r.is_ok()).count();
                let err_count = results.len() - ok_count;
                info!(
                    ok = ok_count,
                    not_found = err_count,
                    "FetchScheduler: warmup done ({WARMUP_CONCURRENCY} connections primed)"
                );
                Ok(())
            }
            Err(_) => {
                warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    "FetchScheduler: warmup timed out (reads will retry)"
                );
                Err("warmup timed out".into())
            }
        }
    }

    // ── Observability ───────────────────────────────────────────────

    /// Current AIMD window size.
    pub fn window(&self) -> f64 {
        self.aimd.window()
    }

    /// Current in-flight S3 requests (not counting cache hits).
    pub fn in_flight(&self) -> usize {
        self.aimd.in_flight()
    }

    /// Log a summary line (call periodically from a background task).
    pub fn log_stats(&self) {
        info!(
            window = format!("{:.1}/{:.0}", self.aimd.window(), 0),
            ok = self.aimd.total_successes(),
            fail = self.aimd.total_failures(),
            "fetch: {} inflight={}",
            self.metrics,
            self.aimd.in_flight(),
        );
    }

    /// Reference to the underlying data store.
    pub fn data_store(&self) -> &Arc<DataStore> {
        &self.data_store
    }

    // ── Internal ────────────────────────────────────────────────────

    /// Perform the actual S3 GET with retries, AIMD permit, decompress, cache.
    async fn do_fetch_with_retry(
        &self,
        object_id: u64,
        key: &str,
    ) -> Result<Bytes, FetchError> {
        let mut last_err: Option<DataError> = None;
        let overall_start = Instant::now();

        for attempt in 0..MAX_FETCH_ATTEMPTS {
            // Acquire an AIMD permit — blocks until the concurrency window
            // allows another in-flight request.
            let wait_start = Instant::now();
            let permit = self.aimd.acquire().await;
            let wait_ms = wait_start.elapsed().as_millis();
            if wait_ms > 100 {
                debug!(
                    object_id,
                    wait_ms,
                    window = format!("{:.1}", self.aimd.window()),
                    "AIMD permit waited"
                );
            }

            // Hard timeout per request — prevents runaway waits from
            // blocking WinFSP/FUSE dispatch threads indefinitely.
            let req_start = Instant::now();
            let get_result = tokio::time::timeout(
                REQUEST_TIMEOUT,
                self.data_store.get_by_key(key),
            )
            .await;
            let req_ms = req_start.elapsed().as_millis();

            match get_result {
                Ok(Ok(compressed)) => {
                    permit.success();
                    self.metrics.s3_fetches.fetch_add(1, Ordering::Relaxed);
                    self.metrics.bytes_downloaded.fetch_add(
                        compressed.len() as u64,
                        Ordering::Relaxed,
                    );

                    debug!(
                        object_id,
                        key,
                        bytes = compressed.len(),
                        req_ms,
                        total_ms = overall_start.elapsed().as_millis() as u64,
                        attempt,
                        "S3 GET ok"
                    );

                    // Decompress.
                    let decompressed = Bytes::from(read_object(&compressed)?);

                    // Populate cache (L1 + L2).
                    self.cache
                        .put(object_id, decompressed.clone())
                        .await
                        .map_err(FetchError::CacheWrite)?;

                    return Ok(decompressed);
                }
                Ok(Err(e)) => {
                    // NotFound — data consistency issue, NOT a server
                    // overload signal.  Release the permit neutrally
                    // (drop without calling failure()) so the AIMD
                    // window is unaffected.
                    if matches!(&e, DataError::OpenDal(oe) if oe.kind() == ErrorKind::NotFound) {
                        drop(permit);
                        return Err(FetchError::NotFound {
                            key: key.to_owned(),
                        });
                    }

                    // Classify the error: only *transient* server errors
                    // (HTTP 5xx, rate limiting) should shrink the AIMD
                    // window.  Persistent/timeout errors are connection-
                    // level issues — penalising them causes cascading
                    // window collapse that stalls the whole system.
                    let is_server_overload = match &e {
                        DataError::OpenDal(oe) => {
                            oe.kind() == ErrorKind::RateLimited || oe.is_temporary()
                        }
                        _ => false,
                    };

                    if is_server_overload {
                        permit.failure();
                    } else {
                        // Timeout / persistent — neutral release.
                        drop(permit);
                    }

                    self.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);

                    if attempt + 1 < MAX_FETCH_ATTEMPTS {
                        let delay = RETRY_BASE_DELAY
                            .saturating_mul(2u32.saturating_pow(attempt))
                            .min(RETRY_MAX_DELAY);
                        self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            attempt = attempt + 1,
                            max = MAX_FETCH_ATTEMPTS,
                            key,
                            req_ms,
                            delay_ms = delay.as_millis() as u64,
                            is_server_overload,
                            error = %e,
                            "fetch failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    } else {
                        error!(
                            attempts = MAX_FETCH_ATTEMPTS,
                            key,
                            req_ms,
                            total_ms = overall_start.elapsed().as_millis() as u64,
                            error = %e,
                            "fetch exhausted all retries"
                        );
                    }

                    last_err = Some(e);
                }
                Err(_elapsed) => {
                    // Hard timeout — connection-level issue, NOT server
                    // overload.  Release permit neutrally so AIMD
                    // window stays intact.
                    drop(permit);
                    self.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);

                    let timeout_err = DataError::Storage(format!(
                        "request timeout after {}s",
                        REQUEST_TIMEOUT.as_secs()
                    ));

                    if attempt + 1 < MAX_FETCH_ATTEMPTS {
                        let delay = RETRY_BASE_DELAY
                            .saturating_mul(2u32.saturating_pow(attempt))
                            .min(RETRY_MAX_DELAY);
                        self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            attempt = attempt + 1,
                            max = MAX_FETCH_ATTEMPTS,
                            key,
                            timeout_s = REQUEST_TIMEOUT.as_secs(),
                            delay_ms = delay.as_millis() as u64,
                            "fetch timed out, retrying (AIMD window unchanged)"
                        );
                        tokio::time::sleep(delay).await;
                    } else {
                        error!(
                            attempts = MAX_FETCH_ATTEMPTS,
                            key,
                            total_ms = overall_start.elapsed().as_millis() as u64,
                            "fetch timed out, all retries exhausted"
                        );
                    }

                    last_err = Some(timeout_err);
                }
            }
        }

        Err(FetchError::Exhausted {
            attempts: MAX_FETCH_ATTEMPTS,
            source: last_err.expect("at least one error"),
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Singleflight actions (internal)
// ─────────────────────────────────────────────────────────────────────────────

enum Action {
    Fetch,
    Wait(broadcast::Receiver<BroadcastResult>),
}

// ─────────────────────────────────────────────────────────────────────────────
// In-flight cleanup guard (panic safety)
// ─────────────────────────────────────────────────────────────────────────────

/// RAII guard that removes the in-flight entry on drop if not defused.
///
/// If the fetcher panics, this ensures the singleflight entry is removed so
/// that waiters receive `RecvError::Closed` (and can re-enter as fetchers)
/// rather than hanging forever.
struct InFlightCleanup<'a> {
    object_id: u64,
    inflight: &'a DashMap<u64, broadcast::Sender<BroadcastResult>>,
    active: std::cell::Cell<bool>,
}

impl InFlightCleanup<'_> {
    fn defuse(self) {
        self.active.set(false);
        // Don't run Drop — we consumed ownership.
        std::mem::forget(self);
    }
}

impl Drop for InFlightCleanup<'_> {
    fn drop(&mut self) {
        if self.active.get() {
            // Panic path: remove entry so waiters get Closed.
            let removed = self.inflight.remove(&self.object_id);
            if let Some((_, tx)) = removed {
                // Send false so waiters know the fetch failed.
                let _ = tx.send(false);
            }
        }
    }
}
