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
//! 4. Routes local cache get/put through Tokio's blocking pool so file-backed
//!    cache I/O never blocks async worker threads.

use crate::aimd::{AimdConfig, AimdController, AimdPermit};
use crate::qos::{QosController, TransitMemoryBudget};
use crate::runtime::LocalCacheIoTiming;
use crate::vfs::VfsConfig;
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use opendal::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use studiofs_cache::CachedStore;
use studiofs_data::object::{CLOUD_FRAGMENT_SIZE, read_object, validate_cloud_fragment_layout};
use studiofs_data::store::{DataError, DataStore};
use tokio::sync::broadcast;
use tokio::task;
use tracing::{debug, error, info, warn};

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Capacity of the broadcast channel used for singleflight notifications.
const BROADCAST_CAPACITY: usize = 2;
/// Cap concurrent fragment GETs per logical object fetch to keep peak
/// in-flight fragment memory bounded.
const MAX_FRAGMENT_FETCH_IN_FLIGHT: usize = 8;

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

/// Error returned by a fetch operation.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// Remote read or metadata lookup found no object by the requested key.
    #[error("object not found: {key}")]
    NotFound {
        /// Object key requested by the fetcher that was not found.
        key: String,
    },

    /// Remote fetch failed after exhausting all attempts.
    #[error("fetch failed after {attempts} attempts: {source}")]
    Exhausted {
        /// Number of attempts that were made for this fetch.
        attempts: u32,
        /// Underlying storage or protocol error returned by the final attempt.
        source: DataError,
    },

    /// Object decode failed after downloading a remote fragment payload.
    #[error("object decode error: {0}")]
    Decode(#[from] studiofs_data::object::ObjectError),

    /// Local cache layer returned an error.
    #[error("cache access error: {0}")]
    CacheAccess(String),

    /// Prefetch was skipped due to temporary scheduling pressure.
    #[error("prefetch skipped: no foreground headroom")]
    PrefetchSkipped,

    /// The fetched object fragment layout did not pass validation.
    #[error("corrupt fragment layout for object {object_id}: {reason}")]
    CorruptFragment {
        /// Object identifier associated with the invalid fragment payload.
        object_id: u64,
        /// Validation failure detail.
        reason: String,
    },

    /// Transit-memory budget was insufficient to stage in-flight fragment buffers.
    #[error(
        "transit memory budget too small: required={required_bytes} bytes, budget={budget_bytes} bytes"
    )]
    TransitBudgetTooSmall {
        /// Bytes required for the requested transit buffer layout.
        required_bytes: usize,
        /// Available budget bytes at the time of fetch scheduling.
        budget_bytes: usize,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics
// ─────────────────────────────────────────────────────────────────────────────

/// Lock-free counters for observability (logged periodically by the VFS).
pub struct FetchMetrics {
    /// Cache hit count.
    pub cache_hits: AtomicU64,
    /// Number of S3 fetch attempts (including retries that progressed to fetch stage).
    pub s3_fetches: AtomicU64,
    /// Number of fetch errors reported by S3.
    pub s3_errors: AtomicU64,
    /// Singleflight join count (non-owner readers for in-flight objects).
    pub singleflight_joins: AtomicU64,
    /// Retry count across fetch stages.
    pub retries: AtomicU64,
    /// Total bytes downloaded from remote and validated payloads.
    pub bytes_downloaded: AtomicU64,
    /// Number of prefetches skipped because admission was closed.
    pub prefetch_skips: AtomicU64,
    /// Total count of local cache I/O calls.
    pub local_cache_io_calls: AtomicU64,
    /// Total local cache handoff latency in nanoseconds.
    pub local_cache_io_handoff_total_ns: AtomicU64,
    /// Peak local cache handoff latency in nanoseconds.
    pub local_cache_io_handoff_max_ns: AtomicU64,
    /// Total local cache backend I/O latency in nanoseconds.
    pub local_cache_io_backend_total_ns: AtomicU64,
    /// Peak local cache backend I/O latency in nanoseconds.
    pub local_cache_io_backend_max_ns: AtomicU64,
    /// Count of S3 GET requests.
    pub s3_get_calls: AtomicU64,
    /// Total S3 GET latency in nanoseconds.
    pub s3_get_total_ns: AtomicU64,
    /// Peak S3 GET latency in nanoseconds.
    pub s3_get_max_ns: AtomicU64,
    /// Count of object decode calls.
    pub object_decode_calls: AtomicU64,
    /// Total object decode latency in nanoseconds.
    pub object_decode_total_ns: AtomicU64,
    /// Peak object decode latency in nanoseconds.
    pub object_decode_max_ns: AtomicU64,
    /// Count of assembled-copy operations during reassembly.
    pub assemble_copy_calls: AtomicU64,
    /// Total assembled-copy latency in nanoseconds.
    pub assemble_copy_total_ns: AtomicU64,
    /// Peak assembled-copy latency in nanoseconds.
    pub assemble_copy_max_ns: AtomicU64,
    /// Count of local cache put calls.
    pub local_cache_put_calls: AtomicU64,
    /// Total local cache put latency in nanoseconds.
    pub local_cache_put_total_ns: AtomicU64,
    /// Peak local cache put latency in nanoseconds.
    pub local_cache_put_max_ns: AtomicU64,
    /// Total fetch call count.
    pub fetch_total_calls: AtomicU64,
    /// Total end-to-end fetch latency in nanoseconds.
    pub fetch_total_ns: AtomicU64,
    /// Peak end-to-end fetch latency in nanoseconds.
    pub fetch_total_max_ns: AtomicU64,
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
            prefetch_skips: AtomicU64::new(0),
            local_cache_io_calls: AtomicU64::new(0),
            local_cache_io_handoff_total_ns: AtomicU64::new(0),
            local_cache_io_handoff_max_ns: AtomicU64::new(0),
            local_cache_io_backend_total_ns: AtomicU64::new(0),
            local_cache_io_backend_max_ns: AtomicU64::new(0),
            s3_get_calls: AtomicU64::new(0),
            s3_get_total_ns: AtomicU64::new(0),
            s3_get_max_ns: AtomicU64::new(0),
            object_decode_calls: AtomicU64::new(0),
            object_decode_total_ns: AtomicU64::new(0),
            object_decode_max_ns: AtomicU64::new(0),
            assemble_copy_calls: AtomicU64::new(0),
            assemble_copy_total_ns: AtomicU64::new(0),
            assemble_copy_max_ns: AtomicU64::new(0),
            local_cache_put_calls: AtomicU64::new(0),
            local_cache_put_total_ns: AtomicU64::new(0),
            local_cache_put_max_ns: AtomicU64::new(0),
            fetch_total_calls: AtomicU64::new(0),
            fetch_total_ns: AtomicU64::new(0),
            fetch_total_max_ns: AtomicU64::new(0),
        }
    }
}

impl std::fmt::Display for FetchMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cache_hits={} s3={} errors={} singleflight={} retries={} bytes={} prefetch_skips={} local_cache_io_calls={} local_cache_handoff_max_ms={:.3} local_cache_backend_max_ms={:.3} s3_get_max_ms={:.3} decode_max_ms={:.3} assemble_copy_max_ms={:.3} local_cache_put_max_ms={:.3} fetch_total_max_ms={:.3}",
            self.cache_hits.load(Ordering::Relaxed),
            self.s3_fetches.load(Ordering::Relaxed),
            self.s3_errors.load(Ordering::Relaxed),
            self.singleflight_joins.load(Ordering::Relaxed),
            self.retries.load(Ordering::Relaxed),
            self.bytes_downloaded.load(Ordering::Relaxed),
            self.prefetch_skips.load(Ordering::Relaxed),
            self.local_cache_io_calls.load(Ordering::Relaxed),
            self.local_cache_io_handoff_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            self.local_cache_io_backend_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            self.s3_get_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            self.object_decode_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            self.assemble_copy_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            self.local_cache_put_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            self.fetch_total_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FetchScheduler
// ─────────────────────────────────────────────────────────────────────────────

/// Result broadcast to singleflight waiters.
#[derive(Clone, Debug)]
pub(crate) enum BroadcastResult {
    /// Singleflight fetch completed successfully and carried the payload.
    Success(Bytes),
    /// Singleflight fetch failed; waiters must retry via cache or upstream logic.
    Failure,
}

/// Adaptive block fetcher with singleflight dedup and AIMD concurrency.
pub struct FetchScheduler {
    /// AIMD-based adaptive concurrency controller.
    aimd: AimdController,
    /// In-flight fetch map for singleflight dedup.
    /// Key: `object_id`.  Value: broadcast sender that signals completion.
    inflight: DashMap<u64, broadcast::Sender<BroadcastResult>>,
    /// S3 data store (read-optimised operator).
    data_store: Arc<DataStore>,
    /// Shared cached-store runtime used by the foreground read path.
    cached_store: Arc<CachedStore>,
    /// Maximum attempts per fetch operation.
    max_fetch_attempts: u32,
    /// Maximum attempts for best-effort prefetch fetches.
    prefetch_max_fetch_attempts: u32,
    /// Per-request hard timeout.
    request_timeout: Duration,
    /// Base backoff delay between retries.
    retry_base_delay: Duration,
    /// Maximum per-attempt backoff delay.
    retry_max_delay: Duration,
    /// Last observed send-request network error timestamp (unix ms).
    last_send_error_ms: AtomicU64,
    /// Keep this many AIMD slots free for foreground demand reads.
    prefetch_reserve_slots: usize,
    /// Max time (ms) prefetch waits to acquire an AIMD slot before skipping.
    prefetch_acquire_wait_ms: AtomicU64,
    /// Metrics.
    pub metrics: FetchMetrics,
    /// Independent fetch-stage credits so uploads/materialize cannot consume
    /// all execution capacity even if AIMD window is large.
    qos: Arc<QosController>,
    /// Shared transit-memory budget to cap active in-flight buffer footprint.
    transit_memory_budget: Arc<TransitMemoryBudget>,
}

macro_rules! impl_record_stage {
    ($fn_name:ident, $calls:ident, $total_ns:ident, $max_ns:ident) => {
        fn $fn_name(&self, duration: Duration) {
            Self::record_stage_duration(
                &self.metrics.$calls,
                &self.metrics.$total_ns,
                &self.metrics.$max_ns,
                duration,
            );
        }
    };
}

impl FetchScheduler {
    fn record_stage_duration(
        calls: &AtomicU64,
        total: &AtomicU64,
        max: &AtomicU64,
        duration: Duration,
    ) {
        let ns = duration.to_nanos_u64();
        calls.fetch_add(1, Ordering::Relaxed);
        total.fetch_add(ns, Ordering::Relaxed);
        max.fetch_max(ns, Ordering::Relaxed);
    }

    impl_record_stage!(record_s3_get, s3_get_calls, s3_get_total_ns, s3_get_max_ns);
    impl_record_stage!(
        record_object_decode,
        object_decode_calls,
        object_decode_total_ns,
        object_decode_max_ns
    );
    impl_record_stage!(
        record_assemble_copy,
        assemble_copy_calls,
        assemble_copy_total_ns,
        assemble_copy_max_ns
    );
    impl_record_stage!(
        record_local_cache_put,
        local_cache_put_calls,
        local_cache_put_total_ns,
        local_cache_put_max_ns
    );
    impl_record_stage!(
        record_fetch_total,
        fetch_total_calls,
        fetch_total_ns,
        fetch_total_max_ns
    );

    fn record_local_cache_io(&self, timing: LocalCacheIoTiming) {
        self.metrics
            .local_cache_io_calls
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .local_cache_io_handoff_total_ns
            .fetch_add(timing.handoff_ns, Ordering::Relaxed);
        self.metrics
            .local_cache_io_handoff_max_ns
            .fetch_max(timing.handoff_ns, Ordering::Relaxed);
        self.metrics
            .local_cache_io_backend_total_ns
            .fetch_add(timing.backend_io_ns, Ordering::Relaxed);
        self.metrics
            .local_cache_io_backend_max_ns
            .fetch_max(timing.backend_io_ns, Ordering::Relaxed);
    }

    /// Create a new fetch scheduler.
    ///
    /// # Arguments
    /// - `data_store`: read-optimised `DataStore` (no `ConcurrentLimitLayer` — AIMD manages concurrency).
    /// - `cached_store`: shared cached-store runtime used by the committed read path.
    /// - `aimd_config`: AIMD parameters (use [`AimdConfig::fetch_default()`] for sane defaults).
    pub fn new(
        data_store: Arc<DataStore>,
        cached_store: Arc<CachedStore>,
        aimd_config: AimdConfig,
        default_prefetch_reserve_slots: usize,
        qos: Arc<QosController>,
        transit_memory_budget: Arc<TransitMemoryBudget>,
        config: &VfsConfig,
    ) -> Self {
        let max_fetch_attempts = config.fetch_max_attempts.max(1);
        let prefetch_max_fetch_attempts = config.prefetch_fetch_max_attempts.max(1);
        let request_timeout_s = config.fetch_request_timeout_secs.max(1);
        let retry_base_ms = config.fetch_retry_base_ms.max(1);
        let retry_max_ms = config.fetch_retry_max_ms.max(retry_base_ms);
        let prefetch_reserve_slots = if config.fetch_prefetch_reserve > 0 {
            config.fetch_prefetch_reserve
        } else {
            default_prefetch_reserve_slots
        };
        let prefetch_acquire_wait_ms = config.fetch_prefetch_acquire_wait_ms.max(1);

        debug!(
            initial_window = aimd_config.initial_window,
            max_window = aimd_config.max_window,
            max_fetch_attempts,
            prefetch_max_fetch_attempts,
            request_timeout_s,
            retry_base_ms,
            retry_max_ms,
            prefetch_reserve_slots,
            prefetch_acquire_wait_ms,
            "FetchScheduler created"
        );
        Self {
            aimd: AimdController::new(aimd_config),
            inflight: DashMap::new(),
            data_store,
            cached_store,
            max_fetch_attempts,
            prefetch_max_fetch_attempts,
            request_timeout: Duration::from_secs(request_timeout_s),
            retry_base_delay: Duration::from_millis(retry_base_ms),
            retry_max_delay: Duration::from_millis(retry_max_ms),
            last_send_error_ms: AtomicU64::new(0),
            prefetch_reserve_slots,
            prefetch_acquire_wait_ms: AtomicU64::new(prefetch_acquire_wait_ms),
            metrics: FetchMetrics::new(),
            qos,
            transit_memory_budget,
        }
    }

    // ── Public API ──────────────────────────────────────────────────────

    /// Fetch a block, returning validated raw payload data.
    ///
    /// This is the **single entry-point** for all remote reads.  It handles:
    /// 1. L1/L2 cache check (fast path).
    /// 2. Singleflight dedup (only one S3 GET per `object_id`).
    /// 3. AIMD-governed concurrency for the actual HTTP request.
    /// 4. Retry with exponential backoff on transient errors.
    /// 5. Cache population on success.
    pub async fn fetch_block(&self, object_id: u64, keys: &[String]) -> Result<Bytes, FetchError> {
        self.fetch_block_with_class(
            object_id,
            keys,
            FetchClass::Foreground,
            LocalCacheHint::CheckFastPath,
        )
        .await
    }

    /// Fetch a block after the caller already confirmed a local cache miss.
    ///
    /// This preserves singleflight and cache population while skipping the
    /// eager pre-singleflight whole-block cache lookup, so the read path does
    /// not immediately repeat a just-failed direct `read_into()` probe.
    pub async fn fetch_block_after_local_miss(
        &self,
        object_id: u64,
        keys: &[String],
    ) -> Result<Bytes, FetchError> {
        self.fetch_block_with_class(
            object_id,
            keys,
            FetchClass::Foreground,
            LocalCacheHint::ConfirmedMiss,
        )
        .await
    }

    /// Best-effort prefetch fetch.
    ///
    /// Uses the same singleflight map as foreground reads but never consumes
    /// the last reserved foreground slots.
    pub async fn fetch_block_prefetch(
        &self,
        object_id: u64,
        keys: &[String],
    ) -> Result<Bytes, FetchError> {
        self.fetch_block_with_class(
            object_id,
            keys,
            FetchClass::Prefetch,
            LocalCacheHint::CheckFastPath,
        )
        .await
    }

    async fn fetch_block_with_class(
        &self,
        object_id: u64,
        keys: &[String],
        class: FetchClass,
        local_cache_hint: LocalCacheHint,
    ) -> Result<Bytes, FetchError> {
        // ── Fast path: cache ────────────────────────────────────────
        if matches!(local_cache_hint, LocalCacheHint::CheckFastPath) {
            if let Some(cached) = self.payload_store_get_async(object_id).await? {
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(cached);
            }
        }

        if matches!(class, FetchClass::Prefetch) && !self.prefetch_admission_open() {
            self.metrics.prefetch_skips.fetch_add(1, Ordering::Relaxed);
            return Err(FetchError::PrefetchSkipped);
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
                        self.metrics
                            .singleflight_joins
                            .fetch_add(1, Ordering::Relaxed);
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
                Action::Fetch => {
                    if matches!(local_cache_hint, LocalCacheHint::ConfirmedMiss) {
                        match self.payload_store_get_async(object_id).await {
                            Ok(Some(cached)) => {
                                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                                if let Some((_, tx)) = self.inflight.remove(&object_id) {
                                    let _ = tx.send(BroadcastResult::Success(cached.clone()));
                                }
                                return Ok(cached);
                            }
                            Ok(None) => {}
                            Err(error) => {
                                if let Some((_, tx)) = self.inflight.remove(&object_id) {
                                    let _ = tx.send(BroadcastResult::Failure);
                                }
                                return Err(error);
                            }
                        }
                    }
                    break;
                }
                Action::Wait(mut rx) => {
                    // Wait for the fetcher to finish (with a generous timeout
                    // to avoid infinite hangs).
                    let recv = tokio::time::timeout(Duration::from_secs(300), rx.recv()).await;
                    match recv {
                        Ok(Ok(BroadcastResult::Success(payload))) => {
                            return Ok(payload);
                        }
                        // Fetcher failed, channel closed, or timeout.
                        // Become a new fetcher ourselves.
                        Ok(Ok(BroadcastResult::Failure)) | Ok(Err(_)) | Err(_) => {
                            debug!(
                                object_id,
                                "singleflight: fetcher failed, becoming new fetcher"
                            );
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

        let result = self.do_fetch_with_retry(object_id, keys, class).await;

        // Notify waiters before cleanup removes the entry.
        if let Some((_, tx)) = self.inflight.remove(&object_id) {
            let broadcast_result = match &result {
                Ok(payload) => BroadcastResult::Success(payload.clone()),
                Err(_) => BroadcastResult::Failure,
            };
            let _ = tx.send(broadcast_result);
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
                debug!(
                    ok = ok_count,
                    not_found = err_count,
                    "FetchScheduler: warmup done ({WARMUP_CONCURRENCY} connections primed)"
                );
                Ok(())
            }
            Err(_) => {
                warn!(
                    timeout_ms = duration_to_millis_u64(timeout),
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

    /// Current effective AIMD concurrency limit.
    pub fn effective_limit(&self) -> usize {
        self.aimd.effective_limit()
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

    /// Current prefetch acquire-wait timeout in milliseconds.
    pub fn prefetch_acquire_wait_ms(&self) -> u64 {
        self.prefetch_acquire_wait_ms.load(Ordering::Acquire)
    }

    /// Adjust the prefetch acquire-wait timeout at runtime.
    /// `0` means "skip immediately" — prefetch attempts will not wait at all.
    pub fn set_prefetch_acquire_wait_ms(&self, ms: u64) {
        self.set_prefetch_wait_ms(ms);
    }

    /// Adjust the prefetch acquire-wait timeout at runtime.
    /// `0` means "skip immediately" — prefetch attempts will not wait at all.
    pub fn set_prefetch_wait_ms(&self, ms: u64) {
        self.prefetch_acquire_wait_ms.store(ms, Ordering::Release);
    }

    /// True if a send-request network error happened recently.
    pub fn recent_send_error(&self, within: Duration) -> bool {
        let ts = self.last_send_error_ms.load(Ordering::Relaxed);
        if ts == 0 {
            return false;
        }
        now_millis().saturating_sub(ts) <= duration_to_millis_u64(within)
    }

    #[cfg(test)]
    /// Insert an inflight singleflight sender for deterministic test coordination.
    pub(crate) fn test_insert_inflight_fetch(
        &self,
        object_id: u64,
    ) -> broadcast::Sender<BroadcastResult> {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        self.inflight.insert(object_id, tx.clone());
        tx
    }

    // ── Internal ────────────────────────────────────────────────────

    /// Fetch all remote fragments, decode them, reassemble, and populate cache.
    async fn do_fetch_with_retry(
        &self,
        object_id: u64,
        keys: &[String],
        class: FetchClass,
    ) -> Result<Bytes, FetchError> {
        let total_start = Instant::now();
        let result = self.do_fetch_with_retry_inner(object_id, keys, class).await;
        self.record_fetch_total(total_start.elapsed());
        result
    }

    async fn do_fetch_with_retry_inner(
        &self,
        object_id: u64,
        keys: &[String],
        class: FetchClass,
    ) -> Result<Bytes, FetchError> {
        let budget_bytes = fetch_transit_budget_bytes(keys.len());
        let budget_limit = self.transit_memory_budget.budget_bytes();
        if budget_bytes > budget_limit {
            return Err(FetchError::TransitBudgetTooSmall {
                required_bytes: budget_bytes,
                budget_bytes: budget_limit,
            });
        }
        let _transit_credit = match class {
            FetchClass::Foreground => {
                self.transit_memory_budget
                    .acquire_foreground(budget_bytes)
                    .await
            }
            FetchClass::Prefetch => {
                let wait = tokio::time::timeout(
                    Duration::from_millis(self.prefetch_acquire_wait_ms.load(Ordering::Acquire)),
                    self.transit_memory_budget.acquire_background(budget_bytes),
                )
                .await;
                match wait {
                    Ok(guard) => guard,
                    Err(_) => {
                        self.metrics.prefetch_skips.fetch_add(1, Ordering::Relaxed);
                        return Err(FetchError::PrefetchSkipped);
                    }
                }
            }
        };

        if let [key] = keys {
            let payload = self
                .fetch_fragment_with_retry(object_id, 0, key, class)
                .await?;
            validate_single_fragment_payload_len(object_id, payload.len())?;
            let cache_put_start = Instant::now();
            self.payload_store_put_async(object_id, payload.clone(), true)
                .await?;
            self.record_local_cache_put(cache_put_start.elapsed());
            return Ok(payload);
        }

        // Fetch fragments concurrently and copy each part into its final offset
        // as soon as it completes to avoid keeping both full `Vec<Bytes>` and
        // final payload in memory at once.
        let fragment_window = keys.len().min(MAX_FRAGMENT_FETCH_IN_FLIGHT);
        let mut in_flight = futures::stream::iter(keys.iter().cloned().enumerate())
            .map(|(fragment_index, key)| async move {
                self.fetch_fragment_with_retry(object_id, fragment_index, &key, class)
                    .await
                    .map(|part| (fragment_index, part))
            })
            .buffer_unordered(fragment_window.max(1));

        let mut assembled = vec![0u8; keys.len().saturating_mul(CLOUD_FRAGMENT_SIZE)];
        let mut lengths = vec![0usize; keys.len()];
        while let Some(next) = in_flight.next().await {
            let (fragment_index, part) = next?;
            let start = fragment_index.saturating_mul(CLOUD_FRAGMENT_SIZE);
            let end = start.saturating_add(part.len());
            if end > assembled.len() {
                return Err(FetchError::CorruptFragment {
                    object_id,
                    reason: "fragment bounds exceed assembled payload buffer".to_string(),
                });
            }
            let copy_start = Instant::now();
            assembled[start..end].copy_from_slice(&part);
            self.record_assemble_copy(copy_start.elapsed());
            lengths[fragment_index] = part.len();
        }

        let total_len =
            validate_cloud_fragment_layout(&lengths).map_err(|e| FetchError::CorruptFragment {
                object_id,
                reason: e.to_string(),
            })?;
        assembled.truncate(total_len);
        let payload = Bytes::from(assembled);

        // Populate cache (L1 + L2) with the reconstructed logical block.
        let cache_put_start = Instant::now();
        self.payload_store_put_async(object_id, payload.clone(), true)
            .await?;
        self.record_local_cache_put(cache_put_start.elapsed());

        Ok(payload)
    }

    async fn payload_store_get_async(&self, object_id: u64) -> Result<Option<Bytes>, FetchError> {
        let cached_store = self.cached_store.clone();
        let submitted_at = Instant::now();
        let (result, timing) = task::spawn_blocking(move || {
            let io_started = Instant::now();
            let handoff_ns = io_started
                .checked_duration_since(submitted_at)
                .unwrap_or_default()
                .to_nanos_u64();
            let result = cached_store.get(object_id);
            let backend_io_ns = io_started.elapsed().to_nanos_u64();
            (
                result,
                LocalCacheIoTiming {
                    handoff_ns,
                    backend_io_ns,
                },
            )
        })
        .await
        .map_err(|error| {
            FetchError::CacheAccess(format!("payload store get task failed: {error}"))
        })?;
        self.record_local_cache_io(timing);
        Ok(result)
    }

    async fn payload_store_put_async(
        &self,
        object_id: u64,
        payload: Bytes,
        evictable: bool,
    ) -> Result<(), FetchError> {
        let cached_store = self.cached_store.clone();
        let submitted_at = Instant::now();
        let (result, timing) = task::spawn_blocking(move || {
            let io_started = Instant::now();
            let handoff_ns = io_started
                .checked_duration_since(submitted_at)
                .unwrap_or_default()
                .to_nanos_u64();
            let result = cached_store.put(object_id, payload.as_ref(), evictable);
            let backend_io_ns = io_started.elapsed().to_nanos_u64();
            (
                result,
                LocalCacheIoTiming {
                    handoff_ns,
                    backend_io_ns,
                },
            )
        })
        .await
        .map_err(|error| {
            FetchError::CacheAccess(format!("payload store put task failed: {error}"))
        })?;
        self.record_local_cache_io(timing);
        result.map_err(|error| FetchError::CacheAccess(error.to_string()))
    }

    /// Perform S3 GET for one fragment with retries + AIMD.
    async fn fetch_fragment_with_retry(
        &self,
        object_id: u64,
        fragment_index: usize,
        key: &str,
        class: FetchClass,
    ) -> Result<Bytes, FetchError> {
        let mut last_err: Option<DataError> = None;
        let overall_start = Instant::now();
        let max_attempts = match class {
            FetchClass::Foreground => self.max_fetch_attempts,
            FetchClass::Prefetch => self.prefetch_max_fetch_attempts.max(1),
        };

        for attempt in 0..max_attempts {
            // Acquire an AIMD permit — blocks until the concurrency window
            // allows another in-flight request.
            let wait_start = Instant::now();
            let permit = match class {
                FetchClass::Foreground => self.aimd.acquire().await,
                FetchClass::Prefetch => {
                    let deadline = Instant::now()
                        + Duration::from_millis(
                            self.prefetch_acquire_wait_ms.load(Ordering::Acquire),
                        );
                    loop {
                        let notified = self.aimd.notified();
                        if let Some(permit) = self.try_acquire_prefetch_permit() {
                            break permit;
                        }
                        let now = Instant::now();
                        if now >= deadline {
                            self.metrics.prefetch_skips.fetch_add(1, Ordering::Relaxed);
                            return Err(FetchError::PrefetchSkipped);
                        }
                        let remaining = deadline.saturating_duration_since(now);
                        if tokio::time::timeout(remaining, notified).await.is_err() {
                            self.metrics.prefetch_skips.fetch_add(1, Ordering::Relaxed);
                            return Err(FetchError::PrefetchSkipped);
                        }
                    }
                }
            };
            let _fetch_qos =
                self.qos.acquire_fetch().await.map_err(|e| {
                    FetchError::CacheAccess(format!("fetch qos permit failed: {e}"))
                })?;
            let wait_ms = duration_to_millis_u64(wait_start.elapsed());
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
            let get_result =
                tokio::time::timeout(self.request_timeout, self.data_store.get_by_key(key)).await;
            let req_elapsed = req_start.elapsed();
            let req_ms = duration_to_millis_u64(req_elapsed);

            match get_result {
                Ok(Ok(compressed)) => {
                    self.record_s3_get(req_elapsed);
                    permit.success();
                    self.metrics.s3_fetches.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .bytes_downloaded
                        .fetch_add(compressed.len() as u64, Ordering::Relaxed);

                    debug!(
                        object_id,
                        fragment_index,
                        key,
                        bytes = compressed.len(),
                        req_ms,
                        total_ms = duration_to_millis_u64(overall_start.elapsed()),
                        attempt,
                        "S3 GET ok"
                    );

                    // Decode STFS object payload.
                    let decode_start = Instant::now();
                    let payload = read_object(compressed);
                    self.record_object_decode(decode_start.elapsed());
                    let payload = payload?;
                    return Ok(payload);
                }
                Ok(Err(e)) => {
                    self.record_s3_get(req_elapsed);
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
                    let is_send_error = is_send_request_error(&e);
                    if is_send_error {
                        self.last_send_error_ms
                            .store(now_millis(), Ordering::Relaxed);
                    }

                    let is_server_overload = match &e {
                        DataError::OpenDal(oe) => {
                            oe.kind() == ErrorKind::RateLimited
                                || (oe.is_temporary() && !is_send_error)
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

                    if attempt + 1 < max_attempts {
                        let retry_base = if is_send_error {
                            self.retry_base_delay.max(Duration::from_millis(200))
                        } else {
                            self.retry_base_delay
                        };
                        let delay = retry_base
                            .saturating_mul(2u32.saturating_pow(attempt))
                            .min(self.retry_max_delay);
                        let delay = jittered_delay(delay, object_id, attempt);
                        self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                        if matches!(class, FetchClass::Prefetch) {
                            debug!(
                                attempt = attempt + 1,
                                max = max_attempts,
                                key,
                                fragment_index,
                                req_ms,
                                delay_ms = duration_to_millis_u64(delay),
                                is_server_overload,
                                is_send_error,
                                error = %e,
                                "prefetch fetch failed, retrying"
                            );
                        } else {
                            warn!(
                                attempt = attempt + 1,
                                max = max_attempts,
                                key,
                                fragment_index,
                                req_ms,
                                delay_ms = duration_to_millis_u64(delay),
                                is_server_overload,
                                is_send_error,
                                error = %e,
                                "fetch failed, retrying"
                            );
                        }
                        drop(_fetch_qos);
                        tokio::time::sleep(delay).await;
                    } else {
                        if matches!(class, FetchClass::Prefetch) {
                            debug!(
                                attempts = max_attempts,
                                key,
                                fragment_index,
                                req_ms,
                                total_ms = duration_to_millis_u64(overall_start.elapsed()),
                                error = %e,
                                "prefetch exhausted retries"
                            );
                        } else {
                            error!(
                                attempts = max_attempts,
                                key,
                                fragment_index,
                                req_ms,
                                total_ms = duration_to_millis_u64(overall_start.elapsed()),
                                error = %e,
                                "fetch exhausted all retries"
                            );
                        }
                    }

                    last_err = Some(e);
                }
                Err(_elapsed) => {
                    self.record_s3_get(req_elapsed);
                    // Hard timeout — connection-level issue, NOT server
                    // overload.  Release permit neutrally so AIMD
                    // window stays intact.
                    drop(permit);
                    self.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);

                    let timeout_err = DataError::Storage(format!(
                        "request timeout after {}s",
                        self.request_timeout.as_secs()
                    ));

                    if attempt + 1 < max_attempts {
                        let delay = self
                            .retry_base_delay
                            .saturating_mul(2u32.saturating_pow(attempt))
                            .min(self.retry_max_delay);
                        self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                        if matches!(class, FetchClass::Prefetch) {
                            debug!(
                                attempt = attempt + 1,
                                max = max_attempts,
                                key,
                                fragment_index,
                                timeout_s = self.request_timeout.as_secs(),
                                delay_ms = duration_to_millis_u64(delay),
                                "prefetch timed out, retrying"
                            );
                        } else {
                            warn!(
                                attempt = attempt + 1,
                                max = max_attempts,
                                key,
                                fragment_index,
                                timeout_s = self.request_timeout.as_secs(),
                                delay_ms = duration_to_millis_u64(delay),
                                "fetch timed out, retrying (AIMD window unchanged)"
                            );
                        }
                        drop(_fetch_qos);
                        tokio::time::sleep(delay).await;
                    } else {
                        if matches!(class, FetchClass::Prefetch) {
                            debug!(
                                attempts = max_attempts,
                                key,
                                fragment_index,
                                total_ms = duration_to_millis_u64(overall_start.elapsed()),
                                "prefetch timed out, retries exhausted"
                            );
                        } else {
                            error!(
                                attempts = max_attempts,
                                key,
                                fragment_index,
                                total_ms = duration_to_millis_u64(overall_start.elapsed()),
                                "fetch timed out, all retries exhausted"
                            );
                        }
                    }

                    last_err = Some(timeout_err);
                }
            }
        }

        Err(FetchError::Exhausted {
            attempts: max_attempts,
            source: last_err.expect("at least one error"),
        })
    }

    fn prefetch_admission_open(&self) -> bool {
        let limit = self.aimd.effective_limit();
        if limit == 0 {
            return false;
        }
        let reserve = effective_prefetch_reserve_slots(self.prefetch_reserve_slots, limit);
        self.aimd.in_flight().saturating_add(reserve) < limit
    }

    fn try_acquire_prefetch_permit(&self) -> Option<AimdPermit<'_>> {
        if !self.prefetch_admission_open() {
            return None;
        }
        self.aimd.try_acquire()
    }
}

fn effective_prefetch_reserve_slots(configured_reserve: usize, limit: usize) -> usize {
    if limit <= 1 {
        return 0;
    }
    let half_window = (limit / 2).max(1);
    configured_reserve.min(limit - 1).min(half_window)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(duration_to_millis_u64)
        .unwrap_or(0)
}

fn fetch_transit_budget_bytes(fragment_count: usize) -> usize {
    let assembled_bytes = fragment_count.saturating_mul(CLOUD_FRAGMENT_SIZE);
    if fragment_count <= 1 {
        return assembled_bytes.max(1);
    }
    let fragment_window = fragment_count.min(MAX_FRAGMENT_FETCH_IN_FLIGHT);
    assembled_bytes
        .saturating_add(fragment_window.saturating_mul(CLOUD_FRAGMENT_SIZE))
        .max(1)
}

fn validate_single_fragment_payload_len(object_id: u64, len: usize) -> Result<(), FetchError> {
    if len > CLOUD_FRAGMENT_SIZE {
        return Err(FetchError::CorruptFragment {
            object_id,
            reason: format!("fragment 0 length {len} exceeds max {CLOUD_FRAGMENT_SIZE}"),
        });
    }
    Ok(())
}

trait DurationToU64 {
    fn to_nanos_u64(self) -> u64;
}

impl DurationToU64 for Duration {
    fn to_nanos_u64(self) -> u64 {
        u64::try_from(self.as_nanos()).unwrap_or(u64::MAX)
    }
}

fn duration_to_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn is_send_request_error(err: &DataError) -> bool {
    match err {
        DataError::OpenDal(oe) => {
            oe.kind() == ErrorKind::Unexpected
                && oe
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("send http request")
        }
        _ => false,
    }
}

fn jittered_delay(base: Duration, object_id: u64, attempt: u32) -> Duration {
    let base_ms = duration_to_millis_u64(base);
    if base_ms == 0 {
        return base;
    }
    let time = now_millis();
    let mut seed = object_id ^ ((attempt as u64) << 32) ^ time;
    seed ^= seed >> 12;
    seed ^= seed << 25;
    seed ^= seed >> 27;
    let jitter_pct = 70 + (seed % 61); // [70..130]
    let ms = base_ms.saturating_mul(jitter_pct).saturating_div(100);
    Duration::from_millis(ms.max(1))
}

// ─────────────────────────────────────────────────────────────────────────────
// Singleflight actions (internal)
// ─────────────────────────────────────────────────────────────────────────────

enum Action {
    Fetch,
    Wait(broadcast::Receiver<BroadcastResult>),
}

#[derive(Clone, Copy, Debug)]
enum FetchClass {
    Foreground,
    Prefetch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalCacheHint {
    CheckFastPath,
    ConfirmedMiss,
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
    /// Object identifier for the singleflight in-flight guard.
    object_id: u64,
    /// Shared singleflight map containing active sender channels.
    inflight: &'a DashMap<u64, broadcast::Sender<BroadcastResult>>,
    /// Whether cleanup should run on drop.
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
                // Tell waiters the fetch failed before the channel closes.
                let _ = tx.send(BroadcastResult::Failure);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StudioVfs, VfsConfig};
    use opendal::{Operator, services::Memory};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use studiofs_cache::{CachedStoreConfig, open_local_payload_backend};
    use studiofs_data::envelope::SFEC_MAGIC;
    use studiofs_data::object::{
        MAGIC as STFS_MAGIC, create_object_buffer, format_object_fragment_key,
    };
    use studiofs_data::store::{DataStoreConfig, DataStoreCryptoConfig};
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{EncryptionConfig, MetaFormat, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("fetch-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("fetch-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            encryption: studiofs_meta::EncryptionConfig::new(
                [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
            ),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn make_unstarted_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = studiofs_data::store::DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let rt = Arc::new(Runtime::new().expect("runtime"));
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        (rt, vfs, meta)
    }

    fn make_encrypted_fetch_scheduler(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (
        Arc<FetchScheduler>,
        Operator,
        Arc<CachedStore>,
        DataStoreConfig,
    ) {
        let volume_name = format!("fetch-encrypted-{volume_suffix}-{}", std::process::id());
        let config = DataStoreConfig {
            endpoint: "memory://".into(),
            bucket: "unit-test".into(),
            access_key: "access".into(),
            secret_key: "secret".into(),
            region: "us-east-1".into(),
            volume_name: volume_name.clone(),
            volume_uuid: format!("fetch-encrypted-uuid-{volume_suffix}"),
            crypto: DataStoreCryptoConfig::from(EncryptionConfig::new([0x35; 32])),
        };
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let data_store = Arc::new(
            DataStore::with_operator_for_test(&config, raw.clone()).expect("test data store"),
        );
        let cache_root = tmp.path().join("fetch-cache");
        let opened = open_local_payload_backend(&cache_root, &config.volume_uuid)
            .expect("open local payload backend");
        let (cached_store, _upload_rx) =
            CachedStore::open(CachedStoreConfig::new(&cache_root), opened.backend)
                .expect("open cached store");
        let fetch_config = VfsConfig {
            fetch_max_attempts: 1,
            prefetch_fetch_max_attempts: 1,
            fetch_request_timeout_secs: 5,
            fetch_retry_base_ms: 1,
            fetch_retry_max_ms: 1,
            fetch_prefetch_reserve: 0,
            fetch_prefetch_acquire_wait_ms: 50,
            ..VfsConfig::default()
        };
        let scheduler = Arc::new(FetchScheduler::new(
            data_store,
            cached_store.clone(),
            AimdConfig::fetch_default(),
            0,
            Arc::new(QosController::from_config(&fetch_config)),
            TransitMemoryBudget::new(64 * 1024 * 1024),
            &fetch_config,
        ));
        (scheduler, raw, cached_store, config)
    }

    fn fragment_key(config: &DataStoreConfig, object_id: u64, fragment_index: u8) -> String {
        format_object_fragment_key(
            &format!("{}/objects", config.volume_name),
            object_id,
            fragment_index,
        )
    }

    async fn put_encrypted_fragment(
        scheduler: &FetchScheduler,
        raw: &Operator,
        key: &str,
        payload: Bytes,
    ) -> Bytes {
        let stfs = create_object_buffer(payload).expect("create STFS object");
        assert!(stfs.to_bytes().starts_with(STFS_MAGIC));

        scheduler
            .data_store
            .put_by_key(key, stfs.clone())
            .await
            .expect("put encrypted fragment");
        let remote = raw.read(key).await.expect("read raw remote").to_bytes();
        assert!(remote.starts_with(SFEC_MAGIC));
        assert!(!remote.starts_with(STFS_MAGIC));
        remote
    }

    #[tokio::test]
    async fn foreground_fetch_decrypts_one_remote_object_and_caches_plaintext_payload() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (scheduler, raw, cached_store, config) =
            make_encrypted_fetch_scheduler(&tmp, "foreground");
        let object_id = 0x3531;
        let key = fragment_key(&config, object_id, 0);
        let expected = Bytes::from_static(b"foreground plaintext payload");
        let remote = put_encrypted_fragment(&scheduler, &raw, &key, expected.clone()).await;

        let fetched = scheduler
            .fetch_block(object_id, std::slice::from_ref(&key))
            .await
            .expect("foreground fetch should decrypt through DataStore");

        assert_eq!(fetched, expected);
        assert_ne!(remote.as_ref(), expected.as_ref());
        assert_eq!(
            cached_store.get(object_id).expect("cache entry"),
            expected,
            "fetch cache should hold plaintext payload bytes, not the SFEC object"
        );
        assert_eq!(scheduler.metrics.s3_fetches.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cache_hit_read_does_not_touch_encrypted_remote_object() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (scheduler, raw, cached_store, config) = make_encrypted_fetch_scheduler(&tmp, "cache");
        let object_id = 0x3532;
        let key = fragment_key(&config, object_id, 0);
        let cached = Bytes::from_static(b"warm plaintext payload");
        cached_store
            .put(object_id, cached.as_ref(), true)
            .expect("seed plaintext cache");
        raw.write(&key, Bytes::from_static(b"not an encrypted object"))
            .await
            .expect("seed corrupt remote object");

        let fetched = scheduler
            .fetch_block(object_id, std::slice::from_ref(&key))
            .await
            .expect("warm cache read should bypass remote decrypt");

        assert_eq!(fetched, cached);
        assert_eq!(scheduler.metrics.cache_hits.load(Ordering::Relaxed), 1);
        assert_eq!(scheduler.metrics.s3_fetches.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn prefetch_uses_same_decrypt_path_and_caches_plaintext_payload() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (scheduler, raw, cached_store, config) =
            make_encrypted_fetch_scheduler(&tmp, "prefetch");
        let object_id = 0x3533;
        let key = fragment_key(&config, object_id, 0);
        let expected = Bytes::from_static(b"prefetch plaintext payload");
        put_encrypted_fragment(&scheduler, &raw, &key, expected.clone()).await;

        let fetched = scheduler
            .fetch_block_prefetch(object_id, std::slice::from_ref(&key))
            .await
            .expect("prefetch should decrypt through DataStore");

        assert_eq!(fetched, expected);
        assert_eq!(
            cached_store.get(object_id).expect("prefetch cache entry"),
            expected
        );
        assert_eq!(scheduler.metrics.s3_fetches.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn random_block_fetch_only_decrypts_requested_remote_object() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (scheduler, raw, cached_store, config) = make_encrypted_fetch_scheduler(&tmp, "random");
        let object_id = 0x3534;
        let requested_key = fragment_key(&config, object_id, 0);
        let unrelated_object_id = object_id + 1;
        let unrelated_key = fragment_key(&config, unrelated_object_id, 0);
        let expected = Bytes::from_static(b"random block plaintext payload");
        put_encrypted_fragment(&scheduler, &raw, &requested_key, expected.clone()).await;
        raw.write(
            &unrelated_key,
            Bytes::from_static(b"corrupt sibling ciphertext"),
        )
        .await
        .expect("seed corrupt adjacent block");

        let fetched = scheduler
            .fetch_block_after_local_miss(object_id, std::slice::from_ref(&requested_key))
            .await
            .expect("random read should fetch only the requested block object");

        assert_eq!(fetched, expected);
        assert_eq!(
            cached_store
                .get(object_id)
                .expect("random read cache entry"),
            expected
        );
        assert_eq!(scheduler.metrics.s3_fetches.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn corrupt_or_swapped_encrypted_objects_fail_before_cache_population() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (scheduler, raw, cached_store, config) =
            make_encrypted_fetch_scheduler(&tmp, "integrity");
        let corrupt_object_id = 0x3535;
        let corrupt_key = fragment_key(&config, corrupt_object_id, 0);
        let mut corrupt_remote = put_encrypted_fragment(
            &scheduler,
            &raw,
            &corrupt_key,
            Bytes::from_static(b"corrupt me"),
        )
        .await
        .to_vec();
        let last = corrupt_remote.len() - 1;
        corrupt_remote[last] ^= 0x55;
        raw.write(&corrupt_key, Bytes::from(corrupt_remote))
            .await
            .expect("write corrupted ciphertext");

        let corrupt_error = scheduler
            .fetch_block(corrupt_object_id, std::slice::from_ref(&corrupt_key))
            .await
            .expect_err("corrupted encrypted object should fail");
        assert!(matches!(
            corrupt_error,
            FetchError::Exhausted {
                source: DataError::Crypto(_),
                ..
            }
        ));
        assert!(cached_store.get(corrupt_object_id).is_none());

        let source_object_id = 0x3536;
        let target_object_id = 0x3537;
        let source_key = fragment_key(&config, source_object_id, 0);
        let target_key = fragment_key(&config, target_object_id, 0);
        let swapped_ciphertext = put_encrypted_fragment(
            &scheduler,
            &raw,
            &source_key,
            Bytes::from_static(b"source plaintext"),
        )
        .await;
        raw.write(&target_key, swapped_ciphertext)
            .await
            .expect("swap ciphertext to wrong key");

        let swap_error = scheduler
            .fetch_block(target_object_id, std::slice::from_ref(&target_key))
            .await
            .expect_err("storage-key AAD should reject swapped ciphertext");
        assert!(matches!(
            swap_error,
            FetchError::Exhausted {
                source: DataError::Crypto(_),
                ..
            }
        ));
        assert!(cached_store.get(target_object_id).is_none());
    }

    #[test]
    fn singleflight_waiter_uses_handoff_payload_without_cache_reread() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "singleflight-handoff-payload");

        rt.block_on(async {
            let scheduler = vfs.fetch_scheduler().clone();
            let object_id = 0xFA77;
            let keys = vec!["unused/test/key".to_string()];
            let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
            scheduler.inflight.insert(object_id, tx.clone());

            let io_calls_before = scheduler.metrics.local_cache_io_calls.load(Ordering::Relaxed);
            let joins_before = scheduler.metrics.singleflight_joins.load(Ordering::Relaxed);

            let task = tokio::spawn({
                let scheduler = scheduler.clone();
                let keys = keys.clone();
                async move { scheduler.fetch_block_after_local_miss(object_id, &keys).await }
            });

            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if scheduler.metrics.singleflight_joins.load(Ordering::Relaxed) > joins_before {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("fetch should join singleflight waiter");

            assert_eq!(
                scheduler.metrics.local_cache_io_calls.load(Ordering::Relaxed),
                io_calls_before,
                "confirmed local misses should not issue an eager whole-block cache get before waiting"
            );

            let _ = tx.send(BroadcastResult::Success(Bytes::from_static(
                b"payload-from-singleflight",
            )));

            let data = task
                .await
                .expect("fetch task should join cleanly")
                .expect("waiter should receive the singleflight payload");
            assert_eq!(data.as_ref(), b"payload-from-singleflight");
            assert_eq!(
                scheduler.metrics.local_cache_io_calls.load(Ordering::Relaxed),
                io_calls_before,
                "waiter should not spawn a blocking cache reread after singleflight notification"
            );
        });
    }

    #[test]
    fn fetch_block_after_local_miss_rechecks_cache_when_becoming_fetcher() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "recheck-cache-before-fetch");

        rt.block_on(async {
            let scheduler = vfs.fetch_scheduler().clone();
            let object_id = 0xFA88;
            let keys = vec!["unused/test/key".to_string()];
            let io_calls_before = scheduler
                .metrics
                .local_cache_io_calls
                .load(Ordering::Relaxed);

            vfs.local_payload_backend()
                .put(object_id, b"payload-from-cache", false)
                .expect("seed cache for fetcher recheck");

            let data = scheduler
                .fetch_block_after_local_miss(object_id, &keys)
                .await
                .expect("fetcher-side cache recheck should avoid remote fetch");
            assert_eq!(data.as_ref(), b"payload-from-cache");
            assert_eq!(
                scheduler
                    .metrics
                    .local_cache_io_calls
                    .load(Ordering::Relaxed),
                io_calls_before + 1,
                "fetcher should do exactly one cache get when rechecking after a confirmed miss"
            );
        });
    }

    #[test]
    fn single_fragment_fetch_budget_does_not_reserve_assembled_copy_headroom() {
        assert_eq!(fetch_transit_budget_bytes(1), CLOUD_FRAGMENT_SIZE);
        assert!(
            fetch_transit_budget_bytes(2) > 2 * CLOUD_FRAGMENT_SIZE,
            "multi-fragment fetches still need assembled payload plus fragment headroom"
        );
    }

    #[test]
    fn single_fragment_payload_validation_rejects_oversized_payload() {
        let object_id = 0x5151;
        let err =
            validate_single_fragment_payload_len(object_id, CLOUD_FRAGMENT_SIZE + 1).unwrap_err();

        assert!(matches!(
            err,
            FetchError::CorruptFragment {
                object_id: found,
                ..
            } if found == object_id
        ));
    }

    #[test]
    fn default_prefetch_admission_keeps_several_background_slots_available() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "default-prefetch-admission-slots");
        let scheduler = vfs.fetch_scheduler().clone();

        let mut permits = Vec::new();
        while let Some(permit) = scheduler.try_acquire_prefetch_permit() {
            permits.push(permit);
        }

        assert!(
            permits.len() >= 8,
            "default cold sequential prefetch should be able to fill the configured 8-worker pipeline, got {} permits",
            permits.len()
        );
    }
}
