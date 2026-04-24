use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

fn saturating_u64_from_u128(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

const LATENCY_BUCKET_UPPER_NS: [u64; 16] = [
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_000_000_000,
    5_000_000_000,
];

const LATENCY_BUCKET_COUNT: usize = LATENCY_BUCKET_UPPER_NS.len() + 1;

fn update_peak(peak: &AtomicUsize, value: usize) {
    peak.fetch_max(value, Ordering::Relaxed);
}

#[derive(Debug, Clone)]
pub struct LatencyHistogramSnapshot {
    pub bucket_upper_bounds_ns: Vec<u64>,
    pub bucket_counts: Vec<u64>,
}

#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKET_COUNT],
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl LatencyHistogram {
    pub fn observe_ns(&self, elapsed_ns: u64) {
        let idx = LATENCY_BUCKET_UPPER_NS
            .iter()
            .position(|upper| elapsed_ns <= *upper)
            .unwrap_or(LATENCY_BUCKET_COUNT - 1);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_duration(&self, elapsed: Duration) {
        let ns = saturating_u64_from_u128(elapsed.as_nanos());
        self.observe_ns(ns);
    }

    pub fn snapshot(&self) -> LatencyHistogramSnapshot {
        LatencyHistogramSnapshot {
            bucket_upper_bounds_ns: LATENCY_BUCKET_UPPER_NS.to_vec(),
            bucket_counts: self
                .buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
        }
    }

    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone)]
pub struct MaterializeBatchStatsSnapshot {
    pub batches: u64,
    pub payloads: u64,
    pub bytes: u64,
    pub inflight_batches: usize,
    pub inflight_batches_peak: usize,
    pub duration_histogram: LatencyHistogramSnapshot,
}

#[derive(Debug, Default)]
pub struct MaterializeBatchStats {
    batches: AtomicU64,
    payloads: AtomicU64,
    bytes: AtomicU64,
    inflight_batches: AtomicUsize,
    inflight_batches_peak: AtomicUsize,
    duration_histogram: LatencyHistogram,
}

impl MaterializeBatchStats {
    pub fn record_batch(&self, payloads: u64, bytes: u64, elapsed: Duration) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.payloads.fetch_add(payloads, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.duration_histogram.observe_duration(elapsed);
    }

    pub fn enter_inflight(&self) {
        let inflight = self.inflight_batches.fetch_add(1, Ordering::Relaxed) + 1;
        update_peak(&self.inflight_batches_peak, inflight);
    }

    pub fn leave_inflight(&self) {
        let _ = self
            .inflight_batches
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                if v > 0 { Some(v - 1) } else { Some(0) }
            });
    }

    pub fn snapshot(&self) -> MaterializeBatchStatsSnapshot {
        MaterializeBatchStatsSnapshot {
            batches: self.batches.load(Ordering::Relaxed),
            payloads: self.payloads.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            inflight_batches: self.inflight_batches.load(Ordering::Relaxed),
            inflight_batches_peak: self.inflight_batches_peak.load(Ordering::Relaxed),
            duration_histogram: self.duration_histogram.snapshot(),
        }
    }

    pub fn reset(&self) {
        self.batches.store(0, Ordering::Relaxed);
        self.payloads.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
        self.inflight_batches.store(0, Ordering::Relaxed);
        self.inflight_batches_peak.store(0, Ordering::Relaxed);
        self.duration_histogram.reset();
    }
}

#[derive(Debug, Clone)]
pub struct UploadQueueStatsSnapshot {
    pub pending_depth: usize,
    pub pending_depth_peak: usize,
    pub submitted: u64,
    pub completed: u64,
    pub requeued: u64,
    pub canceled: u64,
}

#[derive(Debug, Default)]
pub struct UploadQueueStats {
    pending_depth_peak: AtomicUsize,
    submitted: AtomicU64,
    completed: AtomicU64,
    requeued: AtomicU64,
    canceled: AtomicU64,
}

impl UploadQueueStats {
    pub fn record_submit(&self, pending_depth: usize) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
        update_peak(&self.pending_depth_peak, pending_depth);
    }

    pub fn record_complete(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_requeue(&self) {
        self.requeued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cancel(&self) {
        self.canceled.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self, pending_depth: usize) -> UploadQueueStatsSnapshot {
        UploadQueueStatsSnapshot {
            pending_depth,
            pending_depth_peak: self.pending_depth_peak.load(Ordering::Relaxed),
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            requeued: self.requeued.load(Ordering::Relaxed),
            canceled: self.canceled.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.pending_depth_peak.store(0, Ordering::Relaxed);
        self.submitted.store(0, Ordering::Relaxed);
        self.completed.store(0, Ordering::Relaxed);
        self.requeued.store(0, Ordering::Relaxed);
        self.canceled.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct BlockingTaskStatsSnapshot {
    pub active: usize,
    pub active_peak: usize,
    pub submitted: u64,
    pub started: u64,
    pub completed: u64,
    pub queue_wait_total: Duration,
    pub queue_wait_max: Duration,
    pub queue_wait_histogram: LatencyHistogramSnapshot,
    pub execution_total: Duration,
    pub execution_max: Duration,
    pub execution_histogram: LatencyHistogramSnapshot,
}

#[derive(Debug, Clone)]
pub struct BlockingTaskKindStatsSnapshot {
    pub task_kind: String,
    pub stats: BlockingTaskStatsSnapshot,
}

#[derive(Debug, Clone)]
pub struct BlockingTaskMetricsSnapshot {
    pub total: BlockingTaskStatsSnapshot,
    pub by_kind: Vec<BlockingTaskKindStatsSnapshot>,
}

#[derive(Debug, Default)]
pub struct BlockingTaskStats {
    active: AtomicUsize,
    active_peak: AtomicUsize,
    submitted: AtomicU64,
    started: AtomicU64,
    completed: AtomicU64,
    queue_wait_total_ns: AtomicU64,
    queue_wait_max_ns: AtomicU64,
    queue_wait_histogram: LatencyHistogram,
    execution_total_ns: AtomicU64,
    execution_max_ns: AtomicU64,
    execution_histogram: LatencyHistogram,
}

#[must_use = "guard tracks active task count and execution time for metrics accuracy"]
pub struct BlockingTaskExecutionGuard<'a> {
    stats: &'a BlockingTaskStats,
    started_at: Instant,
}

impl BlockingTaskStats {
    pub fn record_submit(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_start(&self, queue_wait: Duration) -> BlockingTaskExecutionGuard<'_> {
        let wait_ns = saturating_u64_from_u128(queue_wait.as_nanos());
        self.started.fetch_add(1, Ordering::Relaxed);
        self.queue_wait_total_ns
            .fetch_add(wait_ns, Ordering::Relaxed);
        self.queue_wait_max_ns.fetch_max(wait_ns, Ordering::Relaxed);
        self.queue_wait_histogram.observe_ns(wait_ns);

        let active_now = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        update_peak(&self.active_peak, active_now);

        BlockingTaskExecutionGuard {
            stats: self,
            started_at: Instant::now(),
        }
    }

    pub fn snapshot(&self) -> BlockingTaskStatsSnapshot {
        BlockingTaskStatsSnapshot {
            active: self.active.load(Ordering::Relaxed),
            active_peak: self.active_peak.load(Ordering::Relaxed),
            submitted: self.submitted.load(Ordering::Relaxed),
            started: self.started.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            queue_wait_total: duration_from_ns(self.queue_wait_total_ns.load(Ordering::Relaxed)),
            queue_wait_max: duration_from_ns(self.queue_wait_max_ns.load(Ordering::Relaxed)),
            queue_wait_histogram: self.queue_wait_histogram.snapshot(),
            execution_total: duration_from_ns(self.execution_total_ns.load(Ordering::Relaxed)),
            execution_max: duration_from_ns(self.execution_max_ns.load(Ordering::Relaxed)),
            execution_histogram: self.execution_histogram.snapshot(),
        }
    }

    /// Best-effort reset of observability counters.
    pub fn reset(&self) {
        self.active.store(0, Ordering::Relaxed);
        self.active_peak.store(0, Ordering::Relaxed);
        self.submitted.store(0, Ordering::Relaxed);
        self.started.store(0, Ordering::Relaxed);
        self.completed.store(0, Ordering::Relaxed);
        self.queue_wait_total_ns.store(0, Ordering::Relaxed);
        self.queue_wait_max_ns.store(0, Ordering::Relaxed);
        self.execution_total_ns.store(0, Ordering::Relaxed);
        self.execution_max_ns.store(0, Ordering::Relaxed);
        self.queue_wait_histogram.reset();
        self.execution_histogram.reset();
    }
}

impl Drop for BlockingTaskExecutionGuard<'_> {
    fn drop(&mut self) {
        self.stats.completed.fetch_add(1, Ordering::Relaxed);
        let elapsed_ns = saturating_u64_from_u128(self.started_at.elapsed().as_nanos());
        self.stats
            .execution_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.stats
            .execution_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
        self.stats.execution_histogram.observe_ns(elapsed_ns);

        let _ = self
            .stats
            .active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                if v > 0 { Some(v - 1) } else { Some(0) }
            });
    }
}

#[derive(Debug, Clone)]
pub struct CachePipelineMetricsSnapshot {
    pub materialize_batches: MaterializeBatchStatsSnapshot,
    pub upload_queue: UploadQueueStatsSnapshot,
    pub blocking_tasks: BlockingTaskMetricsSnapshot,
}

#[derive(Debug, Default)]
pub struct CachePipelineMetrics {
    pub materialize_batches: MaterializeBatchStats,
    pub upload_queue: UploadQueueStats,
}

impl CachePipelineMetrics {
    pub fn snapshot(&self, pending_uploads: usize) -> CachePipelineMetricsSnapshot {
        CachePipelineMetricsSnapshot {
            materialize_batches: self.materialize_batches.snapshot(),
            upload_queue: self.upload_queue.snapshot(pending_uploads),
            blocking_tasks: empty_blocking_task_metrics(),
        }
    }

    pub fn reset(&self) {
        self.materialize_batches.reset();
        self.upload_queue.reset();
    }
}

fn empty_latency_histogram_snapshot() -> LatencyHistogramSnapshot {
    LatencyHistogramSnapshot {
        bucket_upper_bounds_ns: LATENCY_BUCKET_UPPER_NS.to_vec(),
        bucket_counts: vec![0; LATENCY_BUCKET_COUNT],
    }
}

fn duration_from_ns(ns: u64) -> Duration {
    Duration::from_nanos(ns)
}

fn empty_blocking_task_stats() -> BlockingTaskStatsSnapshot {
    BlockingTaskStatsSnapshot {
        active: 0,
        active_peak: 0,
        submitted: 0,
        started: 0,
        completed: 0,
        queue_wait_total: Duration::ZERO,
        queue_wait_max: Duration::ZERO,
        queue_wait_histogram: empty_latency_histogram_snapshot(),
        execution_total: Duration::ZERO,
        execution_max: Duration::ZERO,
        execution_histogram: empty_latency_histogram_snapshot(),
    }
}

fn empty_blocking_task_metrics() -> BlockingTaskMetricsSnapshot {
    BlockingTaskMetricsSnapshot {
        total: empty_blocking_task_stats(),
        by_kind: Vec::new(),
    }
}
