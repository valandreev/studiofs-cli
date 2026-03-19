use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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
        let ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
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
pub struct IoStatsSnapshot {
    pub ops: u64,
    pub bytes: u64,
    pub total_latency_ns: u64,
    pub max_latency_ns: u64,
    pub latency_histogram: LatencyHistogramSnapshot,
}

#[derive(Debug, Default)]
pub struct IoStats {
    ops: AtomicU64,
    bytes: AtomicU64,
    total_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
    latency_histogram: LatencyHistogram,
}

impl IoStats {
    pub fn record(&self, bytes: u64, elapsed: Duration) {
        let elapsed_ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.record_ns(bytes, elapsed_ns);
    }

    pub fn record_ns(&self, bytes: u64, elapsed_ns: u64) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.total_latency_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.latency_histogram.observe_ns(elapsed_ns);

        self.max_latency_ns.fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> IoStatsSnapshot {
        IoStatsSnapshot {
            ops: self.ops.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            total_latency_ns: self.total_latency_ns.load(Ordering::Relaxed),
            max_latency_ns: self.max_latency_ns.load(Ordering::Relaxed),
            latency_histogram: self.latency_histogram.snapshot(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MaterializeBatchStatsSnapshot {
    pub batches: u64,
    pub blocks: u64,
    pub bytes: u64,
    pub inflight_batches: usize,
    pub inflight_batches_peak: usize,
    pub duration_histogram: LatencyHistogramSnapshot,
}

#[derive(Debug, Default)]
pub struct MaterializeBatchStats {
    batches: AtomicU64,
    blocks: AtomicU64,
    bytes: AtomicU64,
    inflight_batches: AtomicUsize,
    inflight_batches_peak: AtomicUsize,
    duration_histogram: LatencyHistogram,
}

impl MaterializeBatchStats {
    pub fn record_batch(&self, blocks: u64, bytes: u64, elapsed: Duration) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.blocks.fetch_add(blocks, Ordering::Relaxed);
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
            blocks: self.blocks.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            inflight_batches: self.inflight_batches.load(Ordering::Relaxed),
            inflight_batches_peak: self.inflight_batches_peak.load(Ordering::Relaxed),
            duration_histogram: self.duration_histogram.snapshot(),
        }
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
}

#[derive(Debug, Clone)]
pub struct BlockingTaskStatsSnapshot {
    pub active: usize,
    pub active_peak: usize,
    pub submitted: u64,
    pub started: u64,
    pub completed: u64,
    pub queue_wait_total_ns: u64,
    pub queue_wait_max_ns: u64,
    pub queue_wait_histogram: LatencyHistogramSnapshot,
    pub execution_total_ns: u64,
    pub execution_max_ns: u64,
    pub execution_histogram: LatencyHistogramSnapshot,
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

pub struct BlockingTaskExecutionGuard<'a> {
    stats: &'a BlockingTaskStats,
    started_at: Instant,
}

impl BlockingTaskStats {
    pub fn record_submit(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_start(&self, queue_wait: Duration) -> BlockingTaskExecutionGuard<'_> {
        let wait_ns = queue_wait.as_nanos().min(u64::MAX as u128) as u64;
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
            queue_wait_total_ns: self.queue_wait_total_ns.load(Ordering::Relaxed),
            queue_wait_max_ns: self.queue_wait_max_ns.load(Ordering::Relaxed),
            queue_wait_histogram: self.queue_wait_histogram.snapshot(),
            execution_total_ns: self.execution_total_ns.load(Ordering::Relaxed),
            execution_max_ns: self.execution_max_ns.load(Ordering::Relaxed),
            execution_histogram: self.execution_histogram.snapshot(),
        }
    }

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
        let elapsed_ns = self.started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
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
    pub l2_read: IoStatsSnapshot,
    pub l2_write: IoStatsSnapshot,
    pub materialize_batches: MaterializeBatchStatsSnapshot,
    pub upload_queue: UploadQueueStatsSnapshot,
    pub blocking_tasks: BlockingTaskStatsSnapshot,
}

#[derive(Debug, Default)]
pub struct CachePipelineMetrics {
    pub l2_read: IoStats,
    pub l2_write: IoStats,
    pub materialize_batches: MaterializeBatchStats,
    pub upload_queue: UploadQueueStats,
}

impl CachePipelineMetrics {
    pub fn snapshot(&self, pending_uploads: usize) -> CachePipelineMetricsSnapshot {
        CachePipelineMetricsSnapshot {
            l2_read: self.l2_read.snapshot(),
            l2_write: self.l2_write.snapshot(),
            materialize_batches: self.materialize_batches.snapshot(),
            upload_queue: self.upload_queue.snapshot(pending_uploads),
            blocking_tasks: crate::runtime::blocking_task_stats_snapshot(),
        }
    }
}
