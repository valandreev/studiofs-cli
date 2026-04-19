use std::time::Duration;

use super::result::{
    AdvancedMetricsReport, CacheLevelReport, CacheLevelsReport, CachePipelineReport,
    LatencyHistogramReport, OpReport, OperationLogReport, SizeClassBoundaryReport,
    SizeClassOpReport, SizeClassStatsReport, StreamResultReport,
};
use super::workload::{SizeClass, SizeClassBounds};

const MAX_LATENCY_SAMPLES: usize = 200_000;

#[derive(Default, Clone)]
pub(super) struct LatencySamples {
    samples_ns: Vec<u64>,
    total_ops: u64,
    total_bytes: u64,
    total_latency_ns: u128,
    sample_keep_every: u64,
}

impl LatencySamples {
    pub(super) fn record(&mut self, elapsed: Duration, bytes: u64) {
        let ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.total_latency_ns = self.total_latency_ns.saturating_add(ns as u128);
        self.total_ops = self.total_ops.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(bytes);

        let keep_every = self.sample_keep_every.max(1);
        if (self.total_ops - 1).is_multiple_of(keep_every) {
            self.samples_ns.push(ns);
        }

        if self.samples_ns.len() > MAX_LATENCY_SAMPLES {
            self.samples_ns = self.samples_ns.iter().step_by(2).copied().collect();
            self.sample_keep_every = keep_every.saturating_mul(2);
        }
    }

    fn total_duration_ms(&self) -> f64 {
        self.total_latency_ns as f64 / 1_000_000.0
    }

    fn avg_ns(&self) -> f64 {
        if self.total_ops == 0 {
            0.0
        } else {
            self.total_latency_ns as f64 / self.total_ops as f64
        }
    }

    fn percentile_from_sorted_ns(samples: &[u64], p: f64) -> u64 {
        if samples.is_empty() {
            return 0;
        }
        let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
        samples[idx.min(samples.len().saturating_sub(1))]
    }

    pub(super) fn to_report(&self, wall_elapsed_ms: f64) -> OpReport {
        let total_ms = self.total_duration_ms();
        let avg_ms = self.avg_ns() / 1_000_000.0;
        let mut sorted_samples = self.samples_ns.clone();
        sorted_samples.sort_unstable();
        let p50_ms = Self::percentile_from_sorted_ns(&sorted_samples, 0.50) as f64 / 1_000_000.0;
        let p95_ms = Self::percentile_from_sorted_ns(&sorted_samples, 0.95) as f64 / 1_000_000.0;
        let p99_ms = Self::percentile_from_sorted_ns(&sorted_samples, 0.99) as f64 / 1_000_000.0;

        let throughput_bytes_per_sec = if wall_elapsed_ms <= 0.0 {
            0.0
        } else {
            self.total_bytes as f64 / (wall_elapsed_ms / 1000.0)
        };

        OpReport {
            operations: self.total_ops,
            bytes: self.total_bytes,
            elapsed_ms: total_ms,
            avg_latency_ms: avg_ms,
            p50_ms,
            p95_ms,
            p99_ms,
            throughput_bytes_per_sec,
            operations_log: Vec::new(),
        }
    }

    fn to_size_class_report(&self, class: SizeClass, wall_elapsed_ms: f64) -> SizeClassOpReport {
        let report = self.to_report(wall_elapsed_ms);
        SizeClassOpReport {
            class: class.label().to_string(),
            operations: report.operations,
            bytes: report.bytes,
            avg_latency_ms: report.avg_latency_ms,
            p50_ms: report.p50_ms,
            p95_ms: report.p95_ms,
            p99_ms: report.p99_ms,
            throughput_bytes_per_sec: report.throughput_bytes_per_sec,
        }
    }

    fn to_histogram_report(&self) -> LatencyHistogramReport {
        let bucket_upper_bounds_ns = vec![
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
        let mut bucket_counts = vec![0u64; bucket_upper_bounds_ns.len() + 1];
        for sample in &self.samples_ns {
            let idx = bucket_upper_bounds_ns
                .iter()
                .position(|upper| sample <= upper)
                .unwrap_or(bucket_counts.len() - 1);
            bucket_counts[idx] = bucket_counts[idx].saturating_add(1);
        }
        LatencyHistogramReport {
            bucket_upper_bounds_ns,
            bucket_counts,
        }
    }

    fn downsample_in_place(samples: &mut Vec<u64>, step: usize) {
        if step <= 1 || samples.len() <= 1 {
            return;
        }
        *samples = samples.iter().step_by(step).copied().collect();
    }

    fn effective_keep_every(&self) -> u64 {
        if self.total_ops == 0 {
            1
        } else {
            self.sample_keep_every.max(1)
        }
    }

    pub(super) fn merge_from_ref(&mut self, other: &Self) {
        let self_keep = self.effective_keep_every();
        let other_keep = other.effective_keep_every();
        let mut target_keep = self_keep.max(other_keep);

        if self_keep < target_keep {
            let step = target_keep.div_ceil(self_keep) as usize;
            Self::downsample_in_place(&mut self.samples_ns, step);
        }

        if other_keep < target_keep {
            let step = target_keep.div_ceil(other_keep) as usize;
            self.samples_ns
                .extend(other.samples_ns.iter().step_by(step).copied());
        } else {
            self.samples_ns.extend_from_slice(&other.samples_ns);
        }

        self.total_ops = self.total_ops.saturating_add(other.total_ops);
        self.total_bytes = self.total_bytes.saturating_add(other.total_bytes);
        self.total_latency_ns = self.total_latency_ns.saturating_add(other.total_latency_ns);

        while self.samples_ns.len() > MAX_LATENCY_SAMPLES {
            Self::downsample_in_place(&mut self.samples_ns, 2);
            target_keep = target_keep.saturating_mul(2);
        }
        self.sample_keep_every = if self.total_ops == 0 { 0 } else { target_keep };
    }
}

#[derive(Default, Clone)]
pub(super) struct SizeClassLatencySamples {
    small: LatencySamples,
    medium: LatencySamples,
    large: LatencySamples,
}

impl SizeClassLatencySamples {
    fn get_mut(&mut self, class: SizeClass) -> &mut LatencySamples {
        match class {
            SizeClass::Small => &mut self.small,
            SizeClass::Medium => &mut self.medium,
            SizeClass::Large => &mut self.large,
        }
    }

    fn to_reports(&self, wall_elapsed_ms: f64) -> Vec<SizeClassOpReport> {
        vec![
            self.small
                .to_size_class_report(SizeClass::Small, wall_elapsed_ms),
            self.medium
                .to_size_class_report(SizeClass::Medium, wall_elapsed_ms),
            self.large
                .to_size_class_report(SizeClass::Large, wall_elapsed_ms),
        ]
    }

    fn merge_from_ref(&mut self, other: &Self) {
        self.small.merge_from_ref(&other.small);
        self.medium.merge_from_ref(&other.medium);
        self.large.merge_from_ref(&other.large);
    }
}

#[derive(Default, Clone)]
pub(super) struct PhaseTelemetry {
    pub(super) overall: LatencySamples,
    by_size: SizeClassLatencySamples,
    operation_logs: Vec<PhaseOperationLog>,
}

#[derive(Clone)]
pub(super) struct PhaseOperationLog {
    pub(super) operation_index: u64,
    pub(super) work_index: usize,
    pub(super) worker_id: usize,
    pub(super) target: String,
    pub(super) size_class: SizeClass,
    pub(super) bytes: u64,
    pub(super) elapsed: Duration,
}

impl PhaseTelemetry {
    pub(super) fn record(&mut self, class: SizeClass, elapsed: Duration, bytes: u64) {
        self.overall.record(elapsed, bytes);
        self.by_size.get_mut(class).record(elapsed, bytes);
    }

    pub(super) fn record_operation(&mut self, operation: PhaseOperationLog) {
        self.record(operation.size_class, operation.elapsed, operation.bytes);
        self.operation_logs.push(operation);
    }

    fn operation_reports(&self) -> Vec<OperationLogReport> {
        let mut logs = self.operation_logs.clone();
        logs.sort_by_key(|log| log.operation_index);
        logs.into_iter()
            .map(|log| {
                let elapsed_ms = log.elapsed.as_secs_f64() * 1000.0;
                let throughput_bytes_per_sec = if elapsed_ms <= f64::EPSILON {
                    0.0
                } else {
                    log.bytes as f64 / (elapsed_ms / 1000.0)
                };
                OperationLogReport {
                    operation_index: log.operation_index,
                    work_index: log.work_index,
                    worker_id: log.worker_id,
                    target: log.target,
                    size_class: log.size_class.label().to_string(),
                    bytes: log.bytes,
                    elapsed_ms,
                    throughput_bytes_per_sec,
                }
            })
            .collect()
    }

    pub(super) fn merge_from_ref(&mut self, other: &Self) {
        self.overall.merge_from_ref(&other.overall);
        self.by_size.merge_from_ref(&other.by_size);
        self.operation_logs
            .extend(other.operation_logs.iter().cloned());
    }
}

#[derive(Default, Clone)]
pub(super) struct CacheLevelTelemetryAccumulator {
    l1_hits: u64,
    l1_misses: u64,
    l1_bytes_served: u64,
    l1_lookup_latency: LatencySamples,
}

impl CacheLevelTelemetryAccumulator {
    pub(super) fn record_l1(&mut self, hit: bool, elapsed: Duration, bytes_served: u64) {
        self.l1_lookup_latency.record(elapsed, bytes_served);
        if hit {
            self.l1_hits = self.l1_hits.saturating_add(1);
            self.l1_bytes_served = self.l1_bytes_served.saturating_add(bytes_served);
        } else {
            self.l1_misses = self.l1_misses.saturating_add(1);
        }
    }

    fn to_report(&self, wall_elapsed_ms: f64) -> CacheLevelsReport {
        let l1_report = self.l1_lookup_latency.to_report(wall_elapsed_ms);
        CacheLevelsReport {
            l1: CacheLevelReport {
                hits: self.l1_hits,
                misses: self.l1_misses,
                bytes_served: self.l1_bytes_served,
                throughput_bytes_per_sec: l1_report.throughput_bytes_per_sec,
                avg_latency_ms: l1_report.avg_latency_ms,
                p50_ms: l1_report.p50_ms,
                p95_ms: l1_report.p95_ms,
                p99_ms: l1_report.p99_ms,
                latency_histogram: self.l1_lookup_latency.to_histogram_report(),
            },
        }
    }

    pub(super) fn merge_from_ref(&mut self, other: &Self) {
        self.l1_hits = self.l1_hits.saturating_add(other.l1_hits);
        self.l1_misses = self.l1_misses.saturating_add(other.l1_misses);
        self.l1_bytes_served = self.l1_bytes_served.saturating_add(other.l1_bytes_served);
        self.l1_lookup_latency
            .merge_from_ref(&other.l1_lookup_latency);
    }
}

#[derive(Default, Clone)]
pub(super) struct DirectReadTelemetry {
    pub(super) phase: PhaseTelemetry,
    pub(super) cache_levels: CacheLevelTelemetryAccumulator,
}

impl DirectReadTelemetry {
    pub(super) fn merge_from_ref(&mut self, other: &Self) {
        self.phase.merge_from_ref(&other.phase);
        self.cache_levels.merge_from_ref(&other.cache_levels);
    }
}

pub(super) fn stream_reports_from_telemetry(
    writes_by_stream: &[PhaseTelemetry],
    reads_by_stream: &[PhaseTelemetry],
    writes_wall_elapsed_ms: f64,
    reads_wall_elapsed_ms: f64,
) -> Option<Vec<StreamResultReport>> {
    if writes_by_stream.len() <= 1 || writes_by_stream.len() != reads_by_stream.len() {
        return None;
    }
    Some(
        writes_by_stream
            .iter()
            .zip(reads_by_stream.iter())
            .enumerate()
            .map(|(idx, (writes, reads))| StreamResultReport {
                stream_id: idx,
                writes: phase_telemetry_to_report(
                    writes,
                    Duration::from_secs_f64(writes_wall_elapsed_ms / 1000.0),
                ),
                reads: phase_telemetry_to_report(
                    reads,
                    Duration::from_secs_f64(reads_wall_elapsed_ms / 1000.0),
                ),
            })
            .collect(),
    )
}

pub(super) fn phase_telemetry_to_report(
    telemetry: &PhaseTelemetry,
    wall_elapsed: Duration,
) -> OpReport {
    let mut report = telemetry
        .overall
        .to_report(wall_elapsed.as_secs_f64() * 1000.0);
    report.operations_log = telemetry.operation_reports();
    report
}

pub(super) fn build_advanced_metrics(
    size_class_bounds: SizeClassBounds,
    writes: &PhaseTelemetry,
    reads: &PhaseTelemetry,
    cache_levels: Option<&CacheLevelTelemetryAccumulator>,
    _cache_pipeline: Option<&CachePipelineReport>,
    writes_wall_elapsed_ms: f64,
    reads_wall_elapsed_ms: f64,
) -> AdvancedMetricsReport {
    AdvancedMetricsReport {
        size_class_boundaries: SizeClassBoundaryReport {
            small_max_bytes: size_class_bounds.small_max,
            medium_max_bytes: size_class_bounds.medium_max,
        },
        size_class_stats: SizeClassStatsReport {
            writes: writes.by_size.to_reports(writes_wall_elapsed_ms),
            reads: reads.by_size.to_reports(reads_wall_elapsed_ms),
        },
        cache_levels: cache_levels.map(|levels| levels.to_report(reads_wall_elapsed_ms)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_merge_upsamples_keep_every_progressively() {
        let mut left = LatencySamples {
            samples_ns: (0..MAX_LATENCY_SAMPLES as u64).collect(),
            total_ops: MAX_LATENCY_SAMPLES as u64,
            total_bytes: 0,
            total_latency_ns: 0,
            sample_keep_every: 2,
        };
        let right = LatencySamples {
            samples_ns: (0..MAX_LATENCY_SAMPLES as u64).collect(),
            total_ops: MAX_LATENCY_SAMPLES as u64,
            total_bytes: 0,
            total_latency_ns: 0,
            sample_keep_every: 2,
        };
        left.merge_from_ref(&right);
        assert_eq!(left.sample_keep_every, 4);
    }

    #[test]
    fn latency_merge_aligns_sampling_density_before_append() {
        let mut left = LatencySamples {
            samples_ns: vec![1, 2, 3, 4],
            total_ops: 4,
            total_bytes: 0,
            total_latency_ns: 0,
            sample_keep_every: 1,
        };
        let right = LatencySamples {
            samples_ns: vec![10, 20, 30, 40],
            total_ops: 8,
            total_bytes: 0,
            total_latency_ns: 0,
            sample_keep_every: 2,
        };
        left.merge_from_ref(&right);
        assert_eq!(left.sample_keep_every, 2);
        assert_eq!(left.samples_ns, vec![1, 3, 10, 20, 30, 40]);
    }

    #[test]
    fn phase_telemetry_to_report_preserves_full_operation_log() {
        let mut telemetry = PhaseTelemetry::default();
        telemetry.record_operation(PhaseOperationLog {
            operation_index: 9,
            work_index: 3,
            worker_id: 2,
            target: "Z:\\bench\\0003.bin".to_string(),
            size_class: SizeClass::Medium,
            bytes: 4 * 1024 * 1024,
            elapsed: Duration::from_millis(12),
        });
        telemetry.record_operation(PhaseOperationLog {
            operation_index: 4,
            work_index: 1,
            worker_id: 0,
            target: "Z:\\bench\\0001.bin".to_string(),
            size_class: SizeClass::Small,
            bytes: 512 * 1024,
            elapsed: Duration::from_millis(3),
        });

        let report = phase_telemetry_to_report(&telemetry, Duration::from_millis(20));

        assert_eq!(report.operations, 2);
        assert_eq!(report.operations_log.len(), 2);
        assert_eq!(report.operations_log[0].operation_index, 4);
        assert_eq!(report.operations_log[0].worker_id, 0);
        assert_eq!(report.operations_log[0].target, "Z:\\bench\\0001.bin");
        assert_eq!(report.operations_log[0].size_class, "small");
        assert_eq!(report.operations_log[1].operation_index, 9);
        assert_eq!(report.operations_log[1].worker_id, 2);
        assert_eq!(report.operations_log[1].target, "Z:\\bench\\0003.bin");
        assert_eq!(report.operations_log[1].size_class, "medium");
    }
}
