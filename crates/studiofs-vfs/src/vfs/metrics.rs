use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::file_session::VfsFileSessionStats;
use studiofs_meta::MetadataWriteObservability;
use studiofs_meta::schema::{InodeLifecycleState, PayloadRetentionClass};

/// Current version of the cross-platform runtime stats snapshot schema.
pub const RUNTIME_STATS_SCHEMA_VERSION: u16 = 1;

/// Stable runtime stats sections exposed by all StudioFS bridge implementations.
pub const RUNTIME_STATS_SECTIONS: &[&str] = &[
    "system",
    "bridge",
    "vfs_read",
    "vfs_write",
    "cache",
    "upload_queue",
    "object_io",
    "metadata",
];

/// Cross-platform runtime statistics snapshot for benchmarks and live performance reporting.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RuntimeStatsSnapshot {
    /// Stable schema version. Readers must tolerate missing newer fields.
    pub schema_version: u16,
    /// Snapshot generation timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// System and runtime process counters.
    #[serde(default, deserialize_with = "deserialize_system_runtime_stats_section")]
    pub system: RuntimeStatsSection,
    /// FUSE, macFUSE, WinFSP, or future bridge callback counters.
    pub bridge: RuntimeStatsSection,
    /// VFS read-path counters.
    pub vfs_read: RuntimeStatsSection,
    /// VFS write-path counters.
    pub vfs_write: RuntimeStatsSection,
    /// Local cache counters and gauges.
    pub cache: RuntimeStatsSection,
    /// Upload queue counters and gauges.
    pub upload_queue: RuntimeStatsSection,
    /// Object-store I/O counters.
    pub object_io: RuntimeStatsSection,
    /// Metadata operation counters.
    pub metadata: RuntimeStatsSection,
}

impl Default for RuntimeStatsSnapshot {
    fn default() -> Self {
        Self {
            schema_version: RUNTIME_STATS_SCHEMA_VERSION,
            generated_unix_ms: 0,
            system: RuntimeStatsSection::default(),
            bridge: RuntimeStatsSection::default(),
            vfs_read: RuntimeStatsSection::default(),
            vfs_write: RuntimeStatsSection::default(),
            cache: RuntimeStatsSection::default(),
            upload_queue: RuntimeStatsSection::default(),
            object_io: RuntimeStatsSection::default(),
            metadata: RuntimeStatsSection::default(),
        }
    }
}

impl RuntimeStatsSnapshot {
    /// Render a stable, JuiceFS-style local stats text surface.
    ///
    /// Each non-comment line has `kind name unit value`. Histogram bucket lines
    /// have `histogram_bucket name le_ns count`.
    pub fn to_text(&self) -> String {
        self.to_string()
    }

    /// Parse the stable local stats text representation.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeStatsParseError`] when a non-comment line is malformed.
    /// Unknown runtime stats sections are skipped for forward compatibility.
    pub fn parse_text(input: &str) -> Result<Self, RuntimeStatsParseError> {
        input.parse()
    }
}

impl fmt::Display for RuntimeStatsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "# studiofs_runtime_stats schema_version={}",
            self.schema_version
        )?;
        writeln!(
            f,
            "gauge system.schema_version count {}",
            self.schema_version
        )?;
        writeln!(
            f,
            "gauge system.generated_unix_ms ms {}",
            self.generated_unix_ms
        )?;

        for (section_name, section) in self.sections() {
            section.write_text(section_name, f)?;
        }
        Ok(())
    }
}

impl FromStr for RuntimeStatsSnapshot {
    type Err = RuntimeStatsParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut snapshot = Self::default();
        let mut explicit_histogram_units = BTreeMap::new();
        for (line_index, raw_line) in input.lines().enumerate() {
            let line = raw_line
                .split_once('#')
                .map_or(raw_line, |(before_comment, _)| before_comment)
                .trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let Some(kind) = parts.next() else { continue };
            match kind {
                "counter" | "gauge" => {
                    let name = parts.next().ok_or_else(|| {
                        RuntimeStatsParseError::new(line_index + 1, "missing metric name")
                    })?;
                    let unit = parts.next().ok_or_else(|| {
                        RuntimeStatsParseError::new(line_index + 1, "missing metric unit")
                    })?;
                    let value = parts
                        .next()
                        .ok_or_else(|| {
                            RuntimeStatsParseError::new(line_index + 1, "missing metric value")
                        })?
                        .parse::<u64>()
                        .map_err(|_| {
                            RuntimeStatsParseError::new(line_index + 1, "invalid metric value")
                        })?;
                    if parts.next().is_some() {
                        return Err(RuntimeStatsParseError::new(
                            line_index + 1,
                            "unexpected trailing fields",
                        ));
                    }
                    snapshot.set_scalar_metric(kind, name, unit, value, line_index + 1)?;
                }
                "histogram_count" | "histogram_sum" => {
                    let name = parts.next().ok_or_else(|| {
                        RuntimeStatsParseError::new(line_index + 1, "missing histogram name")
                    })?;
                    let unit = parts.next().ok_or_else(|| {
                        RuntimeStatsParseError::new(line_index + 1, "missing histogram unit")
                    })?;
                    let value = parts
                        .next()
                        .ok_or_else(|| {
                            RuntimeStatsParseError::new(line_index + 1, "missing histogram value")
                        })?
                        .parse::<u64>()
                        .map_err(|_| {
                            RuntimeStatsParseError::new(line_index + 1, "invalid histogram value")
                        })?;
                    if parts.next().is_some() {
                        return Err(RuntimeStatsParseError::new(
                            line_index + 1,
                            "unexpected trailing fields",
                        ));
                    }
                    snapshot.set_histogram_total(
                        kind,
                        name,
                        unit,
                        value,
                        line_index + 1,
                        &mut explicit_histogram_units,
                    )?;
                }
                "histogram_bucket" => {
                    let name = parts.next().ok_or_else(|| {
                        RuntimeStatsParseError::new(line_index + 1, "missing histogram name")
                    })?;
                    let le_ns = parts
                        .next()
                        .ok_or_else(|| {
                            RuntimeStatsParseError::new(line_index + 1, "missing bucket bound")
                        })?
                        .parse::<u64>()
                        .map_err(|_| {
                            RuntimeStatsParseError::new(line_index + 1, "invalid bucket bound")
                        })?;
                    let count = parts
                        .next()
                        .ok_or_else(|| {
                            RuntimeStatsParseError::new(line_index + 1, "missing bucket count")
                        })?
                        .parse::<u64>()
                        .map_err(|_| {
                            RuntimeStatsParseError::new(line_index + 1, "invalid bucket count")
                        })?;
                    if parts.next().is_some() {
                        return Err(RuntimeStatsParseError::new(
                            line_index + 1,
                            "unexpected trailing fields",
                        ));
                    }
                    snapshot.set_histogram_bucket(name, le_ns, count, line_index + 1)?;
                }
                _ => {
                    return Err(RuntimeStatsParseError::new(
                        line_index + 1,
                        "unknown runtime stats line kind",
                    ));
                }
            }
        }
        Ok(snapshot)
    }
}

impl RuntimeStatsSnapshot {
    fn sections(&self) -> impl Iterator<Item = (&'static str, &RuntimeStatsSection)> {
        [
            ("system", &self.system),
            ("bridge", &self.bridge),
            ("vfs_read", &self.vfs_read),
            ("vfs_write", &self.vfs_write),
            ("cache", &self.cache),
            ("upload_queue", &self.upload_queue),
            ("object_io", &self.object_io),
            ("metadata", &self.metadata),
        ]
        .into_iter()
    }

    fn section_mut(&mut self, section: &str) -> Option<&mut RuntimeStatsSection> {
        match section {
            "system" => Some(&mut self.system),
            "bridge" => Some(&mut self.bridge),
            "vfs_read" => Some(&mut self.vfs_read),
            "vfs_write" => Some(&mut self.vfs_write),
            "cache" => Some(&mut self.cache),
            "upload_queue" => Some(&mut self.upload_queue),
            "object_io" => Some(&mut self.object_io),
            "metadata" => Some(&mut self.metadata),
            _ => None,
        }
    }

    fn split_metric_name(name: &str, line: usize) -> Result<(&str, &str), RuntimeStatsParseError> {
        let (section, key) = name
            .split_once('.')
            .ok_or_else(|| RuntimeStatsParseError::new(line, "metric name must be section.key"))?;
        if section.is_empty() || key.is_empty() {
            return Err(RuntimeStatsParseError::new(
                line,
                "metric section and key must not be empty",
            ));
        }
        if key.contains('.') {
            return Err(RuntimeStatsParseError::new(
                line,
                "metric key must not contain dots",
            ));
        }
        Ok((section, key))
    }

    fn set_scalar_metric(
        &mut self,
        kind: &str,
        name: &str,
        unit: &str,
        value: u64,
        line: usize,
    ) -> Result<(), RuntimeStatsParseError> {
        let (section, key) = Self::split_metric_name(name, line)?;
        if section == "system" && key == "schema_version" {
            if kind != "gauge" {
                return Err(RuntimeStatsParseError::new(
                    line,
                    "schema_version must be a gauge",
                ));
            }
            if unit != "count" {
                return Err(RuntimeStatsParseError::new(
                    line,
                    "schema_version must have unit 'count'",
                ));
            }
            self.schema_version = value.try_into().map_err(|_| {
                RuntimeStatsParseError::new(line, "schema_version does not fit u16")
            })?;
            return Ok(());
        }
        if section == "system" && key == "generated_unix_ms" {
            if kind != "gauge" {
                return Err(RuntimeStatsParseError::new(
                    line,
                    "generated_unix_ms must be a gauge",
                ));
            }
            if unit != "ms" {
                return Err(RuntimeStatsParseError::new(
                    line,
                    "generated_unix_ms must have unit 'ms'",
                ));
            }
            self.generated_unix_ms = value;
            return Ok(());
        }
        let Some(target) = self.section_mut(section) else {
            return Ok(());
        };
        let metric = RuntimeScalarMetric {
            value,
            unit: parse_runtime_stats_unit(unit),
        };
        match kind {
            "counter" => {
                target.counters.insert(key.to_owned(), metric);
            }
            "gauge" => {
                target.gauges.insert(key.to_owned(), metric);
            }
            _ => unreachable!("kind is validated by caller"),
        }
        Ok(())
    }

    fn set_histogram_total<'a>(
        &mut self,
        kind: &str,
        name: &'a str,
        unit: &str,
        value: u64,
        line: usize,
        explicit_histogram_units: &mut BTreeMap<(&'a str, &'a str), Cow<'static, str>>,
    ) -> Result<(), RuntimeStatsParseError> {
        let (section, key) = Self::split_metric_name(name, line)?;
        Self::validate_not_root_system_histogram(section, key, line)?;
        let Some(target) = self.section_mut(section) else {
            return Ok(());
        };
        let unit = parse_runtime_stats_unit(unit);
        let explicit_unit_key = (section, key);
        if let Some(existing_unit) = explicit_histogram_units.get(&explicit_unit_key) {
            if existing_unit != &unit {
                return Err(RuntimeStatsParseError::new(line, "histogram unit mismatch"));
            }
        } else {
            explicit_histogram_units.insert(explicit_unit_key, unit.clone());
        }
        if let Some(histogram) = target.latencies.get_mut(key) {
            set_histogram_unit(histogram, unit, line)?;
            match kind {
                "histogram_count" => histogram.count = value,
                "histogram_sum" => histogram.sum = value,
                _ => unreachable!("kind is validated by caller"),
            }
            return Ok(());
        }

        let mut histogram = RuntimeLatencyHistogram {
            unit,
            ..RuntimeLatencyHistogram::default()
        };
        match kind {
            "histogram_count" => histogram.count = value,
            "histogram_sum" => histogram.sum = value,
            _ => unreachable!("kind is validated by caller"),
        }
        target.latencies.insert(key.to_owned(), histogram);
        Ok(())
    }

    fn set_histogram_bucket(
        &mut self,
        name: &str,
        le_ns: u64,
        count: u64,
        line: usize,
    ) -> Result<(), RuntimeStatsParseError> {
        let (section, key) = Self::split_metric_name(name, line)?;
        Self::validate_not_root_system_histogram(section, key, line)?;
        let Some(target) = self.section_mut(section) else {
            return Ok(());
        };
        // Avoid `entry(key.to_owned())` here: histogram buckets repeat the same
        // key, and the borrowed lookup avoids allocating a discarded `String`.
        if let Some(histogram) = target.latencies.get_mut(key) {
            insert_latency_bucket(
                &mut histogram.buckets,
                RuntimeLatencyBucket { le_ns, count },
            );
        } else {
            target.latencies.insert(
                key.to_owned(),
                RuntimeLatencyHistogram {
                    buckets: vec![RuntimeLatencyBucket { le_ns, count }],
                    ..RuntimeLatencyHistogram::default()
                },
            );
        }
        Ok(())
    }

    fn validate_not_root_system_histogram(
        section: &str,
        key: &str,
        line: usize,
    ) -> Result<(), RuntimeStatsParseError> {
        if is_root_runtime_stats_gauge(section, key) {
            return Err(RuntimeStatsParseError::new(
                line,
                "system schema_version and generated_unix_ms cannot be histograms",
            ));
        }
        Ok(())
    }
}

/// One runtime stats section containing monotonic counters, gauges, and latency histograms.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RuntimeStatsSection {
    /// Monotonically increasing counters keyed by stable snake_case names.
    pub counters: BTreeMap<String, RuntimeScalarMetric>,
    /// Point-in-time gauges keyed by stable snake_case names.
    pub gauges: BTreeMap<String, RuntimeScalarMetric>,
    /// Latency histograms keyed by stable snake_case operation names.
    pub latencies: BTreeMap<String, RuntimeLatencyHistogram>,
}

impl RuntimeStatsSection {
    fn write_text(&self, section_name: &str, out: &mut impl fmt::Write) -> fmt::Result {
        for (name, metric) in &self.counters {
            writeln!(
                out,
                "counter {section_name}.{name} {} {}",
                metric.unit, metric.value
            )?;
        }
        for (name, metric) in &self.gauges {
            if is_root_runtime_stats_gauge(section_name, name) {
                continue;
            }
            writeln!(
                out,
                "gauge {section_name}.{name} {} {}",
                metric.unit, metric.value
            )?;
        }
        for (name, histogram) in &self.latencies {
            writeln!(
                out,
                "histogram_count {section_name}.{name} {} {}",
                histogram.unit, histogram.count
            )?;
            writeln!(
                out,
                "histogram_sum {section_name}.{name} {} {}",
                histogram.unit, histogram.sum
            )?;
            for bucket in &histogram.buckets {
                writeln!(
                    out,
                    "histogram_bucket {section_name}.{name} {} {}",
                    bucket.le_ns, bucket.count
                )?;
            }
        }
        Ok(())
    }
}

fn is_root_runtime_stats_gauge(section_name: &str, name: &str) -> bool {
    section_name == "system" && matches!(name, "schema_version" | "generated_unix_ms")
}

fn remove_root_runtime_stats_metrics(section: &mut RuntimeStatsSection) {
    section.counters.remove("schema_version");
    section.counters.remove("generated_unix_ms");
    section.gauges.remove("schema_version");
    section.gauges.remove("generated_unix_ms");
    section.latencies.remove("schema_version");
    section.latencies.remove("generated_unix_ms");
}

fn deserialize_system_runtime_stats_section<'de, D>(
    deserializer: D,
) -> Result<RuntimeStatsSection, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mut section = RuntimeStatsSection::deserialize(deserializer)?;
    remove_root_runtime_stats_metrics(&mut section);
    Ok(section)
}

fn parse_runtime_stats_unit(unit: &str) -> Cow<'static, str> {
    match unit {
        "count" => Cow::Borrowed("count"),
        "bytes" => Cow::Borrowed("bytes"),
        "ns" => Cow::Borrowed("ns"),
        "ms" => Cow::Borrowed("ms"),
        "percent" => Cow::Borrowed("percent"),
        _ => Cow::Owned(unit.to_owned()),
    }
}

fn set_histogram_unit(
    histogram: &mut RuntimeLatencyHistogram,
    unit: Cow<'static, str>,
    line: usize,
) -> Result<(), RuntimeStatsParseError> {
    let can_replace_bucket_default = histogram.count == 0
        && histogram.sum == 0
        && histogram.unit == "ns"
        && !histogram.buckets.is_empty();
    if histogram.unit != unit && !can_replace_bucket_default {
        return Err(RuntimeStatsParseError::new(line, "histogram unit mismatch"));
    }
    histogram.unit = unit;
    Ok(())
}

fn insert_latency_bucket(buckets: &mut Vec<RuntimeLatencyBucket>, bucket: RuntimeLatencyBucket) {
    match buckets.binary_search_by_key(&bucket.le_ns, |existing| existing.le_ns) {
        Ok(index) => buckets[index] = bucket,
        Err(index) => buckets.insert(index, bucket),
    }
}

fn deserialize_sorted_latency_buckets<'de, D>(
    deserializer: D,
) -> Result<Vec<RuntimeLatencyBucket>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mut raw_buckets = Vec::<RuntimeLatencyBucket>::deserialize(deserializer)?;
    raw_buckets.sort_by_key(|bucket| bucket.le_ns);
    if raw_buckets.len() < 2 {
        return Ok(raw_buckets);
    }

    let mut write_index = 0;
    for read_index in 1..raw_buckets.len() {
        if raw_buckets[write_index].le_ns == raw_buckets[read_index].le_ns {
            // Keep the last duplicate bound; `Vec::dedup_by_key` would retain
            // the first and change backwards-compatible parsing behavior.
            raw_buckets[write_index] = raw_buckets[read_index];
        } else {
            write_index += 1;
            if write_index != read_index {
                raw_buckets[write_index] = raw_buckets[read_index];
            }
        }
    }
    raw_buckets.truncate(write_index + 1);
    Ok(raw_buckets)
}

/// Scalar counter or gauge value with a stable unit.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeScalarMetric {
    /// Numeric value for this counter or gauge.
    pub value: u64,
    /// Stable unit string, such as `count`, `bytes`, `ns`, or `ms`.
    pub unit: Cow<'static, str>,
}

/// Latency histogram with cumulative count and sum for interval averages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RuntimeLatencyHistogram {
    /// Total observations.
    pub count: u64,
    /// Sum of observed latency values in `unit`.
    pub sum: u64,
    /// Unit used for `sum`; bucket bounds are always nanoseconds.
    pub unit: Cow<'static, str>,
    /// Bucket counts keyed by inclusive upper-bound nanoseconds.
    #[serde(default, deserialize_with = "deserialize_sorted_latency_buckets")]
    pub buckets: Vec<RuntimeLatencyBucket>,
}

impl Default for RuntimeLatencyHistogram {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0,
            unit: Cow::Borrowed("ns"),
            buckets: Vec::new(),
        }
    }
}

/// One latency histogram bucket.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeLatencyBucket {
    /// Inclusive upper-bound in nanoseconds.
    pub le_ns: u64,
    /// Count observed in this bucket.
    pub count: u64,
}

/// Error returned when parsing the local runtime stats text surface.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("failed to parse runtime stats at line {line}: {message}")]
pub struct RuntimeStatsParseError {
    line: usize,
    message: &'static str,
}

impl RuntimeStatsParseError {
    fn new(line: usize, message: &'static str) -> Self {
        Self { line, message }
    }

    /// One-based input line where parsing failed.
    pub fn line(&self) -> usize {
        self.line
    }

    /// Static parser error reason.
    pub fn message(&self) -> &'static str {
        self.message
    }
}

/// Filesystem statistics returned by `statfs()`.
#[derive(Debug, Clone)]
pub struct StatFs {
    /// Total number of allocation units in the filesystem.
    pub blocks: u64,
    /// Number of free allocation units.
    pub bfree: u64,
    /// Allocation units available to unprivileged callers.
    pub bavail: u64,
    /// Total inode count available to the filesystem.
    pub files: u64,
    /// Number of free inodes.
    pub ffree: u64,
    /// Preferred filesystem block size in bytes.
    pub bsize: u32,
    /// Maximum filename length in bytes.
    pub namelen: u32,
    /// Fundamental filesystem block size in bytes.
    pub frsize: u32,
}

/// Ingress pressure counters for the write path.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsIngressStats {
    /// Dirty memory currently tracked by the ingress pipeline, in bytes.
    pub dirty_mem_bytes: usize,
    /// Number of dirty memory blocks currently tracked.
    pub dirty_mem_blocks: usize,
    /// Cumulative wait time in milliseconds caused by write backpressure.
    pub write_backpressure_wait_ms: u64,
    /// Number of write backpressure wait events.
    pub write_backpressure_events: u64,
    /// Cumulative wait time in milliseconds caused by fast backpressure.
    pub fast_backpressure_wait_ms: u64,
    /// Number of fast backpressure wait events.
    pub fast_backpressure_events: u64,
    /// Maximum observed fast backpressure stall duration in milliseconds.
    pub fast_backpressure_stall_max_ms: u64,
    /// Current prefetch queue depth.
    pub prefetch_queue_depth: u64,
}

/// Write-path latency quantile measurements in milliseconds.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLatencyQuantilesMs {
    /// 50th percentile value.
    pub p50: f64,
    /// 95th percentile value.
    pub p95: f64,
    /// 99th percentile value.
    pub p99: f64,
}

/// Flags indicating whether quantile values are saturated.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLatencyQuantilesSaturated {
    /// Indicates saturation for `p50`.
    pub p50: bool,
    /// Indicates saturation for `p95`.
    pub p95: bool,
    /// Indicates saturation for `p99`.
    pub p99: bool,
}

/// Histogram snapshot with bucket bounds and counts.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsLatencyHistogramSnapshot {
    /// Upper bounds (nanoseconds) for histogram buckets.
    pub bucket_upper_bounds_ns: Vec<u64>,
    /// Count values for each bucket.
    pub bucket_counts: Vec<u64>,
}

/// Aggregated sync-data statistics for the write path.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathSyncDataStats {
    /// Total sync-data calls.
    pub calls: u64,
    /// Sync-data call rate in calls per second.
    pub calls_per_sec: f64,
    /// Number of deferred sync calls.
    pub deferred_calls: u64,
    /// Total synced bytes.
    pub synced_bytes: u64,
    /// Total synced bytes in GiB.
    pub synced_gib: f64,
    /// Calls per GiB value.
    pub calls_per_gib: f64,
    /// Total sync time in milliseconds.
    pub total_ms: f64,
    /// Average sync time in milliseconds.
    pub avg_ms: f64,
    /// Maximum sync time in milliseconds.
    pub max_ms: f64,
    /// Latency quantiles in milliseconds.
    pub latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    /// Saturation markers for latency quantiles.
    #[serde(default)]
    pub latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Distribution histogram of sync latencies.
    pub latency_histogram: VfsLatencyHistogramSnapshot,
    /// Total queue wait time in milliseconds.
    #[serde(default)]
    pub queue_wait_total_ms: f64,
    /// Average queue wait time in milliseconds.
    #[serde(default)]
    pub queue_wait_avg_ms: f64,
    /// Maximum queue wait time in milliseconds.
    #[serde(default)]
    pub queue_wait_max_ms: f64,
    /// Queue latency quantiles in milliseconds.
    #[serde(default)]
    pub queue_wait_latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    /// Saturation markers for queue quantiles.
    #[serde(default)]
    pub queue_wait_latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Histogram of queue wait latencies.
    #[serde(default)]
    pub queue_wait_latency_histogram: VfsLatencyHistogramSnapshot,
    /// Average barrier interval in milliseconds.
    #[serde(default)]
    pub barrier_interval_avg_ms: f64,
    /// Maximum barrier interval in milliseconds.
    #[serde(default)]
    pub barrier_interval_max_ms: f64,
    /// Barrier interval quantiles in milliseconds.
    #[serde(default)]
    pub barrier_interval_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    /// Saturation markers for barrier interval quantiles.
    #[serde(default)]
    pub barrier_interval_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Histogram of barrier intervals.
    #[serde(default)]
    pub barrier_interval_histogram: VfsLatencyHistogramSnapshot,
}

/// WinFSP callback counters for the write path.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathWinfspCallbackStats {
    /// Number of read callbacks.
    #[serde(default)]
    pub read: u64,
    /// Bytes handled by read callbacks.
    #[serde(default)]
    pub read_bytes: u64,
    /// Total read callback wall time in nanoseconds.
    #[serde(default)]
    pub read_total_ns: u64,
    /// Number of write callbacks.
    pub write: u64,
    /// Number of overwrite callbacks.
    #[serde(default)]
    pub overwrite: u64,
    /// Number of cleanup callbacks.
    #[serde(default)]
    pub cleanup: u64,
    /// Number of flush callbacks.
    pub flush: u64,
    /// Number of close callbacks.
    #[serde(default)]
    pub close: u64,
    /// Number of cleanup+flush handle pairs.
    #[serde(default)]
    pub cleanup_flush_fh: u64,
    /// Number of WinFSP opens that allowed the Windows file cache.
    #[serde(default)]
    pub os_cache_enabled_open: u64,
    /// Number of WinFSP opens that bypassed the Windows file cache.
    #[serde(default)]
    pub os_cache_disabled_open: u64,
    /// Read callbacks observed on cacheable handles.
    #[serde(default)]
    pub os_cache_miss_read_callbacks: u64,
    /// Bytes read through callbacks on cacheable handles.
    #[serde(default)]
    pub os_cache_miss_read_bytes: u64,
}

/// WinFSP write-path stage metrics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathWinfspWritePathStats {
    /// Total write bytes handled by WinFSP callbacks.
    pub write_bytes: u64,
    /// Average write size in bytes.
    pub avg_write_bytes: f64,
    /// Number of strict write calls.
    #[serde(default)]
    pub strict_calls: u64,
    /// Bytes written by strict mode writes.
    #[serde(default)]
    pub strict_bytes: u64,
    /// Number of write-through calls.
    #[serde(default)]
    pub write_through_calls: u64,
    /// Bytes written by write-through calls.
    #[serde(default)]
    pub write_through_bytes: u64,
    /// Number of opportunistic write calls.
    #[serde(default)]
    pub opportunistic_calls: u64,
    /// Bytes written opportunistically.
    #[serde(default)]
    pub opportunistic_bytes: u64,
    /// Callback-stage latency metrics.
    #[serde(default)]
    pub callback: VfsWritePathStageLatencyStats,
    /// VFS write-stage latency metrics.
    #[serde(default)]
    pub vfs_write: VfsWritePathStageLatencyStats,
    /// Post-write getattr latency metrics.
    #[serde(default)]
    pub post_write_getattr: VfsWritePathStageLatencyStats,
}

/// VFS write-stage latency and throughput metrics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathVfsWriteStats {
    /// Total VFS write calls.
    pub calls: u64,
    /// Total bytes written in VFS path.
    pub write_bytes: u64,
    /// Average write size in bytes.
    pub avg_write_bytes: f64,
    /// Foreground create operation latency.
    #[serde(default)]
    pub create: VfsWritePathStageLatencyStats,
    /// Foreground mkdir operation latency.
    #[serde(default)]
    pub mkdir: VfsWritePathStageLatencyStats,
    /// Foreground open operation latency.
    #[serde(default)]
    pub open_file: VfsWritePathStageLatencyStats,
    /// Foreground setattr operation latency.
    #[serde(default)]
    pub setattr: VfsWritePathStageLatencyStats,
    /// Pre-drain/admission pressure stage metrics.
    #[serde(default)]
    pub pre_drain: VfsWritePathStageLatencyStats,
    /// Soft admission throttle stage metrics.
    #[serde(default)]
    pub admission_throttle: VfsWritePathStageLatencyStats,
    /// Per-inode file-session gate wait metrics.
    #[serde(default)]
    pub file_session_wait: VfsWritePathStageLatencyStats,
    /// Writer stack state recording stage metrics.
    #[serde(default)]
    pub record_writer_stack_state: VfsWritePathStageLatencyStats,
    /// Attribute lookup fallback metrics inside writer-stack admission.
    #[serde(default)]
    pub getattr_async: VfsWritePathStageLatencyStats,
    /// Dirty-attr fast-path hits.
    #[serde(default)]
    pub dirty_attr_hits: u64,
    /// Open-handle attr fast-path hits.
    #[serde(default)]
    pub open_attr_hits: u64,
    /// Active-open overlay fallback hits after attr lookup miss.
    #[serde(default)]
    pub active_open_overlay_fallbacks: u64,
    /// Materialization completion stage metrics.
    #[serde(default)]
    pub materialize_completed: VfsWritePathStageLatencyStats,
    /// Strict materialization stage metrics.
    #[serde(default)]
    pub materialize_strict: VfsWritePathStageLatencyStats,
    /// Materialization-for-limits stage metrics.
    #[serde(default)]
    pub materialize_for_limits: VfsWritePathStageLatencyStats,
    /// Deferred materialization task scheduling stage metrics.
    #[serde(default)]
    pub spawn_deferred_materialization: VfsWritePathStageLatencyStats,
}

/// FUSE callback latency and byte counters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsFuseCallbackStats {
    /// FUSE read callback metrics.
    #[serde(default)]
    pub read: VfsFuseIoStats,
    /// FUSE write callback metrics.
    #[serde(default)]
    pub write: VfsFuseIoStats,
}

/// macFUSE read-open kernel-cache decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VfsMacFuseOpenCacheDecision {
    /// Read-only open kept kernel data cache for this handle.
    ReadOnlyKeep,
    /// Read-only open kept kernel data cache for a locally stable unpublished generation.
    ReadOnlyLocalStableKeep,
    /// Read-only open dropped cache because the inode state could not be read.
    ReadOnlyUnknownState,
    /// Read-only open dropped cache because the inode is not a regular file.
    ReadOnlyNotRegularFile,
    /// Read-only open dropped cache because the inode still has local dirty state.
    ReadOnlyLocalDirtyState,
    /// Read-only open dropped cache because a write-capable handle is still active.
    ReadOnlyActiveWriter,
    /// Read-only open dropped cache because this generation was not observed before.
    ReadOnlyGenerationChanged,
    /// Read-only open dropped a locally stable unpublished generation that was not observed before.
    ReadOnlyLocalStableGenerationChanged,
    /// Write-capable open intentionally disabled read-only cache retention.
    WriteCapable,
    /// Clean final write-capable release seeded a cacheable generation snapshot.
    CleanWriteReleaseSeeded,
    /// Final write-capable release seeded a locally stable unpublished generation snapshot.
    LocalStableWriteReleaseSeeded,
}

/// macFUSE open-cache decision counters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsMacFuseOpenCacheStats {
    /// Total macFUSE opens observed by the cache policy.
    pub total: u64,
    /// Read-only opens that returned keep-cache.
    pub read_only_keep: u64,
    /// Read-only opens that kept a locally stable unpublished generation.
    #[serde(default)]
    pub read_only_local_stable_keep: u64,
    /// Read-only opens that returned no keep-cache.
    pub read_only_drop: u64,
    /// Drops because the inode state could not be read.
    pub read_only_unknown_state: u64,
    /// Drops because the inode is not a regular file.
    #[serde(default)]
    pub read_only_not_regular_file: u64,
    /// Deprecated legacy key kept at zero; locally stable unpublished files are cacheable now.
    #[serde(default)]
    pub read_only_not_fully_committed: u64,
    /// Drops because local dirty state is still present.
    pub read_only_local_dirty_state: u64,
    /// Drops because a write-capable handle is still active.
    #[serde(default)]
    pub read_only_active_writer: u64,
    /// Drops because the current generation was not previously observed.
    pub read_only_generation_changed: u64,
    /// Drops because the locally stable unpublished generation was not previously observed.
    #[serde(default)]
    pub read_only_local_stable_generation_changed: u64,
    /// Write-capable opens that intentionally disabled keep-cache.
    pub write_capable: u64,
    /// Clean final write-capable releases that seeded a cacheable generation snapshot.
    #[serde(default)]
    pub clean_write_release_seeded: u64,
    /// Final write-capable releases that seeded a locally stable unpublished generation.
    #[serde(default)]
    pub local_stable_write_release_seeded: u64,
}

/// Async FUSE write bridge diagnostics supplied by the FUSE adapter.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct VfsFuseAsyncWriteSnapshot {
    /// Monotonic generation of the FUSE tracker snapshot used to reject stale gauge updates.
    pub snapshot_generation: u64,
    /// Whether async write replies are enabled for this FUSE mount.
    pub enabled: bool,
    /// Selected Linux FUSE profile label.
    pub linux_fuse_profile: Cow<'static, str>,
    /// Requested kernel `max_write` value.
    pub fuse_max_write_bytes: u32,
    /// Requested kernel `max_readahead` value.
    pub fuse_max_readahead_bytes: u32,
    /// Requested kernel `max_background` value.
    pub fuse_max_background: Option<u16>,
    /// Requested kernel `congestion_threshold` value.
    pub fuse_congestion_threshold: Option<u16>,
    /// Shared buffer budget selected by the Linux FUSE profile.
    pub shared_buffer_budget_bytes: Option<u64>,
    /// Read buffer budget selected by the Linux FUSE profile.
    pub read_buffer_budget_bytes: Option<u64>,
    /// Write buffer budget selected by the Linux FUSE profile.
    pub write_buffer_budget_bytes: Option<u64>,
    /// Prefetch concurrency selected by the Linux FUSE profile.
    pub prefetch_concurrency: Option<u16>,
    /// Upload concurrency selected by the Linux FUSE profile.
    pub upload_concurrency: Option<u16>,
    /// Download concurrency selected by the Linux FUSE profile.
    pub download_concurrency: Option<u16>,
    /// Whether the profile requested Linux kernel writeback cache.
    pub kernel_writeback_cache: Option<bool>,
    /// Maximum pending bytes admitted across the adapter.
    pub max_pending_bytes: Option<usize>,
    /// Maximum pending write requests admitted across the adapter.
    pub max_pending_writes: Option<usize>,
    /// Maximum pending bytes admitted per file handle.
    pub per_handle_pending_bytes: Option<usize>,
    /// Current pending async write requests.
    pub pending_writes: usize,
    /// Current pending async write bytes.
    pub pending_bytes: usize,
    /// Peak pending async write bytes.
    pub peak_pending_bytes: usize,
    /// Completed async writes.
    pub completed_writes: u64,
    /// Async writes completed with a VFS error.
    pub failed_writes: u64,
    /// Async write tasks dropped before explicit completion.
    pub task_failed_writes: u64,
    /// Admission waits caused by pending-write caps.
    pub admission_waits: u64,
    /// Total admission wait time in milliseconds.
    pub admission_wait_total_ms: f64,
    /// Barrier waits for flush/fsync/release callbacks.
    pub barrier_waits: u64,
    /// Total barrier wait time in milliseconds.
    pub barrier_wait_total_ms: f64,
}

/// Async FUSE read bridge diagnostics supplied by the FUSE adapter.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct VfsFuseAsyncReadSnapshot {
    /// Maximum pending read bytes admitted across the adapter.
    pub max_pending_bytes: Option<usize>,
    /// Current pending async read requests.
    pub pending_reads: usize,
    /// Current pending async read bytes.
    pub pending_bytes: usize,
    /// Peak pending async read requests.
    pub peak_pending_reads: usize,
    /// Peak pending async read bytes.
    pub peak_pending_bytes: usize,
    /// Completed async reads.
    pub completed_reads: u64,
    /// Async reads completed with a VFS error.
    pub failed_reads: u64,
    /// Async reads canceled before reply completion.
    pub canceled_reads: u64,
    /// Admission waits caused by pending-read byte caps.
    pub admission_waits: u64,
    /// Total admission wait time in milliseconds.
    pub admission_wait_total_ms: f64,
}

/// FUSE I/O callback metrics for one direction.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsFuseIoStats {
    /// Total callback count.
    pub calls: u64,
    /// Total requested bytes observed at the FUSE boundary.
    pub bytes: u64,
    /// Average request size in bytes.
    pub avg_bytes: f64,
    /// Callback errors.
    pub errors: u64,
    /// Total callback wall time in milliseconds.
    pub total_ms: f64,
    /// Average callback wall time in milliseconds.
    pub avg_ms: f64,
    /// Maximum callback wall time in milliseconds.
    pub max_ms: f64,
    /// Requested MiB per second over process uptime.
    pub requested_mib_per_sec: f64,
}

/// VFS read callback counters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsReadPathStats {
    /// Total VFS read calls.
    pub calls: u64,
    /// Total bytes requested through VFS reads.
    pub requested_bytes: u64,
    /// Average read request size in bytes.
    pub avg_requested_bytes: f64,
    /// Foreground local payload reads served for VFS reads.
    pub foreground_local_payload_reads: u64,
    /// Foreground local payload bytes served for VFS reads.
    pub foreground_local_payload_read_bytes: u64,
}

/// Landing-zone statistics for accepted writes and sync points.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLandingZoneStats {
    /// Number of accepted write operations.
    pub accepted_writes: u64,
    /// Total bytes accepted.
    pub accepted_bytes: u64,
    /// Accepted throughput in MiB/s.
    #[serde(default)]
    pub accepted_mib_per_sec: f64,
    /// Number of currently recoverable segments.
    #[serde(default)]
    pub recoverable_segments: usize,
    /// Total accept time in milliseconds.
    #[serde(default)]
    pub accept_total_ms: f64,
    /// Average accept time in milliseconds.
    #[serde(default)]
    pub accept_avg_ms: f64,
    /// Maximum accept time in milliseconds.
    #[serde(default)]
    pub accept_max_ms: f64,
    /// Count of data sync operations.
    #[serde(default)]
    pub data_syncs: u64,
    /// Count of manifest sync operations.
    #[serde(default)]
    pub manifest_syncs: u64,
    /// Count of manifest rewrites.
    #[serde(default)]
    pub manifest_rewrites: u64,
    /// Total sync time in milliseconds.
    #[serde(default)]
    pub sync_total_ms: f64,
    /// Average sync time in milliseconds.
    #[serde(default)]
    pub sync_avg_ms: f64,
    /// Maximum sync time in milliseconds.
    #[serde(default)]
    pub sync_max_ms: f64,
}

/// Drain lag and pacing details for write admission.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathDrainLagStats {
    /// Current lag in bytes.
    pub current_bytes: usize,
    /// Soft lag quota in bytes.
    pub steady_quota_bytes: usize,
    /// Count of soft pacing events.
    pub soft_pacing_events: u64,
    /// Total soft pacing wait time in milliseconds.
    pub soft_pacing_total_ms: u64,
    /// Maximum soft pacing wait in milliseconds.
    pub soft_pacing_max_ms: u64,
    /// Ingress bytes admitted per second.
    #[serde(default)]
    pub admission_ingress_bytes_per_sec: u64,
    /// Drain throughput in bytes per second.
    #[serde(default)]
    pub admission_drain_bytes_per_sec: u64,
    /// Remaining runway in milliseconds.
    #[serde(default)]
    pub admission_runway_ms: u64,
    /// Last admission delay in milliseconds.
    #[serde(default)]
    pub admission_last_delay_ms: u64,
    /// Current admission pressure in bytes.
    #[serde(default)]
    pub admission_pressure_bytes: usize,
    /// Current admission budget in bytes.
    #[serde(default)]
    pub admission_budget_bytes: usize,
    /// Unsynced bytes in local sync state.
    #[serde(default)]
    pub local_sync_unsynced_bytes: usize,
}

/// Foreground flush counters for cleanup/flush/close callbacks.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathWinfspForegroundFlushStats {
    /// Cleanup flush foreground calls.
    pub cleanup: u64,
    /// Flush foreground calls.
    pub flush: u64,
    /// Close foreground calls.
    pub close: u64,
    /// Clean cleanup callbacks skipped by guard logic.
    #[serde(default)]
    pub cleanup_skipped_clean: u64,
    /// Clean flush callbacks skipped by guard logic.
    #[serde(default)]
    pub flush_skipped_clean: u64,
    /// Clean close callbacks skipped by guard logic.
    #[serde(default)]
    pub close_skipped_clean: u64,
}

/// Local cache I/O latency and histogram breakdown.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLocalCacheIoStats {
    /// Execution model name used by local cache path.
    pub execution_model: String,
    /// Total local cache I/O calls.
    pub calls: u64,
    /// Total handoff time in milliseconds.
    pub handoff_total_ms: f64,
    /// Average handoff time in milliseconds.
    pub handoff_avg_ms: f64,
    /// Maximum handoff time in milliseconds.
    pub handoff_max_ms: f64,
    /// Total backend I/O time in milliseconds.
    pub backend_io_total_ms: f64,
    /// Average backend I/O time in milliseconds.
    pub backend_io_avg_ms: f64,
    /// Maximum backend I/O time in milliseconds.
    pub backend_io_max_ms: f64,
    /// Handoff latency histogram.
    pub handoff_histogram: VfsLatencyHistogramSnapshot,
    /// Backend I/O latency histogram.
    pub backend_io_histogram: VfsLatencyHistogramSnapshot,
}

/// Backpressure counters and stall metrics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathBackpressureStats {
    /// Backpressure event count.
    pub events: u64,
    /// Total backpressure wait time in milliseconds.
    pub wait_total_ms: u64,
    /// Maximum backpressure stall in milliseconds.
    pub stall_max_ms: u64,
    /// Number of soft throttle events.
    #[serde(default)]
    pub soft_throttle_events: u64,
    /// Number of hard throttle events.
    #[serde(default)]
    pub hard_events: u64,
}

/// Dirty-pressure metrics in write path memory and upload buffers.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathDirtyPressureStats {
    /// Pending local dirty bytes.
    pub pending_local_dirty_bytes: usize,
    /// Pending persisted slice bytes.
    #[serde(default)]
    pub pending_persisted_slice_bytes: usize,
    /// Dirty slice count.
    #[serde(default)]
    pub dirty_slice_count: usize,
    /// Frozen dirty slice count.
    #[serde(default)]
    pub frozen_slice_count: usize,
    /// Dirty bytes on writer stack.
    pub writer_stack_dirty_bytes: usize,
    /// Total dirty bytes.
    pub total_dirty_bytes: usize,
    /// Dirty bytes limit in bytes.
    pub dirty_limit_bytes: usize,
    /// Pending upload bytes.
    #[serde(default)]
    pub pending_upload_bytes: usize,
    /// Cache pressure bytes.
    #[serde(default)]
    pub cache_pressure_bytes: usize,
    /// Total accounted bytes.
    #[serde(default)]
    pub total_accounted_bytes: usize,
    /// Peak dirty bytes observed.
    #[serde(default)]
    pub dirty_peak_bytes: usize,
    /// Peak staged bytes observed.
    #[serde(default)]
    pub staged_peak_bytes: usize,
}

/// Burst telemetry for grouped payload writes.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathBurstStats {
    /// Number of burst events.
    pub bursts: u64,
    /// Burst count per second.
    pub bursts_per_sec: f64,
    /// Total payload count across bursts.
    pub total_payloads: u64,
    /// Total bytes across bursts.
    pub total_bytes: u64,
    /// Average payload count per burst.
    pub avg_payloads_per_burst: f64,
    /// Average bytes per burst.
    pub avg_bytes_per_burst: f64,
    /// Maximum payloads in one burst.
    pub max_payloads_per_burst: u64,
    /// Maximum bytes in one burst.
    pub max_bytes_per_burst: u64,
    /// Average burst interval in milliseconds.
    pub avg_interval_ms: f64,
    /// Maximum burst interval in milliseconds.
    pub max_interval_ms: f64,
}

/// Latency statistics for a single stage in the write pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathStageLatencyStats {
    /// Total call count.
    pub calls: u64,
    /// Calls per second.
    pub calls_per_sec: f64,
    /// Total time in milliseconds.
    pub total_ms: f64,
    /// Average time in milliseconds.
    pub avg_ms: f64,
    /// Max time in milliseconds.
    pub max_ms: f64,
    /// Latency quantiles in milliseconds.
    pub latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    /// Saturation markers for quantiles.
    #[serde(default)]
    pub latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Latency histogram.
    pub latency_histogram: VfsLatencyHistogramSnapshot,
}

/// Cadence metrics and interval distributions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathCadenceStats {
    /// Number of events.
    pub events: u64,
    /// Events per second.
    pub events_per_sec: f64,
    /// Average interval in milliseconds.
    pub avg_interval_ms: f64,
    /// Maximum interval in milliseconds.
    pub max_interval_ms: f64,
    /// Interval quantiles in milliseconds.
    pub interval_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    /// Saturation markers for interval quantiles.
    #[serde(default)]
    pub interval_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Interval histogram.
    pub interval_histogram: VfsLatencyHistogramSnapshot,
}

/// Metadata transactions required to create one namespace entry.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsNamespaceCreateObservabilityStats {
    /// Inode ID reservation transaction latency.
    #[serde(default)]
    pub inode_allocation: VfsWritePathStageLatencyStats,
    /// Namespace mutation transaction latency.
    #[serde(default)]
    pub namespace_mutation: VfsWritePathStageLatencyStats,
    /// Successfully committed file or directory entries.
    #[serde(default)]
    pub completed_entries: u64,
    /// Metadata write transactions per successfully created entry.
    #[serde(default)]
    pub transactions_per_created_entry: f64,
}

/// Eager flush counters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathEagerFlushStats {
    /// Total eager flush checks.
    pub checks: u64,
    /// Number of eager flushes triggered.
    pub triggered: u64,
    /// Checks skipped because queue was empty.
    pub skipped_empty: u64,
    /// Checks skipped because payload was below threshold.
    pub skipped_below_threshold: u64,
}

/// Runtime-tuning knobs reported for write path diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathTuningStats {
    /// Eager flush threshold in bytes.
    pub eager_flush_threshold_bytes: usize,
    /// Eager flush chunk size for drain in bytes.
    pub eager_flush_drain_chunk_bytes: usize,
    /// Maximum concurrent payload-put operations.
    pub payload_put_concurrency: usize,
    /// Minimum bytes required to sync a group.
    pub sync_group_min_bytes: u64,
}

/// Fetch metrics for read path operations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct VfsFetchStats {
    /// Cache hits in fetch flow.
    pub cache_hits: u64,
    /// Bytes served by local payload-cache hits.
    #[serde(default)]
    pub cache_hit_bytes: u64,
    /// S3 fetch request count.
    pub s3_fetches: u64,
    /// S3 fetch error count.
    pub s3_errors: u64,
    /// Singleflight join count.
    pub singleflight_joins: u64,
    /// Retry attempt count.
    pub retries: u64,
    /// Total bytes downloaded.
    pub bytes_downloaded: u64,
    /// Number of encrypted remote objects successfully read and decrypted.
    pub encrypted_objects_read: u64,
    /// Number of encrypted remote bytes successfully read and decrypted.
    pub encrypted_bytes_read: u64,
    /// Number of deterministic encrypted object read failures.
    pub encrypted_object_read_failures: u64,
    /// Number of malformed or unsupported encrypted objects.
    pub malformed_encrypted_objects: u64,
    /// Prefetch skips count.
    pub prefetch_skips: u64,
    /// Readahead blocks scheduled for background prefetch.
    #[serde(default)]
    pub readahead_scheduled_blocks: u64,
    /// Readahead blocks dropped by duplicate suppression or stale queue pruning.
    #[serde(default)]
    pub readahead_dropped_blocks: u64,
    /// Bytes later served from objects populated by readahead.
    #[serde(default)]
    pub readahead_used_bytes: u64,
    /// Local cache I/O calls count.
    pub local_cache_io_calls: u64,
    /// Total local cache handoff time in milliseconds.
    pub local_cache_handoff_total_ms: f64,
    /// Average local cache handoff time in milliseconds.
    pub local_cache_handoff_avg_ms: f64,
    /// Maximum local cache handoff time in milliseconds.
    pub local_cache_handoff_max_ms: f64,
    /// Total backend I/O time for local cache in milliseconds.
    pub local_cache_backend_io_total_ms: f64,
    /// Average backend I/O time for local cache in milliseconds.
    pub local_cache_backend_io_avg_ms: f64,
    /// Maximum backend I/O time for local cache in milliseconds.
    pub local_cache_backend_io_max_ms: f64,
    /// S3 GET call count.
    #[serde(default)]
    pub s3_get_calls: u64,
    /// Total S3 GET time in milliseconds.
    #[serde(default)]
    pub s3_get_total_ms: f64,
    /// Average S3 GET time in milliseconds.
    #[serde(default)]
    pub s3_get_avg_ms: f64,
    /// Maximum S3 GET time in milliseconds.
    #[serde(default)]
    pub s3_get_max_ms: f64,
    /// Object decode call count.
    #[serde(default)]
    pub object_decode_calls: u64,
    /// Total object decode time in milliseconds.
    #[serde(default)]
    pub object_decode_total_ms: f64,
    /// Average object decode time in milliseconds.
    #[serde(default)]
    pub object_decode_avg_ms: f64,
    /// Maximum object decode time in milliseconds.
    #[serde(default)]
    pub object_decode_max_ms: f64,
    /// Assemble/copy call count.
    #[serde(default)]
    pub assemble_copy_calls: u64,
    /// Total assemble/copy time in milliseconds.
    #[serde(default)]
    pub assemble_copy_total_ms: f64,
    /// Average assemble/copy time in milliseconds.
    #[serde(default)]
    pub assemble_copy_avg_ms: f64,
    /// Maximum assemble/copy time in milliseconds.
    #[serde(default)]
    pub assemble_copy_max_ms: f64,
    /// Local cache put call count.
    #[serde(default)]
    pub local_cache_put_calls: u64,
    /// Total local cache put time in milliseconds.
    #[serde(default)]
    pub local_cache_put_total_ms: f64,
    /// Average local cache put time in milliseconds.
    #[serde(default)]
    pub local_cache_put_avg_ms: f64,
    /// Maximum local cache put time in milliseconds.
    #[serde(default)]
    pub local_cache_put_max_ms: f64,
    /// Total fetch calls across all stages.
    #[serde(default)]
    pub fetch_total_calls: u64,
    /// Total fetch time in milliseconds.
    #[serde(default)]
    pub fetch_total_ms: f64,
    /// Average total fetch time in milliseconds.
    #[serde(default)]
    pub fetch_total_avg_ms: f64,
    /// Maximum total fetch time in milliseconds.
    #[serde(default)]
    pub fetch_total_max_ms: f64,
}

/// Portable allocator accounting snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsPortableAllocatorStats {
    /// Current bytes allocated.
    pub current_bytes: u64,
    /// Peak bytes observed.
    pub peak_bytes: u64,
    /// Total bytes allocated during process lifetime.
    pub allocated_total_bytes: u64,
    /// Total bytes deallocated during process lifetime.
    pub deallocated_total_bytes: u64,
    /// Number of allocation calls.
    pub allocation_count: u64,
    /// Number of deallocation calls.
    pub deallocation_count: u64,
    /// Number of reallocation calls.
    pub reallocation_count: u64,
}

/// Local cache materialization counters exposed in the live write-path snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsCachePipelineMaterializeStats {
    /// Completed materialization batches.
    #[serde(default)]
    pub batches: u64,
    /// Payloads materialized by completed batches.
    #[serde(default)]
    pub payloads: u64,
    /// Bytes materialized into the local payload cache.
    #[serde(default)]
    pub bytes: u64,
    /// Current in-flight materialization batches.
    #[serde(default)]
    pub inflight_batches: usize,
    /// Peak in-flight materialization batches.
    #[serde(default)]
    pub inflight_batches_peak: usize,
}

/// Local cache pipeline counters exposed in the live write-path snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsCachePipelineStats {
    /// Materialization counters.
    #[serde(default)]
    pub materialize: VfsCachePipelineMaterializeStats,
}

/// Complete write-path observability snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathObservabilitySnapshot {
    /// Process identifier that produced the snapshot.
    #[serde(default)]
    pub publisher_pid: u32,
    /// Snapshot generation timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// Uptime in seconds.
    pub uptime_secs: f64,
    /// Current pending uploads count.
    #[serde(default)]
    pub pending_uploads: usize,
    /// Active backend tag.
    pub backend_tag: String,
    /// Total flush-barrier calls.
    pub flush_barrier_calls: u64,
    /// Flush-barrier call rate per second.
    pub flush_barrier_calls_per_sec: f64,
    /// Sync-data payload metrics.
    pub payload_store_sync_data: VfsWritePathSyncDataStats,
    /// Local cache I/O metrics.
    #[serde(default)]
    pub local_cache_io: VfsWritePathLocalCacheIoStats,
    /// WinFSP callback counters.
    pub winfsp_callbacks: VfsWritePathWinfspCallbackStats,
    /// WinFSP write-path stage metrics.
    #[serde(default)]
    pub winfsp_write_path: VfsWritePathWinfspWritePathStats,
    /// VFS write-stage metrics.
    #[serde(default)]
    pub vfs_write_path: VfsWritePathVfsWriteStats,
    /// VFS read-stage metrics.
    #[serde(default)]
    pub vfs_read_path: VfsReadPathStats,
    /// FUSE callback metrics.
    #[serde(default)]
    pub fuse_callbacks: VfsFuseCallbackStats,
    /// macFUSE open-cache decisions.
    #[serde(default)]
    pub macfuse_open_cache: VfsMacFuseOpenCacheStats,
    /// Async FUSE write diagnostics.
    #[serde(default)]
    pub fuse_async_write: VfsFuseAsyncWriteSnapshot,
    /// Async FUSE read diagnostics.
    #[serde(default)]
    pub fuse_async_read: VfsFuseAsyncReadSnapshot,
    /// Landing-zone metrics.
    #[serde(default)]
    pub landing_zone: VfsWritePathLandingZoneStats,
    /// Drain lag metrics.
    #[serde(default)]
    pub drain_lag: VfsWritePathDrainLagStats,
    /// Foreground flush counters.
    #[serde(default)]
    pub winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats,
    /// Overall write backpressure metrics.
    pub write_backpressure: VfsWritePathBackpressureStats,
    /// Dirty pressure metrics.
    #[serde(default)]
    pub dirty_pressure: VfsWritePathDirtyPressureStats,
    /// Upload metrics.
    #[serde(default)]
    pub upload: VfsUploadStats,
    /// Fetch metrics.
    #[serde(default)]
    pub fetch: VfsFetchStats,
    /// Foreground-read protection and upload reread pressure metrics.
    #[serde(default)]
    pub foreground_reads: VfsForegroundReadProtectionStats,
    /// Local cache pipeline metrics.
    #[serde(default)]
    pub cache_pipeline: VfsCachePipelineStats,
    /// Payload burst metrics.
    #[serde(default)]
    pub payload_burst: VfsWritePathBurstStats,
    /// Payload persist stage latency.
    #[serde(default)]
    pub payload_persist: VfsWritePathStageLatencyStats,
    /// Metadata commit stage latency.
    #[serde(default)]
    pub metadata_commit: VfsWritePathStageLatencyStats,
    /// Metadata commit cadence metrics.
    #[serde(default)]
    pub metadata_commit_cadence: VfsWritePathCadenceStats,
    /// Low-level metadata-engine transaction timing.
    #[serde(default)]
    pub metadata_engine: MetadataWriteObservability,
    /// Namespace create transaction breakdown.
    #[serde(default)]
    pub namespace_create: VfsNamespaceCreateObservabilityStats,
    /// Eager-flush metrics.
    #[serde(default)]
    pub eager_flush: VfsWritePathEagerFlushStats,
    /// Tuning parameter snapshot.
    #[serde(default)]
    pub tuning: VfsWritePathTuningStats,
    /// Optional allocator snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocator: Option<VfsPortableAllocatorStats>,
}

/// Pressure lane classification for ingress scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VfsIngressLane {
    /// Dirty-memory pressure lane.
    DirtyMem,
    /// Generic backpressure lane.
    Backpressure,
}

/// Ingress pressure values used by scheduler decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VfsIngressPressure {
    /// Bytes queued in the next lane.
    pub next_bytes: usize,
    /// Dirty-memory bytes currently counted as pressure.
    pub dirty_mem_bytes: usize,
    /// Dirty-memory limit in bytes.
    pub dirty_mem_limit: usize,
    /// Lane chosen for current pressure accounting.
    pub lane: VfsIngressLane,
}

/// Upload-related counters and throughput metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct VfsUploadStats {
    /// Number of pending uploads.
    pub pending_uploads: usize,
    /// Total bytes of pending uploads.
    pub pending_upload_bytes: u64,
    /// Oldest pending upload age in milliseconds.
    pub oldest_pending_upload_age_ms: u64,
    /// Active uploads count.
    pub active_uploads: usize,
    /// Count of uploads waiting for retry.
    pub pending_retry_count: u64,
    /// Maximum pending-retry count observed.
    pub max_pending_retry_count: u32,
    /// Completed uploads count.
    pub completed_uploads: u64,
    /// Completed upload bytes.
    pub completed_upload_bytes: u64,
    /// Completed uploads per second.
    pub completed_uploads_per_sec: f64,
    /// Completed upload throughput in bytes per second.
    pub completed_upload_bytes_per_sec: f64,
    /// Number of encrypted remote objects successfully written.
    pub encrypted_objects_written: u64,
    /// Number of encrypted remote bytes written.
    pub encrypted_bytes_written: u64,
    /// Number of encryption/config failures before remote write.
    pub encrypted_object_write_failures: u64,
    /// Number of upload requeue operations.
    pub upload_requeues: u64,
    /// Uploads canceled because underlying file was deleted.
    pub canceled_deleted: u64,
    /// Uploads canceled as stale.
    pub canceled_stale: u64,
    /// Stale uploads canceled before PUT.
    pub canceled_stale_before_put: u64,
    /// Stale uploads canceled after PUT.
    pub canceled_stale_after_put: u64,
    /// Bytes in stale uploads canceled after PUT.
    pub canceled_stale_after_put_bytes: u64,
    /// Full pending-upload snapshots requested by hot paths and diagnostics.
    pub pending_upload_snapshot_calls: u64,
}

/// Zero-IO cache byte accounting snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoCacheBytesSnapshot {
    /// Total local bytes in zero-IO cache.
    pub total_local_bytes: u64,
    /// Backend bytes that are evictable.
    pub backend_evictable_bytes: u64,
    /// Backend bytes that are non-evictable.
    pub backend_non_evictable_bytes: u64,
    /// Bytes pinned in unpublished state.
    pub pinned_unpublished_bytes: u64,
    /// Bytes retained in publish-ready state.
    pub publish_ready_retained_bytes: u64,
    /// Warm published bytes.
    pub published_warm_bytes: u64,
    /// Bytes deleted before publish.
    pub deleted_before_publish_bytes: u64,
    /// Unclassified non-evictable bytes.
    pub unclassified_non_evictable_bytes: u64,
}

/// Startup replay metrics for zero-IO mode.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoStartupReplaySnapshot {
    /// Snapshot generation timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// Replayed epochs count.
    pub replayed_epochs: u64,
    /// Replayed blocks count.
    pub replayed_blocks: u64,
    /// Replayed bytes count.
    pub replayed_bytes: u64,
    /// Queued uploads count after replay.
    pub queued_uploads: u64,
    /// Publish-ready queued epochs count.
    pub queued_publish_ready_epochs: u64,
    /// Quarantined inodes count.
    pub quarantined_inodes: u64,
    /// Orphan dirty payloads removed count.
    pub orphan_dirty_payloads_removed: u64,
    /// Unreferenced local payloads removed count.
    pub unreferenced_local_payloads_removed: u64,
    /// Startup cannot inspect bytes that lived only in a crashed process.
    /// Non-zero values require future persistent shutdown/crash evidence.
    pub discarded_memory_only_bytes: u64,
    /// Replay failures count.
    pub replay_failures: u64,
}

/// Shutdown snapshot for zero-IO mode.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoShutdownSnapshot {
    /// Snapshot generation timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// Accepted local dirty bytes before shutdown path.
    pub accepted_local_dirty_bytes_before: u64,
    /// Accepted local dirty bytes after shutdown path.
    pub accepted_local_dirty_bytes_after: u64,
    /// Pending uploads remaining after shutdown.
    pub pending_uploads_after: usize,
    /// Whether uploader drained before cancellation.
    pub uploader_drained_before_cancel: bool,
    /// Human-readable metadata flush outcome.
    pub metadata_flush_outcome: String,
    /// Whether local payload sync succeeded.
    pub local_payload_sync_ok: bool,
    /// Whether state is recoverable on restart.
    pub recoverable_on_restart: bool,
}

/// Health signal emitted for zero-IO diagnostics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoHealthSignal {
    /// Signal severity level.
    pub severity: String,
    /// Signal code.
    pub code: String,
    /// Short signal summary.
    pub summary: String,
    /// Optional inode identifier for the signal.
    #[serde(default)]
    pub ino: Option<u64>,
    /// Optional content epoch for the signal.
    #[serde(default)]
    pub content_epoch: Option<u64>,
    /// Optional file path for the signal.
    #[serde(default)]
    pub path: Option<String>,
}

/// Per-object drill-down item for zero-IO status.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoDrilldownEntry {
    /// Inode identifier.
    pub ino: u64,
    /// Optional human-readable path.
    #[serde(default)]
    pub path: Option<String>,
    /// Current inode lifecycle state.
    pub lifecycle_state: InodeLifecycleState,
    /// Payload retention class.
    pub retention: PayloadRetentionClass,
    /// Local content epoch.
    pub local_content_epoch: u64,
    /// Published content epoch.
    pub published_content_epoch: u64,
    /// Local visible file size.
    pub local_visible_size: u64,
    /// Published visible file size.
    pub published_size: u64,
    /// Number of live extents.
    pub live_extent_count: usize,
    /// Number of pending uploads.
    pub pending_upload_count: usize,
    /// Pending upload bytes.
    pub pending_upload_bytes: u64,
    /// Oldest pending upload age in milliseconds.
    pub oldest_pending_upload_age_ms: u64,
    /// Indicates inode is queued publish-ready.
    pub publish_ready_queued: bool,
    /// Indicates inode was replayed after restart.
    pub replayed_after_restart: bool,
    /// Repeated replay count.
    pub repeated_replay_count: u64,
    /// Deferred cleanup object count.
    pub deferred_cleanup_object_count: usize,
    /// Deferred cleanup bytes.
    pub deferred_cleanup_bytes: u64,
    /// Optional sticky error text.
    #[serde(default)]
    pub sticky_error: Option<String>,
    /// Current status string.
    pub status: String,
}

/// Runtime zero-IO snapshot with nested drill-down details.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoRuntimeSnapshot {
    /// Snapshot generation timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// Number of unpublished files.
    pub unpublished_files: usize,
    /// Number of unpublished epochs.
    pub unpublished_epochs: usize,
    /// Total unpublished payload bytes.
    pub unpublished_payload_bytes: u64,
    /// Uploading epoch count.
    pub uploading_epochs: usize,
    /// Replay backlog epoch count.
    pub replay_backlog_epochs: usize,
    /// Replay backlog bytes.
    pub replay_backlog_bytes: u64,
    /// Publish-ready backlog epoch count.
    pub publish_ready_backlog_epochs: usize,
    /// Epochs replayed after restart.
    pub replayed_after_restart_epochs: u64,
    /// Deferred delete backlog epoch count.
    pub deferred_delete_backlog_epochs: usize,
    /// Deferred delete backlog object count.
    pub deferred_delete_backlog_objects: usize,
    /// Deferred delete backlog bytes.
    pub deferred_delete_backlog_bytes: u64,
    /// Total replay failures.
    pub replay_failures: u64,
    /// Total publish failures.
    pub publish_failures: u64,
    /// Cache accounting snapshot.
    pub cache_bytes: VfsZeroIoCacheBytesSnapshot,
    /// Optional startup replay details.
    #[serde(default)]
    pub startup_replay: Option<VfsZeroIoStartupReplaySnapshot>,
    /// Optional latest graceful-shutdown snapshot.
    #[serde(default)]
    pub last_graceful_shutdown: Option<VfsZeroIoShutdownSnapshot>,
    /// Health signals present in runtime.
    #[serde(default)]
    pub health_signals: Vec<VfsZeroIoHealthSignal>,
    /// Number of truncated drilldown records.
    pub drilldown_truncated: usize,
    /// Optional drilldown entries.
    #[serde(default)]
    pub drilldown: Vec<VfsZeroIoDrilldownEntry>,
}

/// Foreground read protection metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsForegroundReadProtectionStats {
    /// 90th percentile read latency in nanoseconds.
    pub p90_latency_ns: Option<u64>,
    /// Throughput in bytes per second.
    pub throughput_bytes_per_sec: f64,
    /// Samples used by speedometer estimator.
    pub speedometer_samples: usize,
    /// Read window size in bytes.
    pub window_bytes: u64,
    /// Read window coverage in milliseconds.
    pub window_coverage_ms: u64,
    /// Dominant access class name.
    pub dominant_access_class: Option<String>,
    /// Dominant prefetch setting.
    pub dominant_prefetch_enabled: bool,
    /// Dominant cache-promotion-on-read flag.
    pub dominant_cache_promotion_on_read: bool,
    /// Dominant readahead base width.
    pub dominant_readahead_base_width: u32,
    /// Dominant readahead max width.
    pub dominant_readahead_max_width: u32,
    /// Dominant minimum prefetch size.
    pub dominant_readahead_min_prefetch: u32,
    /// Dominant upload-pressure boost.
    pub dominant_upload_pressure_boost: u8,
    /// Active sequential media reads.
    pub active_sequential_media_reads: u64,
    /// Active random interactive reads.
    pub active_random_interactive_reads: u64,
    /// Active sequential writers.
    pub active_sequential_writers: u64,
    /// Active random writers.
    pub active_random_writers: u64,
    /// Upload local payload reads count.
    pub upload_local_payload_reads: u64,
    /// Upload local payload read bytes.
    pub upload_local_payload_read_bytes: u64,
    /// Upload reads served from the hot in-memory payload buffer.
    pub upload_hot_payload_reads: u64,
    /// Upload bytes served from the hot in-memory payload buffer.
    pub upload_hot_payload_read_bytes: u64,
    /// Upload reads served from staged payload hardlinks.
    pub upload_staged_payload_reads: u64,
    /// Upload bytes served from staged payload hardlinks.
    pub upload_staged_payload_read_bytes: u64,
    /// Upload fallback deferrals count.
    pub upload_fallback_deferrals: u64,
    /// Upload fallback rereads count.
    pub upload_fallback_rereads: u64,
    /// Upload fallback reread bytes.
    pub upload_fallback_reread_bytes: u64,
    /// Upload fallback reread cache misses.
    pub upload_fallback_reread_cache_misses: u64,
    /// Upload fallback wait time in nanoseconds.
    pub upload_fallback_wait_ns: u64,
    /// Maximum upload fallback wait time in nanoseconds.
    pub upload_fallback_wait_max_ns: u64,
    /// Upload fallback read time in nanoseconds.
    pub upload_fallback_read_ns: u64,
    /// Maximum upload fallback read time in nanoseconds.
    pub upload_fallback_read_max_ns: u64,
    /// Foreground local payload reads count.
    pub foreground_local_payload_reads: u64,
    /// Foreground local payload read bytes.
    pub foreground_local_payload_read_bytes: u64,
    /// Foreground local payload backend I/O time in nanoseconds.
    pub foreground_local_payload_read_io_ns: u64,
    /// Maximum foreground local payload backend I/O time in nanoseconds.
    pub foreground_local_payload_read_io_max_ns: u64,
    /// Foreground cache I/O cost metric.
    pub upload_cache_io_foreground_cost: u32,
    /// Scheduler penalty for upload in milliseconds.
    pub upload_scheduler_penalty_ms: u64,
}

/// Namespace cache accounting snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsNamespaceCacheSnapshot {
    /// Cache hit count.
    pub hits: u64,
    /// Protected cache hits count.
    pub protected_hits: u64,
    /// Cache miss count.
    pub misses: u64,
    /// Negative cache hit count.
    pub negative_hits: u64,
    /// Eviction count.
    pub evictions: u64,
    /// Negative hit ratio.
    pub negative_hit_ratio: f64,
    /// Hot-hit ratio.
    pub hot_hit_ratio: f64,
    /// Total entries.
    pub occupancy_entries: usize,
    /// Total bytes occupied.
    pub occupancy_bytes: usize,
    /// Protected occupancy entries.
    pub protected_occupancy_entries: usize,
    /// Protected occupancy bytes.
    pub protected_occupancy_bytes: usize,
    /// Entry budget.
    pub entry_budget: usize,
    /// Byte budget.
    pub byte_budget: usize,
}

/// Attribute cache accounting snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsAttrReadCacheSnapshot {
    /// Cache hit count.
    pub hits: u64,
    /// Protected cache hits.
    pub protected_hits: u64,
    /// Cache miss count.
    pub misses: u64,
    /// Eviction count.
    pub evictions: u64,
    /// Hot-hit ratio.
    pub hot_hit_ratio: f64,
    /// Total entries.
    pub occupancy_entries: usize,
    /// Total bytes occupied.
    pub occupancy_bytes: usize,
    /// Protected occupancy entries.
    pub protected_occupancy_entries: usize,
    /// Protected occupancy bytes.
    pub protected_occupancy_bytes: usize,
    /// Entry budget.
    pub entry_budget: usize,
    /// Byte budget.
    pub byte_budget: usize,
}

/// Transit memory accounting for ongoing work.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsTransitMemorySnapshot {
    /// Transit memory budget in bytes.
    pub budget_bytes: usize,
    /// Transit memory currently used in bytes.
    pub used_bytes: usize,
    /// Transit memory currently available in bytes.
    pub available_bytes: usize,
    /// Number of foreground operations waiting for memory.
    pub waiting_foreground: usize,
    /// Number of background operations waiting for memory.
    pub waiting_background: usize,
}

/// Cached state snapshot for the VFS.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsCacheStateSnapshot {
    /// Dentry cache metrics.
    pub dentry_cache: VfsNamespaceCacheSnapshot,
    /// Path shortcut cache metrics.
    pub path_shortcut_cache: VfsNamespaceCacheSnapshot,
    /// Attr read cache metrics.
    pub attr_read_cache: VfsAttrReadCacheSnapshot,
    /// Number of local backend dirty objects.
    pub local_backend_dirty_objects: usize,
    /// Current prefetch queue depth.
    pub prefetch_queue_depth: u64,
    /// Transit memory snapshot.
    pub transit_memory: VfsTransitMemorySnapshot,
}

/// Snapshot of synchronization state for active reads and writes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsSynchronizationStateSnapshot {
    /// Per-file session statistics.
    pub file_sessions: VfsFileSessionStats,
    /// Foreground read protection metrics.
    pub foreground_reads: VfsForegroundReadProtectionStats,
    /// Pending metadata operation count.
    pub pending_metadata_ops: u32,
    /// In-flight metadata operation count.
    pub inflight_metadata_ops: u32,
}

/// Runtime event emitted by metrics stream.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsRuntimeEvent {
    /// Monotonic sequence number.
    pub sequence: u64,
    /// Event timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// Event subsystem.
    pub subsystem: String,
    /// Event kind.
    pub kind: String,
    /// Event title.
    pub title: String,
    /// Event fields payload.
    #[serde(default)]
    pub fields: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dirty_pressure_serializes_slice_counts_for_mounted_reports() {
        let serialized = serde_json::to_value(VfsWritePathDirtyPressureStats::default())
            .expect("serialize dirty pressure");

        assert!(serialized["dirty_slice_count"].is_number());
        assert!(serialized["frozen_slice_count"].is_number());
    }

    #[test]
    fn fetch_stats_deserializes_missing_future_fields_as_defaults() {
        let stats: VfsFetchStats = serde_json::from_value(serde_json::json!({
            "cache_hits": 7,
            "s3_fetches": 3
        }))
        .expect("deserialize fetch stats with omitted fields");

        assert_eq!(stats.cache_hits, 7);
        assert_eq!(stats.s3_fetches, 3);
        assert_eq!(stats.encrypted_objects_read, 0);
        assert_eq!(stats.encrypted_bytes_read, 0);
        assert_eq!(stats.encrypted_object_read_failures, 0);
        assert_eq!(stats.malformed_encrypted_objects, 0);
    }

    #[test]
    fn macfuse_open_cache_stats_serializes_new_not_regular_file_key_and_zero_legacy_key() {
        let stats = VfsMacFuseOpenCacheStats {
            read_only_not_regular_file: 5,
            ..Default::default()
        };

        let serialized = serde_json::to_value(stats).expect("serialize macFUSE open-cache stats");

        assert_eq!(serialized["read_only_not_regular_file"], 5);
        assert_eq!(serialized["read_only_not_fully_committed"], 0);
    }

    #[test]
    fn macfuse_open_cache_stats_deserializes_new_not_regular_file_key() {
        let serialized = serde_json::to_value(VfsMacFuseOpenCacheStats {
            read_only_not_regular_file: 7,
            ..Default::default()
        })
        .expect("serialize macFUSE open-cache stats");

        let stats: VfsMacFuseOpenCacheStats = serde_json::from_value(serialized)
            .expect("deserialize macFUSE open-cache stats with renamed field");

        assert_eq!(stats.read_only_not_regular_file, 7);
    }

    #[test]
    fn runtime_stats_snapshot_serializes_machine_readable_sections() {
        let mut snapshot = RuntimeStatsSnapshot {
            generated_unix_ms: 1_720_000_000_000,
            ..RuntimeStatsSnapshot::default()
        };
        snapshot.vfs_read.counters.insert(
            "bytes".to_owned(),
            RuntimeScalarMetric {
                value: 4096,
                unit: Cow::Borrowed("bytes"),
            },
        );
        snapshot.upload_queue.gauges.insert(
            "pending_depth".to_owned(),
            RuntimeScalarMetric {
                value: 2,
                unit: Cow::Borrowed("count"),
            },
        );
        snapshot.object_io.latencies.insert(
            "get".to_owned(),
            RuntimeLatencyHistogram {
                count: 3,
                sum: 120_000,
                unit: Cow::Borrowed("ns"),
                buckets: vec![RuntimeLatencyBucket {
                    le_ns: 100_000,
                    count: 2,
                }],
            },
        );

        let serialized = serde_json::to_value(&snapshot).expect("serialize runtime stats snapshot");

        assert_eq!(serialized["schema_version"], RUNTIME_STATS_SCHEMA_VERSION);
        assert_eq!(serialized["vfs_read"]["counters"]["bytes"]["unit"], "bytes");
        assert_eq!(
            serialized["upload_queue"]["gauges"]["pending_depth"]["value"],
            2
        );
        assert_eq!(serialized["object_io"]["latencies"]["get"]["count"], 3);
        assert_eq!(serialized["object_io"]["latencies"]["get"]["sum"], 120_000);
    }

    #[test]
    fn runtime_stats_snapshot_deserializes_missing_optional_fields_as_defaults() {
        let snapshot: RuntimeStatsSnapshot = serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "generated_unix_ms": 44,
            "vfs_write": {
                "counters": {
                    "bytes": { "value": 128, "unit": "bytes" }
                }
            }
        }))
        .expect("deserialize runtime stats snapshot with omitted sections");

        assert_eq!(snapshot.generated_unix_ms, 44);
        assert_eq!(snapshot.vfs_write.counters["bytes"].value, 128);
        assert!(snapshot.system.counters.is_empty());
        assert!(snapshot.bridge.gauges.is_empty());
        assert!(snapshot.metadata.latencies.is_empty());
    }

    #[test]
    fn runtime_stats_text_omits_duplicate_root_system_gauges() {
        let snapshot: RuntimeStatsSnapshot = serde_json::from_value(serde_json::json!({
            "schema_version": 7,
            "generated_unix_ms": 55,
            "system": {
                "gauges": {
                    "schema_version": { "value": 1, "unit": "count" },
                    "generated_unix_ms": { "value": 1, "unit": "ms" },
                    "uptime_ms": { "value": 12, "unit": "ms" }
                }
            }
        }))
        .expect("deserialize runtime stats snapshot with duplicate system gauges");

        let text = snapshot.to_text();

        assert!(!snapshot.system.gauges.contains_key("schema_version"));
        assert!(!snapshot.system.gauges.contains_key("generated_unix_ms"));
        assert_eq!(text.matches("gauge system.schema_version").count(), 1);
        assert_eq!(text.matches("gauge system.generated_unix_ms").count(), 1);
        assert!(text.contains("gauge system.uptime_ms ms 12\n"));
    }

    #[test]
    fn runtime_stats_snapshot_deserializes_duplicate_system_gauges_as_root_fields() {
        let from_json: RuntimeStatsSnapshot = serde_json::from_value(serde_json::json!({
            "schema_version": 7,
            "generated_unix_ms": 55,
            "system": {
                "gauges": {
                    "schema_version": { "value": 7, "unit": "count" },
                    "generated_unix_ms": { "value": 55, "unit": "ms" },
                    "uptime_ms": { "value": 12, "unit": "ms" }
                }
            }
        }))
        .expect("deserialize runtime stats snapshot with duplicate system gauges");

        let from_text =
            RuntimeStatsSnapshot::parse_text(&from_json.to_text()).expect("parse rendered stats");

        assert_eq!(from_json, from_text);
        assert_eq!(from_json.schema_version, 7);
        assert_eq!(from_json.generated_unix_ms, 55);
        assert_eq!(from_json.system.gauges["uptime_ms"].value, 12);
    }

    #[test]
    fn runtime_stats_snapshot_deserializes_reserved_system_keys_only_as_root_fields() {
        let snapshot: RuntimeStatsSnapshot = serde_json::from_value(serde_json::json!({
            "schema_version": 7,
            "generated_unix_ms": 55,
            "system": {
                "counters": {
                    "schema_version": { "value": 1, "unit": "count" },
                    "calls": { "value": 2, "unit": "count" }
                },
                "gauges": {
                    "generated_unix_ms": { "value": 1, "unit": "ms" },
                    "uptime_ms": { "value": 12, "unit": "ms" }
                },
                "latencies": {
                    "schema_version": {
                        "count": 1,
                        "sum": 1,
                        "unit": "ns",
                        "buckets": []
                    },
                    "poll": {
                        "count": 1,
                        "sum": 2,
                        "unit": "ns",
                        "buckets": []
                    }
                }
            }
        }))
        .expect("deserialize runtime stats snapshot with reserved system keys");

        assert!(!snapshot.system.counters.contains_key("schema_version"));
        assert!(!snapshot.system.gauges.contains_key("generated_unix_ms"));
        assert!(!snapshot.system.latencies.contains_key("schema_version"));
        assert_eq!(snapshot.system.counters["calls"].value, 2);
        assert_eq!(snapshot.system.gauges["uptime_ms"].value, 12);
        assert_eq!(snapshot.system.latencies["poll"].sum, 2);
    }

    #[test]
    fn runtime_stats_snapshot_deserializes_sorted_unique_histogram_buckets() {
        let snapshot: RuntimeStatsSnapshot = serde_json::from_value(serde_json::json!({
            "object_io": {
                "latencies": {
                    "get": {
                        "count": 3,
                        "sum": 90_000,
                        "unit": "ns",
                        "buckets": [
                            { "le_ns": 500_000, "count": 1 },
                            { "le_ns": 100_000, "count": 2 },
                            { "le_ns": 500_000, "count": 3 }
                        ]
                    }
                }
            }
        }))
        .expect("deserialize runtime stats snapshot with unordered buckets");

        assert_eq!(
            snapshot.object_io.latencies["get"].buckets,
            vec![
                RuntimeLatencyBucket {
                    le_ns: 100_000,
                    count: 2,
                },
                RuntimeLatencyBucket {
                    le_ns: 500_000,
                    count: 3,
                },
            ]
        );
    }

    #[test]
    fn runtime_stats_text_round_trips_counter_gauge_and_latency_histogram() {
        let mut snapshot = RuntimeStatsSnapshot {
            generated_unix_ms: 1_720_000_000_000,
            ..RuntimeStatsSnapshot::default()
        };
        snapshot.bridge.counters.insert(
            "read_callbacks".to_owned(),
            RuntimeScalarMetric {
                value: 9,
                unit: Cow::Borrowed("count"),
            },
        );
        snapshot.cache.gauges.insert(
            "resident_bytes".to_owned(),
            RuntimeScalarMetric {
                value: 8192,
                unit: Cow::Borrowed("bytes"),
            },
        );
        snapshot.metadata.latencies.insert(
            "commit".to_owned(),
            RuntimeLatencyHistogram {
                count: 2,
                sum: 300_000,
                unit: Cow::Borrowed("ns"),
                buckets: vec![
                    RuntimeLatencyBucket {
                        le_ns: 100_000,
                        count: 1,
                    },
                    RuntimeLatencyBucket {
                        le_ns: 500_000,
                        count: 1,
                    },
                ],
            },
        );

        let text = snapshot.to_text();
        let parsed = RuntimeStatsSnapshot::parse_text(&text).expect("parse runtime stats text");
        let parsed_from_trait: RuntimeStatsSnapshot = snapshot
            .to_string()
            .parse()
            .expect("parse runtime stats text via FromStr");

        assert_eq!(parsed, snapshot);
        assert_eq!(parsed_from_trait, snapshot);
        assert!(matches!(
            parsed.cache.gauges["resident_bytes"].unit,
            Cow::Borrowed("bytes")
        ));
        assert!(matches!(
            parsed.metadata.latencies["commit"].unit,
            Cow::Borrowed("ns")
        ));
    }

    #[test]
    fn runtime_stats_text_rejects_trailing_histogram_fields() {
        let histogram_total =
            RuntimeStatsSnapshot::parse_text("histogram_count object_io.get ns 1 unexpected\n")
                .expect_err("trailing histogram total field should fail");
        let histogram_bucket = RuntimeStatsSnapshot::parse_text(
            "histogram_bucket object_io.get 1000000 1 unexpected\n",
        )
        .expect_err("trailing histogram bucket field should fail");

        assert!(
            histogram_total
                .to_string()
                .contains("unexpected trailing fields")
        );
        assert!(
            histogram_bucket
                .to_string()
                .contains("unexpected trailing fields")
        );
    }

    #[test]
    fn runtime_stats_text_rejects_empty_metric_section_or_key() {
        let empty_key = RuntimeStatsSnapshot::parse_text("counter system. count 1\n")
            .expect_err("empty metric key should fail");
        let empty_section = RuntimeStatsSnapshot::parse_text("counter .calls count 1\n")
            .expect_err("empty metric section should fail");

        assert!(
            empty_key
                .to_string()
                .contains("metric section and key must not be empty")
        );
        assert_eq!(empty_key.line(), 1);
        assert_eq!(
            empty_key.message(),
            "metric section and key must not be empty"
        );
        assert!(
            empty_section
                .to_string()
                .contains("metric section and key must not be empty")
        );
    }

    #[test]
    fn runtime_stats_text_rejects_metric_keys_with_extra_dots() {
        let error =
            RuntimeStatsSnapshot::parse_text("counter vfs_read.read.failed_bytes bytes 1\n")
                .expect_err("dotted metric key should fail");

        assert_eq!(error.line(), 1);
        assert_eq!(error.message(), "metric key must not contain dots");
    }

    #[test]
    fn runtime_stats_text_ignores_unknown_future_sections() {
        let parsed = RuntimeStatsSnapshot::parse_text(
            "\
            counter network.rx_bytes bytes 64\n\
            gauge network.connected count 1\n\
            histogram_count network.round_trip ns 2\n\
            histogram_sum network.round_trip ns 300\n\
            histogram_bucket network.round_trip 500 2\n\
            counter vfs_read.calls count 4\n\
            ",
        )
        .expect("unknown future sections should be skipped");

        assert_eq!(parsed.vfs_read.counters["calls"].value, 4);
        for section in [
            &parsed.system,
            &parsed.bridge,
            &parsed.vfs_write,
            &parsed.cache,
            &parsed.upload_queue,
            &parsed.object_io,
            &parsed.metadata,
        ] {
            assert!(section.counters.is_empty());
            assert!(section.gauges.is_empty());
            assert!(section.latencies.is_empty());
        }
    }

    #[test]
    fn runtime_stats_text_rejects_root_system_metrics_with_wrong_units() {
        let schema_version =
            RuntimeStatsSnapshot::parse_text("gauge system.schema_version bytes 1\n")
                .expect_err("schema version unit should fail");
        let generated_unix_ms =
            RuntimeStatsSnapshot::parse_text("gauge system.generated_unix_ms count 1\n")
                .expect_err("generated timestamp unit should fail");

        assert_eq!(
            schema_version.message(),
            "schema_version must have unit 'count'"
        );
        assert_eq!(
            generated_unix_ms.message(),
            "generated_unix_ms must have unit 'ms'"
        );
    }

    #[test]
    fn runtime_stats_text_rejects_root_system_metrics_as_counters() {
        let schema_version =
            RuntimeStatsSnapshot::parse_text("counter system.schema_version count 1\n")
                .expect_err("schema version counter should fail");
        let generated_unix_ms =
            RuntimeStatsSnapshot::parse_text("counter system.generated_unix_ms ms 1\n")
                .expect_err("generated timestamp counter should fail");

        assert_eq!(schema_version.message(), "schema_version must be a gauge");
        assert_eq!(
            generated_unix_ms.message(),
            "generated_unix_ms must be a gauge"
        );
    }

    #[test]
    fn runtime_stats_text_rejects_root_system_metrics_as_histograms() {
        let total =
            RuntimeStatsSnapshot::parse_text("histogram_count system.schema_version ns 1\n")
                .expect_err("schema version histogram total should fail");
        let bucket =
            RuntimeStatsSnapshot::parse_text("histogram_bucket system.generated_unix_ms 100 1\n")
                .expect_err("generated timestamp histogram bucket should fail");

        assert_eq!(
            total.message(),
            "system schema_version and generated_unix_ms cannot be histograms"
        );
        assert_eq!(
            bucket.message(),
            "system schema_version and generated_unix_ms cannot be histograms"
        );
    }

    #[test]
    fn runtime_stats_text_rejects_histogram_unit_mismatch() {
        let error = RuntimeStatsSnapshot::parse_text(
            "\
            histogram_count object_io.get ns 1\n\
            histogram_sum object_io.get ms 2\n\
            ",
        )
        .expect_err("histogram total unit mismatch should fail");

        assert_eq!(error.line(), 2);
        assert_eq!(error.message(), "histogram unit mismatch");
    }

    #[test]
    fn runtime_stats_text_rejects_zero_value_histogram_unit_mismatch_after_bucket() {
        let error = RuntimeStatsSnapshot::parse_text(
            "\
            histogram_bucket object_io.get 100000 1\n\
            histogram_count object_io.get ns 0\n\
            histogram_sum object_io.get ms 0\n\
            ",
        )
        .expect_err("explicit zero-valued histogram unit mismatch should fail");

        assert_eq!(error.line(), 3);
        assert_eq!(error.message(), "histogram unit mismatch");
    }

    #[test]
    fn runtime_stats_text_allows_bucket_before_histogram_total_unit() {
        let parsed = RuntimeStatsSnapshot::parse_text(
            "\
            histogram_bucket object_io.get 100000 1\n\
            histogram_count object_io.get ms 1\n\
            histogram_sum object_io.get ms 2\n\
            ",
        )
        .expect("bucket-created default unit should accept first total unit");

        assert_eq!(parsed.object_io.latencies["get"].unit, Cow::Borrowed("ms"));
    }

    #[test]
    fn runtime_stats_display_uses_snapshot_schema_version_in_header() {
        let snapshot = RuntimeStatsSnapshot {
            schema_version: 7,
            ..RuntimeStatsSnapshot::default()
        };

        assert!(
            snapshot
                .to_string()
                .starts_with("# studiofs_runtime_stats schema_version=7\n")
        );
    }

    #[test]
    fn runtime_stats_sections_constant_matches_accessors() {
        let mut snapshot = RuntimeStatsSnapshot::default();
        for section in RUNTIME_STATS_SECTIONS {
            assert!(
                snapshot.section_mut(section).is_some(),
                "section {section} is missing from section_mut"
            );
        }

        let section_names: Vec<&str> = snapshot.sections().map(|(name, _)| name).collect();
        assert_eq!(section_names, RUNTIME_STATS_SECTIONS);
    }

    #[test]
    fn runtime_stats_text_ignores_inline_comments() {
        let parsed = RuntimeStatsSnapshot::parse_text(
            "\
            # leading comment\n\
            counter vfs_read.calls count 4 # reads issued\n\
            histogram_count object_io.get ns 2 # get samples\n\
            histogram_bucket object_io.get 500000 1 # p50-ish bucket\n\
            ",
        )
        .expect("parse runtime stats with inline comments");

        assert_eq!(parsed.vfs_read.counters["calls"].value, 4);
        assert_eq!(parsed.object_io.latencies["get"].count, 2);
        assert_eq!(
            parsed.object_io.latencies["get"].buckets,
            vec![RuntimeLatencyBucket {
                le_ns: 500_000,
                count: 1,
            }]
        );
    }

    #[test]
    fn runtime_stats_text_sorts_and_deduplicates_histogram_buckets() {
        let parsed = RuntimeStatsSnapshot::parse_text(
            "\
            histogram_bucket object_io.get 500000 1\n\
            histogram_bucket object_io.get 100000 2\n\
            histogram_bucket object_io.get 500000 3\n\
            ",
        )
        .expect("parse runtime stats histogram buckets");

        assert_eq!(
            parsed.object_io.latencies["get"].buckets,
            vec![
                RuntimeLatencyBucket {
                    le_ns: 100_000,
                    count: 2,
                },
                RuntimeLatencyBucket {
                    le_ns: 500_000,
                    count: 3,
                },
            ]
        );
        assert_eq!(parsed.object_io.latencies["get"].unit, Cow::Borrowed("ns"));
    }
}
