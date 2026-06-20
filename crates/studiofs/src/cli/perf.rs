//! `studiofs perf` - live runtime stats viewer for mounted StudioFS volumes.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use clap::{Args, ValueEnum};
use serde::Serialize;
use studiofs_vfs::RuntimeScalarMetric;
use studiofs_vfs::{
    RUNTIME_STATS_FILE_NAME, RUNTIME_STATS_SECTIONS, RuntimeLatencyHistogram, RuntimeStatsSection,
    RuntimeStatsSnapshot,
};

use crate::diagnostics;

const DEFAULT_INTERVAL_MS: u64 = 1_000;
const TABLE_HEADER_REPEAT_SAMPLES: u64 = 30;
const TABLE_CELL_WIDTH: usize = 8;
const SECTION_ALIAS_NAMES: &[&str] = &["runtime", "vfs", "upload", "object"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum PerfVerbosity {
    Normal,
    Verbose,
}

#[derive(Debug, Args)]
#[command(about = "Watch live StudioFS runtime performance counters from a mounted volume")]
#[command(after_help = "\
EXAMPLES:
  studiofs perf Z:
  studiofs perf /mnt/studio --interval 500 --count 10
  studiofs perf Z: --section bridge --section vfs-read
  studiofs perf Z: --json-lines --count 5
  studiofs perf --schema
")]
pub(crate) struct PerfArgs {
    /// StudioFS mount point containing the hidden runtime stats file
    #[arg(required_unless_present = "schema")]
    pub(crate) mountpoint: Option<PathBuf>,

    /// Sampling interval in milliseconds
    #[arg(long, default_value_t = DEFAULT_INTERVAL_MS)]
    pub(crate) interval: u64,

    /// Number of interval samples to print. Omit for a continuous live view.
    #[arg(long)]
    pub(crate) count: Option<u64>,

    /// Print the stable runtime stats schema and exit
    #[arg(long)]
    pub(crate) schema: bool,

    /// Restrict output to a stable section. Repeat to include multiple sections.
    #[arg(long = "section")]
    pub(crate) sections: Vec<String>,

    /// Output detail level
    #[arg(long, value_enum, default_value_t = PerfVerbosity::Normal)]
    pub(crate) verbosity: PerfVerbosity,

    /// Print a finite buffered sample set as pretty JSON. Requires --count.
    #[arg(long)]
    pub(crate) json: bool,

    /// Print each sample as compact newline-delimited JSON
    #[arg(long)]
    pub(crate) json_lines: bool,
}

impl PerfArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        let options = self.into_options()?;
        let mut stdout = io::stdout().lock();
        run_perf(&options, &mut stdout)
    }

    fn into_options(self) -> anyhow::Result<PerfOptions> {
        if self.interval == 0 {
            bail!("--interval must be greater than zero milliseconds");
        }
        if self.json && self.json_lines {
            bail!("--json and --json-lines are mutually exclusive");
        }
        if self.json && !self.schema && self.count.is_none() {
            bail!(
                "--json requires --count for finite output; use --json-lines for continuous streaming"
            );
        }
        if !self.schema && self.mountpoint.is_none() {
            bail!("<mountpoint> is required unless --schema is passed");
        }

        Ok(PerfOptions {
            mountpoint: self.mountpoint,
            interval: Duration::from_millis(self.interval),
            count: self.count,
            schema: self.schema,
            sections: parse_sections(&self.sections)?,
            verbosity: self.verbosity,
            output: if self.json {
                PerfOutput::Json
            } else if self.json_lines {
                PerfOutput::JsonLines
            } else {
                PerfOutput::Table
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PerfOptions {
    mountpoint: Option<PathBuf>,
    interval: Duration,
    count: Option<u64>,
    schema: bool,
    sections: Vec<&'static str>,
    verbosity: PerfVerbosity,
    output: PerfOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PerfOutput {
    Table,
    Json,
    JsonLines,
}

#[derive(Debug, Serialize, PartialEq)]
struct PerfSample {
    schema_version: u16,
    generated_unix_ms: u64,
    interval_ms: u64,
    sections: BTreeMap<&'static str, PerfSectionSample>,
    warnings: Vec<String>,
}

#[derive(Debug, Default, Serialize, PartialEq)]
struct PerfSectionSample {
    counters: BTreeMap<String, PerfCounterSample>,
    gauges: BTreeMap<String, PerfGaugeSample>,
    latencies: BTreeMap<String, PerfLatencySample>,
}

#[derive(Debug, Serialize, PartialEq)]
struct PerfCounterSample {
    total: u64,
    delta: u64,
    rate_per_sec: f64,
    unit: Cow<'static, str>,
}

#[derive(Debug, Serialize, PartialEq)]
struct PerfGaugeSample {
    value: u64,
    unit: Cow<'static, str>,
}

#[derive(Debug, Serialize, PartialEq)]
struct PerfLatencySample {
    count_delta: u64,
    sum_delta: u64,
    avg: Option<f64>,
    unit: Cow<'static, str>,
}

#[derive(Debug, Default)]
struct TableOutputState {
    samples_printed: u64,
    printed_warnings: BTreeSet<String>,
}

fn run_perf(options: &PerfOptions, out: &mut impl Write) -> anyhow::Result<()> {
    if options.schema {
        write_schema(out, options)?;
        return Ok(());
    }

    let mountpoint = options
        .mountpoint
        .as_deref()
        .context("<mountpoint> is required unless --schema is passed")?;
    if let Some(client) = diagnostics_client_for_mount(mountpoint)? {
        return run_perf_with_sampler(
            options,
            out,
            || read_diagnostics_snapshot(&client),
            thread::sleep,
        );
    }

    let stats_path = stats_path(mountpoint);
    run_perf_with_sampler(options, out, || read_snapshot(&stats_path), thread::sleep)
}

fn run_perf_with_sampler(
    options: &PerfOptions,
    out: &mut impl Write,
    mut read_next: impl FnMut() -> anyhow::Result<RuntimeStatsSnapshot>,
    mut sleep: impl FnMut(Duration),
) -> anyhow::Result<()> {
    if matches!(options.output, PerfOutput::Json) {
        return run_json_perf_with_sampler(options, out, read_next, sleep);
    }

    if matches!(options.count, Some(0)) {
        return Ok(());
    }

    let mut previous = read_next()?;
    let mut remaining = options.count;
    let mut table_state = TableOutputState::default();
    let mut next_deadline = Instant::now() + options.interval;

    loop {
        if matches!(remaining, Some(0)) {
            return Ok(());
        }

        sleep_until_next_deadline(&mut next_deadline, options.interval, &mut sleep);
        let current = read_next()?;
        let sample = build_sample(&previous, &current, options);
        write_sample(out, &sample, options, &mut table_state)?;
        out.flush()?;
        previous = current;

        if let Some(count) = remaining.as_mut() {
            *count = count.saturating_sub(1);
        }
    }
}

fn run_json_perf_with_sampler(
    options: &PerfOptions,
    out: &mut impl Write,
    mut read_next: impl FnMut() -> anyhow::Result<RuntimeStatsSnapshot>,
    mut sleep: impl FnMut(Duration),
) -> anyhow::Result<()> {
    let Some(count) = options.count else {
        bail!(
            "--json requires --count for finite output; use --json-lines for continuous streaming"
        );
    };
    if count == 0 {
        return write_json_samples(out, &[]);
    }

    let mut previous = read_next()?;
    let mut samples = Vec::new();
    let mut next_deadline = Instant::now() + options.interval;
    for _ in 0..count {
        sleep_until_next_deadline(&mut next_deadline, options.interval, &mut sleep);
        let current = read_next()?;
        samples.push(build_sample(&previous, &current, options));
        previous = current;
    }
    write_json_samples(out, &samples)?;
    out.flush()?;
    Ok(())
}

fn sleep_until_next_deadline(
    next_deadline: &mut Instant,
    interval: Duration,
    sleep: &mut impl FnMut(Duration),
) {
    let now = Instant::now();
    if *next_deadline > now {
        sleep(*next_deadline - now);
        *next_deadline += interval;
    } else {
        *next_deadline = now + interval;
    }
}

fn stats_path(mountpoint: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};

        let mut components = mountpoint.components();
        match (components.next(), components.next()) {
            (Some(Component::Prefix(prefix)), None) if matches!(prefix.kind(), Prefix::Disk(_)) => {
                return PathBuf::from(format!(
                    "{}\\{}",
                    mountpoint.display(),
                    runtime_stats_file_name()
                ));
            }
            _ => {}
        }
    }

    mountpoint.join(runtime_stats_file_name())
}

fn runtime_stats_file_name() -> &'static str {
    std::str::from_utf8(RUNTIME_STATS_FILE_NAME)
        .expect("StudioFS runtime stats file name must be UTF-8")
}

fn read_snapshot(path: &Path) -> anyhow::Result<RuntimeStatsSnapshot> {
    let text = fs::read_to_string(path).with_context(|| runtime_stats_unavailable_message(path))?;
    RuntimeStatsSnapshot::parse_text(&text).with_context(|| {
        format!(
            "StudioFS runtime stats surface at '{}' is malformed; ensure this is a mounted StudioFS volume with a compatible stats schema",
            path.display()
        )
    })
}

fn runtime_stats_unavailable_message(path: &Path) -> String {
    format!(
        "StudioFS runtime stats surface is unavailable at '{}'; ensure this is a mounted StudioFS volume",
        path.display()
    )
}

fn diagnostics_client_for_mount(
    mountpoint: &Path,
) -> anyhow::Result<Option<diagnostics::LocalDiagnosticsClient>> {
    let requested = mountpoint.to_string_lossy();
    let client = diagnostics::discover_local_sessions()?
        .into_iter()
        .find(|session| mount_points_match(&session.mount_point, &requested))
        .filter(|session| {
            session.protocol_version >= diagnostics::DIAGNOSTICS_RUNTIME_SURFACE_PROTOCOL_VERSION
        })
        .map(diagnostics::LocalDiagnosticsClient::new);
    Ok(client)
}

fn read_diagnostics_snapshot(
    client: &diagnostics::LocalDiagnosticsClient,
) -> anyhow::Result<RuntimeStatsSnapshot> {
    let snapshot = crate::runtime_bridge::block_on(async { client.runtime_snapshot().await })
        .context("StudioFS diagnostics runtime stats surface is unavailable")?;
    Ok(runtime_stats_from_diagnostics(snapshot))
}

fn mount_points_match(left: &str, right: &str) -> bool {
    let left = normalize_perf_mountpoint(left);
    let right = normalize_perf_mountpoint(right);
    #[cfg(windows)]
    {
        left.eq_ignore_ascii_case(&right)
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn normalize_perf_mountpoint(raw: &str) -> String {
    let normalized = crate::cli::utils::normalize_mount_point(raw.trim());
    #[cfg(windows)]
    {
        let normalized = normalized.replace('/', "\\");
        let mut chars = normalized.chars();
        match (chars.next(), chars.next()) {
            (Some(drive), Some(':')) if drive.is_ascii_alphabetic() => {
                let rest = chars.as_str();
                if rest.chars().all(|ch| ch == '\\') {
                    return format!("{}:", drive.to_ascii_uppercase());
                }
            }
            _ => {}
        }
        normalized.trim_end_matches('\\').to_string()
    }
    #[cfg(not(windows))]
    {
        normalized
    }
}

fn runtime_stats_from_diagnostics(
    diagnostics: diagnostics::DiagnosticsRuntimeSnapshot,
) -> RuntimeStatsSnapshot {
    let ingress = diagnostics.ingress;
    let upload = diagnostics.upload;
    let write_path = diagnostics.write_path;
    let cache = diagnostics.cache.state;

    let mut snapshot = RuntimeStatsSnapshot {
        generated_unix_ms: diagnostics.generated_unix_ms,
        ..RuntimeStatsSnapshot::default()
    };

    insert_gauge(
        &mut snapshot.system,
        "cpu_percent_x10",
        "percent_x10",
        (diagnostics.process.cpu_percent * 10.0).round() as u64,
    );
    insert_gauge(
        &mut snapshot.system,
        "rss_bytes",
        "bytes",
        diagnostics.process.rss_bytes,
    );
    insert_gauge(
        &mut snapshot.system,
        "uptime_ms",
        "ms",
        (write_path.uptime_secs * 1000.0) as u64,
    );
    insert_counter(
        &mut snapshot.bridge,
        "read_callbacks",
        "count",
        write_path
            .fuse_callbacks
            .read
            .calls
            .saturating_add(write_path.winfsp_callbacks.read),
    );
    insert_counter(
        &mut snapshot.bridge,
        "write_callbacks",
        "count",
        write_path
            .fuse_callbacks
            .write
            .calls
            .saturating_add(write_path.winfsp_callbacks.write),
    );
    insert_counter(
        &mut snapshot.bridge,
        "flush_callbacks",
        "count",
        write_path.winfsp_callbacks.flush,
    );
    insert_counter(
        &mut snapshot.bridge,
        "cleanup_callbacks",
        "count",
        write_path.winfsp_callbacks.cleanup,
    );
    insert_counter(
        &mut snapshot.bridge,
        "close_callbacks",
        "count",
        write_path.winfsp_callbacks.close,
    );
    insert_counter(
        &mut snapshot.bridge,
        "os_cache_enabled_open",
        "count",
        write_path.winfsp_callbacks.os_cache_enabled_open,
    );
    insert_counter(
        &mut snapshot.bridge,
        "os_cache_disabled_open",
        "count",
        write_path.winfsp_callbacks.os_cache_disabled_open,
    );
    insert_counter(
        &mut snapshot.bridge,
        "os_cache_miss_read_callbacks",
        "count",
        write_path.winfsp_callbacks.os_cache_miss_read_callbacks,
    );
    insert_counter(
        &mut snapshot.bridge,
        "os_cache_miss_read_bytes",
        "bytes",
        write_path.winfsp_callbacks.os_cache_miss_read_bytes,
    );
    insert_counter(
        &mut snapshot.bridge,
        "callback_errors",
        "count",
        write_path
            .fuse_callbacks
            .read
            .errors
            .saturating_add(write_path.fuse_callbacks.write.errors),
    );
    insert_counter(
        &mut snapshot.bridge,
        "read_callback_bytes",
        "bytes",
        write_path
            .fuse_callbacks
            .read
            .bytes
            .saturating_add(write_path.winfsp_callbacks.read_bytes),
    );
    insert_counter(
        &mut snapshot.bridge,
        "write_callback_bytes",
        "bytes",
        write_path
            .fuse_callbacks
            .write
            .bytes
            .saturating_add(write_path.winfsp_write_path.write_bytes),
    );
    insert_latency(
        &mut snapshot.bridge,
        "callback",
        "ns",
        write_path
            .fuse_callbacks
            .read
            .calls
            .saturating_add(write_path.fuse_callbacks.write.calls)
            .saturating_add(write_path.winfsp_callbacks.read)
            .saturating_add(write_path.winfsp_callbacks.write),
        ms_sum_to_ns(write_path.fuse_callbacks.read.total_ms)
            .saturating_add(ms_sum_to_ns(write_path.fuse_callbacks.write.total_ms))
            .saturating_add(write_path.winfsp_callbacks.read_total_ns)
            .saturating_add(ms_sum_to_ns(write_path.winfsp_write_path.callback.total_ms)),
    );
    insert_counter(
        &mut snapshot.vfs_read,
        "calls",
        "count",
        write_path.vfs_read_path.calls,
    );
    insert_counter(
        &mut snapshot.vfs_read,
        "requested_bytes",
        "bytes",
        write_path.vfs_read_path.requested_bytes,
    );
    insert_counter(
        &mut snapshot.vfs_write,
        "calls",
        "count",
        write_path.vfs_write_path.calls,
    );
    insert_counter(
        &mut snapshot.vfs_write,
        "accepted_bytes",
        "bytes",
        write_path.vfs_write_path.write_bytes,
    );
    insert_counter(
        &mut snapshot.vfs_write,
        "flush_barrier_calls",
        "count",
        write_path.flush_barrier_calls,
    );
    insert_counter(
        &mut snapshot.vfs_write,
        "backpressure_events",
        "count",
        ingress.write_backpressure_events,
    );
    insert_gauge(
        &mut snapshot.cache,
        "dirty_bytes",
        "bytes",
        ingress.dirty_mem_bytes as u64,
    );
    insert_gauge(
        &mut snapshot.cache,
        "prefetch_queue_depth",
        "count",
        ingress.prefetch_queue_depth,
    );
    insert_gauge(
        &mut snapshot.cache,
        "resident_bytes",
        "bytes",
        cache.transit_memory.used_bytes as u64,
    );
    insert_gauge(&mut snapshot.cache, "evictable_bytes", "bytes", 0);
    insert_gauge(
        &mut snapshot.cache,
        "local_backend_dirty_objects",
        "count",
        cache.local_backend_dirty_objects as u64,
    );
    insert_counter(
        &mut snapshot.cache,
        "hit_calls",
        "count",
        write_path.fetch.cache_hits,
    );
    insert_counter(
        &mut snapshot.cache,
        "hit_bytes",
        "bytes",
        write_path.fetch.cache_hit_bytes,
    );
    insert_counter(
        &mut snapshot.cache,
        "read_calls",
        "count",
        write_path.fetch.local_cache_io_calls,
    );
    insert_counter(
        &mut snapshot.cache,
        "read_bytes",
        "bytes",
        write_path.fetch.cache_hit_bytes,
    );
    insert_gauge(
        &mut snapshot.upload_queue,
        "pending_depth",
        "count",
        upload.pending_uploads as u64,
    );
    insert_gauge(
        &mut snapshot.upload_queue,
        "pending_bytes",
        "bytes",
        upload.pending_upload_bytes,
    );
    insert_gauge(
        &mut snapshot.upload_queue,
        "active_uploads",
        "count",
        upload.active_uploads as u64,
    );
    insert_gauge(
        &mut snapshot.upload_queue,
        "oldest_pending_age_ms",
        "ms",
        upload.oldest_pending_upload_age_ms,
    );
    insert_counter(
        &mut snapshot.upload_queue,
        "completed",
        "count",
        upload.completed_uploads,
    );
    insert_counter(
        &mut snapshot.upload_queue,
        "completed_bytes",
        "bytes",
        upload.completed_upload_bytes,
    );
    insert_counter(
        &mut snapshot.upload_queue,
        "requeued",
        "count",
        upload.upload_requeues,
    );
    insert_counter(
        &mut snapshot.upload_queue,
        "canceled",
        "count",
        upload
            .canceled_deleted
            .saturating_add(upload.canceled_stale),
    );
    insert_counter(
        &mut snapshot.upload_queue,
        "snapshot_pending_calls",
        "count",
        upload.pending_upload_snapshot_calls,
    );
    insert_counter(
        &mut snapshot.object_io,
        "get_calls",
        "count",
        write_path.fetch.s3_get_calls,
    );
    insert_counter(
        &mut snapshot.object_io,
        "get_bytes",
        "bytes",
        write_path
            .fetch
            .encrypted_bytes_read
            .max(write_path.fetch.bytes_downloaded),
    );
    insert_counter(
        &mut snapshot.object_io,
        "put_calls",
        "count",
        upload.completed_uploads,
    );
    insert_counter(
        &mut snapshot.object_io,
        "put_bytes",
        "bytes",
        upload.completed_upload_bytes,
    );
    insert_counter(
        &mut snapshot.object_io,
        "encrypted_put_bytes",
        "bytes",
        upload.encrypted_bytes_written,
    );
    insert_counter(
        &mut snapshot.object_io,
        "errors",
        "count",
        upload.encrypted_object_write_failures,
    );
    insert_counter(
        &mut snapshot.metadata,
        "commit_calls",
        "count",
        write_path.metadata_commit.calls,
    );
    insert_latency(
        &mut snapshot.metadata,
        "commit",
        "ns",
        write_path.metadata_commit.calls,
        ms_sum_to_ns(write_path.metadata_commit.total_ms),
    );

    snapshot
}

fn insert_counter(section: &mut RuntimeStatsSection, name: &str, unit: &'static str, value: u64) {
    section.counters.insert(
        name.to_owned(),
        RuntimeScalarMetric {
            value,
            unit: unit.into(),
        },
    );
}

fn insert_gauge(section: &mut RuntimeStatsSection, name: &str, unit: &'static str, value: u64) {
    section.gauges.insert(
        name.to_owned(),
        RuntimeScalarMetric {
            value,
            unit: unit.into(),
        },
    );
}

fn insert_latency(
    section: &mut RuntimeStatsSection,
    name: &str,
    unit: &'static str,
    count: u64,
    sum: u64,
) {
    section.latencies.insert(
        name.to_owned(),
        RuntimeLatencyHistogram {
            count,
            sum,
            unit: unit.into(),
            buckets: Vec::new(),
        },
    );
}

fn ms_sum_to_ns(value: f64) -> u64 {
    (value.max(0.0) * 1_000_000.0).round() as u64
}

fn build_sample(
    previous: &RuntimeStatsSnapshot,
    current: &RuntimeStatsSnapshot,
    options: &PerfOptions,
) -> PerfSample {
    let interval_ms = interval_ms(previous, current, options.interval);
    let seconds = interval_ms as f64 / 1000.0;
    let mut sections = BTreeMap::new();
    let mut warnings = Vec::new();

    for section_name in &options.sections {
        let Some(before) = section(previous, section_name) else {
            warnings.push(format!(
                "unsupported stats section '{section_name}' in this CLI version"
            ));
            continue;
        };
        let Some(after) = section(current, section_name) else {
            warnings.push(format!(
                "unsupported stats section '{section_name}' in this CLI version"
            ));
            continue;
        };
        if section_is_empty(after) {
            warnings.push(format!("missing stats for section '{section_name}'"));
        }
        sections.insert(
            *section_name,
            build_section_sample(
                before,
                after,
                seconds,
                if matches!(options.output, PerfOutput::Table) {
                    PerfVerbosity::Verbose
                } else {
                    options.verbosity
                },
            ),
        );
    }

    PerfSample {
        schema_version: current.schema_version,
        generated_unix_ms: current.generated_unix_ms,
        interval_ms,
        sections,
        warnings,
    }
}

fn interval_ms(
    previous: &RuntimeStatsSnapshot,
    current: &RuntimeStatsSnapshot,
    fallback: Duration,
) -> u64 {
    current
        .generated_unix_ms
        .checked_sub(previous.generated_unix_ms)
        .filter(|delta| *delta > 0)
        .unwrap_or_else(|| fallback.as_millis().min(u128::from(u64::MAX)) as u64)
}

fn build_section_sample(
    previous: &RuntimeStatsSection,
    current: &RuntimeStatsSection,
    seconds: f64,
    verbosity: PerfVerbosity,
) -> PerfSectionSample {
    let mut sample = PerfSectionSample::default();

    for (name, metric) in &current.counters {
        let previous_value = previous.counters.get(name).map_or(0, |metric| metric.value);
        let delta = reset_aware_delta(previous_value, metric.value);
        sample.counters.insert(
            name.clone(),
            PerfCounterSample {
                total: metric.value,
                delta,
                rate_per_sec: rate(delta, seconds),
                unit: metric.unit.clone(),
            },
        );
    }

    for (name, metric) in &current.gauges {
        sample.gauges.insert(
            name.clone(),
            PerfGaugeSample {
                value: metric.value,
                unit: metric.unit.clone(),
            },
        );
    }

    if matches!(verbosity, PerfVerbosity::Verbose) {
        for (name, histogram) in &current.latencies {
            let previous_histogram = previous.latencies.get(name);
            sample.latencies.insert(
                name.clone(),
                build_latency_sample(previous_histogram, histogram),
            );
        }
    }

    sample
}

fn build_latency_sample(
    previous: Option<&RuntimeLatencyHistogram>,
    current: &RuntimeLatencyHistogram,
) -> PerfLatencySample {
    let previous_count = previous.map_or(0, |histogram| histogram.count);
    let previous_sum = previous.map_or(0, |histogram| histogram.sum);
    let count_delta = reset_aware_delta(previous_count, current.count);
    let sum_delta = reset_aware_delta(previous_sum, current.sum);
    PerfLatencySample {
        count_delta,
        sum_delta,
        avg: (count_delta > 0).then_some(sum_delta as f64 / count_delta as f64),
        unit: current.unit.clone(),
    }
}

fn reset_aware_delta(previous: u64, current: u64) -> u64 {
    if current >= previous {
        current - previous
    } else {
        current
    }
}

fn rate(delta: u64, seconds: f64) -> f64 {
    if seconds > 0.0 {
        delta as f64 / seconds
    } else {
        0.0
    }
}

fn write_sample(
    out: &mut impl Write,
    sample: &PerfSample,
    options: &PerfOptions,
    table_state: &mut TableOutputState,
) -> anyhow::Result<()> {
    match options.output {
        PerfOutput::Table => write_table_sample(out, sample, table_state),
        PerfOutput::Json => bail!("finite JSON output must be written as a sample array"),
        PerfOutput::JsonLines => {
            let mut buffer = serde_json::to_vec(sample)?;
            buffer.push(b'\n');
            out.write_all(&buffer)?;
            Ok(())
        }
    }
}

fn write_json_samples(out: &mut impl Write, samples: &[PerfSample]) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(&mut *out, samples)?;
    writeln!(out)?;
    Ok(())
}

fn write_table_sample(
    out: &mut impl Write,
    sample: &PerfSample,
    state: &mut TableOutputState,
) -> anyhow::Result<()> {
    for warning in &sample.warnings {
        if !state.printed_warnings.contains(warning) {
            state.printed_warnings.insert(warning.clone());
            writeln!(out, "warning: {warning}")?;
        }
    }

    if state
        .samples_printed
        .is_multiple_of(TABLE_HEADER_REPEAT_SAMPLES)
    {
        write_table_header(out)?;
    }

    let row = dstat_row(sample);
    writeln!(
        out,
        "{} {} {} | {} {} {} {} | {} {} | {} {} {} | {} {}",
        table_cell(&row.cpu),
        table_cell(&row.mem),
        table_cell(&row.buf),
        table_cell(&row.fuse_ops),
        table_cell(&row.fuse_lat),
        table_cell(&row.fuse_read),
        table_cell(&row.fuse_write),
        table_cell(&row.meta_ops),
        table_cell(&row.meta_lat),
        table_cell(&row.cache_hit),
        table_cell(&row.cache_read),
        table_cell(&row.cache_write),
        table_cell(&row.object_get),
        table_cell(&row.object_put)
    )?;
    state.samples_printed = state.samples_printed.saturating_add(1);
    Ok(())
}

fn write_table_header(out: &mut impl Write) -> anyhow::Result<()> {
    writeln!(
        out,
        "{} | {} | {} | {} | {}",
        table_group("usage", 3),
        table_group("fuse", 4),
        table_group("meta", 2),
        table_group("blockcache", 3),
        table_group("object", 2),
    )?;
    writeln!(
        out,
        "{} {} {} | {} {} {} {} | {} {} | {} {} {} | {} {}",
        table_label("cpu"),
        table_label("mem"),
        table_label("buf"),
        table_label("ops"),
        table_label("lat"),
        table_label("read"),
        table_label("write"),
        table_label("ops"),
        table_label("lat"),
        table_label("hit"),
        table_label("read"),
        table_label("write"),
        table_label("get"),
        table_label("put"),
    )?;
    Ok(())
}

fn table_group(label: &str, columns: usize) -> String {
    let width = columns * TABLE_CELL_WIDTH + columns.saturating_sub(1);
    if label.len() >= width {
        return label[..width].to_string();
    }
    let padding = width - label.len();
    let left = padding / 2;
    let right = padding - left;
    format!("{}{}{}", "-".repeat(left), label, "-".repeat(right))
}

fn table_label(label: &str) -> String {
    format!("{label:>width$}", width = TABLE_CELL_WIDTH)
}

fn table_cell(value: &str) -> String {
    let value = if value.len() > TABLE_CELL_WIDTH {
        &value[value.len() - TABLE_CELL_WIDTH..]
    } else {
        value
    };
    format!("{value:>width$}", width = TABLE_CELL_WIDTH)
}

struct DstatRow {
    cpu: String,
    mem: String,
    buf: String,
    fuse_ops: String,
    fuse_lat: String,
    fuse_read: String,
    fuse_write: String,
    meta_ops: String,
    meta_lat: String,
    cache_hit: String,
    cache_read: String,
    cache_write: String,
    object_get: String,
    object_put: String,
}

fn dstat_row(sample: &PerfSample) -> DstatRow {
    let bridge = sample.sections.get("bridge");
    let vfs_read = sample.sections.get("vfs_read");
    let vfs_write = sample.sections.get("vfs_write");
    let cache = sample.sections.get("cache");
    let object = sample.sections.get("object_io");
    let metadata = sample.sections.get("metadata");
    let system = sample.sections.get("system");

    DstatRow {
        cpu: format_cpu(gauge_value(system, "cpu_percent_x10")),
        mem: format_bytes(gauge_value(system, "rss_bytes")),
        buf: format_bytes(gauge_value(cache, "dirty_bytes")),
        fuse_ops: format_rate(
            counter_rate(bridge, "read_callbacks")
                + counter_rate(bridge, "write_callbacks")
                + counter_rate(bridge, "flush_callbacks")
                + counter_rate(bridge, "cleanup_callbacks")
                + counter_rate(bridge, "close_callbacks"),
        ),
        fuse_lat: format_latency_ms(latency_avg(bridge, "callback")),
        fuse_read: format_bytes_rate(counter_rate(vfs_read, "requested_bytes")),
        fuse_write: format_bytes_rate(counter_rate(vfs_write, "accepted_bytes")),
        meta_ops: format_rate(counter_rate(metadata, "commit_calls")),
        meta_lat: format_latency_ms(latency_avg(metadata, "commit")),
        cache_hit: format_rate(counter_rate(cache, "hit_calls")),
        cache_read: format_bytes_rate(
            counter_rate(cache, "read_bytes").max(counter_rate(bridge, "os_cache_miss_read_bytes")),
        ),
        cache_write: format_bytes_rate(counter_rate(vfs_write, "accepted_bytes")),
        object_get: format_bytes_rate(counter_rate(object, "get_bytes")),
        object_put: format_bytes_rate(counter_rate(object, "put_bytes")),
    }
}

fn counter_rate(section: Option<&PerfSectionSample>, name: &str) -> f64 {
    section
        .and_then(|section| section.counters.get(name))
        .map_or(0.0, |metric| metric.rate_per_sec)
}

fn gauge_value(section: Option<&PerfSectionSample>, name: &str) -> u64 {
    section
        .and_then(|section| section.gauges.get(name))
        .map_or(0, |metric| metric.value)
}

fn latency_avg(section: Option<&PerfSectionSample>, name: &str) -> Option<f64> {
    section
        .and_then(|section| section.latencies.get(name))
        .and_then(|metric| metric.avg)
}

fn format_rate(value: f64) -> String {
    if value == 0.0 {
        "0".to_string()
    } else if value < 10.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.0}")
    }
}

fn format_bytes_rate(value: f64) -> String {
    format_bytes(value.max(0.0) as u64)
}

fn format_latency_ms(value_ns: Option<f64>) -> String {
    let Some(value_ns) = value_ns else {
        return "0".to_string();
    };
    let value_ms = value_ns / 1_000_000.0;
    if value_ms == 0.0 {
        "0".to_string()
    } else if value_ms < 10.0 {
        format!("{value_ms:.1}")
    } else {
        format!("{value_ms:.0}")
    }
}

fn format_cpu(value_x10: u64) -> String {
    if value_x10 < 10_000 {
        format!("{:.1}%", value_x10 as f64 / 10.0)
    } else {
        format!("{}%", value_x10 / 10)
    }
}

fn format_bytes(value: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;

    match value {
        0..KIB => value.to_string(),
        KIB..MIB => format!("{}K", value / KIB),
        MIB..GIB => format!("{}M", value / MIB),
        _ => format!("{}G", value / GIB),
    }
}

fn write_schema(out: &mut impl Write, options: &PerfOptions) -> anyhow::Result<()> {
    let payload = serde_json::json!({
        "schema_version": studiofs_vfs::RUNTIME_STATS_SCHEMA_VERSION,
        "stats_file": runtime_stats_file_name(),
        "sections": &options.sections,
        "line_kinds": ["counter", "gauge", "histogram_count", "histogram_sum", "histogram_bucket"],
        "units": ["count", "bytes", "ns", "ms", "percent"],
    });
    serde_json::to_writer_pretty(&mut *out, &payload)?;
    writeln!(out)?;
    Ok(())
}

fn section<'a>(
    snapshot: &'a RuntimeStatsSnapshot,
    section_name: &str,
) -> Option<&'a RuntimeStatsSection> {
    match section_name {
        "system" => Some(&snapshot.system),
        "bridge" => Some(&snapshot.bridge),
        "vfs_read" => Some(&snapshot.vfs_read),
        "vfs_write" => Some(&snapshot.vfs_write),
        "cache" => Some(&snapshot.cache),
        "upload_queue" => Some(&snapshot.upload_queue),
        "object_io" => Some(&snapshot.object_io),
        "metadata" => Some(&snapshot.metadata),
        _ => None,
    }
}

fn section_is_empty(section: &RuntimeStatsSection) -> bool {
    section.counters.is_empty() && section.gauges.is_empty() && section.latencies.is_empty()
}

fn parse_sections(raw: &[String]) -> anyhow::Result<Vec<&'static str>> {
    if raw.is_empty() {
        return Ok(RUNTIME_STATS_SECTIONS.to_vec());
    }

    let mut sections = Vec::new();
    let mut seen = BTreeSet::new();
    for section in raw {
        let normalized = section.to_lowercase().replace('-', "_");
        let expanded = match normalized.as_str() {
            "runtime" => vec!["system"],
            "vfs" => vec!["vfs_read", "vfs_write"],
            "upload" => vec!["upload_queue"],
            "object" => vec!["object_io"],
            _ => {
                let Some(&known_section) = RUNTIME_STATS_SECTIONS
                    .iter()
                    .find(|&&name| name == normalized.as_str())
                else {
                    bail!(
                        "unknown runtime stats section '{}'; expected one of: {}",
                        section,
                        section_help()
                    );
                };
                vec![known_section]
            }
        };
        for known_section in expanded {
            if seen.insert(known_section) {
                sections.push(known_section);
            }
        }
    }
    Ok(sections)
}

fn section_help() -> String {
    RUNTIME_STATS_SECTIONS
        .iter()
        .chain(SECTION_ALIAS_NAMES.iter())
        .copied()
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    use studiofs_vfs::{RuntimeLatencyBucket, RuntimeLatencyHistogram, RuntimeScalarMetric};

    #[derive(Default)]
    struct FlushingBuffer {
        bytes: Vec<u8>,
        writes: usize,
        flushes: usize,
    }

    impl Write for FlushingBuffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    fn scalar(value: u64, unit: &'static str) -> RuntimeScalarMetric {
        RuntimeScalarMetric {
            value,
            unit: unit.into(),
        }
    }

    fn target_tempdir() -> tempfile::TempDir {
        let target = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("workspace root")
            .join("target");
        std::fs::create_dir_all(&target).expect("target dir");
        tempfile::tempdir_in(target).expect("tempdir")
    }

    fn snapshot_at(
        generated_unix_ms: u64,
        read_bytes: u64,
        pending_uploads: u64,
    ) -> RuntimeStatsSnapshot {
        let mut snapshot = RuntimeStatsSnapshot {
            generated_unix_ms,
            ..RuntimeStatsSnapshot::default()
        };
        snapshot
            .vfs_read
            .counters
            .insert("requested_bytes".to_string(), scalar(read_bytes, "bytes"));
        snapshot.upload_queue.gauges.insert(
            "pending_depth".to_string(),
            scalar(pending_uploads, "count"),
        );
        snapshot.object_io.latencies.insert(
            "get".to_string(),
            RuntimeLatencyHistogram {
                count: read_bytes / 100,
                sum: read_bytes * 10,
                unit: "ns".into(),
                buckets: vec![RuntimeLatencyBucket {
                    le_ns: 1000,
                    count: read_bytes / 100,
                }],
            },
        );
        snapshot
    }

    fn dstat_snapshot_at(generated_unix_ms: u64, multiplier: u64) -> RuntimeStatsSnapshot {
        let mut snapshot = RuntimeStatsSnapshot {
            generated_unix_ms,
            ..RuntimeStatsSnapshot::default()
        };
        snapshot.system.gauges.insert(
            "cpu_percent_x10".to_string(),
            scalar(12 * multiplier, "percent_x10"),
        );
        snapshot
            .system
            .gauges
            .insert("rss_bytes".to_string(), scalar(50 * 1024 * 1024, "bytes"));
        snapshot
            .cache
            .gauges
            .insert("dirty_bytes".to_string(), scalar(4096 * 1024, "bytes"));
        snapshot.bridge.counters.insert(
            "read_callbacks".to_string(),
            scalar(10 * multiplier, "count"),
        );
        snapshot.bridge.counters.insert(
            "write_callbacks".to_string(),
            scalar(20 * multiplier, "count"),
        );
        snapshot.bridge.latencies.insert(
            "callback".to_string(),
            RuntimeLatencyHistogram {
                count: 10 * multiplier,
                sum: 20_000_000 * multiplier,
                unit: "ns".into(),
                buckets: Vec::new(),
            },
        );
        snapshot.vfs_read.counters.insert(
            "requested_bytes".to_string(),
            scalar(55 * 1024 * 1024 * multiplier, "bytes"),
        );
        snapshot.vfs_write.counters.insert(
            "accepted_bytes".to_string(),
            scalar(9 * 1024 * 1024 * multiplier, "bytes"),
        );
        snapshot
            .metadata
            .counters
            .insert("commit_calls".to_string(), scalar(15 * multiplier, "count"));
        snapshot.metadata.latencies.insert(
            "commit".to_string(),
            RuntimeLatencyHistogram {
                count: 15 * multiplier,
                sum: 45_000_000 * multiplier,
                unit: "ns".into(),
                buckets: Vec::new(),
            },
        );
        snapshot
            .cache
            .counters
            .insert("hit_calls".to_string(), scalar(8 * multiplier, "count"));
        snapshot.cache.counters.insert(
            "read_bytes".to_string(),
            scalar(33 * 1024 * 1024 * multiplier, "bytes"),
        );
        snapshot.object_io.counters.insert(
            "get_bytes".to_string(),
            scalar(44 * 1024 * 1024 * multiplier, "bytes"),
        );
        snapshot.object_io.counters.insert(
            "put_bytes".to_string(),
            scalar(7 * 1024 * 1024 * multiplier, "bytes"),
        );
        snapshot.bridge.counters.insert(
            "os_cache_miss_read_bytes".to_string(),
            scalar(3 * 1024 * 1024 * multiplier, "bytes"),
        );
        snapshot
    }

    #[cfg(windows)]
    #[test]
    fn mount_points_match_treats_drive_root_forms_as_equivalent() {
        assert!(mount_points_match("Z:", r"Z:\"));
        assert!(mount_points_match(r"\\?\Z:\", "z:"));
    }

    #[test]
    fn runtime_stats_from_diagnostics_maps_perf_sections() {
        let mut diagnostics = diagnostics::DiagnosticsRuntimeSnapshot {
            generated_unix_ms: 42,
            ..diagnostics::DiagnosticsRuntimeSnapshot::default()
        };
        diagnostics.write_path.uptime_secs = 3.5;
        diagnostics.write_path.fuse_callbacks.read.calls = 2;
        diagnostics.write_path.winfsp_callbacks.write = 3;
        diagnostics
            .write_path
            .winfsp_callbacks
            .os_cache_enabled_open = 5;
        diagnostics
            .write_path
            .winfsp_callbacks
            .os_cache_disabled_open = 7;
        diagnostics
            .write_path
            .winfsp_callbacks
            .os_cache_miss_read_callbacks = 11;
        diagnostics
            .write_path
            .winfsp_callbacks
            .os_cache_miss_read_bytes = 13 * 1024 * 1024;
        diagnostics.write_path.winfsp_callbacks.read = 17;
        diagnostics.write_path.winfsp_callbacks.read_bytes = 19 * 1024 * 1024;
        diagnostics.write_path.winfsp_callbacks.read_total_ns = 23_000_000;
        diagnostics.write_path.fuse_callbacks.read.bytes = 3 * 1024 * 1024;
        diagnostics.write_path.fuse_callbacks.read.total_ms = 5.0;
        diagnostics.write_path.winfsp_write_path.write_bytes = 2048;
        diagnostics.write_path.winfsp_write_path.callback.total_ms = 7.0;
        diagnostics.write_path.vfs_read_path.calls = 5;
        diagnostics.write_path.vfs_read_path.requested_bytes = 4096;
        diagnostics.write_path.vfs_write_path.calls = 7;
        diagnostics.write_path.vfs_write_path.write_bytes = 8192;
        diagnostics.write_path.metadata_commit.calls = 11;
        diagnostics.write_path.metadata_commit.total_ms = 13.0;
        diagnostics.write_path.fetch.cache_hits = 31;
        diagnostics.write_path.fetch.cache_hit_bytes = 37 * 1024 * 1024;
        diagnostics.write_path.fetch.local_cache_io_calls = 41;
        diagnostics.write_path.fetch.s3_get_calls = 43;
        diagnostics.write_path.fetch.bytes_downloaded = 47 * 1024 * 1024;
        diagnostics.write_path.fetch.encrypted_bytes_read = 53 * 1024 * 1024;
        diagnostics.process.cpu_percent = 12.3;
        diagnostics.process.rss_bytes = 51 * 1024 * 1024;
        diagnostics.ingress.dirty_mem_bytes = 13;
        diagnostics.ingress.write_backpressure_events = 17;
        diagnostics.cache.state.transit_memory.used_bytes = 19;
        diagnostics.cache.state.local_backend_dirty_objects = 23;
        diagnostics.upload.pending_uploads = 29;
        diagnostics.upload.pending_upload_bytes = 31;
        diagnostics.upload.completed_uploads = 37;
        diagnostics.upload.completed_upload_bytes = 41;
        diagnostics.upload.encrypted_bytes_written = 43;
        diagnostics.upload.encrypted_object_write_failures = 47;

        let snapshot = runtime_stats_from_diagnostics(diagnostics);

        assert_eq!(snapshot.generated_unix_ms, 42);
        assert_eq!(snapshot.system.gauges["cpu_percent_x10"].value, 123);
        assert_eq!(snapshot.system.gauges["rss_bytes"].value, 51 * 1024 * 1024);
        assert_eq!(snapshot.system.gauges["uptime_ms"].value, 3500);
        assert_eq!(snapshot.bridge.counters["read_callbacks"].value, 19);
        assert_eq!(snapshot.bridge.counters["write_callbacks"].value, 3);
        assert_eq!(snapshot.bridge.counters["os_cache_enabled_open"].value, 5);
        assert_eq!(snapshot.bridge.counters["os_cache_disabled_open"].value, 7);
        assert_eq!(
            snapshot.bridge.counters["os_cache_miss_read_callbacks"].value,
            11
        );
        assert_eq!(
            snapshot.bridge.counters["os_cache_miss_read_bytes"].value,
            13 * 1024 * 1024
        );
        assert_eq!(
            snapshot.bridge.counters["read_callback_bytes"].value,
            22 * 1024 * 1024
        );
        assert_eq!(snapshot.bridge.counters["write_callback_bytes"].value, 2048);
        assert_eq!(snapshot.bridge.latencies["callback"].count, 22);
        assert_eq!(snapshot.bridge.latencies["callback"].sum, 35_000_000);
        assert_eq!(snapshot.vfs_read.counters["calls"].value, 5);
        assert_eq!(snapshot.vfs_read.counters["requested_bytes"].value, 4096);
        assert_eq!(snapshot.vfs_write.counters["calls"].value, 7);
        assert_eq!(snapshot.vfs_write.counters["accepted_bytes"].value, 8192);
        assert_eq!(snapshot.vfs_write.counters["backpressure_events"].value, 17);
        assert_eq!(snapshot.cache.gauges["dirty_bytes"].value, 13);
        assert_eq!(snapshot.cache.gauges["resident_bytes"].value, 19);
        assert_eq!(
            snapshot.cache.gauges["local_backend_dirty_objects"].value,
            23
        );
        assert_eq!(snapshot.cache.counters["hit_calls"].value, 31);
        assert_eq!(snapshot.cache.counters["hit_bytes"].value, 37 * 1024 * 1024);
        assert_eq!(snapshot.cache.counters["read_calls"].value, 41);
        assert_eq!(
            snapshot.cache.counters["read_bytes"].value,
            37 * 1024 * 1024
        );
        assert_eq!(snapshot.upload_queue.gauges["pending_depth"].value, 29);
        assert_eq!(snapshot.upload_queue.gauges["pending_bytes"].value, 31);
        assert_eq!(snapshot.upload_queue.counters["completed"].value, 37);
        assert_eq!(snapshot.upload_queue.counters["completed_bytes"].value, 41);
        assert_eq!(snapshot.object_io.counters["get_calls"].value, 43);
        assert_eq!(
            snapshot.object_io.counters["get_bytes"].value,
            53 * 1024 * 1024
        );
        assert_eq!(snapshot.object_io.counters["put_calls"].value, 37);
        assert_eq!(snapshot.object_io.counters["put_bytes"].value, 41);
        assert_eq!(snapshot.object_io.counters["encrypted_put_bytes"].value, 43);
        assert_eq!(snapshot.object_io.counters["errors"].value, 47);
        assert_eq!(snapshot.metadata.counters["commit_calls"].value, 11);
        assert_eq!(snapshot.metadata.latencies["commit"].count, 11);
        assert_eq!(snapshot.metadata.latencies["commit"].sum, 13_000_000);
    }

    #[test]
    fn parse_sections_accepts_kebab_case_aliases_and_deduplicates() {
        let sections = parse_sections(&[
            "vfs-read".to_string(),
            "vfs_read".to_string(),
            "upload-queue".to_string(),
        ])
        .expect("parse sections");

        assert_eq!(sections, vec!["vfs_read", "upload_queue"]);
    }

    #[test]
    fn parse_sections_accepts_case_insensitive_names() {
        let sections = parse_sections(&["VFS".to_string(), "BRIDGE".to_string()])
            .expect("parse uppercase sections");

        assert_eq!(sections, vec!["vfs_read", "vfs_write", "bridge"]);
    }

    #[test]
    fn parse_sections_expands_operator_aliases_to_stable_schema_sections() {
        let sections = parse_sections(&[
            "runtime".to_string(),
            "vfs".to_string(),
            "upload".to_string(),
            "object".to_string(),
        ])
        .expect("parse aliases");

        assert_eq!(
            sections,
            vec![
                "system",
                "vfs_read",
                "vfs_write",
                "upload_queue",
                "object_io"
            ]
        );
    }

    #[test]
    fn parse_sections_defaults_to_all_stable_schema_sections() {
        let sections = parse_sections(&[]).expect("default sections");

        assert_eq!(sections, RUNTIME_STATS_SECTIONS);
    }

    #[test]
    fn into_options_rejects_zero_interval() {
        let args = PerfArgs {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: 0,
            count: Some(1),
            schema: false,
            sections: Vec::new(),
            verbosity: PerfVerbosity::Normal,
            json: false,
            json_lines: false,
        };

        let error = args.into_options().expect_err("zero interval should fail");

        assert_eq!(
            error.to_string(),
            "--interval must be greater than zero milliseconds"
        );
    }

    #[test]
    fn into_options_rejects_json_and_json_lines_together() {
        let args = PerfArgs {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: 1,
            count: Some(1),
            schema: false,
            sections: Vec::new(),
            verbosity: PerfVerbosity::Normal,
            json: true,
            json_lines: true,
        };

        let error = args
            .into_options()
            .expect_err("json output modes should conflict");

        assert_eq!(
            error.to_string(),
            "--json and --json-lines are mutually exclusive"
        );
    }

    #[test]
    fn into_options_rejects_continuous_json_without_count() {
        let args = PerfArgs {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: 1,
            count: None,
            schema: false,
            sections: Vec::new(),
            verbosity: PerfVerbosity::Normal,
            json: true,
            json_lines: false,
        };

        let error = args
            .into_options()
            .expect_err("continuous pretty json should fail");

        assert_eq!(
            error.to_string(),
            "--json requires --count for finite output; use --json-lines for continuous streaming"
        );
    }

    #[test]
    fn parse_sections_rejects_unknown_section() {
        let error = parse_sections(&["unknown".to_string()]).expect_err("unknown section");
        let message = error.to_string();

        assert!(message.contains("unknown runtime stats section 'unknown'"));
        assert!(message.contains("runtime"));
        assert!(message.contains("vfs"));
    }

    #[test]
    fn build_sample_computes_counter_rates_and_gauges_from_interval_delta() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: vec!["vfs_read", "upload_queue"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Table,
        };

        let sample = build_sample(
            &snapshot_at(1_000, 100, 1),
            &snapshot_at(1_500, 600, 3),
            &options,
        );

        assert_eq!(
            sample.sections["vfs_read"].counters["requested_bytes"].rate_per_sec,
            1000.0
        );
        assert_eq!(
            sample.sections["upload_queue"].gauges["pending_depth"].value,
            3
        );
    }

    #[test]
    fn build_sample_treats_counter_resets_as_current_interval_activity() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: vec!["vfs_read", "object_io"],
            verbosity: PerfVerbosity::Verbose,
            output: PerfOutput::Table,
        };
        let sample = build_sample(
            &snapshot_at(1_000, 1_000, 0),
            &snapshot_at(2_000, 300, 0),
            &options,
        );

        assert_eq!(
            sample.sections["vfs_read"].counters["requested_bytes"].delta,
            300
        );
        assert_eq!(sample.sections["object_io"].latencies["get"].count_delta, 3);
        assert_eq!(
            sample.sections["object_io"].latencies["get"].sum_delta,
            3_000
        );
    }

    #[test]
    fn build_sample_warns_for_unsupported_schema_section_without_panicking() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: vec!["future_section"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Table,
        };

        let sample = build_sample(
            &snapshot_at(1_000, 100, 0),
            &snapshot_at(2_000, 300, 0),
            &options,
        );

        assert!(sample.sections.is_empty());
        assert_eq!(
            sample.warnings,
            vec!["unsupported stats section 'future_section' in this CLI version"]
        );
    }

    #[test]
    fn build_sample_uses_configured_interval_when_snapshot_timestamps_do_not_advance() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_millis(250),
            count: Some(1),
            schema: false,
            sections: vec!["vfs_read"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Table,
        };

        let sample = build_sample(
            &snapshot_at(1_000, 100, 0),
            &snapshot_at(1_000, 600, 0),
            &options,
        );

        assert_eq!(sample.interval_ms, 250);
    }

    #[test]
    fn sleep_until_next_deadline_resets_when_sampler_falls_behind() {
        let before = Instant::now();
        let mut next_deadline = before - Duration::from_millis(250);
        let mut sleeps = 0;

        sleep_until_next_deadline(&mut next_deadline, Duration::from_secs(1), &mut |_| {
            sleeps += 1;
        });

        assert_eq!(sleeps, 0);
        assert!(next_deadline >= before);
    }

    #[test]
    fn build_sample_includes_latency_averages_when_verbose() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: vec!["object_io"],
            verbosity: PerfVerbosity::Verbose,
            output: PerfOutput::Table,
        };

        let sample = build_sample(
            &snapshot_at(1_000, 100, 0),
            &snapshot_at(2_000, 300, 0),
            &options,
        );

        assert_eq!(
            sample.sections["object_io"].latencies["get"].avg,
            Some(1000.0)
        );
    }

    #[test]
    fn table_output_warns_when_selected_section_has_no_stats() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: vec!["metadata"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Table,
        };
        let sample = build_sample(
            &snapshot_at(1_000, 100, 0),
            &snapshot_at(2_000, 300, 0),
            &options,
        );
        let mut out = Vec::new();
        let mut table_state = TableOutputState::default();

        write_sample(&mut out, &sample, &options, &mut table_state).expect("write table");
        let output = String::from_utf8(out).expect("table is utf8");

        assert!(output.contains("warning: missing stats for section 'metadata'"));
    }

    #[test]
    fn table_output_deduplicates_repeated_missing_section_warnings() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: vec!["metadata"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Table,
        };
        let sample = build_sample(
            &snapshot_at(1_000, 100, 0),
            &snapshot_at(2_000, 300, 0),
            &options,
        );
        let mut out = Vec::new();
        let mut table_state = TableOutputState::default();

        write_sample(&mut out, &sample, &options, &mut table_state)
            .expect("write first table sample");
        write_sample(&mut out, &sample, &options, &mut table_state)
            .expect("write second table sample");
        let output = String::from_utf8(out).expect("table is utf8");

        assert_eq!(
            output
                .matches("missing stats for section 'metadata'")
                .count(),
            1
        );
    }

    #[test]
    fn table_output_repeats_header_periodically() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(TABLE_HEADER_REPEAT_SAMPLES + 1),
            schema: false,
            sections: vec!["vfs_read"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Table,
        };
        let sample = build_sample(
            &snapshot_at(1_000, 100, 0),
            &snapshot_at(2_000, 300, 0),
            &options,
        );
        let mut out = Vec::new();
        let mut table_state = TableOutputState::default();

        for _ in 0..=TABLE_HEADER_REPEAT_SAMPLES {
            write_sample(&mut out, &sample, &options, &mut table_state)
                .expect("write table sample");
        }
        let output = String::from_utf8(out).expect("table is utf8");

        assert_eq!(output.matches("------usage------").count(), 2);
    }

    #[test]
    fn table_output_uses_juicefs_stats_style_sections() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: RUNTIME_STATS_SECTIONS.to_vec(),
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Table,
        };
        let sample = build_sample(
            &dstat_snapshot_at(1_000, 1),
            &dstat_snapshot_at(2_000, 2),
            &options,
        );
        let mut out = Vec::new();
        let mut table_state = TableOutputState::default();

        write_sample(&mut out, &sample, &options, &mut table_state).expect("write table");
        let output = String::from_utf8(out).expect("table is utf8");

        assert!(output.contains("------usage------"));
        assert!(output.contains("---------fuse---------"));
        assert!(output.contains("-----meta----"));
        assert!(output.contains("-------blockcache-------"));
        assert!(output.contains("---object---"));
        assert!(output.contains("     cpu      mem      buf |"));
        assert!(output.contains("     hit     read    write |"));
        assert!(output.contains("2.4%"));
        assert!(output.contains("50M"));
        assert!(output.contains("4M"));
        assert!(output.contains("30"));
        assert!(output.contains("2.0"));
        assert!(output.contains("55M"));
        assert!(output.contains("9M"));
        assert!(output.contains("15"));
        assert!(output.contains("3.0"));
        assert!(output.contains("8.0"));
        assert!(output.contains("33M"));
        assert!(output.contains("44M"));
        assert!(output.contains("7M"));
    }

    #[test]
    fn table_output_formats_zeroes_for_unavailable_dstat_columns() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: RUNTIME_STATS_SECTIONS.to_vec(),
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Table,
        };
        let sample = build_sample(
            &RuntimeStatsSnapshot::default(),
            &RuntimeStatsSnapshot::default(),
            &options,
        );
        let mut out = Vec::new();
        let mut table_state = TableOutputState::default();

        write_sample(&mut out, &sample, &options, &mut table_state).expect("write table");
        let output = String::from_utf8(out).expect("table is utf8");

        assert!(output.contains("0.0%"));
        assert!(output.lines().any(|line| line.starts_with("    0.0%")));
    }

    #[test]
    fn table_cell_truncates_overflow_without_changing_width() {
        let cell = table_cell("1234567890%");

        assert_eq!(cell, "4567890%");
        assert_eq!(cell.len(), TABLE_CELL_WIDTH);
    }

    #[test]
    fn json_lines_output_is_one_compact_line_per_sample() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: vec!["vfs_read"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::JsonLines,
        };
        let sample = build_sample(
            &snapshot_at(1_000, 100, 0),
            &snapshot_at(2_000, 300, 0),
            &options,
        );
        let mut out = Vec::new();
        let mut table_state = TableOutputState::default();

        write_sample(&mut out, &sample, &options, &mut table_state).expect("write json lines");
        let output = String::from_utf8(out).expect("json lines is utf8");

        assert_eq!(output.lines().count(), 1);
    }

    #[test]
    fn json_lines_output_writes_record_and_newline_together() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: vec!["vfs_read"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::JsonLines,
        };
        let sample = build_sample(
            &snapshot_at(1_000, 100, 0),
            &snapshot_at(2_000, 300, 0),
            &options,
        );
        let mut out = FlushingBuffer::default();
        let mut table_state = TableOutputState::default();

        write_sample(&mut out, &sample, &options, &mut table_state).expect("write json line");

        assert_eq!(out.writes, 1);
        assert!(out.bytes.ends_with(b"\n"));
    }

    #[test]
    fn json_output_is_parseable_pretty_sample_array() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_millis(250),
            count: Some(2),
            schema: false,
            sections: vec!["vfs_read"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Json,
        };
        let snapshots = [
            snapshot_at(1_000, 100, 0),
            snapshot_at(1_250, 200, 0),
            snapshot_at(1_500, 450, 0),
        ];
        let mut next_snapshot = 0;
        let mut sleeps = Vec::new();
        let mut out = Vec::new();

        run_perf_with_sampler(
            &options,
            &mut out,
            || {
                let snapshot = snapshots[next_snapshot].clone();
                next_snapshot += 1;
                Ok(snapshot)
            },
            |duration| sleeps.push(duration),
        )
        .expect("write json");
        let payload: serde_json::Value = serde_json::from_slice(&out).expect("json samples");

        let samples = payload.as_array().expect("json sample array");
        assert_eq!(samples.len(), 2);
        assert_eq!(
            samples[1]["sections"]["vfs_read"]["counters"]["requested_bytes"]["delta"],
            250
        );
        assert_eq!(sleeps.len(), 2);
        assert!(sleeps[1] >= sleeps[0]);
    }

    #[test]
    fn json_output_count_zero_is_empty_array_without_reading_stats() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_millis(250),
            count: Some(0),
            schema: false,
            sections: vec!["vfs_read"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Json,
        };
        let mut reads = 0;
        let mut out = Vec::new();

        run_perf_with_sampler(
            &options,
            &mut out,
            || {
                reads += 1;
                Ok(snapshot_at(1_000, 100, 0))
            },
            |_| {},
        )
        .expect("write empty json");
        let payload: serde_json::Value = serde_json::from_slice(&out).expect("json samples");

        assert_eq!(payload, serde_json::json!([]));
        assert_eq!(reads, 0);
    }

    #[test]
    fn run_perf_count_zero_exits_without_reading_stats() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_secs(1),
            count: Some(0),
            schema: false,
            sections: vec!["vfs_read"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::JsonLines,
        };
        let mut reads = 0;
        let mut sleeps = 0;
        let mut out = Vec::new();

        run_perf_with_sampler(
            &options,
            &mut out,
            || {
                reads += 1;
                Ok(snapshot_at(1_000, 100, 0))
            },
            |_| sleeps += 1,
        )
        .expect("count zero should exit cleanly");

        assert_eq!((reads, sleeps, out.len()), (0, 0, 0));
    }

    #[test]
    fn run_perf_count_limited_run_prints_one_record_per_interval_sample() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_millis(250),
            count: Some(2),
            schema: false,
            sections: vec!["vfs_read"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::JsonLines,
        };
        let snapshots = [
            snapshot_at(1_000, 100, 0),
            snapshot_at(1_250, 200, 0),
            snapshot_at(1_500, 450, 0),
        ];
        let mut next_snapshot = 0;
        let mut sleeps = Vec::new();
        let mut out = Vec::new();

        run_perf_with_sampler(
            &options,
            &mut out,
            || {
                let snapshot = snapshots[next_snapshot].clone();
                next_snapshot += 1;
                Ok(snapshot)
            },
            |duration| sleeps.push(duration),
        )
        .expect("count limited sampling");
        let output = String::from_utf8(out).expect("json lines utf8");

        assert_eq!(output.lines().count(), 2);
        assert_eq!(next_snapshot, 3);
        assert_eq!(sleeps.len(), 2);
        assert!(sleeps[1] >= sleeps[0]);
    }

    #[test]
    fn run_perf_flushes_after_each_streamed_sample() {
        let options = PerfOptions {
            mountpoint: Some(PathBuf::from("Z:")),
            interval: Duration::from_millis(250),
            count: Some(2),
            schema: false,
            sections: vec!["vfs_read"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::JsonLines,
        };
        let snapshots = [
            snapshot_at(1_000, 100, 0),
            snapshot_at(1_250, 200, 0),
            snapshot_at(1_500, 450, 0),
        ];
        let mut next_snapshot = 0;
        let mut out = FlushingBuffer::default();

        run_perf_with_sampler(
            &options,
            &mut out,
            || {
                let snapshot = snapshots[next_snapshot].clone();
                next_snapshot += 1;
                Ok(snapshot)
            },
            |_| {},
        )
        .expect("count limited sampling");

        assert_eq!(out.flushes, 2);
    }

    #[test]
    fn schema_output_includes_stable_sections_and_stats_file() {
        let options = PerfOptions {
            mountpoint: None,
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: true,
            sections: RUNTIME_STATS_SECTIONS.to_vec(),
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Table,
        };
        let mut out = Vec::new();

        write_schema(&mut out, &options).expect("write schema");
        let payload: serde_json::Value = serde_json::from_slice(&out).expect("schema json");

        assert_eq!(payload["stats_file"], ".studiofs-runtime-stats");
        assert!(
            payload["sections"]
                .as_array()
                .expect("sections")
                .contains(&serde_json::json!("bridge"))
        );
    }

    #[cfg(windows)]
    #[test]
    fn stats_path_treats_drive_letter_mountpoint_as_drive_root() {
        assert_eq!(
            stats_path(Path::new("Z:")),
            PathBuf::from(r"Z:\.studiofs-runtime-stats")
        );
    }

    #[test]
    fn read_snapshot_reports_clear_non_studiofs_mount_error() {
        let dir = target_tempdir();
        let error =
            read_snapshot(&stats_path(dir.path())).expect_err("missing stats file should fail");

        assert!(
            error
                .to_string()
                .contains("StudioFS runtime stats surface is unavailable"),
            "{error:#}"
        );
    }

    #[test]
    fn read_snapshot_reports_clear_unreadable_stats_surface_error() {
        let dir = target_tempdir();
        let path = stats_path(dir.path());
        std::fs::create_dir(&path).expect("stats path directory");

        let error = read_snapshot(&path).expect_err("directory stats path should fail");

        assert!(
            error
                .to_string()
                .contains("StudioFS runtime stats surface is unavailable"),
            "{error:#}"
        );
    }

    #[test]
    fn read_snapshot_reports_clear_malformed_stats_surface_error() {
        let dir = target_tempdir();
        let path = stats_path(dir.path());
        std::fs::write(&path, "counter vfs_read.calls count nope\n").expect("stats text");

        let error = read_snapshot(&path).expect_err("malformed stats should fail");

        assert!(
            error.to_string().contains("StudioFS runtime stats surface"),
            "{error:#}"
        );
        assert!(error.to_string().contains("malformed"), "{error:#}");
    }
}
