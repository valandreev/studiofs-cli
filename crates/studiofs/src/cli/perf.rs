//! `studiofs perf` - live runtime stats viewer for mounted StudioFS volumes.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::{Context, bail};
use clap::{Args, ValueEnum};
use serde::Serialize;
use studiofs_vfs::{
    RUNTIME_STATS_FILE_NAME, RUNTIME_STATS_SECTIONS, RuntimeLatencyHistogram, RuntimeStatsSection,
    RuntimeStatsSnapshot,
};

const DEFAULT_INTERVAL_MS: u64 = 1_000;
const RUNTIME_STATS_FILE: &str = ".studiofs-runtime-stats";
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
  studiofs perf Z: --schema
")]
pub(crate) struct PerfArgs {
    /// StudioFS mount point containing the hidden runtime stats file
    pub(crate) mountpoint: PathBuf,

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

    /// Print each sample as pretty JSON
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
    mountpoint: PathBuf,
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
    unit: String,
}

#[derive(Debug, Serialize, PartialEq)]
struct PerfGaugeSample {
    value: u64,
    unit: String,
}

#[derive(Debug, Serialize, PartialEq)]
struct PerfLatencySample {
    count_delta: u64,
    sum_delta: u64,
    avg: Option<f64>,
    unit: String,
}

fn run_perf(options: &PerfOptions, out: &mut impl Write) -> anyhow::Result<()> {
    if options.schema {
        write_schema(out, options)?;
        return Ok(());
    }

    let stats_path = stats_path(&options.mountpoint);
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
    let mut printed_header = false;

    loop {
        if matches!(remaining, Some(0)) {
            return Ok(());
        }

        sleep(options.interval);
        let current = read_next()?;
        let sample = build_sample(&previous, &current, options);
        write_sample(out, &sample, options, &mut printed_header)?;
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
    for _ in 0..count {
        sleep(options.interval);
        let current = read_next()?;
        samples.push(build_sample(&previous, &current, options));
        previous = current;
    }
    write_json_samples(out, &samples)
}

fn stats_path(mountpoint: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};

        let mut components = mountpoint.components();
        if let (Some(Component::Prefix(prefix)), None) = (components.next(), components.next())
            && matches!(prefix.kind(), Prefix::Disk(_))
        {
            return PathBuf::from(format!("{}\\{RUNTIME_STATS_FILE}", mountpoint.display()));
        }
    }

    mountpoint.join(RUNTIME_STATS_FILE)
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
        let before = section(previous, section_name);
        let after = section(current, section_name);
        if section_is_empty(after) {
            warnings.push(format!("missing stats for section '{section_name}'"));
        }
        sections.insert(
            *section_name,
            build_section_sample(before, after, seconds, options.verbosity),
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
        let delta = metric.value.saturating_sub(previous_value);
        sample.counters.insert(
            name.clone(),
            PerfCounterSample {
                total: metric.value,
                delta,
                rate_per_sec: rate(delta, seconds),
                unit: metric.unit.to_string(),
            },
        );
    }

    for (name, metric) in &current.gauges {
        sample.gauges.insert(
            name.clone(),
            PerfGaugeSample {
                value: metric.value,
                unit: metric.unit.to_string(),
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
    let count_delta = current.count.saturating_sub(previous_count);
    let sum_delta = current.sum.saturating_sub(previous_sum);
    PerfLatencySample {
        count_delta,
        sum_delta,
        avg: (count_delta > 0).then_some(sum_delta as f64 / count_delta as f64),
        unit: current.unit.to_string(),
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
    printed_header: &mut bool,
) -> anyhow::Result<()> {
    match options.output {
        PerfOutput::Table => write_table_sample(out, sample, printed_header),
        PerfOutput::Json => {
            serde_json::to_writer_pretty(&mut *out, sample)?;
            writeln!(out)?;
            Ok(())
        }
        PerfOutput::JsonLines => {
            serde_json::to_writer(&mut *out, sample)?;
            writeln!(out)?;
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
    printed_header: &mut bool,
) -> anyhow::Result<()> {
    if !*printed_header {
        writeln!(
            out,
            "{:<13} {:<13} {:<9} {:<28} {:>14} {:>14} {:<10}",
            "time_ms", "section", "kind", "metric", "rate/s", "current", "unit"
        )?;
        *printed_header = true;
    }

    for warning in &sample.warnings {
        writeln!(out, "warning: {warning}")?;
    }

    for (section_name, section) in &sample.sections {
        for (name, metric) in &section.counters {
            writeln!(
                out,
                "{:<13} {:<13} {:<9} {:<28} {:>14.2} {:>14} {:<10}",
                sample.generated_unix_ms,
                display_section_name(section_name),
                "counter",
                display_metric_name(name),
                metric.rate_per_sec,
                metric.total,
                metric.unit
            )?;
        }
        for (name, metric) in &section.gauges {
            writeln!(
                out,
                "{:<13} {:<13} {:<9} {:<28} {:>14} {:>14} {:<10}",
                sample.generated_unix_ms,
                display_section_name(section_name),
                "gauge",
                display_metric_name(name),
                "-",
                metric.value,
                metric.unit
            )?;
        }
        for (name, latency) in &section.latencies {
            let avg = latency
                .avg
                .map(|value| format!("{value:.2}"))
                .unwrap_or_else(|| "-".to_string());
            writeln!(
                out,
                "{:<13} {:<13} {:<9} {:<28} {:>14} {:>14} {:<10}",
                sample.generated_unix_ms,
                display_section_name(section_name),
                "latency",
                display_metric_name(name),
                avg,
                latency.count_delta,
                latency.unit
            )?;
        }
    }
    Ok(())
}

fn display_section_name(section_name: &str) -> &'static str {
    match section_name {
        "system" => "runtime",
        "bridge" => "bridge",
        "vfs_read" => "VFS read",
        "vfs_write" => "VFS write",
        "cache" => "cache",
        "upload_queue" => "upload",
        "object_io" => "object",
        "metadata" => "metadata",
        _ => "unknown",
    }
}

fn display_metric_name(name: &str) -> String {
    name.replace('_', " ")
}

fn write_schema(out: &mut impl Write, options: &PerfOptions) -> anyhow::Result<()> {
    let sections = options.sections.to_vec();
    let payload = serde_json::json!({
        "schema_version": studiofs_vfs::RUNTIME_STATS_SCHEMA_VERSION,
        "stats_file": String::from_utf8_lossy(RUNTIME_STATS_FILE_NAME),
        "sections": sections,
        "line_kinds": ["counter", "gauge", "histogram_count", "histogram_sum", "histogram_bucket"],
        "units": ["count", "bytes", "ns", "ms", "percent"],
    });
    serde_json::to_writer_pretty(&mut *out, &payload)?;
    writeln!(out)?;
    Ok(())
}

fn section<'a>(snapshot: &'a RuntimeStatsSnapshot, section_name: &str) -> &'a RuntimeStatsSection {
    match section_name {
        "system" => &snapshot.system,
        "bridge" => &snapshot.bridge,
        "vfs_read" => &snapshot.vfs_read,
        "vfs_write" => &snapshot.vfs_write,
        "cache" => &snapshot.cache,
        "upload_queue" => &snapshot.upload_queue,
        "object_io" => &snapshot.object_io,
        "metadata" => &snapshot.metadata,
        _ => unreachable!("sections are validated before sampling"),
    }
}

fn section_is_empty(section: &RuntimeStatsSection) -> bool {
    section.counters.is_empty() && section.gauges.is_empty() && section.latencies.is_empty()
}

fn parse_sections(raw: &[String]) -> anyhow::Result<Vec<&'static str>> {
    if raw.is_empty() {
        return Ok(RUNTIME_STATS_SECTIONS.to_vec());
    }

    let known = RUNTIME_STATS_SECTIONS
        .iter()
        .copied()
        .collect::<BTreeSet<&'static str>>();
    let mut sections = Vec::new();
    let mut seen = BTreeSet::new();
    for section in raw {
        let normalized = section.replace('-', "_");
        let expanded = match normalized.as_str() {
            "runtime" => vec!["system"],
            "vfs" => vec!["vfs_read", "vfs_write"],
            "upload" => vec!["upload_queue"],
            "object" => vec!["object_io"],
            _ => {
                let Some(&known_section) = known.get(normalized.as_str()) else {
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
    use studiofs_vfs::{RuntimeLatencyBucket, RuntimeLatencyHistogram, RuntimeScalarMetric};

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
            mountpoint: PathBuf::from("Z:"),
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
            mountpoint: PathBuf::from("Z:"),
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
            mountpoint: PathBuf::from("Z:"),
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
            mountpoint: PathBuf::from("Z:"),
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
    fn build_sample_uses_configured_interval_when_snapshot_timestamps_do_not_advance() {
        let options = PerfOptions {
            mountpoint: PathBuf::from("Z:"),
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
    fn build_sample_includes_latency_averages_when_verbose() {
        let options = PerfOptions {
            mountpoint: PathBuf::from("Z:"),
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
            mountpoint: PathBuf::from("Z:"),
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
        let mut printed_header = false;

        write_sample(&mut out, &sample, &options, &mut printed_header).expect("write table");
        let output = String::from_utf8(out).expect("table is utf8");

        assert!(output.contains("warning: missing stats for section 'metadata'"));
    }

    #[test]
    fn table_output_uses_readable_section_metric_and_kind_labels() {
        let options = PerfOptions {
            mountpoint: PathBuf::from("Z:"),
            interval: Duration::from_secs(1),
            count: Some(1),
            schema: false,
            sections: vec!["vfs_read", "upload_queue"],
            verbosity: PerfVerbosity::Normal,
            output: PerfOutput::Table,
        };
        let sample = build_sample(
            &snapshot_at(1_000, 100, 1),
            &snapshot_at(2_000, 600, 3),
            &options,
        );
        let mut out = Vec::new();
        let mut printed_header = false;

        write_sample(&mut out, &sample, &options, &mut printed_header).expect("write table");
        let output = String::from_utf8(out).expect("table is utf8");

        assert!(output.contains("time_ms"));
        assert!(output.contains("section"));
        assert!(output.contains("kind"));
        assert!(output.contains("VFS read"));
        assert!(output.contains("counter"));
        assert!(output.contains("requested bytes"));
        assert!(output.contains("500.00"));
        assert!(output.contains("upload"));
        assert!(output.contains("gauge"));
        assert!(output.contains("pending depth"));
    }

    #[test]
    fn json_lines_output_is_one_compact_line_per_sample() {
        let options = PerfOptions {
            mountpoint: PathBuf::from("Z:"),
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
        let mut printed_header = false;

        write_sample(&mut out, &sample, &options, &mut printed_header).expect("write json lines");
        let output = String::from_utf8(out).expect("json lines is utf8");

        assert_eq!(output.lines().count(), 1);
    }

    #[test]
    fn json_output_is_parseable_pretty_sample_array() {
        let options = PerfOptions {
            mountpoint: PathBuf::from("Z:"),
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
        let mut out = Vec::new();

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
        .expect("write json");
        let payload: serde_json::Value = serde_json::from_slice(&out).expect("json samples");

        let samples = payload.as_array().expect("json sample array");
        assert_eq!(samples.len(), 2);
        assert_eq!(
            samples[1]["sections"]["vfs_read"]["counters"]["requested_bytes"]["delta"],
            250
        );
    }

    #[test]
    fn json_output_count_zero_is_empty_array_without_reading_stats() {
        let options = PerfOptions {
            mountpoint: PathBuf::from("Z:"),
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
            mountpoint: PathBuf::from("Z:"),
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
            mountpoint: PathBuf::from("Z:"),
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
        assert_eq!(sleeps, vec![Duration::from_millis(250); 2]);
    }

    #[test]
    fn schema_output_includes_stable_sections_and_stats_file() {
        let options = PerfOptions {
            mountpoint: PathBuf::from("Z:"),
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
