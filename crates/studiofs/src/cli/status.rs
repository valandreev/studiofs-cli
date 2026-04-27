//! `studiofs status` — show filesystem status.

use crate::cli::WRITE_PATH_OBSERVABILITY_FILE;
use crate::cli::cache_layout::describe_cache_layout_state;
use crate::cli::utils::format_bytes;
use clap::Args;
use std::path::{Path, PathBuf};
use studiofs_cache::runtime::{
    CacheIndex, LocalPayloadBackendKind, LocalPayloadRuntimeDetection, detect_cache_backends,
};
use studiofs_cache::{OpenLocalPayloadBackendError, open_local_payload_backend};
use studiofs_meta::counters;
use studiofs_vfs::VfsWritePathObservabilitySnapshot;
#[cfg(test)]
use uuid::Uuid;

const WRITE_PATH_STALE_WARN_MS: u64 = 5_000;

#[derive(Debug, Clone, Copy, Default)]
struct CacheUsageStats {
    usage_bytes: u64,
    dirty_bytes: u64,
    evictable_bytes: u64,
}

fn local_cache_path(meta_dir: &Path, cache_dir: Option<&Path>, volume_uuid: &str) -> PathBuf {
    let base = cache_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| meta_dir.join("cache"));
    base.join(volume_uuid)
}

#[derive(Debug, Clone, Copy)]
pub enum Verbosity {
    Normal,
    Verbose,
}

impl Verbosity {
    fn is_verbose(self) -> bool {
        matches!(self, Self::Verbose)
    }
}

#[derive(Debug, Args)]
#[command(about = "Show volume status, PostgreSQL metadata binding, and local counters")]
#[command(after_help = "\
EXAMPLES:
  studiofs status myvolume
  studiofs status myvolume --verbose
  studiofs status myvolume --cache-dir D:\\cache\\studiofs
")]
pub(crate) struct StatusArgs {
    /// Volume name
    name: String,

    /// Override local cache base directory (defaults to `<meta-dir>/cache`)
    #[arg(long)]
    cache_dir: Option<PathBuf>,

    /// Include mounted write-path bottleneck metrics when available
    #[arg(long)]
    verbose: bool,
}

impl StatusArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        let meta_dir = super::utils::get_default_meta_dir(&self.name)?;
        let verbosity = if self.verbose {
            Verbosity::Verbose
        } else {
            Verbosity::Normal
        };
        run(&self.name, &meta_dir, self.cache_dir.as_deref(), verbosity)
    }
}

fn detect_backends_or_warn(
    cache_path: &Path,
    detector: impl FnOnce(&Path) -> std::io::Result<LocalPayloadRuntimeDetection>,
) -> Option<LocalPayloadRuntimeDetection> {
    match detector(cache_path) {
        Ok(detection) => Some(detection),
        Err(error) => {
            eprintln!(
                "warning: failed to inspect cache backend contract at '{}': {error}",
                cache_path.display()
            );
            None
        }
    }
}

fn detect_backends(cache_path: &Path) -> Option<LocalPayloadRuntimeDetection> {
    detect_backends_or_warn(cache_path, detect_cache_backends)
}

fn format_ms(value: f64) -> String {
    format!("{value:.3}ms")
}

fn format_quantile_ms(value: f64, saturated: bool) -> String {
    if saturated {
        format!("{}+ (saturated)", format_ms(value))
    } else {
        format_ms(value)
    }
}

fn format_staged_upload_cache_pressure_line(
    snapshot: &VfsWritePathObservabilitySnapshot,
) -> String {
    format!(
        "  Staged current/peak/upload/cache pressure: {}/{}/{}/{}",
        format_bytes(snapshot.dirty_pressure.pending_persisted_slice_bytes as u64),
        format_bytes(snapshot.dirty_pressure.staged_peak_bytes as u64),
        format_bytes(snapshot.dirty_pressure.pending_upload_bytes as u64),
        format_bytes(snapshot.dirty_pressure.cache_pressure_bytes as u64)
    )
}

fn read_write_path_observability(
    cache_path: &Path,
) -> Option<(PathBuf, VfsWritePathObservabilitySnapshot)> {
    let metrics_path = cache_path.join(WRITE_PATH_OBSERVABILITY_FILE);
    let bytes = match std::fs::read(&metrics_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => {
            eprintln!(
                "warning: failed to read mounted write-path metrics '{}': {error}",
                metrics_path.display()
            );
            return None;
        }
    };
    match serde_json::from_slice::<VfsWritePathObservabilitySnapshot>(&bytes) {
        Ok(snapshot) => Some((metrics_path, snapshot)),
        Err(error) => {
            eprintln!(
                "warning: failed to parse mounted write-path metrics '{}': {error}",
                metrics_path.display()
            );
            None
        }
    }
}

fn read_write_path_observability_from_cache_root(
    cache_root: &Path,
) -> Option<(PathBuf, VfsWritePathObservabilitySnapshot)> {
    let mut best = read_write_path_observability(cache_root);
    let entries = match std::fs::read_dir(cache_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return best,
        Err(error) => {
            eprintln!(
                "warning: failed to scan cache root for mounted write-path metrics '{}': {error}",
                cache_root.display()
            );
            return best;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if let Some(candidate) = read_write_path_observability(&path) {
            let replace = best
                .as_ref()
                .map(|(_, current)| candidate.1.generated_unix_ms > current.generated_unix_ms)
                .unwrap_or(true);
            if replace {
                best = Some(candidate);
            }
        }
    }
    best
}

fn print_mounted_write_path_fallback(
    mounted_write_path: Option<(PathBuf, VfsWritePathObservabilitySnapshot)>,
) {
    println!("\nMounted Write Path");
    println!("──────────────────");
    let Some((metrics_path, snapshot)) = mounted_write_path else {
        println!("  Metrics file:              unavailable");
        println!("  Hint:                      check --cache-dir or wait for mounted publisher");
        return;
    };

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0);
    let snapshot_age_ms = now_ms.saturating_sub(snapshot.generated_unix_ms);

    println!("  Metrics file:              {}", metrics_path.display());
    println!("  Snapshot age:              {}ms", snapshot_age_ms);
    println!("  Backend tag:               {}", snapshot.backend_tag);
    println!("  Uptime:                    {:.1}s", snapshot.uptime_secs);
    println!(
        "  flush barrier calls:       {} ({:.2}/s)",
        snapshot.flush_barrier_calls, snapshot.flush_barrier_calls_per_sec
    );
    println!(
        "  landing accepted writes:   {} ({}, {:.2}MiB/s)",
        snapshot.landing_zone.accepted_writes,
        format_bytes(snapshot.landing_zone.accepted_bytes),
        snapshot.landing_zone.accepted_mib_per_sec
    );
    println!(
        "  landing recoverable segs:  {}",
        snapshot.landing_zone.recoverable_segments
    );
    println!(
        "  landing accept avg/max:    {}/{}",
        format_ms(snapshot.landing_zone.accept_avg_ms),
        format_ms(snapshot.landing_zone.accept_max_ms)
    );
    println!(
        "  landing syncs data/manifest/rewrite: {}/{}/{}",
        snapshot.landing_zone.data_syncs,
        snapshot.landing_zone.manifest_syncs,
        snapshot.landing_zone.manifest_rewrites
    );
    println!(
        "  payload_store_sync_data calls: {} ({:.2}/s)",
        snapshot.payload_store_sync_data.calls, snapshot.payload_store_sync_data.calls_per_sec
    );
    println!(
        "  payload bursts count/bytes: {}/{}",
        snapshot.payload_burst.bursts,
        format_bytes(snapshot.payload_burst.total_bytes)
    );
    println!(
        "  payload persist calls/avg/max: {}/{}/{}",
        snapshot.payload_persist.calls,
        format_ms(snapshot.payload_persist.avg_ms),
        format_ms(snapshot.payload_persist.max_ms)
    );
    println!(
        "  metadata commit calls/avg/max: {}/{}/{}",
        snapshot.metadata_commit.calls,
        format_ms(snapshot.metadata_commit.avg_ms),
        format_ms(snapshot.metadata_commit.max_ms)
    );
}

fn map_cache_backend_error(
    cache_dir: &Path,
    source: OpenLocalPayloadBackendError,
) -> anyhow::Error {
    anyhow::anyhow!(
        "failed to open local cache backend at '{}': {source}",
        cache_dir.display()
    )
}

fn scan_local_payload_cache(
    cache_backend_detection: &LocalPayloadRuntimeDetection,
) -> anyhow::Result<CacheUsageStats> {
    // Status is allowed to open the index directly to read aggregate counters.
    let index = CacheIndex::open(&cache_backend_detection.layout.redb_index_path)?;
    let stats = index.stats()?;
    Ok(CacheUsageStats {
        usage_bytes: stats.current_bytes,
        dirty_bytes: stats.dirty_bytes,
        evictable_bytes: stats.evictable_bytes,
    })
}

fn scan_cache_usage(
    cache_backend_detection: Option<&LocalPayloadRuntimeDetection>,
) -> anyhow::Result<Option<CacheUsageStats>> {
    match cache_backend_detection {
        Some(detection)
            if detection.runtime_backend() == Some(LocalPayloadBackendKind::Current) =>
        {
            scan_local_payload_cache(detection).map(Some)
        }
        _ => Ok(None),
    }
}

fn scan_pending_uploads(
    cache_dir: &Path,
    volume_uuid: &str,
    cache_backend_detection: Option<&LocalPayloadRuntimeDetection>,
) -> anyhow::Result<usize> {
    let Some(detection) = cache_backend_detection else {
        return Ok(0);
    };
    if detection.runtime_backend() != Some(LocalPayloadBackendKind::Current) {
        return Ok(0);
    }

    let store = open_local_payload_backend(cache_dir, volume_uuid)
        .map_err(|source| map_cache_backend_error(cache_dir, source))?
        .backend;
    Ok(store.dirty_object_ids().len())
}

fn scan_pending_uploads_or_warn(
    cache_path: &Path,
    volume_uuid: &str,
    cache_backend_detection: Option<&LocalPayloadRuntimeDetection>,
) -> usize {
    match scan_pending_uploads(cache_path, volume_uuid, cache_backend_detection) {
        Ok(count) => count,
        Err(error) => {
            eprintln!(
                "warning: failed to compute pending upload count at '{}': {error}",
                cache_path.display()
            );
            0
        }
    }
}

pub fn run(
    name: &str,
    meta_dir: &Path,
    cache_dir: Option<&Path>,
    verbosity: Verbosity,
) -> anyhow::Result<()> {
    let db_path = meta_dir.join(format!("{name}.db"));

    if !db_path.exists() {
        println!("✗ Volume '{}' not found at {:?}", name, db_path);
        println!("  Run `studiofs init {}` first.", name);
        return Ok(());
    }

    let engine = match studiofs_meta::open_meta_engine(&db_path) {
        Ok(engine) => engine,
        Err(error) if verbosity.is_verbose() => {
            eprintln!(
                "warning: failed to open volume metadata at '{}': {error}",
                db_path.display()
            );
            println!("StudioFS Volume Status");
            println!("──────────────────────");
            println!("  Name:       {}", name);
            println!("  Metadata:   {:?}", db_path);
            println!(
                "  Warning:    metadata DB is locked; showing mounted write-path metrics only"
            );
            let cache_root = cache_dir
                .map(Path::to_path_buf)
                .unwrap_or_else(|| meta_dir.join("cache"));
            println!("\nLocal Cache");
            println!("───────────");
            println!("  Root:                {}", cache_root.display());
            print_mounted_write_path_fallback(read_write_path_observability_from_cache_root(
                &cache_root,
            ));
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let format_info = engine.get_format_info()?;

    let total_inodes = engine.get_counter(counters::TOTAL_INODES)?;
    let used_space = engine.get_counter(counters::USED_SPACE)?;
    let next_inode = engine.get_counter(counters::NEXT_INODE)?;

    let cache_path = local_cache_path(meta_dir, cache_dir, &format_info.uuid);
    let cache_backend_detection = detect_backends(&cache_path);
    let pending_uploads = scan_pending_uploads_or_warn(
        &cache_path,
        &format_info.uuid,
        cache_backend_detection.as_ref(),
    );
    let cache_usage = match scan_cache_usage(cache_backend_detection.as_ref()) {
        Ok(stats) => Some(stats),
        Err(error) => {
            eprintln!(
                "warning: failed to compute local cache usage stats at '{}': {error}",
                cache_path.display()
            );
            None
        }
    };
    let mounted_write_path = if verbosity.is_verbose() {
        read_write_path_observability(&cache_path)
    } else {
        None
    };

    let remote_size = engine.remote_metadata_size()?.unwrap_or(0);
    let local_db_size = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);

    println!("StudioFS Volume Status");
    println!("──────────────────────");
    println!("  Name:       {}", format_info.name);
    println!("  UUID:       {}", format_info.uuid);
    println!("  Endpoint:   {}", format_info.endpoint);
    println!("  Bucket:     {}", format_info.bucket);
    println!("  Data prefix: {}", format_info.data_prefix);
    println!(
        "  Block size: {} MiB",
        format_info.block_size / (1024 * 1024)
    );

    if let Some(pg) = &format_info.metadata.postgres {
        println!("\n  Meta backend: Postgres");
        println!("  Postgres URL:  {}", pg.url);
        println!("  Postgres vol:  {}", pg.volume_id);
        println!("  Postgres size: {}", format_bytes(remote_size));
    } else {
        println!("\n  Meta backend: {:?}", format_info.metadata.backend);
    }

    println!("\n  Inodes:     {}", total_inodes);
    println!("  Next inode: {}", next_inode);
    println!("  Next inode/object ID: {}", next_inode);

    println!("\n  Used space: {}", format_bytes(used_space));

    println!("\n  Metadata:   {:?}", db_path);
    println!("  Meta size:   {}", format_bytes(local_db_size));

    println!("\nLocal Cache");
    println!("───────────");
    println!("  Root:                {}", cache_path.display());
    let runtime_ready = cache_backend_detection
        .as_ref()
        .and_then(LocalPayloadRuntimeDetection::runtime_backend)
        == Some(LocalPayloadBackendKind::Current);
    let contract_state = describe_cache_layout_state(cache_backend_detection.as_ref());
    let remediation = if let Some(detection) = cache_backend_detection.as_ref() {
        if detection.runtime_backend() == Some(LocalPayloadBackendKind::Current) {
            "none"
        } else if detection.has_runtime_layout() {
            "runtime layout is incomplete; mount will reprovision missing runtime directories on first use"
        } else {
            "mount will provision the local cache layout on first use"
        }
    } else {
        "inspect the cache root manually"
    };
    let reconcile_needed = pending_uploads > 0
        || cache_usage
            .and_then(|stats| stats.map(|stats| stats.dirty_bytes > 0))
            .unwrap_or(false);
    println!(
        "  Runtime:             {}",
        LocalPayloadBackendKind::Current
    );
    println!(
        "  Runtime readiness:   {}",
        if runtime_ready { "ready" } else { "blocked" }
    );
    println!("  Cache layout state:  {}", contract_state);
    if let Some(detection) = cache_backend_detection.as_ref() {
        println!(
            "  Local cache index:   {} ({})",
            detection.layout.redb_index_path.display(),
            if detection.index_exists {
                "present"
            } else {
                "absent"
            }
        );
        println!(
            "  Local cache payload: {} ({})",
            detection.layout.payload_objects_dir.display(),
            if detection.payload_objects_dir_exists {
                "present"
            } else {
                "absent"
            }
        );
        println!(
            "  Local cache upload:  {} ({})",
            detection.layout.upload_stage_objects_dir.display(),
            if detection.upload_objects_dir_exists {
                "present"
            } else {
                "absent"
            }
        );
        println!(
            "  Runtime root:        {}",
            detection.layout.runtime_root.display()
        );
    }
    println!("  Init guidance:       {}", remediation);
    println!(
        "  Reconcile needed:    {}",
        if reconcile_needed { "yes" } else { "no" }
    );
    if let Some(stats) = cache_usage.flatten() {
        println!("  Usage bytes:         {}", format_bytes(stats.usage_bytes));
        println!("  Dirty bytes:         {}", format_bytes(stats.dirty_bytes));
        println!(
            "  Evictable bytes:     {}",
            format_bytes(stats.evictable_bytes)
        );
    } else {
        println!("  Usage bytes:         unknown");
        println!("  Dirty bytes:         unknown");
        println!("  Evictable bytes:     unknown");
    }

    println!("  Pending uploads:    {}", pending_uploads);

    if verbosity.is_verbose() {
        println!("\nMounted Write Path");
        println!("──────────────────");
        if let Some((metrics_path, snapshot)) = mounted_write_path {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| u64::try_from(d.as_millis()).ok())
                .unwrap_or(0);
            let snapshot_age_ms = now_ms.saturating_sub(snapshot.generated_unix_ms);

            println!("  Metrics file:              {}", metrics_path.display());
            println!("  Snapshot age:              {}ms", snapshot_age_ms);
            println!(
                "  Snapshot freshness:        {}",
                if snapshot_age_ms > WRITE_PATH_STALE_WARN_MS {
                    "stale"
                } else {
                    "fresh"
                }
            );
            if snapshot_age_ms > WRITE_PATH_STALE_WARN_MS {
                println!(
                    "  Warning:                   mounted metrics are stale; publisher may be stopped"
                );
            }
            println!("  Backend tag:               {}", snapshot.backend_tag);
            println!("  Uptime:                    {:.1}s", snapshot.uptime_secs);
            println!(
                "  flush barrier calls:       {} ({:.2}/s)",
                snapshot.flush_barrier_calls, snapshot.flush_barrier_calls_per_sec
            );
            println!(
                "  landing accepted writes:   {} ({}, {:.2}MiB/s)",
                snapshot.landing_zone.accepted_writes,
                format_bytes(snapshot.landing_zone.accepted_bytes),
                snapshot.landing_zone.accepted_mib_per_sec
            );
            println!(
                "  landing recoverable segs:  {}",
                snapshot.landing_zone.recoverable_segments
            );
            println!(
                "  landing accept avg/max:    {}/{}",
                format_ms(snapshot.landing_zone.accept_avg_ms),
                format_ms(snapshot.landing_zone.accept_max_ms)
            );
            println!(
                "  landing syncs data/manifest/rewrite: {}/{}/{}",
                snapshot.landing_zone.data_syncs,
                snapshot.landing_zone.manifest_syncs,
                snapshot.landing_zone.manifest_rewrites
            );
            println!(
                "  landing sync total/avg/max: {}/{}/{}",
                format_ms(snapshot.landing_zone.sync_total_ms),
                format_ms(snapshot.landing_zone.sync_avg_ms),
                format_ms(snapshot.landing_zone.sync_max_ms)
            );
            println!(
                "  payload_store_sync_data calls: {} ({:.2}/s)",
                snapshot.payload_store_sync_data.calls,
                snapshot.payload_store_sync_data.calls_per_sec
            );
            println!(
                "  payload_store_sync_data total: {}",
                format_ms(snapshot.payload_store_sync_data.total_ms)
            );
            println!(
                "  payload_store_sync_data avg:   {}",
                format_ms(snapshot.payload_store_sync_data.avg_ms)
            );
            println!(
                "  payload_store_sync_data max:   {}",
                format_ms(snapshot.payload_store_sync_data.max_ms)
            );
            println!(
                "  payload_store_sync_data p50/p95/p99: {}/{}/{}",
                format_quantile_ms(
                    snapshot.payload_store_sync_data.latency_quantiles_ms.p50,
                    snapshot
                        .payload_store_sync_data
                        .latency_quantiles_saturated
                        .p50
                ),
                format_quantile_ms(
                    snapshot.payload_store_sync_data.latency_quantiles_ms.p95,
                    snapshot
                        .payload_store_sync_data
                        .latency_quantiles_saturated
                        .p95
                ),
                format_quantile_ms(
                    snapshot.payload_store_sync_data.latency_quantiles_ms.p99,
                    snapshot
                        .payload_store_sync_data
                        .latency_quantiles_saturated
                        .p99
                )
            );
            println!(
                "  sync barrier queue wait total/avg/max: {}/{}/{}",
                format_ms(snapshot.payload_store_sync_data.queue_wait_total_ms),
                format_ms(snapshot.payload_store_sync_data.queue_wait_avg_ms),
                format_ms(snapshot.payload_store_sync_data.queue_wait_max_ms)
            );
            println!(
                "  sync barrier interval avg/max: {}/{}",
                format_ms(snapshot.payload_store_sync_data.barrier_interval_avg_ms),
                format_ms(snapshot.payload_store_sync_data.barrier_interval_max_ms)
            );
            println!(
                "  payload bursts count/bytes: {}/{}",
                snapshot.payload_burst.bursts,
                format_bytes(snapshot.payload_burst.total_bytes)
            );
            println!(
                "  payload burst avg bytes/max bytes: {}/{}",
                format_bytes(snapshot.payload_burst.avg_bytes_per_burst as u64),
                format_bytes(snapshot.payload_burst.max_bytes_per_burst)
            );
            println!(
                "  payload burst avg payloads/max payloads: {:.2}/{}",
                snapshot.payload_burst.avg_payloads_per_burst,
                snapshot.payload_burst.max_payloads_per_burst
            );
            println!(
                "  payload burst avg interval/max interval: {}/{}",
                format_ms(snapshot.payload_burst.avg_interval_ms),
                format_ms(snapshot.payload_burst.max_interval_ms)
            );
            println!(
                "  payload persist calls/avg/max: {}/{}/{}",
                snapshot.payload_persist.calls,
                format_ms(snapshot.payload_persist.avg_ms),
                format_ms(snapshot.payload_persist.max_ms)
            );
            println!(
                "  payload persist p50/p95/p99: {}/{}/{}",
                format_quantile_ms(
                    snapshot.payload_persist.latency_quantiles_ms.p50,
                    snapshot.payload_persist.latency_quantiles_saturated.p50
                ),
                format_quantile_ms(
                    snapshot.payload_persist.latency_quantiles_ms.p95,
                    snapshot.payload_persist.latency_quantiles_saturated.p95
                ),
                format_quantile_ms(
                    snapshot.payload_persist.latency_quantiles_ms.p99,
                    snapshot.payload_persist.latency_quantiles_saturated.p99
                )
            );
            println!(
                "  metadata commit calls/avg/max: {}/{}/{}",
                snapshot.metadata_commit.calls,
                format_ms(snapshot.metadata_commit.avg_ms),
                format_ms(snapshot.metadata_commit.max_ms)
            );
            println!(
                "  metadata commit p50/p95/p99: {}/{}/{}",
                format_quantile_ms(
                    snapshot.metadata_commit.latency_quantiles_ms.p50,
                    snapshot.metadata_commit.latency_quantiles_saturated.p50
                ),
                format_quantile_ms(
                    snapshot.metadata_commit.latency_quantiles_ms.p95,
                    snapshot.metadata_commit.latency_quantiles_saturated.p95
                ),
                format_quantile_ms(
                    snapshot.metadata_commit.latency_quantiles_ms.p99,
                    snapshot.metadata_commit.latency_quantiles_saturated.p99
                )
            );
            println!(
                "  metadata commit interval avg/max: {}/{}",
                format_ms(snapshot.metadata_commit_cadence.avg_interval_ms),
                format_ms(snapshot.metadata_commit_cadence.max_interval_ms)
            );
            println!(
                "  eager flush checks/triggered/empty/below-threshold: {}/{}/{}/{}",
                snapshot.eager_flush.checks,
                snapshot.eager_flush.triggered,
                snapshot.eager_flush.skipped_empty,
                snapshot.eager_flush.skipped_below_threshold
            );
            println!(
                "  local_cache_io model/calls: {}/{}",
                snapshot.local_cache_io.execution_model, snapshot.local_cache_io.calls
            );
            println!(
                "  local_cache_io handoff total/avg/max: {}/{}/{}",
                format_ms(snapshot.local_cache_io.handoff_total_ms),
                format_ms(snapshot.local_cache_io.handoff_avg_ms),
                format_ms(snapshot.local_cache_io.handoff_max_ms)
            );
            println!(
                "  local_cache_io backend total/avg/max: {}/{}/{}",
                format_ms(snapshot.local_cache_io.backend_io_total_ms),
                format_ms(snapshot.local_cache_io.backend_io_avg_ms),
                format_ms(snapshot.local_cache_io.backend_io_max_ms)
            );
            println!(
                "  WinFSP callbacks write/flush/cleanup->flush_fh: {}/{}/{}",
                snapshot.winfsp_callbacks.write,
                snapshot.winfsp_callbacks.flush,
                snapshot.winfsp_callbacks.cleanup_flush_fh
            );
            println!(
                "  Write backpressure events/wait/max: {}/{}/{}ms",
                snapshot.write_backpressure.events,
                snapshot.write_backpressure.wait_total_ms,
                snapshot.write_backpressure.stall_max_ms
            );
            println!(
                "  Dirty pressure current/peak/limit: {}/{}/{}",
                format_bytes(snapshot.dirty_pressure.total_dirty_bytes as u64),
                format_bytes(snapshot.dirty_pressure.dirty_peak_bytes as u64),
                format_bytes(snapshot.dirty_pressure.dirty_limit_bytes as u64)
            );
            println!("{}", format_staged_upload_cache_pressure_line(&snapshot));
            println!(
                "  Soft throttle / hard backpressure: {}/{}",
                snapshot.write_backpressure.soft_throttle_events,
                snapshot.write_backpressure.hard_events
            );
        } else {
            println!("  Metrics file:              unavailable");
            println!("  Hint:                      mount the volume and rerun with --verbose");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_pending_uploads_supports_current_local_payload_runtime() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = studiofs_cache::runtime::LocalPayloadLayout::from_cache_root(tmp.path());
        layout.provision().unwrap();
        let volume_uuid = Uuid::new_v4();

        let detection = detect_backends(tmp.path()).unwrap();
        assert_eq!(
            detection.runtime_backend(),
            Some(LocalPayloadBackendKind::Current)
        );
        assert_eq!(
            scan_pending_uploads(tmp.path(), &volume_uuid.to_string(), Some(&detection)).unwrap(),
            0
        );
    }

    #[test]
    fn scan_pending_uploads_or_warn_returns_zero_when_cache_index_is_locked() {
        let tmp = tempfile::tempdir().unwrap();
        let volume_uuid = Uuid::new_v4();
        let _mounted_backend =
            studiofs_cache::open_local_payload_backend(tmp.path(), &volume_uuid.to_string())
                .expect("open mounted local payload backend")
                .backend;

        let detection = detect_backends(tmp.path()).unwrap();
        assert_eq!(
            scan_pending_uploads_or_warn(tmp.path(), &volume_uuid.to_string(), Some(&detection)),
            0
        );
    }

    #[test]
    fn scan_cache_usage_reads_runtime_written_index() {
        let tmp = tempfile::tempdir().unwrap();
        let volume_uuid = Uuid::new_v4();
        {
            let backend =
                studiofs_cache::open_local_payload_backend(tmp.path(), &volume_uuid.to_string())
                    .expect("open local payload backend")
                    .backend;
            backend.put(10, b"dirty", false).unwrap();
            backend.put(11, b"cached", true).unwrap();
            backend.sync_data().unwrap();
        }

        let detection = detect_backends(tmp.path()).unwrap();
        let usage = scan_cache_usage(Some(&detection))
            .expect("scan cache usage")
            .expect("cache usage present");
        assert_eq!(usage.usage_bytes, 11);
        assert_eq!(usage.dirty_bytes, 5);
        assert_eq!(usage.evictable_bytes, 6);
    }

    #[test]
    fn scan_cache_usage_skips_incomplete_runtime_without_creating_index() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = studiofs_cache::runtime::LocalPayloadLayout::from_cache_root(tmp.path());
        std::fs::create_dir_all(&layout.index_dir).unwrap();

        let detection = detect_backends(tmp.path()).unwrap();
        assert!(detection.has_runtime_layout());
        assert_eq!(detection.runtime_backend(), None);
        assert!(!layout.redb_index_path.exists());

        let usage = scan_cache_usage(Some(&detection)).expect("scan cache usage");
        assert!(usage.is_none(), "incomplete runtimes should be skipped");
        assert!(
            !layout.redb_index_path.exists(),
            "status inspection must not create a cache index for partial runtimes"
        );
    }

    #[test]
    fn detect_cache_backends_with_fallback_returns_none_on_error() {
        let tmp = tempfile::tempdir().unwrap();
        let detection = detect_backends_or_warn(tmp.path(), |_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated permission error",
            ))
        });
        assert!(detection.is_none());
    }

    #[test]
    fn detect_backends_returns_detection_when_probe_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let detection = detect_backends(tmp.path());
        assert!(detection.is_some());
    }

    #[test]
    fn read_write_path_observability_returns_none_when_file_missing() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read_write_path_observability(tmp.path()).is_none());
    }

    #[test]
    fn read_write_path_observability_reads_snapshot_file() {
        let tmp = tempfile::tempdir().unwrap();
        let metrics_path = tmp.path().join(WRITE_PATH_OBSERVABILITY_FILE);
        let snapshot = VfsWritePathObservabilitySnapshot {
            publisher_pid: 42,
            generated_unix_ms: 123,
            uptime_secs: 4.0,
            pending_uploads: 7,
            backend_tag: "local-payload-runtime".to_string(),
            flush_barrier_calls: 11,
            flush_barrier_calls_per_sec: 2.5,
            payload_store_sync_data: Default::default(),
            local_cache_io: Default::default(),
            winfsp_callbacks: Default::default(),
            winfsp_write_path: Default::default(),
            vfs_write_path: Default::default(),
            landing_zone: Default::default(),
            drain_lag: Default::default(),
            winfsp_foreground_flushes: Default::default(),
            write_backpressure: Default::default(),
            dirty_pressure: Default::default(),
            payload_burst: Default::default(),
            payload_persist: Default::default(),
            metadata_commit: Default::default(),
            metadata_commit_cadence: Default::default(),
            eager_flush: Default::default(),
            tuning: Default::default(),
        };
        std::fs::write(&metrics_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();

        let loaded = read_write_path_observability(tmp.path()).expect("metrics");
        assert_eq!(loaded.0, metrics_path);
        assert_eq!(loaded.1.backend_tag, "local-payload-runtime");
        assert_eq!(loaded.1.flush_barrier_calls, 11);
    }

    #[test]
    fn mounted_status_labels_staged_current_and_peak_pressure() {
        let mut snapshot = VfsWritePathObservabilitySnapshot::default();
        snapshot.dirty_pressure.pending_persisted_slice_bytes = 512;
        snapshot.dirty_pressure.staged_peak_bytes = 1024;
        snapshot.dirty_pressure.pending_upload_bytes = 2048;
        snapshot.dirty_pressure.cache_pressure_bytes = 4096;

        assert_eq!(
            format_staged_upload_cache_pressure_line(&snapshot),
            "  Staged current/peak/upload/cache pressure: 512B/1.0KiB/2.0KiB/4.0KiB"
        );
    }

    #[test]
    fn read_write_path_observability_from_cache_root_finds_uuid_child_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let older_dir = tmp.path().join(Uuid::new_v4().to_string());
        let newer_dir = tmp.path().join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&older_dir).unwrap();
        std::fs::create_dir_all(&newer_dir).unwrap();
        let older = VfsWritePathObservabilitySnapshot {
            generated_unix_ms: 100,
            backend_tag: "older".to_string(),
            ..Default::default()
        };
        let newer = VfsWritePathObservabilitySnapshot {
            generated_unix_ms: 200,
            backend_tag: "newer".to_string(),
            ..Default::default()
        };
        std::fs::write(
            older_dir.join(WRITE_PATH_OBSERVABILITY_FILE),
            serde_json::to_vec(&older).unwrap(),
        )
        .unwrap();
        std::fs::write(
            newer_dir.join(WRITE_PATH_OBSERVABILITY_FILE),
            serde_json::to_vec(&newer).unwrap(),
        )
        .unwrap();

        let loaded = read_write_path_observability_from_cache_root(tmp.path()).expect("metrics");
        assert_eq!(loaded.1.backend_tag, "newer");
        assert_eq!(loaded.0, newer_dir.join(WRITE_PATH_OBSERVABILITY_FILE));
    }

    #[test]
    fn format_quantile_ms_marks_saturated_values() {
        assert_eq!(format_quantile_ms(5000.0, true), "5000.000ms+ (saturated)");
        assert_eq!(format_quantile_ms(120.5, false), "120.500ms");
    }

    #[test]
    fn status_parses_cache_dir() {
        let cli =
            crate::cli::parse_for_test(&["status", "vol", "--cache-dir", "D:\\cache\\studiofs"])
                .unwrap();
        match cli.command {
            crate::cli::Commands::Status(StatusArgs {
                name,
                cache_dir,
                verbose,
            }) => {
                assert_eq!(name, "vol");
                assert_eq!(cache_dir.unwrap(), PathBuf::from("D:\\cache\\studiofs"));
                assert!(!verbose);
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[test]
    fn status_parses_verbose_flag() {
        let cli = crate::cli::parse_for_test(&["status", "vol", "--verbose"]).unwrap();
        match cli.command {
            crate::cli::Commands::Status(StatusArgs {
                name,
                cache_dir,
                verbose,
            }) => {
                assert_eq!(name, "vol");
                assert!(cache_dir.is_none());
                assert!(verbose);
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }
}
