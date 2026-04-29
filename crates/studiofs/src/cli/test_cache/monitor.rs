use crate::cli::utils::{format_bytes, format_throughput};
use crate::diagnostics;
use anyhow::{Context, Result, anyhow};
use std::path::Path;
use std::time::{Duration, Instant};
use studiofs_vfs::VfsWritePathObservabilitySnapshot;

use super::{resolve_mounted_runtime, test_cache_runtime};

const MONITOR_HEADER_EVERY: u64 = 20;

pub(super) fn run_write_path_monitor(
    target: &Path,
    interval: Duration,
    duration: Option<Duration>,
) -> Result<()> {
    if interval.is_zero() {
        return Err(anyhow!("--monitor-interval-ms must be >= 1"));
    }
    if duration.is_some_and(|value| value.is_zero()) {
        return Err(anyhow!("--monitor-duration-ms must be >= 1 when provided"));
    }

    let runtime = resolve_mounted_runtime(target)?.ok_or_else(|| {
        anyhow!(
            "'{}' is not inside an active StudioFS mount",
            target.display()
        )
    })?;
    let mount_point = runtime.mount_point.to_string_lossy().into_owned();
    let client = live_client_for_mount(&mount_point)?;
    let cache_label = runtime
        .cache_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "unpublished".to_string());

    eprintln!(
        "Monitoring StudioFS write path: mount={} volume={} pid={} cache={} interval={}ms",
        runtime.mount_point.display(),
        runtime.volume,
        runtime.pid,
        cache_label,
        interval.as_millis()
    );
    eprintln!("Start the external writer now; stop this command with Ctrl+C.");

    test_cache_runtime()?
        .block_on(async move { monitor_write_path_loop(client, interval, duration).await })
}

fn live_client_for_mount(mount_point: &str) -> Result<diagnostics::LocalDiagnosticsClient> {
    let mount_key = normalize_mount_key(mount_point);
    let discovery = diagnostics::discover_local_sessions()?
        .into_iter()
        .find(|session| normalize_mount_key(&session.mount_point) == mount_key)
        .ok_or_else(|| anyhow!("no live diagnostics session found for '{}'", mount_point))?;
    Ok(diagnostics::LocalDiagnosticsClient::new(discovery))
}

fn normalize_mount_key(mount_point: &str) -> String {
    let normalized = crate::cli::utils::normalize_mount_point(mount_point);
    #[cfg(windows)]
    {
        normalized
            .replace('/', "\\")
            .trim_end_matches('\\')
            .to_lowercase()
    }
    #[cfg(not(windows))]
    {
        normalized.trim_end_matches('/').to_string()
    }
}

async fn monitor_write_path_loop(
    client: diagnostics::LocalDiagnosticsClient,
    interval: Duration,
    duration: Option<Duration>,
) -> Result<()> {
    let started = Instant::now();
    let mut previous: Option<VfsWritePathObservabilitySnapshot> = None;
    let mut sample_count = 0u64;

    loop {
        let runtime = client
            .runtime_snapshot()
            .await
            .context("read live diagnostics runtime snapshot")?;
        let current = runtime.write_path;
        if sample_count.is_multiple_of(MONITOR_HEADER_EVERY) {
            println!("{}", monitor_header());
        }
        println!(
            "{}",
            monitor_row(started.elapsed(), previous.as_ref(), &current)
        );
        previous = Some(current);
        sample_count = sample_count.saturating_add(1);

        if duration.is_some_and(|limit| started.elapsed() >= limit) {
            break;
        }
        tokio::time::sleep(interval).await;
    }

    Ok(())
}

fn monitor_header() -> &'static str {
    "time    fg/s       persist/s  upload/s   writer   local    staged   pendUp   acct     upQ/a  infl hard soft bpWait sync/d syncMs qWait mat/ms   fl/cl ioMs gate grp cb       cbAge status"
}

fn monitor_row(
    elapsed: Duration,
    before: Option<&VfsWritePathObservabilitySnapshot>,
    after: &VfsWritePathObservabilitySnapshot,
) -> String {
    let fg_rate = bytes_per_sec_delta(before, after, |snapshot| {
        snapshot.winfsp_write_path.write_bytes
    });
    let persist_rate =
        bytes_per_sec_delta(before, after, |snapshot| snapshot.payload_burst.total_bytes);
    let upload_rate = bytes_per_sec_delta(before, after, |snapshot| {
        snapshot.upload.completed_upload_bytes
    });
    let hard_delta = delta_u64(before, after, |snapshot| {
        snapshot.write_backpressure.hard_events
    });
    let soft_delta = delta_u64(before, after, |snapshot| {
        snapshot.write_backpressure.soft_throttle_events
    });
    let wait_delta = delta_u64(before, after, |snapshot| {
        snapshot.write_backpressure.wait_total_ms
    });
    let sync_delta = delta_u64(before, after, |snapshot| {
        snapshot.payload_store_sync_data.calls
    });
    let sync_deferred_delta = delta_u64(before, after, |snapshot| {
        snapshot.payload_store_sync_data.deferred_calls
    });
    let sync_ms_delta = delta_f64(before, after, |snapshot| {
        snapshot.payload_store_sync_data.total_ms
    });
    let payload_persist_delta = delta_u64(before, after, |snapshot| snapshot.payload_persist.calls);
    let payload_persist_ms_delta =
        delta_f64(before, after, |snapshot| snapshot.payload_persist.total_ms);
    let flush_delta = delta_u64(before, after, |snapshot| {
        snapshot.winfsp_foreground_flushes.flush
    });
    let close_delta = delta_u64(before, after, |snapshot| {
        snapshot.winfsp_foreground_flushes.close
    });
    let local_cache_backend_ms_delta = delta_f64(before, after, |snapshot| {
        snapshot.local_cache_io.backend_io_total_ms
    });
    let no_write_progress = before.is_some()
        && fg_rate.unwrap_or_default() == 0.0
        && persist_rate.unwrap_or_default() == 0.0
        && upload_rate.unwrap_or_default() == 0.0;
    let has_pressure = after.dirty_pressure.total_accounted_bytes > 0
        || after.drain_lag.local_sync_unsynced_bytes > 0
        || after.dirty_pressure.deferred_materialization_inflight > 0;
    let has_old_active_callback = after.winfsp_callback_activity.total_active > 0
        && after.winfsp_callback_activity.oldest_active_ms >= 1_000;
    let status = if has_old_active_callback {
        "CB-WAIT"
    } else if no_write_progress && has_pressure {
        "NO-PROGRESS"
    } else if after.dirty_pressure.local_sync_gate_held {
        "SYNC-GATE"
    } else if after.dirty_pressure.deferred_materialization_inflight > 0 {
        "MATERIALIZE"
    } else if hard_delta > 0 {
        "HARD-BP"
    } else {
        "ok"
    };
    let group_pending = after
        .dirty_pressure
        .local_sync_group_pending_bytes
        .map(|bytes| format_bytes(bytes as u64))
        .unwrap_or_else(|| "locked".to_string());

    format!(
        "{:>6.1}s {:>10} {:>10} {:>9} {:>8} {:>8} {:>8} {:>8} {:>8} {:>3}/{:<2} {:>4} {:>4} {:>4} {:>6} {:>3}/{:<3} {:>6} {:>5} {:>3}/{:<4} {:>3}/{:<3} {:>5} {:>4} {:>7} {:>8} {:>5} {}",
        elapsed.as_secs_f64(),
        format_optional_throughput(fg_rate),
        format_optional_throughput(persist_rate),
        format_optional_throughput(upload_rate),
        format_bytes(after.dirty_pressure.writer_stack_dirty_bytes as u64),
        format_bytes(after.dirty_pressure.pending_local_dirty_bytes as u64),
        format_bytes(after.dirty_pressure.pending_persisted_slice_bytes as u64),
        format_bytes(after.dirty_pressure.pending_upload_bytes as u64),
        format_bytes(after.dirty_pressure.total_accounted_bytes as u64),
        after.upload.pending_uploads,
        after.upload.active_uploads,
        after.dirty_pressure.deferred_materialization_inflight,
        hard_delta,
        soft_delta,
        format_duration_ms(wait_delta as f64),
        sync_delta,
        sync_deferred_delta,
        format_duration_ms(sync_ms_delta),
        format_duration_ms(after.payload_store_sync_data.queue_wait_max_ms),
        payload_persist_delta,
        format_duration_ms(payload_persist_ms_delta),
        flush_delta,
        close_delta,
        format_duration_ms(local_cache_backend_ms_delta),
        flag(after.dirty_pressure.local_sync_gate_held),
        group_pending,
        format_callback_activity(&after.winfsp_callback_activity),
        format_duration_ms(after.winfsp_callback_activity.oldest_active_ms as f64),
        status,
    )
}

fn bytes_per_sec_delta(
    before: Option<&VfsWritePathObservabilitySnapshot>,
    after: &VfsWritePathObservabilitySnapshot,
    get: impl Fn(&VfsWritePathObservabilitySnapshot) -> u64,
) -> Option<f64> {
    let before = before?;
    let delta_ms = after
        .generated_unix_ms
        .saturating_sub(before.generated_unix_ms);
    if delta_ms == 0 {
        return Some(0.0);
    }
    Some(get(after).saturating_sub(get(before)) as f64 * 1000.0 / delta_ms as f64)
}

fn delta_u64(
    before: Option<&VfsWritePathObservabilitySnapshot>,
    after: &VfsWritePathObservabilitySnapshot,
    get: impl Fn(&VfsWritePathObservabilitySnapshot) -> u64,
) -> u64 {
    before
        .map(|before| get(after).saturating_sub(get(before)))
        .unwrap_or_default()
}

fn delta_f64(
    before: Option<&VfsWritePathObservabilitySnapshot>,
    after: &VfsWritePathObservabilitySnapshot,
    get: impl Fn(&VfsWritePathObservabilitySnapshot) -> f64,
) -> f64 {
    before
        .map(|before| (get(after) - get(before)).max(0.0))
        .unwrap_or_default()
}

fn format_optional_throughput(value: Option<f64>) -> String {
    value
        .map(format_throughput)
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_duration_ms(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.1}s", ms / 1000.0)
    } else {
        format!("{ms:.0}ms")
    }
}

fn flag(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn format_callback_activity(stats: &studiofs_vfs::VfsWinfspCallbackActivityStats) -> String {
    if stats.total_active == 0 {
        return "-".to_string();
    }
    let kind = if stats.oldest_active_kind.is_empty() {
        "?"
    } else {
        short_callback_kind(&stats.oldest_active_kind)
    };
    format!("{}x{kind}", stats.total_active)
}

fn short_callback_kind(kind: &str) -> &str {
    match kind {
        "get_security_by_name" => "secname",
        "get_security" => "sec",
        "get_file_info" => "info",
        "read_directory" => "readdir",
        "set_basic_info" => "setinfo",
        "set_file_size" => "setsize",
        "get_reparse_point_by_name" => "rparseN",
        "get_reparse_point" => "rparse",
        "set_reparse_point" => "setrp",
        "delete_reparse_point" => "delrp",
        "get_volume_info" => "volume",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot_at(ms: u64) -> VfsWritePathObservabilitySnapshot {
        VfsWritePathObservabilitySnapshot {
            generated_unix_ms: ms,
            ..VfsWritePathObservabilitySnapshot::default()
        }
    }

    #[test]
    fn monitor_row_reports_delta_rates_and_pressure_status() {
        let mut before = snapshot_at(1_000);
        before.winfsp_write_path.write_bytes = 512 * 1024 * 1024;
        before.payload_burst.total_bytes = 256 * 1024 * 1024;

        let mut after = snapshot_at(2_000);
        after.winfsp_write_path.write_bytes = 1024 * 1024 * 1024;
        after.payload_burst.total_bytes = 512 * 1024 * 1024;
        after.upload.completed_upload_bytes = 128 * 1024 * 1024;
        after.upload.pending_uploads = 3;
        after.upload.active_uploads = 2;
        after.dirty_pressure.writer_stack_dirty_bytes = 64 * 1024 * 1024;
        after.dirty_pressure.total_accounted_bytes = 512 * 1024 * 1024;
        after.dirty_pressure.deferred_materialization_inflight = 1;
        after.dirty_pressure.local_sync_gate_held = true;
        after.dirty_pressure.local_sync_group_pending_bytes = Some(32 * 1024 * 1024);

        let row = monitor_row(Duration::from_secs(1), Some(&before), &after);

        assert!(row.contains("512.00MiB/s"));
        assert!(row.contains("256.00MiB/s"));
        assert!(row.contains("128.00MiB/s"));
        assert!(row.contains("64.0MiB"));
        assert!(row.contains("MATERIALIZE") || row.contains("SYNC-GATE"));
        assert!(row.contains("yes"));
    }
}
