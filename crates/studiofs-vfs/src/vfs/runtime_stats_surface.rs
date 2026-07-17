use studiofs_meta::{Ino, InodeAttr, ROOT_INO};

use super::{
    MEMORY_PRESSURE_SOURCE_CGROUP, MEMORY_PRESSURE_SOURCE_DISABLED,
    MEMORY_PRESSURE_SOURCE_FALLBACK, MEMORY_PRESSURE_SOURCE_HOST,
    MEMORY_PRESSURE_SOURCE_HOST_AND_CGROUP, MEMORY_PRESSURE_STATE_CONSTRAINED,
    MEMORY_PRESSURE_STATE_DISABLED, MEMORY_PRESSURE_STATE_NORMAL, RuntimeLatencyBucket,
    RuntimeLatencyHistogram, RuntimeScalarMetric, RuntimeStatsSection, RuntimeStatsSnapshot,
    StudioVfs, VfsFuseAsyncWriteSnapshot, VfsFuseSizeHistogram, VfsWritePathObservabilitySnapshot,
    VfsWritePathStageLatencyStats,
};
use crate::utils::now_unix_millis;
use crate::{MemoryPressureSnapshot, MemoryPressureSource, MemoryPressureState};

/// Root-local hidden file that exposes the cross-platform runtime stats text surface.
pub const RUNTIME_STATS_FILE_NAME_STR: &str = ".studiofs-runtime-stats";
/// Byte representation of [`RUNTIME_STATS_FILE_NAME_STR`] used by VFS and bridge callbacks.
pub const RUNTIME_STATS_FILE_NAME: &[u8] = RUNTIME_STATS_FILE_NAME_STR.as_bytes();
/// Synthetic inode reserved for the runtime stats surface.
pub const RUNTIME_STATS_FILE_INO: Ino = Ino::MAX - 1;
/// Stable reported size for the synthetic stats file.
///
/// The actual content is generated on open by each bridge and reads naturally
/// hit EOF for the captured buffer. Keeping getattr cheap avoids collecting
/// and serializing full stats from high-frequency attribute probes.
pub const RUNTIME_STATS_FILE_REPORTED_SIZE: u64 = 64 * 1024;

impl StudioVfs {
    /// Return true when `parent/name` addresses the runtime stats virtual file.
    pub fn is_runtime_stats_file_name(&self, parent: Ino, name: &[u8]) -> bool {
        parent == ROOT_INO && name == RUNTIME_STATS_FILE_NAME
    }

    /// Return true when `ino` is the runtime stats virtual file.
    pub fn is_runtime_stats_file_ino(&self, ino: Ino) -> bool {
        ino == RUNTIME_STATS_FILE_INO
    }

    /// Return synthetic attributes for the runtime stats virtual file.
    pub fn runtime_stats_file_attr(&self) -> InodeAttr {
        let mut attr = InodeAttr::new_file(RUNTIME_STATS_FILE_INO, ROOT_INO, 0, 0, 0o444);
        attr.atime = self.runtime_stats_file_time_nanos;
        attr.mtime = self.runtime_stats_file_time_nanos;
        attr.ctime = self.runtime_stats_file_time_nanos;
        attr.crtime = self.runtime_stats_file_time_nanos;
        attr.size = RUNTIME_STATS_FILE_REPORTED_SIZE;
        attr.committed_size = attr.size;
        attr.win_attrs = 0x02 | 0x20; // HIDDEN | ARCHIVE
        attr
    }

    /// Render the live runtime stats virtual file contents.
    pub fn runtime_stats_file_text(&self) -> String {
        self.runtime_stats_snapshot().to_text()
    }

    /// Read a slice from the live runtime stats virtual file.
    pub fn read_runtime_stats_file(&self, offset: u64, out: &mut [u8]) -> u32 {
        let text = self.runtime_stats_file_text();
        read_runtime_stats_buffer(text.as_bytes(), offset, out)
    }
}

/// Read a slice from a captured runtime stats buffer.
pub fn read_runtime_stats_buffer(bytes: &[u8], offset: u64, out: &mut [u8]) -> u32 {
    let offset = match usize::try_from(offset) {
        Ok(offset) => offset.min(bytes.len()),
        Err(_) => bytes.len(),
    };
    let readable = bytes
        .len()
        .saturating_sub(offset)
        .min(out.len())
        .min(u32::MAX as usize);
    out[..readable].copy_from_slice(&bytes[offset..offset + readable]);
    readable as u32
}

impl StudioVfs {
    /// Build memory-pressure runtime stats from the current controller and one write-path snapshot.
    pub fn memory_pressure_runtime_stats(
        &self,
        write_path: &VfsWritePathObservabilitySnapshot,
    ) -> RuntimeStatsSection {
        build_memory_pressure_runtime_stats(
            self.memory_pressure_controller().snapshot(),
            &write_path.fuse_async_write,
            self.admitted_managed_bytes_with_async_write(
                write_path.fuse_async_write.pending_bytes as u64,
            ),
        )
    }

    /// Build the shared cross-platform runtime stats snapshot for mounted readers.
    pub fn runtime_stats_snapshot(&self) -> RuntimeStatsSnapshot {
        let generated_unix_ms = now_unix_millis();
        let ingress = self.ingress_stats();
        let upload = self.upload_stats();
        let write_path = self.write_path_observability_snapshot();
        let cache = self.cache_state_snapshot();

        let mut snapshot = RuntimeStatsSnapshot {
            generated_unix_ms,
            ..RuntimeStatsSnapshot::default()
        };
        snapshot.memory_pressure = self.memory_pressure_runtime_stats(&write_path);

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
            write_path
                .winfsp_callbacks
                .flush
                .saturating_add(write_path.fuse_callbacks.flush.calls),
        );
        insert_counter(
            &mut snapshot.bridge,
            "fsync_callbacks",
            "count",
            write_path.fuse_callbacks.fsync.calls,
        );
        insert_counter(
            &mut snapshot.bridge,
            "release_callbacks",
            "count",
            write_path.fuse_callbacks.release.calls,
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
        insert_gauge(
            &mut snapshot.bridge,
            "os_cache.active_writers",
            "count",
            write_path.os_cache.active_writers,
        );
        insert_gauge(
            &mut snapshot.bridge,
            "os_cache.active_foreground_handles",
            "count",
            write_path.os_cache.active_foreground_handles,
        );
        for (name, value) in [
            (
                "fuse_liveness.snapshot_generation",
                write_path.fuse_liveness.snapshot_generation,
            ),
            (
                "fuse_liveness.observed_references",
                write_path.fuse_liveness.observed_references,
            ),
            (
                "fuse_liveness.forgotten_references",
                write_path.fuse_liveness.forgotten_references,
            ),
            (
                "fuse_liveness.retired_inodes",
                write_path.fuse_liveness.retired_inodes,
            ),
            (
                "fuse_liveness.open_unknown_inode",
                write_path.fuse_liveness.open_unknown_inode,
            ),
            (
                "fuse_liveness.notifications_forwarded",
                write_path.fuse_liveness.notifications_forwarded,
            ),
            (
                "fuse_liveness.notifications_suppressed",
                write_path.fuse_liveness.notifications_suppressed,
            ),
        ] {
            insert_counter(&mut snapshot.bridge, name, "count", value);
        }
        insert_counter(
            &mut snapshot.bridge,
            "callback_errors",
            "count",
            write_path
                .fuse_callbacks
                .read
                .errors
                .saturating_add(write_path.fuse_callbacks.write.errors)
                .saturating_add(write_path.fuse_callbacks.flush.errors)
                .saturating_add(write_path.fuse_callbacks.fsync.errors)
                .saturating_add(write_path.fuse_callbacks.release.errors),
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
        insert_size_histogram_counters(
            &mut snapshot.bridge,
            "read_callback_size",
            &write_path.fuse_callbacks.read.size_histogram,
        );
        insert_size_histogram_counters(
            &mut snapshot.bridge,
            "write_callback_size",
            &write_path.fuse_callbacks.write.size_histogram,
        );
        for (name, max_ms) in [
            (
                "read_callback_max_ns",
                write_path.fuse_callbacks.read.max_ms,
            ),
            (
                "write_callback_max_ns",
                write_path.fuse_callbacks.write.max_ms,
            ),
            (
                "flush_callback_max_ns",
                write_path.fuse_callbacks.flush.max_ms,
            ),
            (
                "fsync_callback_max_ns",
                write_path.fuse_callbacks.fsync.max_ms,
            ),
            (
                "release_callback_max_ns",
                write_path.fuse_callbacks.release.max_ms,
            ),
        ] {
            insert_gauge(&mut snapshot.bridge, name, "ns", ms_sum_to_ns(max_ms));
        }
        for (name, unit, value) in [
            (
                "fuse_negotiated_max_read_bytes",
                "bytes",
                write_path
                    .fuse_connection
                    .negotiated_max_read_bytes
                    .map(u64::from),
            ),
            (
                "fuse_negotiated_max_write_bytes",
                "bytes",
                write_path
                    .fuse_connection
                    .negotiated_max_write_bytes
                    .map(u64::from),
            ),
            (
                "fuse_negotiated_max_readahead_bytes",
                "bytes",
                write_path
                    .fuse_connection
                    .negotiated_max_readahead_bytes
                    .map(u64::from),
            ),
            (
                "fuse_negotiated_max_background",
                "count",
                write_path
                    .fuse_connection
                    .negotiated_max_background
                    .map(u64::from),
            ),
            (
                "fuse_negotiated_congestion_threshold",
                "count",
                write_path
                    .fuse_connection
                    .negotiated_congestion_threshold
                    .map(u64::from),
            ),
            (
                "fuse_negotiated_async_dio",
                "count",
                write_path
                    .fuse_connection
                    .negotiated_async_dio
                    .map(u64::from),
            ),
            (
                "fuse_negotiated_writeback_cache",
                "count",
                write_path
                    .fuse_connection
                    .negotiated_writeback_cache
                    .map(u64::from),
            ),
            (
                "fuse_parallel_direct_writes_supported",
                "count",
                write_path
                    .fuse_connection
                    .parallel_direct_writes_supported
                    .map(u64::from),
            ),
            (
                "fuse_single_thread_receive_loop",
                "count",
                write_path
                    .fuse_connection
                    .single_thread_receive_loop
                    .map(u64::from),
            ),
        ] {
            if let Some(value) = value {
                insert_gauge(&mut snapshot.bridge, name, unit, value);
            }
        }
        for (name, unit, value) in [
            (
                "fuse_shared_buffer_budget_bytes",
                "bytes",
                write_path.fuse_async_write.shared_buffer_budget_bytes,
            ),
            (
                "fuse_read_buffer_budget_bytes",
                "bytes",
                write_path.fuse_async_write.read_buffer_budget_bytes,
            ),
            (
                "fuse_write_buffer_budget_bytes",
                "bytes",
                write_path.fuse_async_write.write_buffer_budget_bytes,
            ),
            (
                "fuse_prefetch_concurrency",
                "count",
                write_path
                    .fuse_async_write
                    .prefetch_concurrency
                    .map(u64::from),
            ),
            (
                "fuse_upload_concurrency",
                "count",
                write_path
                    .fuse_async_write
                    .upload_concurrency
                    .map(u64::from),
            ),
            (
                "fuse_download_concurrency",
                "count",
                write_path
                    .fuse_async_write
                    .download_concurrency
                    .map(u64::from),
            ),
        ] {
            if let Some(value) = value {
                insert_gauge(&mut snapshot.bridge, name, unit, value);
            }
        }
        for (name, unit, value) in [
            (
                "fuse_async_write.active_requests",
                "count",
                write_path.fuse_async_write.active_requests as u64,
            ),
            (
                "fuse_async_write.admission_waiters",
                "count",
                write_path.fuse_async_write.admission_waiters as u64,
            ),
            (
                "fuse_async_write.pending_handles",
                "count",
                write_path.fuse_async_write.pending_handles as u64,
            ),
            (
                "fuse_async_write.pending_writes",
                "count",
                write_path.fuse_async_write.pending_writes as u64,
            ),
            (
                "fuse_async_write.pending_bytes",
                "bytes",
                write_path.fuse_async_write.pending_bytes as u64,
            ),
        ] {
            insert_gauge(&mut snapshot.bridge, name, unit, value);
        }
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
        for (name, latency) in [
            ("read_callback", &write_path.fuse_callbacks.read.latency),
            ("write_callback", &write_path.fuse_callbacks.write.latency),
            ("flush_callback", &write_path.fuse_callbacks.flush.latency),
            ("fsync_callback", &write_path.fuse_callbacks.fsync.latency),
            (
                "release_callback",
                &write_path.fuse_callbacks.release.latency,
            ),
        ] {
            insert_latency_with_buckets(&mut snapshot.bridge, name, latency);
        }

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
            // `FetchMetrics` currently tracks local cache I/O calls but not total
            // local-cache bytes, so this mirrors hit bytes until that counter exists.
            write_path.fetch.cache_hit_bytes,
        );
        insert_counter(
            &mut snapshot.cache,
            "prefetch_skips",
            "count",
            write_path.fetch.prefetch_skips,
        );
        insert_counter(
            &mut snapshot.cache,
            "readahead_scheduled_ops",
            "count",
            write_path.fetch.readahead_scheduled_blocks,
        );
        insert_counter(
            &mut snapshot.cache,
            "readahead_dropped_ops",
            "count",
            write_path.fetch.readahead_dropped_blocks,
        );
        insert_counter(
            &mut snapshot.cache,
            "readahead_useful_bytes",
            "bytes",
            write_path.fetch.readahead_used_bytes,
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
}

fn memory_pressure_source_code(source: MemoryPressureSource) -> u64 {
    match source {
        MemoryPressureSource::Disabled => MEMORY_PRESSURE_SOURCE_DISABLED,
        MemoryPressureSource::Host => MEMORY_PRESSURE_SOURCE_HOST,
        MemoryPressureSource::Cgroup => MEMORY_PRESSURE_SOURCE_CGROUP,
        MemoryPressureSource::HostAndCgroup => MEMORY_PRESSURE_SOURCE_HOST_AND_CGROUP,
        MemoryPressureSource::Fallback => MEMORY_PRESSURE_SOURCE_FALLBACK,
    }
}

fn memory_pressure_state_code(state: MemoryPressureState) -> u64 {
    match state {
        MemoryPressureState::Disabled => MEMORY_PRESSURE_STATE_DISABLED,
        MemoryPressureState::Normal => MEMORY_PRESSURE_STATE_NORMAL,
        MemoryPressureState::Constrained => MEMORY_PRESSURE_STATE_CONSTRAINED,
    }
}

fn build_memory_pressure_runtime_stats(
    pressure: MemoryPressureSnapshot,
    async_write: &VfsFuseAsyncWriteSnapshot,
    admitted_managed_bytes: u64,
) -> RuntimeStatsSection {
    let mut section = RuntimeStatsSection::default();
    for (name, unit, value) in [
        (
            "source",
            "count",
            memory_pressure_source_code(pressure.source),
        ),
        ("state", "count", memory_pressure_state_code(pressure.state)),
        ("generation", "count", pressure.generation),
        (
            "effective_limit_bytes",
            "bytes",
            pressure.effective_limit_bytes,
        ),
        ("available_bytes", "bytes", pressure.available_bytes),
        ("reserve_bytes", "bytes", pressure.reserve_bytes),
        ("envelope_bytes", "bytes", pressure.envelope_bytes),
        ("admitted_managed_bytes", "bytes", admitted_managed_bytes),
        (
            "memory_cache_limit_bytes",
            "bytes",
            pressure.limits.memory_cache_bytes,
        ),
        ("dirty_limit_bytes", "bytes", pressure.limits.dirty_bytes),
        (
            "transit_limit_bytes",
            "bytes",
            pressure.limits.transit_bytes,
        ),
        (
            "async_write_limit_bytes",
            "bytes",
            pressure.limits.async_write_bytes,
        ),
        (
            "overcommitted_minimum",
            "count",
            u64::from(pressure.overcommitted_minimum),
        ),
    ] {
        insert_gauge(&mut section, name, unit, value);
    }
    for (name, unit, value) in [
        ("transition_count", "count", pressure.transition_count),
        (
            "sampling_failure_count",
            "count",
            pressure.sampling_failure_count,
        ),
        (
            "write_admission_wait_count",
            "count",
            async_write.admission_waits,
        ),
        (
            "write_admission_wait_ns",
            "ns",
            ms_sum_to_ns(async_write.admission_wait_total_ms),
        ),
    ] {
        insert_counter(&mut section, name, unit, value);
    }
    section
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

fn insert_latency_with_buckets(
    section: &mut RuntimeStatsSection,
    name: &str,
    latency: &VfsWritePathStageLatencyStats,
) {
    let histogram = &latency.latency_histogram;
    let count = histogram.bucket_counts.iter().sum();
    let mut buckets = histogram
        .bucket_upper_bounds_ns
        .iter()
        .copied()
        .zip(histogram.bucket_counts.iter().copied())
        .map(|(le_ns, count)| RuntimeLatencyBucket { le_ns, count })
        .collect::<Vec<_>>();
    buckets.push(RuntimeLatencyBucket {
        le_ns: u64::MAX,
        count: histogram
            .bucket_counts
            .get(histogram.bucket_upper_bounds_ns.len())
            .copied()
            .unwrap_or(0),
    });
    section.latencies.insert(
        name.to_owned(),
        RuntimeLatencyHistogram {
            count,
            sum: ms_sum_to_ns(latency.total_ms),
            unit: "ns".into(),
            buckets,
        },
    );
}

fn insert_size_histogram_counters(
    section: &mut RuntimeStatsSection,
    prefix: &str,
    histogram: &VfsFuseSizeHistogram,
) {
    for (upper, count) in histogram
        .bucket_upper_bounds_bytes
        .iter()
        .zip(&histogram.bucket_counts)
    {
        insert_counter(section, &format!("{prefix}_le_{upper}"), "count", *count);
    }
    insert_counter(
        section,
        &format!("{prefix}_overflow"),
        "count",
        histogram
            .bucket_counts
            .get(histogram.bucket_upper_bounds_bytes.len())
            .copied()
            .unwrap_or(0),
    );
}

fn ms_sum_to_ns(value: f64) -> u64 {
    (value.max(0.0) * 1_000_000.0).round() as u64
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        MemoryComponentLimits, MemoryPressureSnapshot, MemoryPressureSource, MemoryPressureState,
        VfsFuseAsyncWriteSnapshot,
    };

    fn metric(value: u64, unit: &'static str) -> RuntimeScalarMetric {
        RuntimeScalarMetric {
            value,
            unit: Cow::Borrowed(unit),
        }
    }

    #[test]
    fn memory_pressure_runtime_stats_codes_are_stable() {
        let public_sources = [
            crate::MEMORY_PRESSURE_SOURCE_DISABLED,
            crate::MEMORY_PRESSURE_SOURCE_HOST,
            crate::MEMORY_PRESSURE_SOURCE_CGROUP,
            crate::MEMORY_PRESSURE_SOURCE_HOST_AND_CGROUP,
            crate::MEMORY_PRESSURE_SOURCE_FALLBACK,
        ];
        let public_states = [
            crate::MEMORY_PRESSURE_STATE_DISABLED,
            crate::MEMORY_PRESSURE_STATE_NORMAL,
            crate::MEMORY_PRESSURE_STATE_CONSTRAINED,
        ];
        let sources = [
            memory_pressure_source_code(MemoryPressureSource::Disabled),
            memory_pressure_source_code(MemoryPressureSource::Host),
            memory_pressure_source_code(MemoryPressureSource::Cgroup),
            memory_pressure_source_code(MemoryPressureSource::HostAndCgroup),
            memory_pressure_source_code(MemoryPressureSource::Fallback),
        ];
        let states = [
            memory_pressure_state_code(MemoryPressureState::Disabled),
            memory_pressure_state_code(MemoryPressureState::Normal),
            memory_pressure_state_code(MemoryPressureState::Constrained),
        ];

        assert_eq!(public_sources, [0, 1, 2, 3, 4]);
        assert_eq!(public_states, [0, 1, 2]);
        assert_eq!(sources, public_sources);
        assert_eq!(states, public_states);
    }

    #[test]
    fn memory_pressure_runtime_stats_populates_snapshot_and_async_waits() {
        let pressure = MemoryPressureSnapshot {
            source: MemoryPressureSource::HostAndCgroup,
            state: MemoryPressureState::Constrained,
            generation: 7,
            effective_limit_bytes: 8_000,
            available_bytes: 3_000,
            reserve_bytes: 1_000,
            envelope_bytes: 1_500,
            limits: MemoryComponentLimits {
                memory_cache_bytes: 600,
                dirty_bytes: 375,
                transit_bytes: 225,
                async_write_bytes: 150,
            },
            transition_count: 2,
            sampling_failure_count: 3,
            overcommitted_minimum: true,
        };
        let async_write = VfsFuseAsyncWriteSnapshot {
            admission_waits: 13,
            admission_wait_total_ms: 2.25,
            ..VfsFuseAsyncWriteSnapshot::default()
        };

        let section = build_memory_pressure_runtime_stats(pressure, &async_write, 750);

        assert_eq!(
            section.gauges,
            BTreeMap::from([
                ("admitted_managed_bytes".to_owned(), metric(750, "bytes")),
                ("available_bytes".to_owned(), metric(3_000, "bytes")),
                ("async_write_limit_bytes".to_owned(), metric(150, "bytes")),
                ("dirty_limit_bytes".to_owned(), metric(375, "bytes")),
                ("effective_limit_bytes".to_owned(), metric(8_000, "bytes")),
                ("envelope_bytes".to_owned(), metric(1_500, "bytes")),
                ("generation".to_owned(), metric(7, "count")),
                ("memory_cache_limit_bytes".to_owned(), metric(600, "bytes")),
                ("overcommitted_minimum".to_owned(), metric(1, "count")),
                ("reserve_bytes".to_owned(), metric(1_000, "bytes")),
                ("source".to_owned(), metric(3, "count")),
                ("state".to_owned(), metric(2, "count")),
                ("transit_limit_bytes".to_owned(), metric(225, "bytes")),
            ])
        );
        assert_eq!(
            section.counters,
            BTreeMap::from([
                ("sampling_failure_count".to_owned(), metric(3, "count")),
                ("transition_count".to_owned(), metric(2, "count")),
                ("write_admission_wait_count".to_owned(), metric(13, "count"),),
                (
                    "write_admission_wait_ns".to_owned(),
                    metric(2_250_000, "ns"),
                ),
            ])
        );
        assert!(section.latencies.is_empty());
    }

    #[test]
    fn disabled_memory_pressure_runtime_stats_keeps_configured_limits() {
        let limits = MemoryComponentLimits {
            memory_cache_bytes: 64,
            dirty_bytes: 32,
            transit_bytes: 16,
            async_write_bytes: 8,
        };

        let section = build_memory_pressure_runtime_stats(
            MemoryPressureSnapshot::disabled(limits),
            &VfsFuseAsyncWriteSnapshot::default(),
            0,
        );

        assert_eq!(section.gauges["source"].value, 0);
        assert_eq!(section.gauges["state"].value, 0);
        assert_eq!(section.gauges["memory_cache_limit_bytes"].value, 64);
        assert_eq!(section.gauges["dirty_limit_bytes"].value, 32);
        assert_eq!(section.gauges["transit_limit_bytes"].value, 16);
        assert_eq!(section.gauges["async_write_limit_bytes"].value, 8);
    }
}
