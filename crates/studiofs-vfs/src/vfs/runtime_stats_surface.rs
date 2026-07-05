use studiofs_meta::{Ino, InodeAttr, ROOT_INO};

use super::{
    RuntimeLatencyHistogram, RuntimeScalarMetric, RuntimeStatsSection, RuntimeStatsSnapshot,
    StudioVfs,
};
use crate::utils::now_unix_millis;

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
    /// Build the shared cross-platform runtime stats snapshot for mounted readers.
    pub fn runtime_stats_snapshot(&self) -> RuntimeStatsSnapshot {
        let generated_unix_ms = now_unix_millis();
        let ingress = self.ingress_stats();
        let upload = self.upload_stats();
        let write_path = self.write_path_observability_snapshot();
        let linux_bridge = self.state.linux_bridge_snapshot.lock().clone();
        let cache = self.cache_state_snapshot();

        let mut snapshot = RuntimeStatsSnapshot {
            generated_unix_ms,
            ..RuntimeStatsSnapshot::default()
        };

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
        insert_gauge(
            &mut snapshot.bridge,
            "fuse_max_write_bytes",
            "bytes",
            u64::from(write_path.fuse_async_write.fuse_max_write_bytes),
        );
        insert_gauge(
            &mut snapshot.bridge,
            "fuse_max_readahead_bytes",
            "bytes",
            u64::from(write_path.fuse_async_write.fuse_max_readahead_bytes),
        );
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
            (
                "fuse_kernel_writeback_cache",
                "count",
                write_path
                    .fuse_async_write
                    .kernel_writeback_cache
                    .map(u64::from),
            ),
        ] {
            if let Some(value) = value {
                insert_gauge(&mut snapshot.bridge, name, unit, value);
            }
        }
        if let Some(max_background) = write_path.fuse_async_write.fuse_max_background {
            insert_gauge(
                &mut snapshot.bridge,
                "fuse_max_background",
                "count",
                u64::from(max_background),
            );
        }
        if let Some(congestion_threshold) = write_path.fuse_async_write.fuse_congestion_threshold {
            insert_gauge(
                &mut snapshot.bridge,
                "fuse_congestion_threshold",
                "count",
                u64::from(congestion_threshold),
            );
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
        if linux_bridge.enabled {
            for (name, value) in [
                (
                    "linux_work_backpressure_events",
                    linux_bridge.work_backpressure_count,
                ),
                (
                    "linux_reply_backpressure_events",
                    linux_bridge.reply_backpressure_count,
                ),
                ("linux_receive_errors", linux_bridge.receive_error_count),
                ("linux_work_errors", linux_bridge.work_error_count),
                ("linux_reply_errors", linux_bridge.reply_error_count),
                ("linux_read_bytes", linux_bridge.read_bytes),
                ("linux_writev_bytes", linux_bridge.writev_bytes),
                ("linux_splice_bytes", linux_bridge.splice_bytes),
            ] {
                let unit = if name.ends_with("_bytes") {
                    "bytes"
                } else {
                    "count"
                };
                insert_counter(&mut snapshot.bridge, name, unit, value);
            }
            for (name, value) in [
                ("linux_fd_count", linux_bridge.fd_count),
                ("linux_work_queue_depth", linux_bridge.work_queue_depth),
                (
                    "linux_metadata_queue_depth",
                    linux_bridge.metadata_queue_depth,
                ),
                ("linux_stream_queue_depth", linux_bridge.stream_queue_depth),
                ("linux_reply_queue_depth", linux_bridge.reply_queue_depth),
            ] {
                insert_gauge(&mut snapshot.bridge, name, "count", value);
            }
            for (name, histogram) in [
                ("linux_receive", linux_bridge.receive),
                ("linux_dispatch_wait", linux_bridge.dispatch_wait),
                ("linux_vfs_time", linux_bridge.vfs_time),
                ("linux_reply_queue_wait", linux_bridge.reply_queue_wait),
                ("linux_reply_write", linux_bridge.reply_write),
                ("linux_total_callback", linux_bridge.total_callback),
            ] {
                snapshot.bridge.latencies.insert(name.to_owned(), histogram);
            }
            for op in linux_bridge.operations {
                let name = op.name.replace('-', "_");
                insert_counter(
                    &mut snapshot.bridge,
                    &format!("linux_op_{name}_callbacks"),
                    "count",
                    op.count,
                );
                insert_counter(
                    &mut snapshot.bridge,
                    &format!("linux_op_{name}_errors"),
                    "count",
                    op.errors,
                );
                insert_latency(
                    &mut snapshot.bridge,
                    &format!("linux_op_{name}"),
                    "ns",
                    op.count,
                    op.latency_sum_ns,
                );
            }
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
