use studiofs_meta::{Ino, InodeAttr, ROOT_INO};

use super::{RuntimeScalarMetric, RuntimeStatsSection, RuntimeStatsSnapshot, StudioVfs};
use crate::utils::now_unix_millis;

/// Root-local hidden file that exposes the cross-platform runtime stats text surface.
pub const RUNTIME_STATS_FILE_NAME: &[u8] = b".studiofs-runtime-stats";
/// Synthetic inode reserved for the runtime stats surface.
pub const RUNTIME_STATS_FILE_INO: Ino = Ino::MAX - 1;

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
        let text_len = self.runtime_stats_file_text().len();
        attr.size = u64::try_from(text_len).unwrap_or(u64::MAX);
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
        let bytes = text.as_bytes();
        let offset = match usize::try_from(offset) {
            Ok(offset) => offset.min(bytes.len()),
            Err(_) => bytes.len(),
        };
        let readable = bytes.len().saturating_sub(offset).min(out.len());
        out[..readable].copy_from_slice(&bytes[offset..offset + readable]);
        u32::try_from(readable).unwrap_or(u32::MAX)
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

        insert_gauge(
            &mut snapshot.system,
            "uptime_ms",
            "ms",
            write_path.uptime_secs as u64 * 1000,
        );
        insert_counter(
            &mut snapshot.bridge,
            "read_callbacks",
            "count",
            write_path.fuse_callbacks.read.calls,
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
            write_path.fuse_callbacks.read.bytes,
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
