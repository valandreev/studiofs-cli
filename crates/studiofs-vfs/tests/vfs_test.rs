//! Integration tests for the StudioFS VFS layer.
//!
//! Tests the slice-native VFS implementation with:
//! - Sequential object IDs
//! - Immutable payload objects
//! - LZ4 compression
//! - Crash recovery via journaling
//!
//! Prerequisites:
//!   rustfs server
//!
//! Run with:
//!   cargo test -p studiofs-vfs --test vfs_test -- --ignored

use bytes::Bytes;
use parking_lot::{Mutex, ReentrantMutex, ReentrantMutexGuard};
use std::sync::Arc;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use studiofs_cache::runtime::LocalBlockLayout;
use studiofs_cache::{PersistentLocalBlockBackend, open_local_block_backend};
use studiofs_data::object::{
    CLOUD_FRAGMENT_COUNT, CLOUD_FRAGMENT_SIZE, create_object_buffer, format_object_fragment_key,
    format_object_fragment_keys, format_object_key, read_object,
};
use studiofs_data::store::{DataStore, DataStoreConfig};
use studiofs_meta::engine::{MetaFsOps, PendingCreateRecord, UndoLogKind};
use studiofs_meta::types::SetattrArgs;
use studiofs_meta::types::TimeSpec;
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaBlocks, MetaEngine, MetaError, MetaFormat, MetaGc,
    MetaInodes, MetaReplication, MetadataBackend, MetadataConfig, PgMetaEngine, PgOpenOptions,
    PostgresConfig, ROOT_INO, RedbMetaEngine, ensure_studiofs_database,
    schema::{BlockRefState, InodeLifecycleState},
    schema::{ChunkId, SliceId, SliceOrder, SliceRange, SliceRecord},
    types::{AccessLevel, BlockFlags, BlockRef, Ino, UploadState},
};
use studiofs_vfs::{
    KernelInvalidator, LockKind, OpenFlags, StudioVfs, VfsConfigRaw, VfsConfigValidated, VfsError,
    VfsForegroundReadProtectionStats, VfsIngressLane, VfsUploadState, parse_size_bytes,
};
use uuid::Uuid;

// PR-01 Gate-Named Scenarios (reserved)
//
// - writer_reads_full_file_before_any_remote_commit
// - writer_recovers_local_file_after_restart_before_upload_complete
// - remote_clients_see_uploading_marker_without_precise_progress
// - remote_open_on_uploading_new_file_returns_transient_unavailable
// - remote_clients_keep_previous_committed_version_during_update
// - large_file_unlink_cancels_pending_uploads
// - large_file_partial_overwrite_reuploads_only_touched_extents
// - stale_upload_completion_is_ignored_after_extent_invalidate
// - large_file_rename_does_not_restart_or_duplicate_uploads

// =============================================================================
// Test Helpers
// =============================================================================

trait FileGraphMetaFixture {
    fn stage_logical_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> Result<Option<BlockRef>, MetaError>;
    fn lookup_logical_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
    ) -> Result<Option<BlockRef>, MetaError>;
    fn lookup_logical_payload_slices(&self, ino: Ino) -> Result<Vec<(u32, BlockRef)>, MetaError>;
    fn publish_uploaded_slice_object(
        &self,
        ino: Ino,
        block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> Result<bool, MetaError>;
}

fn fixture_logical_payload_slices(
    block_size: u32,
    block_index: u32,
    block: Option<&BlockRef>,
) -> Result<Vec<(ChunkId, Vec<SliceRecord>)>, MetaError> {
    let block_start = u64::from(block_index)
        .checked_mul(u64::from(block_size))
        .ok_or_else(|| MetaError::Storage("fixture block offset overflow".to_string()))?;
    let data_end = block_start
        .checked_add(u64::from(block.map_or(0, |block| block.length)))
        .ok_or_else(|| MetaError::Storage("fixture block data overflow".to_string()))?;
    let block_end = block_start
        .checked_add(u64::from(block_size))
        .ok_or_else(|| MetaError::Storage("fixture block range overflow".to_string()))?;
    let mut chunks = std::collections::BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
    let mut push = |start: u64, end: u64, source: Option<&BlockRef>| -> Result<(), MetaError> {
        let mut cursor = start;
        while cursor < end {
            let chunk_id =
                studiofs_meta::chunk_id_from_offset(cursor, studiofs_meta::DEFAULT_CHUNK_SIZE);
            let chunk_start =
                studiofs_meta::chunk_start_offset(chunk_id, studiofs_meta::DEFAULT_CHUNK_SIZE);
            let piece_end =
                end.min(chunk_start.saturating_add(studiofs_meta::DEFAULT_CHUNK_SIZE as u64));
            let range = SliceRange::new(
                (cursor - chunk_start) as u32,
                (piece_end - chunk_start) as u32,
            )
            .ok_or_else(|| MetaError::Storage("fixture invalid slice range".to_string()))?;
            let mut slice = if let Some(source) = source {
                let delta = u32::try_from(cursor.saturating_sub(block_start))
                    .map_err(|_| MetaError::Storage("fixture slice delta overflow".to_string()))?;
                SliceRecord::new(
                    SliceId(0),
                    SliceOrder(0),
                    range,
                    BlockRef {
                        object_id: source.object_id,
                        offset: source.offset.saturating_add(delta),
                        length: u32::try_from(piece_end.saturating_sub(cursor)).map_err(|_| {
                            MetaError::Storage("fixture slice length overflow".to_string())
                        })?,
                        flags: source.flags,
                    },
                )
            } else {
                SliceRecord::tombstone(SliceId(0), SliceOrder(0), range)
            };
            slice.block_state = BlockRefState::LocalOnly;
            chunks.entry(chunk_id).or_default().push(slice);
            cursor = piece_end;
        }
        Ok(())
    };
    push(block_start, data_end, block)?;
    if data_end < block_end {
        push(data_end, block_end, None)?;
    }
    Ok(chunks.into_iter().collect())
}

fn fixture_logical_payload_summaries_from_spans(
    spans: Vec<studiofs_meta::ResolvedSliceSpan>,
    block_size: u32,
) -> Result<Vec<(u32, BlockRef)>, MetaError> {
    let mut blocks = std::collections::BTreeMap::<u32, BlockRef>::new();
    for span in spans {
        let Some(block) = span.block else {
            continue;
        };
        let chunk_start =
            studiofs_meta::chunk_start_offset(span.chunk_id, studiofs_meta::DEFAULT_CHUNK_SIZE);
        let span_start = chunk_start.saturating_add(u64::from(span.range.start));
        let span_end = chunk_start.saturating_add(u64::from(span.range.end));
        let mut cursor = span_start;
        while cursor < span_end {
            let block_index = u32::try_from(cursor / u64::from(block_size))
                .map_err(|_| MetaError::Storage("fixture block index overflow".to_string()))?;
            let logical_block_end =
                (u64::from(block_index) + 1).saturating_mul(u64::from(block_size));
            let piece_end = span_end.min(logical_block_end);
            let piece_len = u32::try_from(piece_end.saturating_sub(cursor))
                .map_err(|_| MetaError::Storage("fixture block length overflow".to_string()))?;
            let delta = u32::try_from(cursor.saturating_sub(span_start))
                .map_err(|_| MetaError::Storage("fixture block offset overflow".to_string()))?;
            blocks
                .entry(block_index)
                .and_modify(|existing| {
                    existing.length = existing.length.saturating_add(piece_len);
                    existing.flags |= block.flags;
                })
                .or_insert_with(|| BlockRef {
                    object_id: block.object_id,
                    offset: block.offset.saturating_add(delta),
                    length: piece_len,
                    flags: block.flags,
                });
            cursor = piece_end;
        }
    }
    Ok(blocks.into_iter().collect())
}

impl<T> FileGraphMetaFixture for Arc<T>
where
    T: ?Sized + MetaBlocks + MetaFormat + MetaInodes,
{
    fn stage_logical_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> Result<Option<BlockRef>, MetaError> {
        let old = self
            .lookup_logical_payload_slice(ino, block_index)
            .ok()
            .flatten();
        let block_size = self.get_format_info()?.block_size;
        self.flush_regular_file_overlay_slices(
            ino,
            fixture_logical_payload_slices(block_size, block_index, Some(block))?.as_slice(),
        )?;
        Ok(old)
    }

    fn lookup_logical_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
    ) -> Result<Option<BlockRef>, MetaError> {
        let block_size = self.get_format_info()?.block_size;
        Ok(fixture_logical_payload_summaries_from_spans(
            self.lookup_regular_file_slices(
                ino,
                u64::from(block_index) * u64::from(block_size),
                u64::from(block_size),
            )?,
            block_size,
        )?
        .into_iter()
        .next()
        .map(|(_, block)| block))
    }

    fn lookup_logical_payload_slices(&self, ino: Ino) -> Result<Vec<(u32, BlockRef)>, MetaError> {
        let attr = self.get_attr(ino)?;
        let block_size = self.get_format_info()?.block_size;
        fixture_logical_payload_summaries_from_spans(
            self.lookup_regular_file_slices(ino, 0, attr.size)?,
            block_size,
        )
    }

    fn publish_uploaded_slice_object(
        &self,
        ino: Ino,
        _block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> Result<bool, MetaError> {
        self.mark_regular_file_overlay_object_uploaded_if_matches(
            ino,
            expected_epoch,
            block.object_id,
        )?;
        self.publish_regular_file_overlay_to_base_if_matches(ino, expected_epoch)
    }
}

impl FileGraphMetaFixture for RedbMetaEngine {
    fn stage_logical_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> Result<Option<BlockRef>, MetaError> {
        let old = self
            .lookup_logical_payload_slice(ino, block_index)
            .ok()
            .flatten();
        let block_size = self.get_format_info()?.block_size;
        self.flush_regular_file_overlay_slices(
            ino,
            fixture_logical_payload_slices(block_size, block_index, Some(block))?.as_slice(),
        )?;
        Ok(old)
    }

    fn lookup_logical_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
    ) -> Result<Option<BlockRef>, MetaError> {
        let block_size = self.get_format_info()?.block_size;
        Ok(fixture_logical_payload_summaries_from_spans(
            self.lookup_regular_file_slices(
                ino,
                u64::from(block_index) * u64::from(block_size),
                u64::from(block_size),
            )?,
            block_size,
        )?
        .into_iter()
        .next()
        .map(|(_, block)| block))
    }

    fn lookup_logical_payload_slices(&self, ino: Ino) -> Result<Vec<(u32, BlockRef)>, MetaError> {
        let attr = self.get_attr(ino)?;
        let block_size = self.get_format_info()?.block_size;
        fixture_logical_payload_summaries_from_spans(
            self.lookup_regular_file_slices(ino, 0, attr.size)?,
            block_size,
        )
    }

    fn publish_uploaded_slice_object(
        &self,
        ino: Ino,
        _block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> Result<bool, MetaError> {
        self.mark_regular_file_overlay_object_uploaded_if_matches(
            ino,
            expected_epoch,
            block.object_id,
        )?;
        self.publish_regular_file_overlay_to_base_if_matches(ino, expected_epoch)
    }
}

/// Create a test format info.
fn make_format_info(volume_suffix: &str) -> FormatInfo {
    load_test_env();
    let endpoint = test_env_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:9000");
    let bucket = test_env_var("STUDIOFS_S3_BUCKET", "studiofs-test");
    let access_key = test_env_var("STUDIOFS_S3_ACCESS_KEY", "studiofs");
    let secret_key = test_env_var("STUDIOFS_S3_SECRET_KEY", "studiofs123");
    static RUN_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let run_id = RUN_ID.get_or_init(|| {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        format!("{}-{ts}", std::process::id())
    });
    let volume_name = format!("vfs-test-{volume_suffix}-{run_id}");
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.clone(),
        uuid: format!("test-uuid-{volume_suffix}"),
        endpoint: endpoint.clone(),
        bucket: bucket.clone(),
        access_key: access_key.clone(),
        secret_key: secret_key.clone(),
        region: "us-east-1".into(),
        data_prefix: format!("{volume_name}/objects/"),
        metadata: MetadataConfig::default(),
        block_size: 4 * 1024 * 1024, // 4 MiB
    }
}

fn provision_cache_runtime_layout(cache_root: &std::path::Path) {
    let layout = LocalBlockLayout::from_cache_root(cache_root);
    layout.provision().unwrap();
}

fn workspace_root() -> &'static std::path::Path {
    static ROOT: OnceLock<std::path::PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .and_then(|dir| dir.parent())
            .unwrap_or(manifest_dir)
            .to_path_buf()
    })
    .as_path()
}

fn workspace_env_path() -> std::path::PathBuf {
    workspace_root().join(".env")
}

fn workspace_dotenv_vars() -> &'static std::collections::HashMap<String, String> {
    static VARS: OnceLock<std::collections::HashMap<String, String>> = OnceLock::new();
    VARS.get_or_init(|| {
        let mut vars = std::collections::HashMap::new();
        let env_path = workspace_env_path();
        if let Ok(iter) = dotenvy::from_path_iter(&env_path) {
            for item in iter.flatten() {
                vars.insert(item.0, item.1);
            }
        }
        vars
    })
}

fn workspace_dotenv_var(key: &str) -> Option<String> {
    workspace_dotenv_vars().get(key).cloned()
}

fn test_online_env_var(primary: &str, legacy: &str, default: &str) -> String {
    std::env::var(primary)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| workspace_dotenv_var(primary))
        .or_else(|| workspace_dotenv_var(legacy))
        .unwrap_or_else(|| default.to_string())
}

fn test_env_var(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .or_else(|| workspace_dotenv_var(key))
        .unwrap_or_else(|| default.to_string())
}

fn parse_env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
}

fn parse_env_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
}

fn parse_env_u64_any(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse::<u64>().ok())
}

fn parse_env_f64(key: &str) -> Option<f64> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0)
}

fn test_vfs_config_from_env() -> VfsConfigValidated {
    load_test_env();
    let mut config = VfsConfigRaw::default();
    if let Ok(v) = std::env::var("STUDIOFS_MAX_DIRTY_BYTES")
        && let Some(bytes) = parse_size_bytes(&v)
        && bytes > 0
        && let Ok(bytes) = usize::try_from(bytes)
    {
        config.max_dirty_bytes = Some(bytes);
    }
    if let Ok(v) = std::env::var("STUDIOFS_WRITE_EAGER_FLUSH_BATCH_BYTES")
        && let Some(bytes) = parse_size_bytes(&v)
        && bytes > 0
        && let Ok(bytes) = usize::try_from(bytes)
    {
        config.write_eager_flush_batch_bytes = Some(bytes);
    }
    if let Some(ms) = parse_env_u64("STUDIOFS_SLAB_FLUSH_INTERVAL_MS") {
        config.slab_flush_interval_ms = Some(ms);
    }
    if let Some(secs) = parse_env_u64("STUDIOFS_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS") {
        config.shutdown_metadata_flush_timeout_secs = Some(secs);
    }
    if let Ok(v) = std::env::var("STUDIOFS_PRINCIPAL") {
        let v = v.trim();
        if !v.is_empty() {
            config.principal = Some(v.to_string());
        }
    }
    if let Some(secs) = parse_env_u64("STUDIOFS_JANITOR_INTERVAL_SECS") {
        config.janitor_interval_secs = Some(secs);
    }
    if let Some(secs) = parse_env_u64_any("STUDIOFS_JANITOR_SKIP_NEWER_SECS") {
        config.janitor_skip_newer_secs = Some(secs);
    }
    if let Some(count) = parse_env_u64("STUDIOFS_JANITOR_MAX_DELETES") {
        config.janitor_max_deletes = Some(count);
    }
    if let Some(secs) = parse_env_u64("STUDIOFS_JANITOR_LEASE_TTL_SECS") {
        config.janitor_lease_ttl_secs = Some(secs);
    }
    if let Some(ms) = parse_env_u64("STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS") {
        config.metadata_invalidation_sync_interval_ms = Some(ms);
    }
    if let Some(mb) = parse_env_u64("STUDIOFS_READ_BUFFER_MB") {
        config.read_buffer_mb = Some(mb);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_PREFETCH_CONCURRENCY") {
        config.prefetch_concurrency = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_WIDTH") {
        config.readahead_width = Some(value as u32);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_MIN_PREFETCH") {
        config.readahead_min_prefetch = Some(value as u32);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_SEQ_STEP") {
        config.readahead_seq_step = Some(value as u8);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_RANDOM_PENALTY") {
        config.readahead_random_penalty = Some(value as u8);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_DIR_PREFETCH_MAX_BATCH") {
        config.dir_prefetch_max_batch = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READDIR_ATTR_BATCH_CHUNK") {
        config.readdir_attr_batch_chunk = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READDIR_ATTR_BATCH_MAX_INODES") {
        config.readdir_attr_batch_max_inodes = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_PREFETCH_FOREGROUND_RESERVE") {
        config.prefetch_foreground_reserve = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_PREFETCH_MAX_PER_CYCLE") {
        config.prefetch_max_per_cycle = Some(value);
    }
    if let Some(ms) = parse_env_u64("STUDIOFS_PREFETCH_PAUSE_ON_NETERR_MS") {
        config.prefetch_pause_on_neterr_ms = Some(ms);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_MAX_DRAIN_BATCH") {
        config.max_drain_batch = Some(value);
    }
    if let Some(secs) = parse_env_u64("STUDIOFS_LOCK_TTL_SECS") {
        config.lock_ttl_secs = Some(secs);
    }
    if let Some(ms) = parse_env_u64("STUDIOFS_LOCK_RETRY_BASE_MS") {
        config.lock_retry_base_ms = Some(ms);
    }
    if let Some(ms) = parse_env_u64("STUDIOFS_LOCK_RETRY_MAX_MS") {
        config.lock_retry_max_ms = Some(ms);
    }
    if let Ok(v) = std::env::var("STUDIOFS_XATTR_MAX_BYTES")
        && let Some(bytes) = parse_size_bytes(&v)
        && bytes > 0
        && let Ok(bytes) = usize::try_from(bytes)
    {
        config.max_xattr_bytes = Some(bytes);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_DENTRY_CACHE_CAPACITY") {
        config.dentry_cache_capacity = Some(value);
    }
    if let Ok(v) = std::env::var("STUDIOFS_DENTRY_CACHE_BYTES")
        && let Some(bytes) = parse_size_bytes(&v)
        && bytes > 0
        && let Ok(bytes) = usize::try_from(bytes)
    {
        config.dentry_cache_bytes = Some(bytes);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_PATH_SHORTCUT_CACHE_CAPACITY") {
        config.path_shortcut_cache_capacity = Some(value);
    }
    if let Ok(v) = std::env::var("STUDIOFS_PATH_SHORTCUT_CACHE_BYTES")
        && let Some(bytes) = parse_size_bytes(&v)
        && bytes > 0
        && let Ok(bytes) = usize::try_from(bytes)
    {
        config.path_shortcut_cache_bytes = Some(bytes);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_ATTR_READ_CACHE_CAPACITY") {
        config.attr_read_cache_capacity = Some(value);
    }
    if let Ok(v) = std::env::var("STUDIOFS_ATTR_READ_CACHE_BYTES")
        && let Some(bytes) = parse_size_bytes(&v)
        && bytes > 0
        && let Ok(bytes) = usize::try_from(bytes)
    {
        config.attr_read_cache_bytes = Some(bytes);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_UPLOAD_CACHE_IO_SLOTS") {
        config.upload_cache_io_slots = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS") {
        config.upload_user_idle_grace_ms = Some(value);
    }
    if let Some(value) = parse_env_u64_any("STUDIOFS_UPLOAD_DELAY_MS") {
        config.upload_delay_ms = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_UPLOAD_REQUEUE_BASE_MS") {
        config.upload_requeue_base_ms = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_UPLOAD_REQUEUE_MAX_MS") {
        config.upload_requeue_max_ms = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_UPLOAD_STATS_INTERVAL_SECS") {
        config.upload_stats_interval_secs = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_UPLOAD_MAX_ATTEMPTS") {
        config.upload_max_attempts = Some(value as u32);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS") {
        config.upload_request_timeout_secs = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_UPLOAD_RETRY_BASE_MS") {
        config.upload_retry_base_ms = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_UPLOAD_RETRY_MAX_MS") {
        config.upload_retry_max_ms = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_PUBLISH_VISIBILITY_TIMEOUT_SECS") {
        config.publish_visibility_timeout_secs = Some(value);
    }
    if let Some(value) = parse_env_f64("STUDIOFS_UPLOAD_INITIAL_WINDOW") {
        config.upload_initial_window = Some(value);
    }
    if let Some(value) = parse_env_f64("STUDIOFS_UPLOAD_MIN_WINDOW") {
        config.upload_min_window = Some(value);
    }
    if let Some(value) = parse_env_f64("STUDIOFS_UPLOAD_MAX_WINDOW") {
        config.upload_max_window = Some(value);
    }
    if let Some(value) = parse_env_f64("STUDIOFS_UPLOAD_INCREASE_STEP") {
        config.upload_increase_step = Some(value);
    }
    if let Some(value) = parse_env_f64("STUDIOFS_UPLOAD_DECREASE_FACTOR") {
        config.upload_decrease_factor = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_UPLOAD_DECREASE_COOLDOWN_SECS") {
        config.upload_decrease_cooldown_secs = Some(value);
    }
    if let Some(value) = parse_env_f64("STUDIOFS_FETCH_INITIAL_WINDOW") {
        config.fetch_initial_window = Some(value);
    }
    if let Some(value) = parse_env_f64("STUDIOFS_FETCH_MIN_WINDOW") {
        config.fetch_min_window = Some(value);
    }
    if let Some(value) = parse_env_f64("STUDIOFS_FETCH_MAX_WINDOW") {
        config.fetch_max_window = Some(value);
    }
    if let Some(value) = parse_env_f64("STUDIOFS_FETCH_INCREASE_STEP") {
        config.fetch_increase_step = Some(value);
    }
    if let Some(value) = parse_env_f64("STUDIOFS_FETCH_DECREASE_FACTOR") {
        config.fetch_decrease_factor = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_FETCH_DECREASE_COOLDOWN_SECS") {
        config.fetch_decrease_cooldown_secs = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_FETCH_MAX_ATTEMPTS") {
        config.fetch_max_attempts = Some(value as u32);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_PREFETCH_FETCH_MAX_ATTEMPTS") {
        config.prefetch_fetch_max_attempts = Some(value as u32);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_FETCH_REQUEST_TIMEOUT_SECS") {
        config.fetch_request_timeout_secs = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_FETCH_RETRY_BASE_MS") {
        config.fetch_retry_base_ms = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_FETCH_RETRY_MAX_MS") {
        config.fetch_retry_max_ms = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_FETCH_PREFETCH_RESERVE") {
        config.fetch_prefetch_reserve = Some(value);
    }
    if let Some(value) = parse_env_u64("STUDIOFS_FETCH_PREFETCH_ACQUIRE_WAIT_MS") {
        config.fetch_prefetch_acquire_wait_ms = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_QOS_ACCEPT_SLOTS") {
        config.qos_accept_slots = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_QOS_MATERIALIZE_SLOTS") {
        config.qos_materialize_slots = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_QOS_COMMIT_SLOTS") {
        config.qos_commit_slots = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_QOS_FETCH_SLOTS") {
        config.qos_fetch_slots = Some(value);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_UPLOAD_WORKER_COUNT") {
        config.upload_worker_count = Some(value);
    } else if let Some(value) = parse_env_usize("STUDIOFS_UPLOAD_MAX_SPAWNED") {
        config.upload_worker_count = Some(value);
    }
    config.validate().expect("test VFS config should validate")
}

fn load_test_env() {
    static ONCE: OnceLock<()> = OnceLock::new();
    let _ = ONCE.get_or_init(|| {
        let env_path = workspace_env_path();
        if env_path.exists() {
            let _ = dotenvy::from_path(&env_path);
        } else {
            let _ = dotenvy::dotenv();
        }
    });
}

fn test_env_lock() -> &'static ReentrantMutex<()> {
    static LOCK: OnceLock<ReentrantMutex<()>> = OnceLock::new();
    LOCK.get_or_init(ReentrantMutex::default)
}

fn acquire_test_env_lock() -> ReentrantMutexGuard<'static, ()> {
    test_env_lock().lock()
}

struct TestS3Env {
    endpoint: String,
    bucket: String,
    access_key: String,
    secret_key: String,
}

fn test_online_s3_env() -> &'static TestS3Env {
    static ENV: OnceLock<TestS3Env> = OnceLock::new();
    ENV.get_or_init(|| {
        load_test_env();

        TestS3Env {
            endpoint: test_online_env_var(
                "STUDIOFS_TEST_ONLINE_S3_ENDPOINT",
                "STUDIOFS_S3_ENDPOINT",
                "http://127.0.0.1:9000",
            ),
            bucket: test_online_env_var(
                "STUDIOFS_TEST_ONLINE_S3_BUCKET",
                "STUDIOFS_S3_BUCKET",
                "studiofs-test",
            ),
            access_key: test_online_env_var(
                "STUDIOFS_TEST_ONLINE_S3_ACCESS_KEY",
                "STUDIOFS_S3_ACCESS_KEY",
                "studiofs",
            ),
            secret_key: test_online_env_var(
                "STUDIOFS_TEST_ONLINE_S3_SECRET_KEY",
                "STUDIOFS_S3_SECRET_KEY",
                "studiofs123",
            ),
        }
    })
}

fn apply_test_online_s3_env() {
    let _env_lock = acquire_test_env_lock();
    let env = test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", &env.endpoint);
        std::env::set_var("STUDIOFS_S3_BUCKET", &env.bucket);
        std::env::set_var("STUDIOFS_S3_ACCESS_KEY", &env.access_key);
        std::env::set_var("STUDIOFS_S3_SECRET_KEY", &env.secret_key);
    }
}

fn install_zeroio_replay_offline_env() {
    let _env_lock = acquire_test_env_lock();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_S3_BUCKET", "studiofs-test");
        std::env::set_var("STUDIOFS_S3_ACCESS_KEY", "studiofs");
        std::env::set_var("STUDIOFS_S3_SECRET_KEY", "studiofs123");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }
}

fn install_zeroio_replay_online_env(upload_delay_ms: &str) {
    let _env_lock = acquire_test_env_lock();
    apply_test_online_s3_env();
    unsafe {
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", upload_delay_ms);
    }
}

fn make_format_info_with_endpoint(volume_suffix: &str, endpoint: &str) -> FormatInfo {
    let mut info = make_format_info(volume_suffix);
    info.endpoint = endpoint.to_string();
    info
}

/// Create a fresh VFS instance backed by a temporary redb and S3-compatible storage.
fn make_vfs(tmp: &tempfile::TempDir, volume_suffix: &str) -> Arc<StudioVfs> {
    let db_path = tmp.path().join("meta.redb");

    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("failed to open RedbMetaEngine"));
    meta.init_root(0, 0)
        .expect("failed to initialize root inode");

    // Set format info
    let format_info = make_format_info(volume_suffix);
    meta.set_format_info(&format_info)
        .expect("failed to set format info");

    let data_config = DataStoreConfig::from(&format_info);

    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap();
    let cache_size = 1024 * 1024 * 1024; // 1 GiB

    let vfs = StudioVfs::new(
        meta,
        &data_config,
        cache_dir_str,
        cache_size,
        512 * 1024 * 1024,
        test_vfs_config_from_env(),
    )
    .expect("failed to create StudioVfs");
    vfs.start()
        .expect("failed to start StudioVfs background workers");
    vfs
}

#[allow(dead_code)]
fn make_vfs_from_meta(
    meta: Arc<dyn MetaEngine>,
    format_info: &FormatInfo,
    cache_dir_str: &str,
) -> Arc<StudioVfs> {
    let data_config = DataStoreConfig::from(format_info);
    let vfs = StudioVfs::new(
        meta,
        &data_config,
        cache_dir_str,
        1024 * 1024 * 1024,
        512 * 1024 * 1024,
        test_vfs_config_from_env(),
    )
    .expect("failed to create StudioVfs");
    vfs.start()
        .expect("failed to start StudioVfs background workers");
    vfs
}

fn make_vfs_with_meta(
    meta: Arc<RedbMetaEngine>,
    format_info: &FormatInfo,
    cache_dir_str: &str,
) -> Arc<StudioVfs> {
    meta.set_format_info(format_info)
        .expect("failed to set format info");
    let data_config = DataStoreConfig::from(format_info);
    let vfs = StudioVfs::new(
        meta,
        &data_config,
        cache_dir_str,
        1024 * 1024 * 1024,
        512 * 1024 * 1024,
        test_vfs_config_from_env(),
    )
    .expect("failed to create StudioVfs");
    vfs.start()
        .expect("failed to start StudioVfs background workers");
    vfs
}

fn make_vfs_with_meta_without_background_workers(
    meta: Arc<RedbMetaEngine>,
    format_info: &FormatInfo,
    cache_dir_str: &str,
) -> Arc<StudioVfs> {
    meta.set_format_info(format_info)
        .expect("failed to set format info");
    let data_config = DataStoreConfig::from(format_info);
    StudioVfs::new(
        meta,
        &data_config,
        cache_dir_str,
        1024 * 1024 * 1024,
        512 * 1024 * 1024,
        test_vfs_config_from_env(),
    )
    .expect("failed to create StudioVfs")
}

/// RAII guard that captures and restores all SFS-121 offline test env vars on drop.
/// The explicit `_lock` field holds the global env lock for the **entire** test
/// duration, preventing other parallel tests from observing a partially-set env.
struct OfflineTestEnv {
    _guards: Vec<ScopedEnvVar>,
    _lock: ReentrantMutexGuard<'static, ()>,
}

/// Capture current env state and install the standard offline S3 config for
/// SFS-121 tests: unreachable endpoint + fast upload-retry timeouts.
///
/// The global env lock is acquired **before** any captures so that no other
/// test can observe the environment mid-setup.  The returned guard holds the
/// lock and restores all captured variables when dropped.
fn setup_offline_sfs121_env() -> OfflineTestEnv {
    let lock = acquire_test_env_lock();
    let guards = vec![
        ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES"),
        ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES"),
        ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT"),
        ScopedEnvVar::capture("STUDIOFS_S3_BUCKET"),
        ScopedEnvVar::capture("STUDIOFS_S3_ACCESS_KEY"),
        ScopedEnvVar::capture("STUDIOFS_S3_SECRET_KEY"),
        ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS"),
        ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS"),
        ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS"),
        ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS"),
    ];
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_S3_BUCKET", "studiofs-test");
        std::env::set_var("STUDIOFS_S3_ACCESS_KEY", "studiofs");
        std::env::set_var("STUDIOFS_S3_SECRET_KEY", "studiofs123");
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "0");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }
    OfflineTestEnv {
        _guards: guards,
        _lock: lock,
    }
}

/// RAII guard that captures and restores all SFS-121 S3-backed test env vars on drop.
/// The explicit `_lock` field holds the global env lock for the **entire** test
/// duration, preventing other parallel tests from observing a partially-set env.
struct OnlineTestEnv {
    _guards: Vec<ScopedEnvVar>,
    _lock: ReentrantMutexGuard<'static, ()>,
}

/// Capture current env state and install S3 connection vars for SFS-121 tests
/// that require a real S3 backend.
///
/// Upload timing vars (`STUDIOFS_MAX_DIRTY_BYTES`, `STUDIOFS_MAX_SPILL_BYTES`,
/// `STUDIOFS_UPLOAD_DELAY_MS`, `STUDIOFS_UPLOAD_WORKER_COUNT`) are intentionally
/// left unset — they differ per test and must be set by the caller under the
/// held lock.  All captured vars are restored when the returned guard is dropped.
fn setup_online_sfs121_env() -> OnlineTestEnv {
    let lock = acquire_test_env_lock();
    let guards = vec![
        ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES"),
        ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES"),
        ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT"),
        ScopedEnvVar::capture("STUDIOFS_S3_BUCKET"),
        ScopedEnvVar::capture("STUDIOFS_S3_ACCESS_KEY"),
        ScopedEnvVar::capture("STUDIOFS_S3_SECRET_KEY"),
        ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS"),
        ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT"),
    ];
    let env = test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", &env.endpoint);
        std::env::set_var("STUDIOFS_S3_BUCKET", &env.bucket);
        std::env::set_var("STUDIOFS_S3_ACCESS_KEY", &env.access_key);
        std::env::set_var("STUDIOFS_S3_SECRET_KEY", &env.secret_key);
    }
    OnlineTestEnv {
        _guards: guards,
        _lock: lock,
    }
}

/// Create a VFS wired to an unreachable S3 endpoint for SFS-121 offline tests.
/// Returns `(format_info, vfs)` so callers can derive block sizes and volume
/// names without reconstructing format info.
///
/// Assumes `setup_offline_sfs121_env()` has already been called in the test.
fn make_offline_vfs_sfs121(
    tmp: &tempfile::TempDir,
    suffix: &str,
    block_size_override: Option<u32>,
) -> (FormatInfo, Arc<StudioVfs>) {
    let mut format_info = make_format_info_with_endpoint(
        &format!("sfs121-{suffix}-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    if let Some(bs) = block_size_override {
        format_info.block_size = bs;
    }
    let meta =
        Arc::new(RedbMetaEngine::open(tmp.path().join("meta.redb")).expect("failed to open meta"));
    meta.init_root(0, 0).unwrap();
    let vfs = make_vfs_with_meta(
        meta,
        &format_info,
        tmp.path().join("cache").to_str().unwrap(),
    );
    (format_info, vfs)
}

#[allow(dead_code)]
fn wait_until_blocking(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    cond()
}

#[allow(dead_code)]
fn pg_dsn() -> Option<String> {
    load_test_env();
    let raw = std::env::var("STUDIOFS_TEST_PG_DSN")
        .ok()
        .or_else(|| workspace_dotenv_var("STUDIOFS_TEST_PG_DSN"))
        .filter(|v| !v.trim().is_empty())?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    rt.block_on(async {
        ensure_studiofs_database(&raw)
            .await
            .ok()
            .map(|v| v.metadata_dsn)
    })
}

#[allow(dead_code)]
fn run_pg_async<T, F>(fut: F) -> studiofs_meta::MetaResult<T>
where
    F: std::future::Future<Output = studiofs_meta::MetaResult<T>>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            studiofs_meta::MetaError::Storage(format!("failed to build tokio runtime: {e}"))
        })?;
    rt.block_on(fut)
}

#[allow(dead_code)]
fn setup_pg_volume(
    dsn: &str,
    name: &str,
    uuid: &str,
    block_size: u32,
) -> studiofs_meta::MetaResult<i64> {
    run_pg_async(async move {
        let (mut client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| {
                studiofs_meta::MetaError::Storage(format!("postgres connect failed: {e}"))
            })?;
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        let tx = client.transaction().await.map_err(|e| {
            studiofs_meta::MetaError::Storage(format!("postgres begin tx failed: {e}"))
        })?;

        let row = tx
            .query_one(
                r#"
                INSERT INTO volumes (name, uuid, block_size)
                VALUES ($1, $2, $3)
                RETURNING vol
                "#,
                &[&name, &uuid, &(block_size as i32)],
            )
            .await
            .map_err(|e| {
                studiofs_meta::MetaError::Storage(format!("postgres volume insert failed: {e}"))
            })?;
        let volume_id: i64 = row.get(0);

        tx.execute(
            r#"
            INSERT INTO metadata_state (vol, version, snapshot)
            VALUES ($1, 0, $2)
            ON CONFLICT (vol) DO NOTHING
            "#,
            &[&volume_id, &Vec::<u8>::new()],
        )
        .await
        .map_err(|e| {
            studiofs_meta::MetaError::Storage(format!("postgres metadata_state seed failed: {e}"))
        })?;

        tx.commit().await.map_err(|e| {
            studiofs_meta::MetaError::Storage(format!("postgres commit failed: {e}"))
        })?;
        conn_task.abort();
        Ok(volume_id)
    })
}

#[allow(dead_code)]
fn pg_format_info(name: &str, uuid: &str, dsn: &str, volume_id: i64) -> FormatInfo {
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: name.to_string(),
        uuid: uuid.to_string(),
        endpoint: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_ENDPOINT",
            "STUDIOFS_S3_ENDPOINT",
            "http://127.0.0.1:9000",
        ),
        bucket: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_BUCKET",
            "STUDIOFS_S3_BUCKET",
            "studiofs-test",
        ),
        access_key: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_ACCESS_KEY",
            "STUDIOFS_S3_ACCESS_KEY",
            "studiofs",
        ),
        secret_key: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_SECRET_KEY",
            "STUDIOFS_S3_SECRET_KEY",
            "studiofs123",
        ),
        region: "us-east-1".to_string(),
        data_prefix: format!("{name}/objects/"),
        metadata: MetadataConfig {
            backend: MetadataBackend::Postgres,
            remote: None,
            postgres: Some(PostgresConfig {
                url: dsn.to_string(),
                volume_id,
            }),
        },
        block_size: 4 * 1024 * 1024,
    }
}

#[allow(dead_code)]
fn seed_local(db_path: &std::path::Path, info: &FormatInfo) {
    let seed = RedbMetaEngine::open(db_path).expect("seed redb open");
    seed.set_format_info(info).expect("seed format info");
    seed.init_root(0, 0).expect("seed root");
    let next_event_seq = seed.get_counter("nextEventSeq").expect("seed nextEventSeq");
    seed.set_counter("pgLastFlushedLocalEventSeq", next_event_seq)
        .expect("seed flushed event seq");
}

#[allow(dead_code)]
struct PgTestVolume {
    dsn: String,
    volume_id: i64,
    name: String,
    uuid: String,
}

#[allow(dead_code)]
impl PgTestVolume {
    fn new(dsn: &str, prefix: &str) -> Self {
        let name = format!(
            "{prefix}-{}-{}",
            std::process::id(),
            Uuid::new_v4().simple()
        );
        let uuid = Uuid::new_v4().to_string();
        let volume_id =
            setup_pg_volume(dsn, &name, &uuid, 4 * 1024 * 1024).expect("setup pg volume");
        Self {
            dsn: dsn.to_string(),
            volume_id,
            name,
            uuid,
        }
    }

    fn format_info(&self) -> FormatInfo {
        pg_format_info(&self.name, &self.uuid, &self.dsn, self.volume_id)
    }
}

impl Drop for PgTestVolume {
    fn drop(&mut self) {
        let _ = run_pg_async(async {
            let (client, connection) = tokio_postgres::connect(&self.dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    studiofs_meta::MetaError::Storage(format!("postgres connect failed: {e}"))
                })?;
            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .execute("DELETE FROM volumes WHERE vol = $1", &[&self.volume_id])
                .await
                .map_err(|e| {
                    studiofs_meta::MetaError::Storage(format!("postgres cleanup failed: {e}"))
                })?;
            conn_task.abort();
            Ok(())
        });
    }
}

#[derive(Default)]
#[allow(dead_code)]
struct SpyInvalidator {
    seen: Mutex<Vec<u64>>,
}

#[allow(dead_code)]
impl SpyInvalidator {
    fn snapshot(&self) -> Vec<u64> {
        self.seen.lock().clone()
    }

    fn clear(&self) {
        self.seen.lock().clear();
    }
}

impl KernelInvalidator for SpyInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.seen.lock().push(ino);
    }
}

fn reopen_vfs(db_path: &std::path::Path, cache_dir: &std::path::Path) -> Arc<StudioVfs> {
    let meta = Arc::new(RedbMetaEngine::open(db_path).expect("reopen RedbMetaEngine"));
    let format_info = meta.get_format_info().expect("reload format info");
    make_vfs_with_meta(
        meta,
        &format_info,
        cache_dir.to_str().expect("cache path utf-8"),
    )
}

fn import_remote_snapshot(
    source: &RedbMetaEngine,
    target: &RedbMetaEngine,
    format_info: &FormatInfo,
) {
    let snapshot = source
        .export_remote_snapshot()
        .expect("export remote snapshot");
    target
        .import_remote_snapshot(&snapshot)
        .expect("import remote snapshot");
    target
        .set_format_info(format_info)
        .expect("reset imported format info");
}

/// Ensure the test bucket exists and report whether the endpoint is reachable.
async fn ensure_bucket_ready() -> bool {
    use aws_sdk_s3::Client as S3Client;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
    use opendal::{Operator, services::S3};
    let endpoint = test_env_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:9000");
    let bucket = test_env_var("STUDIOFS_S3_BUCKET", "studiofs-test");
    let access_key = test_env_var("STUDIOFS_S3_ACCESS_KEY", "studiofs");
    let secret_key = test_env_var("STUDIOFS_S3_SECRET_KEY", "studiofs123");

    let builder = S3::default()
        .endpoint(&endpoint)
        .bucket(&bucket)
        .access_key_id(&access_key)
        .secret_access_key(&secret_key)
        .region("us-east-1");

    let op = Operator::new(builder).unwrap().finish();
    if op
        .write(".bucket-marker", Bytes::from_static(b"ok"))
        .await
        .is_ok()
    {
        return true;
    }

    // Fallback: explicitly create bucket if missing.
    let conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            &access_key,
            &secret_key,
            None,
            None,
            "studiofs-test",
        ))
        .region(Region::new("us-east-1"))
        .http_client(SmithyHttpClientBuilder::new().build_http())
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .build();
    let client = S3Client::from_conf(conf);
    let _ = client.create_bucket().bucket(&bucket).send().await;
    op.write(".bucket-marker", Bytes::from_static(b"ok"))
        .await
        .is_ok()
}

/// Ensure the test bucket exists.
async fn ensure_bucket() {
    let _ = ensure_bucket_ready().await;
}

async fn wait_until_object_exists(
    ds: &DataStore,
    data_prefix: &str,
    object_id: u64,
    timeout: Duration,
) -> bool {
    let keys = format_object_fragment_keys(data_prefix, object_id);
    let deadline = Instant::now() + timeout;
    loop {
        let mut all_visible = true;
        for key in &keys {
            match ds.exists(key).await {
                Ok(true) => {}
                _ => {
                    all_visible = false;
                    break;
                }
            }
        }
        if all_visible {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_until_object_absent(
    ds: &DataStore,
    data_prefix: &str,
    object_id: u64,
    timeout: Duration,
) -> bool {
    let keys = format_object_fragment_keys(data_prefix, object_id);
    let deadline = Instant::now() + timeout;
    loop {
        let mut all_absent = true;
        for key in &keys {
            match ds.exists(key).await {
                Ok(false) | Err(_) => {}
                Ok(true) => {
                    all_absent = false;
                    break;
                }
            }
        }
        if all_absent {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn put_fragmented_object(
    ds: &DataStore,
    data_prefix: &str,
    object_id: u64,
    payload: Bytes,
) -> Result<(), String> {
    let keys = format_object_fragment_keys(data_prefix, object_id);
    for (fragment_index, key) in keys.iter().enumerate() {
        let start = fragment_index * CLOUD_FRAGMENT_SIZE;
        let end = (start + CLOUD_FRAGMENT_SIZE).min(payload.len());
        let fragment_payload = if start < payload.len() {
            payload.slice(start..end)
        } else {
            Bytes::new()
        };
        let object_bytes =
            create_object_buffer(fragment_payload).map_err(|e| format!("encode fragment: {e}"))?;
        ds.put_by_key(key, object_bytes)
            .await
            .map_err(|e| format!("write fragment: {e}"))?;
    }
    Ok(())
}

async fn wait_until<F>(timeout: Duration, predicate: F) -> bool
where
    F: FnMut() -> bool,
{
    wait_until_with_interval(timeout, Duration::from_millis(100), predicate).await
}

async fn wait_until_with_interval<F>(
    timeout: Duration,
    poll_interval: Duration,
    mut predicate: F,
) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn measure_foreground_read_phase(
    vfs: &Arc<StudioVfs>,
    ino: u64,
    expected: &[u8],
    read_passes: usize,
    per_read_timeout: Duration,
    phase_name: &str,
) -> (Duration, f64, VfsForegroundReadProtectionStats) {
    let sweep_start = Instant::now();
    for pass in 0..read_passes {
        let bytes = tokio::time::timeout(per_read_timeout, vfs.read(ino, 0, expected.len() as u32))
            .await
            .unwrap_or_else(|_| panic!("{phase_name} read timed out on pass {pass}"))
            .unwrap_or_else(|err| panic!("{phase_name} read failed on pass {pass}: {err}"));
        assert_eq!(
            bytes.as_ref(),
            expected,
            "{phase_name} pass {pass}: data mismatch"
        );
    }
    let sweep_elapsed = sweep_start.elapsed();
    let throughput_mib_s =
        (read_passes * expected.len()) as f64 / (1024.0 * 1024.0) / sweep_elapsed.as_secs_f64();
    let protection_stats = vfs.foreground_read_protection_stats();
    (sweep_elapsed, throughput_mib_s, protection_stats)
}

async fn seed_committed_file(
    meta: &Arc<RedbMetaEngine>,
    format_info: &FormatInfo,
    name: &[u8],
    payload: &[u8],
) -> (u64, Vec<(u32, BlockRef)>) {
    let attr = meta.create_file(1, name, 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    let mut file_attr = meta.get_attr(ino).unwrap();
    file_attr.size = payload.len() as u64;
    meta.set_attr(&file_attr).unwrap();

    let block_size = format_info.block_size as usize;
    let block_count = payload.len().div_ceil(block_size);
    let object_ids = meta.next_block_ids(block_count as u32).unwrap();
    let store = DataStore::new(&DataStoreConfig::from(format_info)).unwrap();

    for (block_index, chunk) in payload.chunks(block_size).enumerate() {
        let object_id = object_ids[block_index];
        put_fragmented_object(
            &store,
            &format_info.data_prefix,
            object_id,
            Bytes::copy_from_slice(chunk),
        )
        .await
        .unwrap();

        let block_ref = if chunk.len() == block_size {
            BlockRef::new_full(object_id, chunk.len() as u32)
        } else {
            BlockRef::new_partial(object_id, chunk.len() as u32)
        };
        meta.stage_logical_payload_slice(ino, block_index as u32, &block_ref)
            .unwrap();
        assert!(
            meta.publish_uploaded_slice_object(
                ino,
                block_index as u32,
                file_attr.content_epoch,
                &block_ref
            )
            .unwrap(),
            "seeded committed payload slice should satisfy metadata preconditions"
        );
    }

    (ino, meta.lookup_logical_payload_slices(ino).unwrap())
}

fn wait_until_payload_slices(
    vfs: &Arc<StudioVfs>,
    ino: u64,
    timeout: Duration,
) -> Option<Vec<(u32, BlockRef)>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(blocks) = vfs.meta().lookup_logical_payload_slices(ino)
            && !blocks.is_empty()
        {
            return Some(blocks);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Wait until at least `expected` resolved payload slices are visible in metadata.
///
/// After `flush()`, payload is moved to the local-dirty pipeline and processed
/// asynchronously by the background slab flusher.  This helper polls until the
/// expected number of payload slices appear (or the timeout expires).
fn wait_until_n_payload_slices(
    vfs: &Arc<StudioVfs>,
    ino: u64,
    expected: usize,
    timeout: Duration,
) -> Option<Vec<(u32, BlockRef)>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(blocks) = vfs.meta().lookup_logical_payload_slices(ino)
            && blocks.len() >= expected
        {
            return Some(blocks);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

// =============================================================================
// Basic Operations Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_create_write_read_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "cwr");

    // Create a file in root directory (inode 1)
    let attr = vfs.create(1, b"hello.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    assert!(ino > 1);

    // Write data
    let payload = b"Hello, StudioFS! This is a slice-native test.";
    let written = vfs.write(ino, 0, payload).await.unwrap();
    assert_eq!(written as usize, payload.len());

    // Flush to ensure data is persisted
    vfs.flush(ino).await.unwrap();

    // Read it back
    let data = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
    assert_eq!(&data[..], payload);

    // Verify file size via getattr
    let attr = vfs.getattr(ino).unwrap();
    assert_eq!(attr.size, payload.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_write_read_large_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "large");

    let attr = vfs.create(1, b"big.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 10 MiB of patterned data (crosses 4 MiB block boundary)
    let size = 10 * 1024 * 1024;
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let written = vfs.write(ino, 0, &payload).await.unwrap();
    assert_eq!(written as usize, size);

    // Flush
    vfs.flush(ino).await.unwrap();

    // Read it all back
    let data = vfs.read(ino, 0, size as u32).await.unwrap();
    assert_eq!(data.len(), size);
    assert_eq!(&data[..], &payload[..], "10 MiB read-back must match write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_write_at_offset() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "offset");

    let attr = vfs.create(1, b"offset.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write at offset 1 MiB
    let offset = 1024 * 1024; // 1 MiB
    let payload = b"Data at 1 MiB offset";
    vfs.write(ino, offset, payload).await.unwrap();

    // Flush
    vfs.flush(ino).await.unwrap();

    // Read from offset
    let data = vfs.read(ino, offset, payload.len() as u32).await.unwrap();
    assert_eq!(&data[..], payload);

    // Verify file size
    let attr = vfs.getattr(ino).unwrap();
    assert_eq!(attr.size, offset + payload.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_overwrite_and_read() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "overwrite");

    let attr = vfs.create(1, b"over.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write initial data
    let v1 = b"AAAAAAAAAA"; // 10 bytes
    vfs.write(ino, 0, v1).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Overwrite the middle (offset 3..8)
    let data2 = b"BBBBB";
    vfs.write(ino, 3, data2).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Read full file — expect "AAABBBBBAA"
    let data = vfs.read(ino, 0, 10).await.unwrap();
    assert_eq!(&data[..], b"AAABBBBBAA");
}

// =============================================================================
// Block Operations Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_block_allocation() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "blocks");

    let attr = vfs.create(1, b"blocks.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write data that spans multiple blocks (4 MiB each)
    let block_size = 4 * 1024 * 1024;
    let payload: Vec<u8> = (0..block_size * 3).map(|i| (i % 256) as u8).collect();

    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Wait for background slab flusher to materialise payload slices.
    let blocks = wait_until_n_payload_slices(&vfs, ino, 3, Duration::from_secs(15))
        .expect("Should have 3 payload slices for 12 MiB file");

    // Verify logical payload indexes.
    assert_eq!(blocks[0].0, 0);
    assert_eq!(blocks[1].0, 1);
    assert_eq!(blocks[2].0, 2);

    // Verify each payload reference is stored uncompressed in metadata flags.
    for (_, block_ref) in &blocks {
        assert!(
            !block_ref.is_compressed(),
            "Payload object should not be marked compressed"
        );
    }

    // Read back and verify
    let data = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
    assert_eq!(&data[..], &payload[..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_partial_block() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "partial");

    let attr = vfs.create(1, b"partial.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write less than a full block
    let payload = b"Small file content";
    vfs.write(ino, 0, payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Wait for background slab flusher to materialise payload slices.
    let blocks = wait_until_n_payload_slices(&vfs, ino, 1, Duration::from_secs(15))
        .expect("Should have 1 payload slice after flush");
    assert_eq!(blocks.len(), 1);

    let (_, block_ref) = &blocks[0];
    assert!(
        block_ref.is_partial(),
        "Small file should have a partial payload reference"
    );
    assert_eq!(block_ref.length as usize, payload.len());
}

// =============================================================================
// Directory Operations Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_directory_operations() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "dirs");

    // Create a subdirectory
    let dir_attr = vfs.mkdir(1, b"subdir", 0, 0, 0o755).unwrap();
    let dir_ino = dir_attr.ino;

    // Create files inside
    let f1 = vfs.create(dir_ino, b"a.txt", 0, 0, 0o644).unwrap();
    let f2 = vfs.create(dir_ino, b"b.txt", 0, 0, 0o644).unwrap();

    // Write data to files
    vfs.write(f1.ino, 0, b"file a content").await.unwrap();
    vfs.write(f2.ino, 0, b"file b content").await.unwrap();
    vfs.flush(f1.ino).await.unwrap();
    vfs.flush(f2.ino).await.unwrap();

    // Readdir should return both files
    let entries = vfs.readdir(dir_ino).unwrap();
    let names: Vec<String> = entries
        .iter()
        .map(|e| String::from_utf8_lossy(&e.name).to_string())
        .collect();
    assert!(names.contains(&"a.txt".to_string()));
    assert!(names.contains(&"b.txt".to_string()));

    // Lookup should work
    let found = vfs.lookup(dir_ino, b"a.txt").unwrap();
    assert_eq!(found.ino, f1.ino);

    // Unlink files
    vfs.unlink(dir_ino, b"a.txt").unwrap();
    vfs.unlink(dir_ino, b"b.txt").unwrap();

    // Rmdir
    vfs.rmdir(1, b"subdir").unwrap();

    // Lookup should fail now
    assert!(vfs.lookup(1, b"subdir").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rename_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename");

    let attr = vfs.create(1, b"old.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write some data
    vfs.write(ino, 0, b"rename test data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Rename
    vfs.rename(1, b"old.txt", 1, b"new.txt").unwrap();

    // Old name should fail
    assert!(vfs.lookup(1, b"old.txt").is_err());

    // New name should resolve to same inode
    let found = vfs.lookup(1, b"new.txt").unwrap();
    assert_eq!(found.ino, ino);

    // Data should still be readable
    let data = vfs.read(ino, 0, 16).await.unwrap();
    assert_eq!(&data[..], b"rename test data");
}

// =============================================================================
// Truncation Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_truncate_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "truncate");

    let attr = vfs.create(1, b"trunc.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 12 MiB (3 blocks)
    let size = 12 * 1024 * 1024;
    let payload: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Wait for background slab flusher to materialise all 3 payload slices.
    let blocks = wait_until_n_payload_slices(&vfs, ino, 3, Duration::from_secs(15))
        .expect("Should have 3 payload slices for 12 MiB file");
    assert_eq!(blocks.len(), 3);

    // Truncate to 5 MiB (should remove 1 block, partial last block)
    let new_size = 5 * 1024 * 1024;
    vfs.setattr(ino, SetattrArgs::size_only(new_size)).unwrap();

    // Verify 2 payload slices now (truncation is synchronous — no wait needed).
    let blocks = vfs.meta().lookup_logical_payload_slices(ino).unwrap();
    assert_eq!(blocks.len(), 2);

    // Verify file size
    let attr = vfs.getattr(ino).unwrap();
    assert_eq!(attr.size, new_size);

    // Verify data is preserved
    let data = vfs.read(ino, 0, new_size as u32).await.unwrap();
    assert_eq!(&data[..], &payload[..new_size as usize]);
}

// =============================================================================
// Crash Recovery Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_pending_deletions_gc() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "gc");
    let info = vfs.meta().get_format_info().unwrap();
    let ds = DataStore::new(&DataStoreConfig::from(&info)).unwrap();

    let attr = vfs.create(1, b"gc_test.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write initial data
    vfs.write(ino, 0, b"initial data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Wait for background slab flusher to materialise payload slices.
    let blocks_before = wait_until_n_payload_slices(&vfs, ino, 1, Duration::from_secs(15))
        .expect("Should have 1 block after initial write");
    assert_eq!(blocks_before.len(), 1);
    let old_block_id = blocks_before[0].1.object_id;

    // Under the committed/overlay model, the old block becomes GC-eligible
    // only after it has actually been uploaded.
    assert!(
        wait_until_object_exists(
            &ds,
            &info.data_prefix,
            old_block_id,
            Duration::from_secs(20)
        )
        .await,
        "initial object should be uploaded before overwrite GC check"
    );
    assert!(
        wait_until(Duration::from_secs(20), || {
            let attr = vfs.meta().get_attr(ino).unwrap();
            attr.upload_state == UploadState::Ready
                && attr.committed_content_epoch == attr.content_epoch
                && attr.committed_size == attr.size
        })
        .await,
        "initial block should be committed before overwrite GC check"
    );

    // Overwrite with new data. The displaced old block should be marked for
    // deletion once the replacement upload commits.
    vfs.write(ino, 0, b"new data here!!").await.unwrap();
    vfs.flush(ino).await.unwrap();

    assert!(
        wait_until(Duration::from_secs(20), || {
            let attr = vfs.meta().get_attr(ino).unwrap();
            attr.upload_state == UploadState::Ready
                && attr.committed_content_epoch == attr.content_epoch
                && vfs
                    .meta()
                    .lookup_logical_payload_slices(ino)
                    .unwrap()
                    .first()
                    .is_some_and(|(_, block)| block.object_id != old_block_id)
        })
        .await,
        "overwrite GC check should begin only after the replacement incarnation is fully published"
    );

    assert!(
        wait_until(Duration::from_secs(20), || {
            let pending = vfs.meta().get_pending_deletions().unwrap();
            pending.iter().any(|(id, _)| *id == old_block_id)
        })
        .await,
        "old committed payload object should eventually enter pending deletions"
    );

    // Verify new block is different
    let blocks_after = vfs.meta().lookup_logical_payload_slices(ino).unwrap();
    assert_ne!(blocks_after[0].1.object_id, old_block_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_background_janitor_removes_orphan_object() {
    ensure_bucket().await;

    type EnvRestore = ScopedEnvVar;

    // Speed up janitor cycle for this test only.
    let _restore_interval = EnvRestore::capture("STUDIOFS_JANITOR_INTERVAL_SECS");
    let _restore_skip = EnvRestore::capture("STUDIOFS_JANITOR_SKIP_NEWER_SECS");
    let _restore_max = EnvRestore::capture("STUDIOFS_JANITOR_MAX_DELETES");
    let _restore_lease_ttl = EnvRestore::capture("STUDIOFS_JANITOR_LEASE_TTL_SECS");
    unsafe {
        std::env::set_var("STUDIOFS_JANITOR_INTERVAL_SECS", "2");
        std::env::set_var("STUDIOFS_JANITOR_SKIP_NEWER_SECS", "0");
        std::env::set_var("STUDIOFS_JANITOR_MAX_DELETES", "50");
        std::env::set_var("STUDIOFS_JANITOR_LEASE_TTL_SECS", "10");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "janitor_orphan");
    let info = vfs.meta().get_format_info().unwrap();
    let ds = DataStore::new(&DataStoreConfig::from(&info)).unwrap();

    // Upload an orphan object directly to data storage (metadata doesn't reference it).
    let orphan_id = 0xdead_beef_u64;
    for fragment_index in 0..CLOUD_FRAGMENT_COUNT {
        let key = format_object_fragment_key(&info.data_prefix, orphan_id, fragment_index as u8);
        if let Err(err) = ds
            .operator()
            .write(&key, Bytes::from_static(b"orphan-object"))
            .await
        {
            if err.to_string().contains("NoSuchBucket") {
                eprintln!(
                    "skip test_background_janitor_removes_orphan_object: {}",
                    err
                );
                return;
            }
            panic!("failed to write orphan object: {}", err);
        }
    }
    assert!(
        wait_until_object_exists(&ds, &info.data_prefix, orphan_id, Duration::from_secs(5)).await,
        "orphan object must exist before janitor cycle"
    );

    assert!(
        wait_until_object_absent(&ds, &info.data_prefix, orphan_id, Duration::from_secs(20)).await,
        "background janitor should delete orphan object"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_background_janitor_cleans_simulated_crash_orphan() {
    ensure_bucket().await;

    type EnvRestore = ScopedEnvVar;

    // Fast janitor cycle for test.
    let _restore_interval = EnvRestore::capture("STUDIOFS_JANITOR_INTERVAL_SECS");
    let _restore_skip = EnvRestore::capture("STUDIOFS_JANITOR_SKIP_NEWER_SECS");
    let _restore_max = EnvRestore::capture("STUDIOFS_JANITOR_MAX_DELETES");
    let _restore_lease_ttl = EnvRestore::capture("STUDIOFS_JANITOR_LEASE_TTL_SECS");
    unsafe {
        std::env::set_var("STUDIOFS_JANITOR_INTERVAL_SECS", "2");
        std::env::set_var("STUDIOFS_JANITOR_SKIP_NEWER_SECS", "0");
        std::env::set_var("STUDIOFS_JANITOR_MAX_DELETES", "50");
        std::env::set_var("STUDIOFS_JANITOR_LEASE_TTL_SECS", "10");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "janitor_crash_orphan");
    let info = vfs.meta().get_format_info().unwrap();
    let ds = DataStore::new(&DataStoreConfig::from(&info)).unwrap();

    // 1) Normal write path uploads object and records metadata ref.
    let attr = vfs.create(1, b"crash_like.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    let payload = vec![0xABu8; 512 * 1024];
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    let blocks = wait_until_payload_slices(&vfs, ino, Duration::from_secs(15))
        .expect("expected at least one payload slice after flush");
    let object_id = blocks[0].1.object_id;
    if let Err(err) = put_fragmented_object(
        &ds,
        &info.data_prefix,
        object_id,
        Bytes::from(payload.clone()),
    )
    .await
    {
        if err.to_string().contains("NoSuchBucket") {
            eprintln!(
                "skip test_background_janitor_cleans_simulated_crash_orphan: {}",
                err
            );
            return;
        }
        panic!("failed to write simulated crash object: {}", err);
    }
    assert!(
        wait_until_object_exists(&ds, &info.data_prefix, object_id, Duration::from_secs(5)).await,
        "uploaded data object should exist before simulated crash"
    );

    // 2) Simulate crash window by "losing" metadata refs while object remains in S3.
    let removed_ids = vfs.meta().remove_regular_file_graph(ino).unwrap();
    assert!(
        removed_ids.contains(&object_id),
        "remove_regular_file_graph should drop the uploaded object reference"
    );

    // 3) Janitor should eventually detect object as orphan and delete it.
    assert!(
        wait_until_object_absent(&ds, &info.data_prefix, object_id, Duration::from_secs(25)).await,
        "background janitor should clean orphan object from simulated crash"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_background_janitor_does_not_heal_dangling_dentry_without_foreground_access() {
    ensure_bucket().await;

    type EnvRestore = ScopedEnvVar;

    let _restore_interval = EnvRestore::capture("STUDIOFS_JANITOR_INTERVAL_SECS");
    let _restore_skip = EnvRestore::capture("STUDIOFS_JANITOR_SKIP_NEWER_SECS");
    let _restore_max = EnvRestore::capture("STUDIOFS_JANITOR_MAX_DELETES");
    let _restore_lease_ttl = EnvRestore::capture("STUDIOFS_JANITOR_LEASE_TTL_SECS");
    unsafe {
        std::env::set_var("STUDIOFS_JANITOR_INTERVAL_SECS", "2");
        std::env::set_var("STUDIOFS_JANITOR_SKIP_NEWER_SECS", "0");
        std::env::set_var("STUDIOFS_JANITOR_MAX_DELETES", "50");
        std::env::set_var("STUDIOFS_JANITOR_LEASE_TTL_SECS", "10");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "janitor_dangling_dentry");

    let dangling = vfs.create(1, b"janitor-dangling.txt", 0, 0, 0o644).unwrap();
    vfs.meta().remove_attr(dangling.ino).unwrap();
    assert!(
        vfs.meta().lookup(1, b"janitor-dangling.txt").is_ok(),
        "test setup requires a dentry that still points to the removed inode"
    );

    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(
        vfs.meta().lookup(1, b"janitor-dangling.txt").is_ok(),
        "runtime janitor should not remove dangling dentries without foreground lookup/getattr"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_background_janitor_recovers_pending_create_intent_runtime() {
    ensure_bucket().await;

    type EnvRestore = ScopedEnvVar;

    let _restore_interval = EnvRestore::capture("STUDIOFS_JANITOR_INTERVAL_SECS");
    let _restore_skip = EnvRestore::capture("STUDIOFS_JANITOR_SKIP_NEWER_SECS");
    let _restore_max = EnvRestore::capture("STUDIOFS_JANITOR_MAX_DELETES");
    let _restore_lease_ttl = EnvRestore::capture("STUDIOFS_JANITOR_LEASE_TTL_SECS");
    unsafe {
        std::env::set_var("STUDIOFS_JANITOR_INTERVAL_SECS", "2");
        std::env::set_var("STUDIOFS_JANITOR_SKIP_NEWER_SECS", "0");
        std::env::set_var("STUDIOFS_JANITOR_MAX_DELETES", "50");
        std::env::set_var("STUDIOFS_JANITOR_LEASE_TTL_SECS", "10");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "janitor_pending_create_runtime");

    let ino = vfs.meta().next_inode().unwrap();
    let intent = PendingCreateRecord {
        ino,
        parent: ROOT_INO,
        name: b"janitor-pending-create.txt".to_vec(),
        kind: studiofs_meta::FileType::RegularFile,
        created_at: 0,
    };
    vfs.meta().add_pending_create(&intent).unwrap();
    vfs.meta()
        .add_entry(
            ROOT_INO,
            b"janitor-pending-create.txt",
            &studiofs_meta::DirEntryValue {
                ino,
                kind: studiofs_meta::FileType::RegularFile,
            },
        )
        .unwrap();

    assert!(
        vfs.meta()
            .lookup(ROOT_INO, b"janitor-pending-create.txt")
            .is_ok()
    );
    assert!(vfs.meta().get_attr(ino).is_err());
    assert!(
        vfs.meta()
            .list_pending_creates()
            .unwrap()
            .iter()
            .any(|record| record.ino == ino)
    );

    assert!(
        wait_until(Duration::from_secs(25), || {
            vfs.meta()
                .lookup(ROOT_INO, b"janitor-pending-create.txt")
                .is_err()
                && !vfs
                    .meta()
                    .list_pending_creates()
                    .unwrap()
                    .iter()
                    .any(|record| record.ino == ino)
        })
        .await,
        "background janitor should replay pending create intents without restart"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_background_janitor_does_not_recover_deferred_delete_undo_runtime() {
    ensure_bucket().await;

    type EnvRestore = ScopedEnvVar;

    let _restore_interval = EnvRestore::capture("STUDIOFS_JANITOR_INTERVAL_SECS");
    let _restore_skip = EnvRestore::capture("STUDIOFS_JANITOR_SKIP_NEWER_SECS");
    let _restore_max = EnvRestore::capture("STUDIOFS_JANITOR_MAX_DELETES");
    let _restore_lease_ttl = EnvRestore::capture("STUDIOFS_JANITOR_LEASE_TTL_SECS");
    unsafe {
        std::env::set_var("STUDIOFS_JANITOR_INTERVAL_SECS", "2");
        std::env::set_var("STUDIOFS_JANITOR_SKIP_NEWER_SECS", "0");
        std::env::set_var("STUDIOFS_JANITOR_MAX_DELETES", "50");
        std::env::set_var("STUDIOFS_JANITOR_LEASE_TTL_SECS", "10");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "janitor_deferred_undo_runtime");

    let attr = vfs
        .create(ROOT_INO, b"janitor-deferred-undo.txt", 0, 0, 0o644)
        .unwrap();
    let ino = attr.ino;

    vfs.meta()
        .unlink_open_handle(ROOT_INO, b"janitor-deferred-undo.txt")
        .unwrap();
    assert!(
        vfs.meta()
            .lookup(ROOT_INO, b"janitor-deferred-undo.txt")
            .is_err()
    );
    assert!(
        vfs.meta()
            .list_deferred_deletes()
            .unwrap()
            .iter()
            .any(|record| record.ino == ino)
    );
    assert!(
        vfs.meta()
            .list_undo_logs()
            .unwrap()
            .iter()
            .any(|record| record.kind == UndoLogKind::DeferredDelete && record.ino == ino)
    );

    assert!(
        wait_until(Duration::from_secs(25), || {
            vfs.meta().get_attr(ino).is_ok()
                && vfs
                    .meta()
                    .list_deferred_deletes()
                    .unwrap()
                    .iter()
                    .any(|record| record.ino == ino)
                && vfs
                    .meta()
                    .list_undo_logs()
                    .unwrap()
                    .iter()
                    .any(|record| record.kind == UndoLogKind::DeferredDelete && record.ino == ino)
        })
        .await,
        "runtime janitor must not finalize deferred-delete undo records for open-unlink files"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_background_janitor_recovers_only_pending_create_in_mixed_runtime() {
    ensure_bucket().await;

    type EnvRestore = ScopedEnvVar;

    let _restore_interval = EnvRestore::capture("STUDIOFS_JANITOR_INTERVAL_SECS");
    let _restore_skip = EnvRestore::capture("STUDIOFS_JANITOR_SKIP_NEWER_SECS");
    let _restore_max = EnvRestore::capture("STUDIOFS_JANITOR_MAX_DELETES");
    let _restore_lease_ttl = EnvRestore::capture("STUDIOFS_JANITOR_LEASE_TTL_SECS");
    unsafe {
        std::env::set_var("STUDIOFS_JANITOR_INTERVAL_SECS", "2");
        std::env::set_var("STUDIOFS_JANITOR_SKIP_NEWER_SECS", "0");
        std::env::set_var("STUDIOFS_JANITOR_MAX_DELETES", "50");
        std::env::set_var("STUDIOFS_JANITOR_LEASE_TTL_SECS", "10");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "janitor_mixed_undo_runtime");

    let pending_ino = vfs.meta().next_inode().unwrap();
    let pending_name = b"janitor-mixed-pending.txt";
    let intent = PendingCreateRecord {
        ino: pending_ino,
        parent: ROOT_INO,
        name: pending_name.to_vec(),
        kind: studiofs_meta::FileType::RegularFile,
        created_at: 0,
    };
    vfs.meta().add_pending_create(&intent).unwrap();
    vfs.meta()
        .add_entry(
            ROOT_INO,
            pending_name,
            &studiofs_meta::DirEntryValue {
                ino: pending_ino,
                kind: studiofs_meta::FileType::RegularFile,
            },
        )
        .unwrap();
    assert!(vfs.meta().lookup(ROOT_INO, pending_name).is_ok());
    assert!(vfs.meta().get_attr(pending_ino).is_err());

    let deferred_name = b"janitor-mixed-deferred.txt";
    let deferred_attr = vfs.create(ROOT_INO, deferred_name, 0, 0, 0o644).unwrap();
    let deferred_ino = deferred_attr.ino;
    vfs.meta()
        .unlink_open_handle(ROOT_INO, deferred_name)
        .unwrap();
    assert!(vfs.meta().lookup(ROOT_INO, deferred_name).is_err());
    assert!(
        vfs.meta()
            .list_deferred_deletes()
            .unwrap()
            .iter()
            .any(|record| record.ino == deferred_ino)
    );
    let undo_before = vfs.meta().list_undo_logs().unwrap();
    assert!(
        undo_before.iter().any(|record| {
            (record.kind == UndoLogKind::PendingCreate && record.ino == pending_ino)
                || (record.kind == UndoLogKind::DeferredDelete && record.ino == deferred_ino)
        }),
        "test setup requires undo-log records for both pending and deferred operations"
    );

    assert!(
        wait_until(Duration::from_secs(25), || {
            vfs.meta().lookup(ROOT_INO, pending_name).is_err()
                && vfs.meta().get_attr(deferred_ino).is_ok()
                && !vfs
                    .meta()
                    .list_pending_creates()
                    .unwrap()
                    .iter()
                    .any(|record| record.ino == pending_ino)
                && vfs
                    .meta()
                    .list_deferred_deletes()
                    .unwrap()
                    .iter()
                    .any(|record| record.ino == deferred_ino)
                && vfs.meta().list_undo_logs().unwrap().iter().any(|record| {
                    record.kind == UndoLogKind::DeferredDelete && record.ino == deferred_ino
                })
        })
        .await,
        "runtime janitor should recover pending-create intents but leave deferred-delete undo for startup/crash recovery"
    );
}

#[test]
#[ignore]
fn test_crash_recovery_replays_pending_journal() {
    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();

    let volume_suffix = format!("crash-{}", std::process::id());
    let offline_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let mut crash_file_ino: u64 = 0;

    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&offline_info).unwrap();

    // Phase 1: create dirty data and pending journals while endpoint is offline.
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let offline_data_config = DataStoreConfig::from(&offline_info);
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &offline_data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            let attr = vfs.create(1, b"crash.bin", 0, 0, 0o644).unwrap();
            crash_file_ino = attr.ino;
            let payload = vec![0x5Au8; 2 * 1024 * 1024];
            vfs.write(attr.ino, 0, &payload).await.unwrap();
            vfs.flush(attr.ino).await.unwrap();
            // Wait until the background slab flusher has materialised block
            // refs + journal entries.  Otherwise dirty_mem hasn't been drained
            // and no journal files exist yet.
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                if let Ok(blocks) = vfs.meta().lookup_logical_payload_slices(attr.ino)
                    && !blocks.is_empty()
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for slab flusher"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            // Simulate abrupt process death: drop runtime without graceful VFS shutdown.
        });
        rt.shutdown_background();
    }

    let pending_count = open_local_block_backend(&cache_dir, &offline_info.uuid)
        .unwrap()
        .backend
        .dirty_object_ids()
        .len();
    assert!(
        pending_count > 0,
        "expected dirty blocks after simulated crash"
    );

    // Phase 2: restart with healthy endpoint and verify recovery drains journals.
    let online_endpoint = test_env_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:9000");
    let online_info = make_format_info_with_endpoint(&volume_suffix, &online_endpoint);
    meta.set_format_info(&online_info).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    let online_data_config = DataStoreConfig::from(&online_info);
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &online_data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let blocks = meta.lookup_logical_payload_slices(crash_file_ino).unwrap();
        assert!(
            !blocks.is_empty(),
            "payload slice metadata must exist before upload"
        );
        let object_id = blocks[0].1.object_id;
        let check_store = DataStore::new(&DataStoreConfig::from(&online_info)).unwrap();

        assert!(
            wait_until_object_exists(
                &check_store,
                &online_info.data_prefix,
                object_id,
                Duration::from_secs(15),
            )
            .await,
            "recovered uploader should eventually upload object {object_id}"
        );

        vfs.shutdown(Duration::from_secs(15)).await;
    });

    // Allow async cleanup to clear DIRTY flags after upload completion.
    thread::sleep(Duration::from_millis(500));
    let pending_after = open_local_block_backend(&cache_dir, &offline_info.uuid)
        .unwrap()
        .backend
        .dirty_object_ids()
        .len();
    assert_eq!(
        pending_after, 0,
        "dirty upload state must be cleaned up after recovery"
    );
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn test_crash_recovery_replays_local_committed_block_before_metadata_flush() {
    // Keep S3 settings stable for this long-running crash/restart scenario.
    // Other tests intentionally toggle endpoint reachability; pinning online
    // values here avoids inheriting a stale offline endpoint.
    apply_test_online_s3_env();

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();

    let volume_suffix = format!("spill-crash-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let baseline_payload = vec![0x11u8; 512 * 1024];
    let replacement_payload = vec![0x22u8; baseline_payload.len()];
    let mut ino = 0u64;
    let mut baseline_object_id = 0u64;

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs_config = VfsConfigRaw {
                max_dirty_bytes: Some(8 * 1024 * 1024),
                ..VfsConfigRaw::default()
            };
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                vfs_config
                    .validate()
                    .expect("test VFS config should validate"),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            let attr = vfs.create(1, b"spill-restart.bin", 0, 0, 0o644).unwrap();
            ino = attr.ino;
            vfs.write(ino, 0, &baseline_payload).await.unwrap();
            vfs.flush(ino).await.unwrap();

            let blocks = wait_until_payload_slices(&vfs, ino, Duration::from_secs(45))
                .expect("expected committed payload slices for baseline payload");
            baseline_object_id = blocks[0].1.object_id;

            let ds = DataStore::new(&data_config).unwrap();
            assert!(
                wait_until_object_exists(
                    &ds,
                    &format_info.data_prefix,
                    baseline_object_id,
                    Duration::from_secs(60),
                )
                .await,
                "baseline object should be uploaded before overwrite crash test"
            );

            vfs.shutdown(Duration::from_secs(15)).await;
        });
    }

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let offline_info =
                make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
            meta.set_format_info(&offline_info).unwrap();
            let data_config_offline = DataStoreConfig::from(&offline_info);
            let vfs_config = VfsConfigRaw {
                max_dirty_bytes: Some(8 * 1024 * 1024),
                ..VfsConfigRaw::default()
            };
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                vfs_config
                    .validate()
                    .expect("test VFS config should validate"),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let before = meta.lookup_logical_payload_slices(ino).unwrap();
            assert_eq!(
                before.len(),
                1,
                "baseline file should have one committed payload slice"
            );
            assert_eq!(
                before[0].1.object_id, baseline_object_id,
                "baseline committed payload slice must remain visible before overwrite"
            );

            vfs.write(ino, 0, &replacement_payload).await.unwrap();
            vfs.flush(ino).await.unwrap();

            let after = meta.lookup_logical_payload_slices(ino).unwrap();
            assert_eq!(
                after.len(),
                1,
                "overwrite should still leave one committed payload slice"
            );
            assert_ne!(
                after[0].1.object_id, baseline_object_id,
                "local flush should advance metadata to the newly committed local cache object"
            );
            assert!(
                vfs.pending_uploads_for_inode(ino).len() == 1,
                "offline overwrite should queue exactly one pending upload"
            );
        });
        rt.shutdown_background();
    }
    let blocks_after_crash = meta.lookup_logical_payload_slices(ino).unwrap();
    assert_ne!(
        blocks_after_crash[0].1.object_id, baseline_object_id,
        "metadata should still point at the locally committed replacement after crash"
    );

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let offline_info =
                make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
            meta.set_format_info(&offline_info).unwrap();
            let data_config_offline = DataStoreConfig::from(&offline_info);
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let blocks = meta.lookup_logical_payload_slices(ino).unwrap();
            assert_eq!(blocks.len(), 1);
            assert_ne!(blocks[0].1.object_id, baseline_object_id);

            let data = vfs
                .read(ino, 0, replacement_payload.len() as u32)
                .await
                .unwrap();
            assert_eq!(
                data.as_ref(),
                replacement_payload.as_slice(),
                "read-after-restart should serve bytes from the locally committed block-store state"
            );
        });
        rt.shutdown_background();
    }
}

#[test]
fn writer_reads_full_file_before_any_remote_commit() {
    type EnvRestore = ScopedEnvVar;

    let _restore_dirty = EnvRestore::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = EnvRestore::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_disable = EnvRestore::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    let _restore_endpoint = EnvRestore::capture("STUDIOFS_S3_ENDPOINT");

    unsafe {
        // Keep uploads remote-offline; local flush should still commit through
        // Local backend + metadata immediately.
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "8MB");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "16MB");
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("writer-full-local-read-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let payload = vec![0xA1u8; 1024 * 1024];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs.create(1, b"local-full-read.bin", 0, 0, 0o644).unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        let blocks = vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap();
        assert!(
            !blocks.is_empty(),
            "local flush should commit metadata even before any remote upload succeeds"
        );

        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            payload.as_slice(),
            "same mounted writer should read full file before any remote commit"
        );
    });
    rt.shutdown_background();
}

#[test]
fn full_block_write_flush_recovers_pending_upload_after_restart() {
    type EnvRestore = ScopedEnvVar;

    let _restore_dirty = EnvRestore::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_eager = EnvRestore::capture("STUDIOFS_WRITE_EAGER_FLUSH_BATCH_BYTES");
    let _restore_spill = EnvRestore::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_disable = EnvRestore::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");

    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "8MB");
        std::env::set_var("STUDIOFS_WRITE_EAGER_FLUSH_BATCH_BYTES", "4MB");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "16MB");
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("eager-full-block-restart-{}", std::process::id());
    let offline_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&offline_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&offline_info).unwrap();

    let payload = vec![0xC3u8; offline_info.block_size as usize];
    let mut inode = 0u64;

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let attr = vfs.create(1, b"eager-full-block.bin", 0, 0, 0o644).unwrap();
            inode = attr.ino;
            vfs.write(inode, 0, &payload).await.unwrap();

            let attr_after = vfs.getattr(inode).unwrap();
            assert_eq!(
                attr_after.size,
                payload.len() as u64,
                "stage-first full-block write must advance the live inode size immediately"
            );
            assert_eq!(
                vfs.meta().lookup_logical_payload_slices(inode).unwrap().len(),
                0,
                "stage-first full-block write should not commit payload slices before an explicit flush"
            );
            assert!(
                vfs.inode_has_local_dirty_state(inode),
                "stage-first full-block write must retain local dirty state until flush commits metadata"
            );
            assert_eq!(
                vfs.pending_uploads_for_inode(inode).len(),
                0,
                "upload intent should not be queued until the staged write crosses the flush barrier"
            );

            let read = vfs.read(inode, 0, payload.len() as u32).await.unwrap();
            assert_eq!(read.as_ref(), payload.as_slice());

            vfs.flush(inode).await.unwrap();
            assert_eq!(
                vfs.meta().lookup_logical_payload_slices(inode).unwrap().len(),
                1,
                "flush should materialize the staged full block into unpublished metadata"
            );
            assert_eq!(
                vfs.pending_uploads_for_inode(inode).len(),
                1,
                "flush should queue the upload intent for the materialized block"
            );
        });
        rt.shutdown_background();
    }

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            assert_eq!(
                vfs.pending_uploads_for_inode(inode).len(),
                1,
                "restart should recover the flushed upload journal entry"
            );
            let attr_after = vfs.getattr(inode).unwrap();
            assert_eq!(attr_after.size, payload.len() as u64);
            assert_eq!(
                vfs.meta()
                    .lookup_logical_payload_slices(inode)
                    .unwrap()
                    .len(),
                1
            );

            let read = vfs.read(inode, 0, payload.len() as u32).await.unwrap();
            assert_eq!(
                read.as_ref(),
                payload.as_slice(),
                "restart should recover eager full-block bytes from committed local state"
            );
        });
        rt.shutdown_background();
    }
}

#[test]
fn writer_recovers_local_file_after_restart_before_upload_complete() {
    type EnvRestore = ScopedEnvVar;

    let _restore_dirty = EnvRestore::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = EnvRestore::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_disable = EnvRestore::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");

    unsafe {
        // Force spill path and avoid background drain so acceptance is WAL-backed.
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "16MB");
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("writer-restart-before-upload-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let payload = vec![0xB2u8; 2 * 1024 * 1024];
    let mut inode = 0u64;

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            let attr = vfs.create(1, b"restart-local.bin", 0, 0, 0o644).unwrap();
            inode = attr.ino;
            vfs.write(inode, 0, &payload).await.unwrap();
            vfs.flush(inode).await.unwrap();
            let read = vfs.read(inode, 0, payload.len() as u32).await.unwrap();
            assert_eq!(
                read.as_ref(),
                payload.as_slice(),
                "writer should read accepted content before upload completion"
            );
        });
        rt.shutdown_background();
    }

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta,
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            let read = vfs.read(inode, 0, payload.len() as u32).await.unwrap();
            assert_eq!(
                read.as_ref(),
                payload.as_slice(),
                "restarted instance should recover from WAL-backed ingress"
            );
        });
        rt.shutdown_background();
    }
}

#[test]
fn graceful_shutdown_remount_recovers_new_unpublished_file_before_publish() {
    let _env = ScopedTestEnv::new();
    install_zeroio_replay_offline_env();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("graceful-remount-new-file-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let payload = vec![0x7Bu8; 512 * 1024];
    let ino = {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let ino = rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let attr = vfs
                .create(1, b"graceful-remount-new.bin", 0, 0, 0o644)
                .unwrap();
            vfs.write(attr.ino, 0, &payload).await.unwrap();
            vfs.flush(attr.ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(10), || {
                    !vfs.pending_uploads_for_inode(attr.ino).is_empty()
                })
                .await,
                "offline setup should leave pending uploads before shutdown"
            );

            tokio::time::timeout(Duration::from_secs(5), vfs.shutdown(Duration::ZERO))
                .await
                .expect("shutdown should stay bounded");
            attr.ino
        });
        rt.shutdown_background();
        ino
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let reopened = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        reopened
            .start()
            .expect("failed to start reopened StudioVfs background workers");

        let read = reopened.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            payload.as_slice(),
            "graceful remount must restore local visibility before publish completes"
        );
        assert!(
            wait_until(Duration::from_secs(10), || {
                !reopened.pending_uploads_for_inode(ino).is_empty()
            })
            .await,
            "graceful remount must rebuild upload work from durable local state"
        );
    });
    rt.shutdown_background();
}

#[test]
fn graceful_shutdown_remount_preserves_newer_local_epoch_over_published_base() {
    let _env = ScopedTestEnv::new();
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let online_info = make_format_info("graceful-remount-overwrite-base");
    let offline_info =
        make_format_info_with_endpoint("graceful-remount-overwrite-base", "http://127.0.0.1:59999");
    let data_config_offline = DataStoreConfig::from(&offline_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&online_info).unwrap();

    install_zeroio_replay_offline_env();

    let baseline_payload = vec![0x31u8; 256 * 1024];
    let replacement_payload = vec![0x92u8; 256 * 1024];
    let ino = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let ino = rt.block_on(async {
            let (seeded_ino, _) = seed_committed_file(
                &meta,
                &online_info,
                b"graceful-remount-overwrite.bin",
                &baseline_payload,
            )
            .await;
            seeded_ino
        });
        rt.shutdown_background();
        ino
    };

    meta.set_format_info(&offline_info).unwrap();
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            vfs.write(ino, 0, &replacement_payload).await.unwrap();
            vfs.flush(ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(10), || {
                    vfs.pending_uploads_for_inode(ino).len() == 1
                })
                .await,
                "offline overwrite should leave one pending upload before shutdown"
            );

            let read = vfs
                .read(ino, 0, replacement_payload.len() as u32)
                .await
                .unwrap();
            assert_eq!(read.as_ref(), replacement_payload.as_slice());

            tokio::time::timeout(Duration::from_secs(5), vfs.shutdown(Duration::ZERO))
                .await
                .expect("shutdown should stay bounded");
        });
        rt.shutdown_background();
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let reopened = StudioVfs::new(
            meta.clone(),
            &data_config_offline,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        reopened
            .start()
            .expect("failed to start reopened StudioVfs background workers");

        let read = reopened
            .read(ino, 0, replacement_payload.len() as u32)
            .await
            .unwrap();
        assert_eq!(
            read.as_ref(),
            replacement_payload.as_slice(),
            "writer remount must see the newer unpublished epoch over the published base"
        );
        assert!(
            wait_until(Duration::from_secs(10), || {
                reopened.pending_uploads_for_inode(ino).len() == 1
            })
            .await,
            "overwrite remount must rebuild exactly the latest unpublished upload work"
        );
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn repeated_graceful_restarts_during_upload_replay_are_idempotent() {
    let _env = ScopedTestEnv::new();

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let online_info = make_format_info("graceful-restart-idempotent");
    let offline_info =
        make_format_info_with_endpoint("graceful-restart-idempotent", "http://127.0.0.1:59999");
    let data_config_offline = DataStoreConfig::from(&offline_info);
    let data_config_online = DataStoreConfig::from(&online_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&offline_info).unwrap();

    install_zeroio_replay_offline_env();
    unsafe {
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "60000");
    }

    let payload = vec![0xA5u8; 512 * 1024];
    let ino = {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let ino = rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let attr = vfs
                .create(1, b"graceful-restart-idempotent.bin", 0, 0, 0o644)
                .unwrap();
            vfs.write(attr.ino, 0, &payload).await.unwrap();
            vfs.flush(attr.ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(10), || {
                    !vfs.pending_uploads_for_inode(attr.ino).is_empty()
                })
                .await,
                "offline setup should create pending upload work before shutdown"
            );

            tokio::time::timeout(Duration::from_secs(5), vfs.shutdown(Duration::ZERO))
                .await
                .expect("shutdown should stay bounded");
            attr.ino
        });
        rt.shutdown_background();
        ino
    };

    let mut pending_snapshots = Vec::new();
    for _ in 0..2 {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let snapshot = rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let read = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
            assert_eq!(read.as_ref(), payload.as_slice());

            assert!(
                wait_until(Duration::from_secs(10), || {
                    !vfs.pending_uploads_for_inode(ino).is_empty()
                })
                .await,
                "each replay pass should rebuild upload work"
            );
            let snapshot = vfs.pending_uploads_for_inode(ino);

            tokio::time::timeout(Duration::from_secs(5), vfs.shutdown(Duration::ZERO))
                .await
                .expect("shutdown should stay bounded");
            snapshot
        });
        rt.shutdown_background();
        pending_snapshots.push(snapshot);
    }

    assert_eq!(
        pending_snapshots[0], pending_snapshots[1],
        "repeated graceful restarts must replay the same latest unfinished epoch without duplicating upload accounting"
    );

    install_zeroio_replay_online_env("0");
    meta.set_format_info(&online_info).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let reopened = StudioVfs::new(
            meta.clone(),
            &data_config_online,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        reopened
            .start()
            .expect("failed to start reopened StudioVfs background workers");

        assert!(
            wait_until(Duration::from_secs(20), || {
                reopened.pending_uploads_for_inode(ino).is_empty()
                    && meta
                        .get_attr(ino)
                        .map(|attr| {
                            attr.upload_state == UploadState::Ready
                                && attr.committed_content_epoch == attr.content_epoch
                        })
                        .unwrap_or(false)
            })
            .await,
            "after repeated replay, publish should still complete exactly once for the latest epoch"
        );
    });
    rt.shutdown_background();
}

#[test]
fn remote_clients_see_uploading_marker_without_precise_progress() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "16MB");
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let writer_tmp = tempfile::tempdir().unwrap();
    let writer_db = writer_tmp.path().join("writer.redb");
    let writer_cache = writer_tmp.path().join("writer-cache");
    let writer_cache_str = writer_cache.to_str().unwrap().to_string();
    let remote_tmp = tempfile::tempdir().unwrap();
    let remote_db = remote_tmp.path().join("remote.redb");
    let remote_cache = remote_tmp.path().join("remote-cache");
    let remote_cache_str = remote_cache.to_str().unwrap().to_string();
    let volume_suffix = format!("pr08-remote-uploading-marker-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let writer_meta = Arc::new(RedbMetaEngine::open(&writer_db).unwrap());
    writer_meta.init_root(0, 0).unwrap();
    writer_meta.set_format_info(&format_info).unwrap();
    let remote_meta = Arc::new(RedbMetaEngine::open(&remote_db).unwrap());
    remote_meta.init_root(0, 0).unwrap();
    remote_meta.set_format_info(&format_info).unwrap();

    let payload = vec![0x11u8; 512 * 1024];

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let writer_vfs = make_vfs_with_meta(writer_meta.clone(), &format_info, &writer_cache_str);
        let attr = writer_vfs
            .create(1, b"uploading-remote.txt", 0, 0, 0o644)
            .unwrap();
        writer_vfs.write(attr.ino, 0, &payload).await.unwrap();
        writer_vfs.flush(attr.ino).await.unwrap();

        import_remote_snapshot(writer_meta.as_ref(), remote_meta.as_ref(), &format_info);
        let remote_vfs = make_vfs_with_meta(remote_meta.clone(), &format_info, &remote_cache_str);
        assert!(matches!(
            remote_vfs.lookup(1, b"uploading-remote.txt"),
            Err(VfsError::Meta(MetaError::EntryNotFound { .. }))
        ));
    });
    rt.shutdown_background();
}

#[test]
fn remote_open_on_uploading_new_file_returns_transient_unavailable() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "16MB");
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let writer_tmp = tempfile::tempdir().unwrap();
    let writer_db = writer_tmp.path().join("writer.redb");
    let writer_cache = writer_tmp.path().join("writer-cache");
    let writer_cache_str = writer_cache.to_str().unwrap().to_string();
    let remote_tmp = tempfile::tempdir().unwrap();
    let remote_db = remote_tmp.path().join("remote.redb");
    let remote_cache = remote_tmp.path().join("remote-cache");
    let remote_cache_str = remote_cache.to_str().unwrap().to_string();
    let volume_suffix = format!("pr08-remote-open-uploading-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let writer_meta = Arc::new(RedbMetaEngine::open(&writer_db).unwrap());
    writer_meta.init_root(0, 0).unwrap();
    writer_meta.set_format_info(&format_info).unwrap();
    let remote_meta = Arc::new(RedbMetaEngine::open(&remote_db).unwrap());
    remote_meta.init_root(0, 0).unwrap();
    remote_meta.set_format_info(&format_info).unwrap();

    let payload = vec![0x22u8; 256 * 1024];

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let writer_vfs = make_vfs_with_meta(writer_meta.clone(), &format_info, &writer_cache_str);
        let attr = writer_vfs
            .create(1, b"remote-open-uploading.txt", 0, 0, 0o644)
            .unwrap();
        writer_vfs.write(attr.ino, 0, &payload).await.unwrap();
        writer_vfs.flush(attr.ino).await.unwrap();

        import_remote_snapshot(writer_meta.as_ref(), remote_meta.as_ref(), &format_info);
        let remote_vfs = make_vfs_with_meta(remote_meta.clone(), &format_info, &remote_cache_str);
        assert!(matches!(
            remote_vfs.lookup(1, b"remote-open-uploading.txt"),
            Err(VfsError::Meta(MetaError::EntryNotFound { .. }))
        ));
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn local_reads_do_not_surface_remote_pending_during_final_upload_transition() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_bucket = ScopedEnvVar::capture("STUDIOFS_S3_BUCKET");
    let _restore_access_key = ScopedEnvVar::capture("STUDIOFS_S3_ACCESS_KEY");
    let _restore_secret_key = ScopedEnvVar::capture("STUDIOFS_S3_SECRET_KEY");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("sfs9-local-finalize-{}", std::process::id());
    let mut format_info = make_format_info(&volume_suffix);
    format_info.block_size = 512 * 1024;
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let payload = vec![0x5Au8; (format_info.block_size as usize) * 4];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");
        let attr = vfs
            .create(1, b"sfs9-local-finalize.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
            })
            .await,
            "test setup requires queued uploads for the new file"
        );

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match vfs.open_file(attr.ino, OpenFlags::RDONLY, false) {
                Ok(fh) => {
                    vfs.release_fh(fh).await.unwrap();
                }
                Err(VfsError::RemoteContentPending) => {
                    panic!("local open_file must not fail with RemoteContentPending");
                }
                Err(err) => {
                    panic!("local open_file failed during upload finalization: {err}");
                }
            }

            match vfs.read(attr.ino, 0, payload.len() as u32).await {
                Ok(bytes) => assert_eq!(bytes.as_ref(), payload.as_slice()),
                Err(VfsError::RemoteContentPending) => {
                    panic!("local read must not fail with RemoteContentPending");
                }
                Err(err) => {
                    panic!("local read failed during upload finalization: {err}");
                }
            }

            let current_attr = vfs.getattr(attr.ino).unwrap();
            if vfs.pending_uploads_for_inode(attr.ino).is_empty()
                && current_attr.upload_state == UploadState::Ready
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "upload pipeline did not stabilize before timeout"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn remote_clients_keep_previous_committed_version_during_update() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let writer_tmp = tempfile::tempdir().unwrap();
    let writer_db = writer_tmp.path().join("writer.redb");
    let writer_cache = writer_tmp.path().join("writer-cache");
    let writer_cache_str = writer_cache.to_str().unwrap().to_string();
    let remote_tmp = tempfile::tempdir().unwrap();
    let remote_db = remote_tmp.path().join("remote.redb");
    let remote_cache = remote_tmp.path().join("remote-cache");
    let remote_cache_str = remote_cache.to_str().unwrap().to_string();
    let volume_suffix = format!("pr08-remote-old-committed-{}", std::process::id());
    let online_info = make_format_info(&volume_suffix);
    let offline_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let writer_meta = Arc::new(RedbMetaEngine::open(&writer_db).unwrap());
    writer_meta.init_root(0, 0).unwrap();
    writer_meta.set_format_info(&online_info).unwrap();
    let remote_meta = Arc::new(RedbMetaEngine::open(&remote_db).unwrap());
    remote_meta.init_root(0, 0).unwrap();
    remote_meta.set_format_info(&online_info).unwrap();

    let block_size = online_info.block_size as usize;
    let baseline_payload = vec![0x33u8; block_size];
    let replacement_payload = vec![0x44u8; block_size];
    let mut ino = 0u64;

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (seeded_ino, _) = seed_committed_file(
                &writer_meta,
                &online_info,
                b"remote-old.bin",
                &baseline_payload,
            )
            .await;
            ino = seeded_ino;
        });
        rt.shutdown_background();
    }

    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let writer_vfs = make_vfs_with_meta(writer_meta.clone(), &offline_info, &writer_cache_str);
        writer_vfs
            .write(ino, 0, &replacement_payload)
            .await
            .unwrap();
        writer_vfs.flush(ino).await.unwrap();

        import_remote_snapshot(writer_meta.as_ref(), remote_meta.as_ref(), &online_info);
        let remote_vfs = make_vfs_with_meta(remote_meta.clone(), &online_info, &remote_cache_str);
        let remote_attr = remote_vfs.lookup(1, b"remote-old.bin").unwrap();
        assert_eq!(remote_attr.upload_state, UploadState::Updating);
        assert_eq!(remote_attr.committed_size, baseline_payload.len() as u64);
        let read = remote_vfs
            .read(remote_attr.ino, 0, baseline_payload.len() as u32)
            .await
            .unwrap();
        assert_eq!(
            read.as_ref(),
            baseline_payload.as_slice(),
            "remote client must keep reading committed content while writer update is pending"
        );
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn atomic_switch_to_new_incarnation_after_full_commit() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_bucket = ScopedEnvVar::capture("STUDIOFS_S3_BUCKET");
    let _restore_access_key = ScopedEnvVar::capture("STUDIOFS_S3_ACCESS_KEY");
    let _restore_secret_key = ScopedEnvVar::capture("STUDIOFS_S3_SECRET_KEY");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "250");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let writer_tmp = tempfile::tempdir().unwrap();
    let writer_db = writer_tmp.path().join("writer.redb");
    let writer_cache = writer_tmp.path().join("writer-cache");
    let writer_cache_str = writer_cache.to_str().unwrap().to_string();
    let remote_tmp = tempfile::tempdir().unwrap();
    let remote_db = remote_tmp.path().join("remote.redb");
    let remote_cache = remote_tmp.path().join("remote-cache");
    let remote_cache_str = remote_cache.to_str().unwrap().to_string();
    let volume_suffix = format!("pr08-atomic-switch-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let writer_meta = Arc::new(RedbMetaEngine::open(&writer_db).unwrap());
    writer_meta.init_root(0, 0).unwrap();
    writer_meta.set_format_info(&format_info).unwrap();
    let remote_meta = Arc::new(RedbMetaEngine::open(&remote_db).unwrap());
    remote_meta.init_root(0, 0).unwrap();
    remote_meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let baseline_payload = vec![0x55u8; block_size * 2];
    let mut replacement_payload = vec![0x66u8; block_size * 2];
    replacement_payload[block_size..].fill(0x77);
    let mut ino = 0u64;
    let mut baseline_blocks = Vec::new();

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (seeded_ino, seeded_blocks) = seed_committed_file(
                &writer_meta,
                &format_info,
                b"atomic-switch.bin",
                &baseline_payload,
            )
            .await;
            ino = seeded_ino;
            baseline_blocks = seeded_blocks;
        });
        rt.shutdown_background();
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let writer_vfs = make_vfs_with_meta(writer_meta.clone(), &format_info, &writer_cache_str);
        writer_vfs.write(ino, 0, &replacement_payload).await.unwrap();
        writer_vfs.flush(ino).await.unwrap();

        import_remote_snapshot(writer_meta.as_ref(), remote_meta.as_ref(), &format_info);
        {
            let remote_vfs = make_vfs_with_meta_without_background_workers(
                remote_meta.clone(),
                &format_info,
                &remote_cache_str,
            );
            let remote_attr = remote_vfs.lookup(1, b"atomic-switch.bin").unwrap();
            assert_eq!(remote_attr.upload_state, UploadState::Updating);
            let remote_blocks_before = remote_meta.lookup_logical_payload_slices(ino).unwrap();
            assert_eq!(remote_blocks_before, baseline_blocks);
            let read_before = remote_vfs
                .read(ino, 0, baseline_payload.len() as u32)
                .await
                .unwrap();
            assert_eq!(read_before.as_ref(), baseline_payload.as_slice());
        }

        assert!(
            wait_until(Duration::from_secs(60), || {
                writer_vfs.pending_uploads_for_inode(ino).is_empty()
                    && writer_meta
                        .get_attr(ino)
                        .map(|attr| attr.upload_state == UploadState::Ready)
                        .unwrap_or(false)
            })
            .await,
            "writer should publish the new incarnation after the full upload set completes"
        );

        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            import_remote_snapshot(writer_meta.as_ref(), remote_meta.as_ref(), &format_info);
            let remote_vfs = make_vfs_with_meta_without_background_workers(
                remote_meta.clone(),
                &format_info,
                &remote_cache_str,
            );
            let remote_attr_after = remote_vfs.lookup(1, b"atomic-switch.bin").unwrap();
            let remote_blocks_after = remote_meta.lookup_logical_payload_slices(ino).unwrap();
            let read_after = remote_vfs
                .read(ino, 0, replacement_payload.len() as u32)
                .await
                .unwrap();

            if remote_attr_after.upload_state == UploadState::Ready
                && remote_blocks_after.len() == 2
                && remote_blocks_after != baseline_blocks
                && read_after.as_ref() == replacement_payload.as_slice()
            {
                break;
            }

            assert!(
                Instant::now() < deadline,
                "remote view should switch atomically from old committed incarnation to the fully published new incarnation"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn remote_snapshot_reads_nested_file_after_temp_rename_publish() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_bucket = ScopedEnvVar::capture("STUDIOFS_S3_BUCKET");
    let _restore_access_key = ScopedEnvVar::capture("STUDIOFS_S3_ACCESS_KEY");
    let _restore_secret_key = ScopedEnvVar::capture("STUDIOFS_S3_SECRET_KEY");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let writer_tmp = tempfile::tempdir().unwrap();
    let writer_db = writer_tmp.path().join("writer.redb");
    let writer_cache = writer_tmp.path().join("writer-cache");
    let writer_cache_str = writer_cache.to_str().unwrap().to_string();
    let remote_tmp = tempfile::tempdir().unwrap();
    let remote_db = remote_tmp.path().join("remote.redb");
    let remote_cache = remote_tmp.path().join("remote-cache");
    let remote_cache_str = remote_cache.to_str().unwrap().to_string();
    let volume_suffix = format!("nested-temp-rename-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let writer_meta = Arc::new(RedbMetaEngine::open(&writer_db).unwrap());
    writer_meta.init_root(0, 0).unwrap();
    writer_meta.set_format_info(&format_info).unwrap();
    let remote_meta = Arc::new(RedbMetaEngine::open(&remote_db).unwrap());
    remote_meta.init_root(0, 0).unwrap();
    remote_meta.set_format_info(&format_info).unwrap();

    let payload = vec![0x91u8; (format_info.block_size as usize) + 64 * 1024];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let writer_vfs = make_vfs_with_meta(writer_meta.clone(), &format_info, &writer_cache_str);
        let dir = writer_vfs.mkdir(1, b"nested", 0, 0, 0o755).unwrap();
        let temp = writer_vfs
            .create(dir.ino, b"temp.bin", 0, 0, 0o644)
            .unwrap();
        writer_vfs.write(temp.ino, 0, &payload).await.unwrap();
        writer_vfs.flush(temp.ino).await.unwrap();
        writer_vfs
            .rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
            .unwrap();

        assert!(
            wait_until(Duration::from_secs(60), || {
                writer_vfs.pending_uploads_for_inode(temp.ino).is_empty()
                    && writer_meta
                        .get_attr(temp.ino)
                        .map(|attr| {
                            attr.upload_state == UploadState::Ready
                                && attr.size == payload.len() as u64
                                && attr.committed_size == payload.len() as u64
                        })
                        .unwrap_or(false)
            })
            .await,
            "writer should publish nested renamed file before snapshot import"
        );

        import_remote_snapshot(writer_meta.as_ref(), remote_meta.as_ref(), &format_info);
        let remote_vfs = make_vfs_with_meta(remote_meta.clone(), &format_info, &remote_cache_str);
        let remote_dir = remote_vfs.lookup(1, b"nested").unwrap();
        let remote_attr = remote_vfs.lookup(remote_dir.ino, b"final.bin").unwrap();
        assert_eq!(remote_attr.ino, temp.ino);
        assert_eq!(remote_attr.size, payload.len() as u64);
        assert_eq!(remote_attr.committed_size, payload.len() as u64);

        let read = remote_vfs
            .read(remote_attr.ino, 0, payload.len() as u32)
            .await
            .unwrap();
        assert_eq!(read.as_ref(), payload.as_slice());
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn remote_snapshot_switches_nested_overwrite_to_replacement_inode() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_bucket = ScopedEnvVar::capture("STUDIOFS_S3_BUCKET");
    let _restore_access_key = ScopedEnvVar::capture("STUDIOFS_S3_ACCESS_KEY");
    let _restore_secret_key = ScopedEnvVar::capture("STUDIOFS_S3_SECRET_KEY");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let writer_tmp = tempfile::tempdir().unwrap();
    let writer_db = writer_tmp.path().join("writer.redb");
    let writer_cache = writer_tmp.path().join("writer-cache");
    let writer_cache_str = writer_cache.to_str().unwrap().to_string();
    let remote_tmp = tempfile::tempdir().unwrap();
    let remote_db = remote_tmp.path().join("remote.redb");
    let remote_cache = remote_tmp.path().join("remote-cache");
    let remote_cache_str = remote_cache.to_str().unwrap().to_string();
    let volume_suffix = format!("nested-overwrite-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let writer_meta = Arc::new(RedbMetaEngine::open(&writer_db).unwrap());
    writer_meta.init_root(0, 0).unwrap();
    writer_meta.set_format_info(&format_info).unwrap();
    let remote_meta = Arc::new(RedbMetaEngine::open(&remote_db).unwrap());
    remote_meta.init_root(0, 0).unwrap();
    remote_meta.set_format_info(&format_info).unwrap();

    let baseline_payload = vec![0x92u8; format_info.block_size as usize];
    let replacement_payload = vec![0x93u8; (format_info.block_size as usize) + 32 * 1024];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let writer_vfs = make_vfs_with_meta(writer_meta.clone(), &format_info, &writer_cache_str);
        let dir = writer_vfs.mkdir(1, b"shared", 0, 0, 0o755).unwrap();
        let baseline = writer_vfs
            .create(dir.ino, b"final.bin", 0, 0, 0o644)
            .unwrap();
        writer_vfs
            .write(baseline.ino, 0, &baseline_payload)
            .await
            .unwrap();
        writer_vfs.flush(baseline.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(60), || {
                writer_vfs
                    .pending_uploads_for_inode(baseline.ino)
                    .is_empty()
                    && writer_meta
                        .get_attr(baseline.ino)
                        .map(|attr| attr.upload_state == UploadState::Ready)
                        .unwrap_or(false)
            })
            .await,
            "baseline nested file should become committed before overwrite"
        );

        import_remote_snapshot(writer_meta.as_ref(), remote_meta.as_ref(), &format_info);
        let replacement = writer_vfs
            .create(dir.ino, b"temp.bin", 0, 0, 0o644)
            .unwrap();
        writer_vfs
            .write(replacement.ino, 0, &replacement_payload)
            .await
            .unwrap();
        writer_vfs.flush(replacement.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(60), || {
                writer_vfs
                    .pending_uploads_for_inode(replacement.ino)
                    .is_empty()
                    && writer_meta
                        .get_attr(replacement.ino)
                        .map(|attr| attr.upload_state == UploadState::Ready)
                        .unwrap_or(false)
            })
            .await,
            "replacement temp should become committed before overwrite rename"
        );

        writer_vfs
            .rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
            .unwrap();

        import_remote_snapshot(writer_meta.as_ref(), remote_meta.as_ref(), &format_info);
        let remote_vfs = make_vfs_with_meta(remote_meta.clone(), &format_info, &remote_cache_str);
        let remote_dir = remote_vfs.lookup(1, b"shared").unwrap();
        let remote_attr = remote_vfs.lookup(remote_dir.ino, b"final.bin").unwrap();
        assert_eq!(remote_attr.ino, replacement.ino);
        assert_ne!(remote_attr.ino, baseline.ino);
        assert_eq!(remote_attr.size, replacement_payload.len() as u64);
        assert_eq!(remote_attr.committed_size, replacement_payload.len() as u64);

        let read = remote_vfs
            .read(remote_attr.ino, 0, replacement_payload.len() as u32)
            .await
            .unwrap();
        assert_eq!(read.as_ref(), replacement_payload.as_slice());
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn remote_attr_cache_refreshes_after_nested_publish_sync() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _restore_sync = ScopedEnvVar::capture("STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS");
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS", "50");
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "250");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let writer_tmp = tempfile::tempdir().unwrap();
    let writer_db = writer_tmp.path().join("meta-a.redb");
    let writer_cache = writer_tmp.path().join("writer-cache");
    let writer_cache_str = writer_cache.to_str().unwrap().to_string();
    let remote_tmp = tempfile::tempdir().unwrap();
    let remote_db = remote_tmp.path().join("meta-b.redb");
    let remote_cache = remote_tmp.path().join("remote-cache");
    let remote_cache_str = remote_cache.to_str().unwrap().to_string();
    let volume = PgTestVolume::new(&dsn, "vfs-nested-attr-invalidation");
    let mut info = volume.format_info();
    let s3 = test_online_s3_env();
    info.endpoint = s3.endpoint.clone();
    info.bucket = s3.bucket.clone();
    info.access_key = s3.access_key.clone();
    info.secret_key = s3.secret_key.clone();
    seed_local(&writer_db, &info);
    seed_local(&remote_db, &info);

    let writer_meta = PgMetaEngine::open(&writer_db).expect("open writer pg meta");
    let remote_meta = PgMetaEngine::open(&remote_db).expect("open remote pg meta");
    let writer_meta_dyn: Arc<dyn MetaEngine> = writer_meta.clone();
    let remote_meta_dyn: Arc<dyn MetaEngine> = remote_meta.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        ensure_bucket().await;

        let writer_vfs = make_vfs_from_meta(writer_meta_dyn, &info, &writer_cache_str);
        let remote_vfs = make_vfs_from_meta(remote_meta_dyn, &info, &remote_cache_str);
        let spy = Arc::new(SpyInvalidator::default());
        remote_vfs.set_kernel_invalidator(spy.clone());

        let payload = vec![0xA4u8; 512 * 1024];
        let dir = writer_vfs.mkdir(ROOT_INO, b"nested", 0, 0, 0o755).unwrap();
        let file = writer_vfs
            .create(dir.ino, b"remote.bin", 0, 0, 0o644)
            .unwrap();
        writer_vfs.flush(file.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(12), || {
                remote_vfs.lookup(ROOT_INO, b"nested").is_ok()
            })
            .await,
            "remote peer should see nested dir"
        );
        let remote_dir = remote_vfs.lookup(ROOT_INO, b"nested").unwrap();

        assert!(
            wait_until(Duration::from_secs(12), || {
                matches!(
                    remote_vfs.lookup(remote_dir.ino, b"remote.bin"),
                    Err(VfsError::Meta(MetaError::EntryNotFound { .. }))
                )
            })
            .await,
            "remote peer should keep the new file hidden before first publish"
        );

        spy.clear();

        writer_vfs.write(file.ino, 0, &payload).await.unwrap();
        writer_vfs.flush(file.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(20), || {
                writer_vfs.pending_uploads_for_inode(file.ino).is_empty()
                    && writer_meta
                        .get_attr(file.ino)
                        .map(|attr| attr.upload_state == UploadState::Ready)
                        .unwrap_or(false)
            })
            .await,
            "writer should finish publishing nested file"
        );

        assert!(
            wait_until(Duration::from_secs(12), || {
                remote_meta
                    .get_attr(file.ino)
                    .map(|attr| attr.size == payload.len() as u64)
                    .unwrap_or(false)
                    && spy.snapshot().contains(&file.ino)
            })
            .await,
            "remote metadata watcher should sync published size and invalidate the inode"
        );

        let refreshed = remote_vfs.getattr(file.ino).expect("refreshed remote attr");
        assert_eq!(
            refreshed.size,
            payload.len() as u64,
            "remote VFS attr cache must refresh after a pure metadata publish"
        );
        assert_eq!(refreshed.committed_size, payload.len() as u64);

        writer_vfs.shutdown(Duration::from_secs(15)).await;
        remote_vfs.shutdown(Duration::from_secs(15)).await;
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_backed_flock_blocks_conflicting_peer_until_unlock() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _restore_sync = ScopedEnvVar::capture("STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS");
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS", "50");
    }

    let writer_tmp = tempfile::tempdir().unwrap();
    let writer_db = writer_tmp.path().join("meta-a.redb");
    let writer_cache = writer_tmp.path().join("writer-cache");
    let writer_cache_str = writer_cache.to_str().unwrap().to_string();
    let remote_tmp = tempfile::tempdir().unwrap();
    let remote_db = remote_tmp.path().join("meta-b.redb");
    let remote_cache = remote_tmp.path().join("remote-cache");
    let remote_cache_str = remote_cache.to_str().unwrap().to_string();
    let volume = PgTestVolume::new(&dsn, "vfs-pg-flock");
    let mut info = volume.format_info();
    let s3 = test_online_s3_env();
    info.endpoint = s3.endpoint.clone();
    info.bucket = s3.bucket.clone();
    info.access_key = s3.access_key.clone();
    info.secret_key = s3.secret_key.clone();
    seed_local(&writer_db, &info);
    seed_local(&remote_db, &info);

    let writer_meta = PgMetaEngine::open_with_options(
        &writer_db,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open writer pg meta");
    let remote_meta = PgMetaEngine::open_with_options(
        &remote_db,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open remote pg meta");
    let writer_meta_dyn: Arc<dyn MetaEngine> = writer_meta.clone();
    let remote_meta_dyn: Arc<dyn MetaEngine> = remote_meta.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        ensure_bucket().await;

        let writer_vfs = make_vfs_from_meta(writer_meta_dyn, &info, &writer_cache_str);
        let remote_vfs = make_vfs_from_meta(remote_meta_dyn, &info, &remote_cache_str);

        let file = writer_vfs
            .create(ROOT_INO, b"remote-flock.txt", 0, 0, 0o644)
            .unwrap();
        writer_vfs.flush(file.ino).await.unwrap();
        writer_meta
            .sync_metadata()
            .expect("writer sync metadata for remote flock test");
        let _ = remote_meta.sync_metadata();

        writer_vfs
            .flock(file.ino, LockKind::Exclusive, 1, false)
            .await
            .unwrap();

        let blocked = remote_vfs
            .flock(file.ino, LockKind::Exclusive, 2, false)
            .await;
        assert!(
            matches!(blocked, Err(VfsError::WouldBlock)),
            "peer exclusive flock should block while writer lease is held, got {blocked:?}"
        );

        writer_vfs
            .flock(file.ino, LockKind::Unlock, 1, false)
            .await
            .unwrap();
        remote_vfs
            .flock(file.ino, LockKind::Exclusive, 2, false)
            .await
            .unwrap();
        remote_vfs
            .flock(file.ino, LockKind::Unlock, 2, false)
            .await
            .unwrap();

        writer_vfs.shutdown(Duration::from_secs(15)).await;
        remote_vfs.shutdown(Duration::from_secs(15)).await;
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn test_crash_recovery_replays_local_committed_block_before_remote_fetch() {
    let _env = ScopedTestEnv::new();

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("crash-remote-fetch-{}", std::process::id());
    let baseline_payload = vec![0xC1u8; 512 * 1024];
    let replacement_payload = vec![0xC2u8; 512 * 1024];
    let mut ino = 0u64;
    let mut baseline_object_id = 0u64;
    let mut replacement_object_id = 0u64;
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    let online_endpoint = test_env_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:9000");
    let mut format_info = make_format_info_with_endpoint(&volume_suffix, &online_endpoint);
    let data_config_online = DataStoreConfig::from(&format_info);

    {
        meta.init_root(0, 0).unwrap();
        meta.set_format_info(&format_info).unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let online_vfs_config = VfsConfigRaw {
                max_dirty_bytes: Some(1),
                ..VfsConfigRaw::default()
            };
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_online,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                online_vfs_config
                    .validate()
                    .expect("test VFS config should validate"),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let attr = vfs
                .create(1, b"remote-fetch-block.bin", 0, 0, 0o644)
                .unwrap();
            ino = attr.ino;
            vfs.write(ino, 0, &baseline_payload).await.unwrap();
            vfs.flush(ino).await.unwrap();
            let baseline = wait_until_payload_slices(&vfs, ino, Duration::from_secs(15))
                .expect("expected committed baseline payload slices");
            baseline_object_id = baseline[0].1.object_id;
            let ds = DataStore::new(&data_config_online).unwrap();
            assert!(
                wait_until_object_exists(
                    &ds,
                    &format_info.data_prefix,
                    baseline_object_id,
                    Duration::from_secs(15),
                )
                .await,
                "baseline object should exist before replacement local-commit replay test"
            );
            vfs.shutdown(Duration::from_secs(15)).await;
        });
        rt.shutdown_background();
    }

    format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");

    let data_config_offline = DataStoreConfig::from(&format_info);
    {
        meta.init_root(0, 0).unwrap();
        meta.set_format_info(&format_info).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs_config = VfsConfigRaw {
                max_dirty_bytes: Some(8 * 1024 * 1024),
                ..VfsConfigRaw::default()
            };
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                vfs_config
                    .validate()
                    .expect("test VFS config should validate"),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            vfs.write(ino, 0, &replacement_payload).await.unwrap();
            vfs.flush(ino).await.unwrap();
            let blocks = vfs.meta().lookup_logical_payload_slices(ino).unwrap();
            assert_eq!(blocks.len(), 1);
            replacement_object_id = blocks[0].1.object_id;
            assert_ne!(replacement_object_id, baseline_object_id);
            let ds = DataStore::new(&data_config_online).unwrap();
            assert!(
                wait_until_object_absent(
                    &ds,
                    &make_format_info_with_endpoint(&volume_suffix, &online_endpoint).data_prefix,
                    replacement_object_id,
                    Duration::from_secs(2),
                )
                .await,
                "replacement should remain local-only before restart while the endpoint is offline"
            );
        });
        rt.shutdown_background();
    }

    {
        meta.init_root(0, 0).unwrap();
        meta.set_format_info(&format_info).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let offline_vfs_config = VfsConfigRaw {
                max_dirty_bytes: Some(1),
                ..VfsConfigRaw::default()
            };
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                offline_vfs_config.validate().expect("test VFS config should validate"),
            )
            .unwrap();
            vfs.start().expect("failed to start StudioVfs background workers");
            let blocks = vfs.meta().lookup_logical_payload_slices(ino).unwrap();
            assert_eq!(blocks.len(), 1);
            assert_eq!(blocks[0].1.object_id, replacement_object_id);
            let read = tokio::time::timeout(
                Duration::from_secs(10),
                vfs.read(ino, 0, replacement_payload.len() as u32),
            )
            .await
            .expect("read should complete from committed local cache state without remote fetch")
            .unwrap();
            assert_eq!(
                read.as_ref(),
                replacement_payload.as_slice(),
                "read after restart with offline endpoint should use committed block-store state; ingress_stats={:?} meta_blocks={:?}",
                vfs.ingress_stats(),
                vfs.meta().lookup_logical_payload_slices(ino).ok(),
            );
        });
        rt.shutdown_background();
    }
}

struct ScopedEnvVar {
    key: &'static str,
    old: Option<String>,
    _env_lock: ReentrantMutexGuard<'static, ()>,
}

impl ScopedEnvVar {
    fn capture(key: &'static str) -> Self {
        let env_lock = acquire_test_env_lock();
        Self {
            key,
            old: std::env::var(key).ok(),
            _env_lock: env_lock,
        }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        unsafe {
            if let Some(value) = &self.old {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
}

/// Blanket env-var isolation guard for VFS integration tests.
#[allow(dead_code)]
struct ScopedTestEnv {
    _guards: Vec<ScopedEnvVar>,
}

#[allow(dead_code)]
impl ScopedTestEnv {
    fn new() -> Self {
        const KEYS: &[&str] = &[
            "STUDIOFS_TEST_ONLINE_S3_ENDPOINT",
            "STUDIOFS_TEST_ONLINE_S3_BUCKET",
            "STUDIOFS_TEST_ONLINE_S3_ACCESS_KEY",
            "STUDIOFS_TEST_ONLINE_S3_SECRET_KEY",
            "STUDIOFS_S3_ENDPOINT",
            "STUDIOFS_S3_BUCKET",
            "STUDIOFS_S3_ACCESS_KEY",
            "STUDIOFS_S3_SECRET_KEY",
            "STUDIOFS_MAX_DIRTY_BYTES",
            "STUDIOFS_MAX_SPILL_BYTES",
            "STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH",
            "STUDIOFS_SLAB_FLUSH_INTERVAL_MS",
            "STUDIOFS_FOREGROUND_DRAIN_ASSIST",
            "STUDIOFS_UPLOAD_DELAY_MS",
            "STUDIOFS_UPLOAD_REQUEUE_BASE_MS",
            "STUDIOFS_UPLOAD_REQUEUE_MAX_MS",
            "STUDIOFS_UPLOAD_STATS_INTERVAL_SECS",
            "STUDIOFS_UPLOAD_WORKER_COUNT",
            "STUDIOFS_UPLOAD_MAX_SPAWNED",
            "STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS",
            "STUDIOFS_UPLOAD_MAX_ATTEMPTS",
            "STUDIOFS_UPLOAD_RETRY_BASE_MS",
            "STUDIOFS_UPLOAD_RETRY_MAX_MS",
            "STUDIOFS_UPLOAD_CACHE_IO_SLOTS",
            "STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS",
            "STUDIOFS_UPLOAD_INITIAL_WINDOW",
            "STUDIOFS_UPLOAD_MIN_WINDOW",
            "STUDIOFS_UPLOAD_MAX_WINDOW",
            "STUDIOFS_UPLOAD_INCREASE_STEP",
            "STUDIOFS_UPLOAD_DECREASE_FACTOR",
            "STUDIOFS_UPLOAD_DECREASE_COOLDOWN_SECS",
            "STUDIOFS_FETCH_MAX_ATTEMPTS",
            "STUDIOFS_PREFETCH_FETCH_MAX_ATTEMPTS",
            "STUDIOFS_FETCH_REQUEST_TIMEOUT_SECS",
            "STUDIOFS_FETCH_RETRY_BASE_MS",
            "STUDIOFS_FETCH_RETRY_MAX_MS",
            "STUDIOFS_FETCH_PREFETCH_RESERVE",
            "STUDIOFS_FETCH_PREFETCH_ACQUIRE_WAIT_MS",
            "STUDIOFS_FETCH_INITIAL_WINDOW",
            "STUDIOFS_FETCH_MIN_WINDOW",
            "STUDIOFS_FETCH_MAX_WINDOW",
            "STUDIOFS_FETCH_INCREASE_STEP",
            "STUDIOFS_FETCH_DECREASE_FACTOR",
            "STUDIOFS_FETCH_DECREASE_COOLDOWN_SECS",
            "STUDIOFS_QOS_ACCEPT_SLOTS",
            "STUDIOFS_QOS_MATERIALIZE_SLOTS",
            "STUDIOFS_QOS_COMMIT_SLOTS",
            "STUDIOFS_QOS_FETCH_SLOTS",
            "STUDIOFS_READ_BUFFER_MB",
            "STUDIOFS_PREFETCH_CONCURRENCY",
            "STUDIOFS_READAHEAD_WIDTH",
            "STUDIOFS_READAHEAD_MIN_PREFETCH",
            "STUDIOFS_READAHEAD_SEQ_STEP",
            "STUDIOFS_READAHEAD_RANDOM_PENALTY",
            "STUDIOFS_DIR_PREFETCH_MAX_BATCH",
            "STUDIOFS_READDIR_ATTR_BATCH_CHUNK",
            "STUDIOFS_READDIR_ATTR_BATCH_MAX_INODES",
            "STUDIOFS_PREFETCH_FOREGROUND_RESERVE",
            "STUDIOFS_PREFETCH_MAX_PER_CYCLE",
            "STUDIOFS_PREFETCH_PAUSE_ON_NETERR_MS",
            "STUDIOFS_MAX_DRAIN_BATCH",
            "STUDIOFS_JANITOR_INTERVAL_SECS",
            "STUDIOFS_JANITOR_SKIP_NEWER_SECS",
            "STUDIOFS_JANITOR_MAX_DELETES",
            "STUDIOFS_JANITOR_LEASE_TTL_SECS",
            "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS",
            "STUDIOFS_DENTRY_CACHE_CAPACITY",
            "STUDIOFS_DENTRY_CACHE_BYTES",
            "STUDIOFS_PATH_SHORTCUT_CACHE_CAPACITY",
            "STUDIOFS_PATH_SHORTCUT_CACHE_BYTES",
            "STUDIOFS_ATTR_READ_CACHE_CAPACITY",
            "STUDIOFS_ATTR_READ_CACHE_BYTES",
            "STUDIOFS_LOCK_TTL_SECS",
            "STUDIOFS_LOCK_RETRY_BASE_MS",
            "STUDIOFS_LOCK_RETRY_MAX_MS",
            "STUDIOFS_PUBLISH_VISIBILITY_TIMEOUT_SECS",
            "STUDIOFS_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS",
            "STUDIOFS_XATTR_MAX_BYTES",
        ];
        Self {
            _guards: KEYS.iter().map(|&k| ScopedEnvVar::capture(k)).collect(),
        }
    }
}

#[test]
fn new_empty_file_stays_idle_without_pending_incarnation() {
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    unsafe {
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr04-new-uploading-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs.create(1, b"pending-new.bin", 0, 0, 0o644).unwrap();
        let state = vfs.inode_incarnation_state(attr.ino).await.unwrap();
        assert_eq!(state.upload_state, VfsUploadState::Idle);
        let committed = state
            .committed_incarnation
            .expect("brand-new empty file should expose the zero-size committed base");
        assert_eq!(committed.size, 0);
        assert!(
            state.pending_incarnation.is_none(),
            "brand-new empty file must stay idle until it has local data or queued uploads"
        );
        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "brand-new empty file must not enqueue uploads before any write"
        );
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn committed_file_mutation_enters_updating_state() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "16MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "50");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr04-updating-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let baseline_payload = vec![0x41u8; 512 * 1024];
    let patch = vec![0x42u8; 4096];
    let mut ino = 0u64;

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            let attr = vfs.create(1, b"updating.bin", 0, 0, 0o644).unwrap();
            ino = attr.ino;
            vfs.write(ino, 0, &baseline_payload).await.unwrap();
            vfs.flush(ino).await.unwrap();
            wait_until_payload_slices(&vfs, ino, Duration::from_secs(30))
                .expect("baseline file must reach committed payload slices before mutation");
            assert!(
                wait_until(Duration::from_secs(30), || {
                    meta.get_attr(ino)
                        .map(|attr| {
                            attr.upload_state == UploadState::Ready
                                && attr.committed_size == baseline_payload.len() as u64
                        })
                        .unwrap_or(false)
                })
                .await,
                "baseline file must become committed before entering updating state"
            );
        });
        rt.shutdown_background();
    }

    unsafe {
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "60000");
    }

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            vfs.write(ino, 0, &patch).await.unwrap();

            let state = vfs.inode_incarnation_state(ino).await.unwrap();
            assert_eq!(state.upload_state, VfsUploadState::Updating);
            let committed = state
                .committed_incarnation
                .expect("committed file must retain committed incarnation during local mutation");
            let pending = state
                .pending_incarnation
                .expect("mutated committed file must expose a pending incarnation");
            assert_eq!(committed.size, baseline_payload.len() as u64);
            assert_eq!(pending.size, baseline_payload.len() as u64);
            assert_eq!(pending.base_size, committed.size);
            assert!(
                pending.content_epoch > committed.content_epoch,
                "updating state must advance into a newer pending incarnation"
            );
        });
        rt.shutdown_background();
    }
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn published_file_returns_to_idle_state_and_invalidates_cached_attr_immediately() {
    let _restore_sync = ScopedEnvVar::capture("STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS");
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS", "60000");
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "200");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr04-post-publish-idle-{}", std::process::id());
    let mut format_info = make_format_info(&volume_suffix);
    format_info.block_size = 512 * 1024;
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let payload = vec![0x5Au8; format_info.block_size as usize * 2];
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = make_vfs_with_meta(meta.clone(), &format_info, &cache_dir_str);
        let spy = Arc::new(SpyInvalidator::default());
        vfs.set_kernel_invalidator(spy.clone());

        let attr = vfs
            .create(1, b"post-publish-idle.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
            })
            .await,
            "upload queue must populate before caching the uploading attr"
        );

        let cached = vfs.getattr(attr.ino).unwrap();
        assert_ne!(cached.upload_state, UploadState::Ready);
        assert!(
            cached.committed_size < cached.size,
            "seeded attr cache must still reflect a pending publish"
        );
        spy.clear();

        assert!(
            wait_until(Duration::from_secs(20), || {
                meta.get_attr(attr.ino)
                    .map(|current| current.upload_state == UploadState::Ready)
                    .unwrap_or(false)
            })
            .await,
            "metadata must publish the inode before validation"
        );

        assert!(
            spy.snapshot().contains(&attr.ino),
            "epoch finalization must invalidate the inode immediately"
        );

        let refreshed = vfs.getattr(attr.ino).unwrap();
        assert_eq!(refreshed.upload_state, UploadState::Ready);
        assert_eq!(refreshed.size, payload.len() as u64);
        assert_eq!(refreshed.committed_size, payload.len() as u64);

        let state = vfs.inode_incarnation_state(attr.ino).await.unwrap();
        assert_eq!(state.upload_state, VfsUploadState::Idle);
        assert!(
            state.pending_incarnation.is_none(),
            "published inode must not retain a pending incarnation"
        );
        let committed = state
            .committed_incarnation
            .expect("published inode must retain a committed incarnation");
        assert_eq!(committed.size, payload.len() as u64);

        vfs.shutdown(Duration::from_secs(15)).await;
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn local_reads_prefer_pending_overlay_over_committed_base() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "16MB");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr04-overlay-read-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let baseline_payload = vec![0x31u8; 512 * 1024];
    let patch = vec![0x7Au8; 8192];
    let mut ino = 0u64;

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            let attr = vfs.create(1, b"overlay-read.bin", 0, 0, 0o644).unwrap();
            ino = attr.ino;
            vfs.write(ino, 0, &baseline_payload).await.unwrap();
            vfs.flush(ino).await.unwrap();
            wait_until_payload_slices(&vfs, ino, Duration::from_secs(30))
                .expect("baseline file must commit before overlay read test");
            assert!(
                wait_until(Duration::from_secs(30), || {
                    meta.get_attr(ino)
                        .map(|attr| {
                            attr.upload_state == UploadState::Ready
                                && attr.committed_size == baseline_payload.len() as u64
                        })
                        .unwrap_or(false)
                })
                .await,
                "baseline file must become committed before overlay read test"
            );
        });
        rt.shutdown_background();
    }

    unsafe {
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            vfs.write(ino, 0, &patch).await.unwrap();
            vfs.flush(ino).await.unwrap();

            let mut expected = baseline_payload.clone();
            expected[..patch.len()].copy_from_slice(&patch);
            let read = vfs.read(ino, 0, expected.len() as u32).await.unwrap();
            assert_eq!(
                read.as_ref(),
                expected.as_slice(),
                "local read must apply pending overlay bytes over the committed base"
            );
        });
        rt.shutdown_background();
    }
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn committed_random_unaligned_reads_match_payload_after_publish_and_cold_reopen() {
    fn assert_range_matches(phase: &str, offset: usize, expected: &[u8], actual: &[u8]) {
        if actual.len() != expected.len() {
            panic!(
                "{phase} length mismatch at offset={} expected_len={} actual_len={}",
                offset,
                expected.len(),
                actual.len()
            );
        }

        if let Some(relative) = expected
            .iter()
            .zip(actual.iter())
            .position(|(expected_byte, actual_byte)| expected_byte != actual_byte)
        {
            let window_start = relative.saturating_sub(16);
            let window_end = (relative + 16).min(expected.len());
            panic!(
                "{phase} first mismatch at absolute_offset={} relative_offset={} expected_byte={} actual_byte={} expected_window={:?} actual_window={:?}",
                offset + relative,
                relative,
                expected[relative],
                actual[relative],
                &expected[window_start..window_end],
                &actual[window_start..window_end],
            );
        }
    }

    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "200");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cold_cache_dir = tmp.path().join("cache-cold");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let cold_cache_dir_str = cold_cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr04-committed-random-read-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let payload: Vec<u8> = (0..(18 * 1024 * 1024 + 97_531))
        .map(|index| (index as u8).wrapping_mul(17).wrapping_add(0x5B))
        .collect();
    let block_size = format_info.block_size as usize;
    let read_patterns = vec![
        (0usize, 64 * 1024usize),
        (1024usize, 16 * 1024usize),
        (4096usize, 32 * 1024usize),
        (block_size - 4096, 8192usize),
        (block_size + 513, 32 * 1024usize),
        ((block_size * 2) - 1024, 8192usize),
        (payload.len() - 65_536, 65_536usize),
    ];

    let ino = {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let ino = rt.block_on(async {
            let vfs = make_vfs_with_meta(meta.clone(), &format_info, &cache_dir_str);
            let attr = vfs
                .create(1, b"committed-random-read.bin", 0, 0, 0o644)
                .unwrap();
            vfs.write(attr.ino, 0, &payload).await.unwrap();
            vfs.flush(attr.ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(30), || {
                    meta.get_attr(attr.ino)
                        .map(|current| {
                            current.upload_state == UploadState::Ready
                                && current.committed_size == payload.len() as u64
                        })
                        .unwrap_or(false)
                })
                .await,
                "committed-random-read file must reach published Ready state"
            );

            for (offset, len) in &read_patterns {
                let read = vfs
                    .read(attr.ino, *offset as u64, *len as u32)
                    .await
                    .unwrap();
                assert_range_matches(
                    &format!("warm committed read mismatch len={}", len),
                    *offset,
                    &payload[*offset..*offset + *len],
                    read.as_ref(),
                );
            }

            let fh = vfs.open_file(attr.ino, OpenFlags::RDONLY, false).unwrap();
            for (offset, len) in &read_patterns {
                let mut buf = vec![0u8; *len];
                let written = vfs
                    .read_into_fh(fh, attr.ino, *offset as u64, &mut buf)
                    .await
                    .unwrap();
                assert_eq!(written as usize, *len);
                assert_range_matches(
                    &format!("warm handle read mismatch len={}", len),
                    *offset,
                    &payload[*offset..*offset + *len],
                    buf.as_slice(),
                );
            }
            vfs.release_fh(fh).await.unwrap();
            vfs.shutdown(Duration::from_secs(15)).await;
            attr.ino
        });
        rt.shutdown_background();
        ino
    };

    drop(meta);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = reopen_vfs(&db_path, &std::path::PathBuf::from(&cold_cache_dir_str));
        assert!(
            wait_until(Duration::from_secs(10), || {
                vfs.getattr(ino)
                    .map(|current| {
                        current.upload_state == UploadState::Ready
                            && current.committed_size == payload.len() as u64
                    })
                    .unwrap_or(false)
            })
            .await,
            "cold reopen must observe published attr state"
        );

        for (offset, len) in &read_patterns {
            let read = vfs.read(ino, *offset as u64, *len as u32).await.unwrap();
            assert_range_matches(
                &format!("cold committed read mismatch len={}", len),
                *offset,
                &payload[*offset..*offset + *len],
                read.as_ref(),
            );
        }

        let fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
        for (offset, len) in &read_patterns {
            let mut buf = vec![0u8; *len];
            let written = vfs
                .read_into_fh(fh, ino, *offset as u64, &mut buf)
                .await
                .unwrap();
            assert_eq!(written as usize, *len);
            assert_range_matches(
                &format!("cold handle read mismatch len={}", len),
                *offset,
                &payload[*offset..*offset + *len],
                buf.as_slice(),
            );
        }
        vfs.release_fh(fh).await.unwrap();
        vfs.shutdown(Duration::from_secs(15)).await;
    });
    rt.shutdown_background();
}

#[test]
fn local_reads_of_pending_incarnation_prefer_materialized_extent_over_spill() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr11-pending-read-fast-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let mut payload = vec![0x41u8; block_size * 2];
    payload[block_size..].fill(0x62u8);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"pending-fast-read.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                let ingress = vfs.ingress_stats();
                vfs.live_extent_records_for_inode(attr.ino).len() == 2
                    && vfs.pending_uploads_for_inode(attr.ino).len() == 2
                    && ingress.dirty_mem_blocks == 0
            })
            .await,
            "pending file should become fully materialized locally before any upload succeeds"
        );

        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            payload.as_slice(),
            "local read must succeed from materialized pending extents even after spill is gone"
        );
    });
    rt.shutdown_background();
}

#[test]
fn partial_overwrite_after_materialization_reuses_pending_extent_as_write_base() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr11-partial-overwrite-base-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let baseline = vec![0x33u8; block_size];
    let patch_offset = 128 * 1024usize;
    let patch = vec![0x7Au8; 8192];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
        test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start().expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"pending-partial-base.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &baseline).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                let ingress = vfs.ingress_stats();
                vfs.live_extent_records_for_inode(attr.ino).len() == 1
                    && vfs.pending_uploads_for_inode(attr.ino).len() == 1
                    && ingress.dirty_mem_blocks == 0
            })
            .await,
            "baseline pending block must be materialized before the follow-up partial write"
        );

        vfs.write(attr.ino, patch_offset as u64, &patch).await.unwrap();

        let mut expected = baseline.clone();
        expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);
        let read = vfs.read(attr.ino, 0, block_size as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            expected.as_slice(),
            "follow-up write must load the materialized pending block as its base, not the committed view"
        );
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn single_inode_never_has_two_pending_incarnations() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "16MB");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr04-single-pending-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let baseline_payload = vec![0x51u8; 512 * 1024];
    let patch_a = vec![0x61u8; 4096];
    let patch_b = vec![0x71u8; 4096];
    let mut ino = 0u64;

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            let attr = vfs.create(1, b"single-pending.bin", 0, 0, 0o644).unwrap();
            ino = attr.ino;
            vfs.write(ino, 0, &baseline_payload).await.unwrap();
            vfs.flush(ino).await.unwrap();
            wait_until_payload_slices(&vfs, ino, Duration::from_secs(30))
                .expect("baseline file must commit before pending incarnation test");
            assert!(
                wait_until(Duration::from_secs(30), || {
                    meta.get_attr(ino)
                        .map(|attr| {
                            attr.upload_state == UploadState::Ready
                                && attr.committed_size == baseline_payload.len() as u64
                        })
                        .unwrap_or(false)
                })
                .await,
                "baseline file must become committed before pending incarnation test"
            );
        });
        rt.shutdown_background();
    }

    unsafe {
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
            test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start().expect("failed to start StudioVfs background workers");

            vfs.write(ino, 0, &patch_a).await.unwrap();
            let first = vfs
                .inode_incarnation_state(ino)
                .await
                .unwrap()
                .pending_incarnation
                .expect("first mutation must create one pending incarnation");

            vfs.write(ino, 64 * 1024, &patch_b).await.unwrap();
            let second_state = vfs.inode_incarnation_state(ino).await.unwrap();
            let second = second_state
                .pending_incarnation
                .expect("second mutation must reuse the same pending incarnation");

            assert_eq!(second_state.upload_state, VfsUploadState::Updating);
            assert_eq!(
                first.content_epoch, second.content_epoch,
                "multiple writes to the same inode before commit must stay in one pending incarnation"
            );
            assert_eq!(second.base_size, baseline_payload.len() as u64);
        });
        rt.shutdown_background();
    }
}

#[test]
fn large_file_unlink_cancels_pending_uploads() {
    let _test_env = ScopedTestEnv::new();
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr05-unlink-cancel-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let payload = vec![0x44u8; block_size * 2];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs.create(1, b"unlink-cancel.bin", 0, 0, 0o644).unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();
        let canceled_before = vfs.upload_stats().canceled_deleted;

        assert!(
            wait_until(Duration::from_secs(10), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
                    || !vfs.live_extent_records_for_inode(attr.ino).is_empty()
            })
            .await,
            "flush should leave upload or live-extent work for unlink to cancel"
        );

        vfs.unlink(1, b"unlink-cancel.bin").unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                vfs.pending_uploads_for_inode(attr.ino).is_empty()
                    && vfs.upload_stats().canceled_deleted > canceled_before
            })
            .await,
            "unlink must cancel the whole pending incarnation and stop useful upload work"
        );
        assert!(
            vfs.live_extent_records_for_inode(attr.ino).is_empty(),
            "unlink must clear live extent identities for the inode"
        );
    });
    rt.shutdown_background();
}

#[test]
fn open_unlink_keeps_data_readable_while_canceling_pending_uploads() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr05-open-unlink-cancel-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let payload = vec![0x5Au8; block_size * 2];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"open-unlink-cancel.bin", 0, 0, 0o644)
            .unwrap();
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open handle before unlink");
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();
        let canceled_before = vfs.upload_stats().canceled_deleted;

        assert!(
            wait_until(Duration::from_secs(10), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
                    || !vfs.live_extent_records_for_inode(attr.ino).is_empty()
            })
            .await,
            "flush should leave upload or live-extent work for open-unlink to cancel"
        );

        vfs.unlink(1, b"open-unlink-cancel.bin").unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                vfs.pending_uploads_for_inode(attr.ino).is_empty()
                    && vfs.upload_stats().canceled_deleted > canceled_before
            })
            .await,
            "open-unlink must cancel pending uploads without waiting for close"
        );

        let data = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            data.as_ref(),
            payload.as_slice(),
            "open-unlink file must remain readable from local cache until last handle closes"
        );

        vfs.release_fh(fh).await.unwrap();
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn large_file_partial_overwrite_reuploads_only_touched_extents() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::remove_var("STUDIOFS_UPLOAD_DELAY_MS");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr05-partial-overwrite-{}", std::process::id());
    let online_info = make_format_info(&volume_suffix);
    let offline_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config_offline = DataStoreConfig::from(&offline_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&online_info).unwrap();

    let block_size = online_info.block_size as usize;
    let baseline_payload = vec![0x21u8; block_size * 3];
    let patch_a = vec![0xA1u8; block_size];
    let patch_b = vec![0xB2u8; block_size];
    let mut ino = 0u64;
    let mut baseline_blocks = Vec::new();

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (seeded_ino, seeded_blocks) = seed_committed_file(
                &meta,
                &online_info,
                b"partial-overwrite.bin",
                &baseline_payload,
            )
            .await;
            ino = seeded_ino;
            baseline_blocks = seeded_blocks;
        });
        rt.shutdown_background();
    }

    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }
    meta.set_format_info(&offline_info).unwrap();

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            vfs.write(ino, 0, &patch_a).await.unwrap();
            vfs.write(ino, (block_size * 2) as u64, &patch_b)
                .await
                .unwrap();
            vfs.flush(ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(10), || {
                    vfs.live_extent_records_for_inode(ino).len() == 2
                })
                .await,
                "only the two touched extents should remain live for re-upload"
            );
            let live_extents = vfs.live_extent_records_for_inode(ino);
            assert_eq!(live_extents.len(), 2);
            assert_eq!(live_extents[0].0, 0);
            assert_eq!(live_extents[1].0, 2);

            let after_blocks = meta.lookup_logical_payload_slices(ino).unwrap();
            assert_eq!(after_blocks.len(), 3);
            assert_ne!(after_blocks[0].1.object_id, baseline_blocks[0].1.object_id);
            assert_eq!(after_blocks[1].1.object_id, baseline_blocks[1].1.object_id);
            assert_ne!(after_blocks[2].1.object_id, baseline_blocks[2].1.object_id);
        });
        rt.shutdown_background();
    }
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn stale_upload_completion_is_ignored_after_extent_invalidate() {
    let _env = ScopedTestEnv::new();
    apply_test_online_s3_env();
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::remove_var("STUDIOFS_UPLOAD_DELAY_MS");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr05-stale-completion-{}", std::process::id());
    let online_info = make_format_info(&volume_suffix);
    let offline_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config_online = DataStoreConfig::from(&online_info);
    let data_config_offline = DataStoreConfig::from(&offline_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&online_info).unwrap();

    let block_size = online_info.block_size as usize;
    let baseline_payload = vec![0x11u8; block_size];
    let patch_a = vec![0x22u8; block_size];
    let patch_b = vec![0x33u8; block_size];
    let mut ino = 0u64;
    let mut first_extent_object_id = 0u64;
    let mut second_extent_object_id = 0u64;

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (seeded_ino, _) = seed_committed_file(
                &meta,
                &online_info,
                b"stale-completion.bin",
                &baseline_payload,
            )
            .await;
            ino = seeded_ino;
        });
        rt.shutdown_background();
    }

    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }
    meta.set_format_info(&offline_info).unwrap();

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            vfs.write(ino, 0, &patch_a).await.unwrap();
            vfs.flush(ino).await.unwrap();
            assert!(
                wait_until(Duration::from_secs(10), || {
                    !vfs.live_extent_records_for_inode(ino).is_empty()
                })
                .await,
                "first patched extent should become live before supersession"
            );
            first_extent_object_id = vfs.live_extent_records_for_inode(ino)[0].1;

            vfs.write(ino, 0, &patch_b).await.unwrap();
            vfs.flush(ino).await.unwrap();
            assert!(
                wait_until(Duration::from_secs(10), || {
                    let extents = vfs.live_extent_records_for_inode(ino);
                    extents.len() == 1 && extents[0].1 != first_extent_object_id
                })
                .await,
                "second patch should supersede the first extent identity"
            );
            second_extent_object_id = vfs.live_extent_records_for_inode(ino)[0].1;
            assert_ne!(first_extent_object_id, second_extent_object_id);
        });
        rt.shutdown_background();
    }

    unsafe {
        apply_test_online_s3_env();
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    }
    meta.set_format_info(&online_info).unwrap();

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_online,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            assert!(
                wait_until(Duration::from_secs(45), || {
                    vfs.pending_uploads_for_inode(ino).is_empty()
                        && vfs.upload_stats().canceled_stale > 0
                })
                .await,
                "stale superseded upload must be canceled instead of committing"
            );
            let blocks = meta.lookup_logical_payload_slices(ino).unwrap();
            assert_eq!(blocks.len(), 1);
            assert_eq!(blocks[0].1.object_id, second_extent_object_id);
            let read = vfs.read(ino, 0, block_size as u32).await.unwrap();
            assert_eq!(
                read.as_ref(),
                patch_b.as_slice(),
                "latest patch must remain authoritative after stale upload recovery"
            );
        });
        rt.shutdown_background();
    }
}

#[test]
fn large_file_rename_does_not_restart_or_duplicate_uploads() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr05-rename-neutral-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let payload = vec![0x66u8; block_size * 2];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs.create(1, b"rename-source.bin", 0, 0, 0o644).unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                vfs.pending_uploads_for_inode(attr.ino).len() == 2
            })
            .await,
            "rename neutrality test needs pending uploads in flight"
        );
        let pending_before = vfs.pending_uploads_for_inode(attr.ino);
        let extents_before = vfs.live_extent_records_for_inode(attr.ino);

        vfs.rename(1, b"rename-source.bin", 1, b"rename-dest.bin")
            .unwrap();

        let renamed = vfs.lookup(1, b"rename-dest.bin").unwrap();
        assert_eq!(renamed.ino, attr.ino);
        assert!(vfs.lookup(1, b"rename-source.bin").is_err());
        assert_eq!(
            vfs.pending_uploads_for_inode(attr.ino),
            pending_before,
            "rename must not restart or duplicate pending uploads"
        );
        assert_eq!(
            vfs.live_extent_records_for_inode(attr.ino),
            extents_before,
            "rename must be namespace-only for live extent identity"
        );
    });
    rt.shutdown_background();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn rename_into_trashes_evicts_local_block_payloads() {
    apply_test_online_s3_env();
    ensure_bucket().await;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("trash-rename-cache-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let payload = vec![0x5au8; block_size + block_size / 2];
    let (ino, blocks) = seed_committed_file(&meta, &format_info, b"trash-me.bin", &payload).await;
    let object_ids: Vec<u64> = blocks.iter().map(|(_, block)| block.object_id).collect();

    let vfs = make_vfs_with_meta(meta.clone(), &format_info, &cache_dir_str);
    let trash_root = vfs.mkdir(1, b".Trashes", 0, 0, 0o755).unwrap();
    let trash_user = vfs.mkdir(trash_root.ino, b"501", 0, 0, 0o755).unwrap();

    let read_back = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
    assert_eq!(read_back, payload);
    assert!(
        wait_until(Duration::from_secs(10), || {
            object_ids
                .iter()
                .all(|object_id| vfs.read_cached_block(*object_id).is_some())
        })
        .await,
        "seeded file should be present in the local block backend before trash rename"
    );

    vfs.rename(1, b"trash-me.bin", trash_user.ino, b"trash-me.bin")
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(10), || {
            object_ids
                .iter()
                .all(|object_id| vfs.read_cached_block(*object_id).is_none())
        })
        .await,
        "moving into .Trashes should evict committed local block payloads promptly"
    );
    assert!(vfs.lookup(trash_user.ino, b"trash-me.bin").is_ok());
    assert!(vfs.lookup(1, b"trash-me.bin").is_err());
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn update_base_graph_reuses_untouched_committed_extents() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr06-base-graph-reuse-{}", std::process::id());
    let online_info = make_format_info(&volume_suffix);
    let offline_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config_offline = DataStoreConfig::from(&offline_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&online_info).unwrap();

    let block_size = online_info.block_size as usize;
    let baseline_payload = vec![0x31u8; block_size * 3];
    let patch = vec![0xABu8; block_size];
    let mut ino = 0u64;
    let mut baseline_blocks = Vec::new();

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (seeded_ino, seeded_blocks) = seed_committed_file(
                &meta,
                &online_info,
                b"base-graph-reuse.bin",
                &baseline_payload,
            )
            .await;
            ino = seeded_ino;
            baseline_blocks = seeded_blocks;
        });
        rt.shutdown_background();
    }

    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }
    meta.set_format_info(&offline_info).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config_offline,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
        test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start().expect("failed to start StudioVfs background workers");

        vfs.write(ino, block_size as u64, &patch).await.unwrap();
        vfs.flush(ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                vfs.live_extent_records_for_inode(ino).len() == 1
            })
            .await,
            "single-block update should materialize exactly one live pending extent"
        );

        let visible_blocks = meta.lookup_logical_payload_slices(ino).unwrap();
        assert_eq!(visible_blocks.len(), 3);
        assert_eq!(visible_blocks[0].1.object_id, baseline_blocks[0].1.object_id);
        assert_ne!(visible_blocks[1].1.object_id, baseline_blocks[1].1.object_id);
        assert_eq!(visible_blocks[2].1.object_id, baseline_blocks[2].1.object_id);

        let read = vfs
            .read(ino, block_size as u64, block_size as u32)
            .await
            .unwrap();
        assert_eq!(
            read.as_ref(),
            patch.as_slice(),
            "pending update graph must expose the touched extent while preserving untouched committed refs"
        );
    });
    rt.shutdown_background();
}

#[test]
fn restart_replays_only_live_pending_extents() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr06-restart-live-only-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let first = vec![0x41u8; block_size];
    let second = vec![0x52u8; block_size];
    let mut ino = 0u64;

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let attr = vfs
                .create(1, b"restart-live-only.bin", 0, 0, 0o644)
                .unwrap();
            ino = attr.ino;

            vfs.write(ino, 0, &first).await.unwrap();
            vfs.flush(ino).await.unwrap();
            vfs.write(ino, 0, &second).await.unwrap();
            vfs.flush(ino).await.unwrap();

            let read = vfs.read(ino, 0, block_size as u32).await.unwrap();
            assert_eq!(read.as_ref(), second.as_slice());
        });
        rt.shutdown_background();
    }

    unsafe {
        std::env::remove_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let read = vfs.read(ino, 0, block_size as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            second.as_slice(),
            "restart must replay the live pending extent before any remote activity"
        );

        assert!(
            wait_until(Duration::from_secs(10), || {
                vfs.pending_uploads_for_inode(ino).len() == 1
                    && vfs.live_extent_records_for_inode(ino).len() == 1
            })
            .await,
            "replay should materialize only the last live pending extent for the block"
        );
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn recovered_pending_uploads_publish_new_file_after_restart() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");

    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "60000");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("restart-publish-ready-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let payload = vec![0x6Du8; 512 * 1024];
    let mut ino = 0u64;

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let attr = vfs
                .create(1, b"restart-publish-ready.bin", 0, 0, 0o644)
                .unwrap();
            ino = attr.ino;
            vfs.write(ino, 0, &payload).await.unwrap();
            vfs.flush(ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(10), || {
                    vfs.pending_uploads_for_inode(ino).len() == 1
                        && meta
                            .get_attr(ino)
                            .map(|attr| attr.upload_state == UploadState::Uploading)
                            .unwrap_or(false)
                })
                .await,
                "crash setup should leave one recovered upload journal entry for the new file"
            );
        });
        rt.shutdown_background();
    }

    unsafe {
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "0");
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        assert!(
            wait_until(Duration::from_secs(20), || {
                vfs.pending_uploads_for_inode(ino).is_empty()
                    && meta
                        .get_attr(ino)
                        .map(|attr| {
                            attr.upload_state == UploadState::Ready
                                && attr.committed_content_epoch == attr.content_epoch
                        })
                        .unwrap_or(false)
            })
            .await,
            "recovered uploads should publish the new file to Ready after restart"
        );

        let fh = vfs
            .open_file(ino, OpenFlags::RDONLY, false)
            .expect("published file should no longer look locked after restart");
        vfs.release_fh(fh).await.unwrap();

        let read = vfs
            .read(ino, 0, payload.len() as u32)
            .await
            .expect("published file should remain readable after restart");
        assert_eq!(read.as_ref(), payload.as_slice());
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn published_file_remains_readable_after_cache_wipe_and_restart() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    apply_test_online_s3_env();

    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "0");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("restart-cache-wipe-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let payload: Vec<u8> = (0..(format_info.block_size as usize * 3 + 12345))
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(17))
        .collect();
    let mut ino = 0u64;

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let attr = vfs
                .create(1, b"restart-cache-wipe.bin", 0, 0, 0o644)
                .unwrap();
            ino = attr.ino;
            vfs.write(ino, 0, &payload).await.unwrap();
            vfs.flush(ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(20), || {
                    vfs.pending_uploads_for_inode(ino).is_empty()
                        && meta
                            .get_attr(ino)
                            .map(|attr| {
                                attr.upload_state == UploadState::Ready
                                    && attr.committed_content_epoch == attr.content_epoch
                            })
                            .unwrap_or(false)
                })
                .await,
                "file should fully publish before wiping local cache"
            );

            let warm_read = vfs
                .read(ino, 0, payload.len() as u32)
                .await
                .expect("warm published read should succeed before cache wipe");
            assert_eq!(warm_read.as_ref(), payload.as_slice());

            vfs.shutdown(Duration::from_secs(15)).await;
        });
        rt.shutdown_background();
    }

    if cache_dir.exists() {
        std::fs::remove_dir_all(&cache_dir).expect("remove cache dir before cold reopen");
    }
    std::fs::create_dir_all(&cache_dir).expect("recreate empty cache dir before cold reopen");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let reopened = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        reopened
            .start()
            .expect("failed to start reopened StudioVfs background workers");

        let cold_read = reopened
            .read(ino, 0, payload.len() as u32)
            .await
            .expect("cold reopened read should fetch published remote content");
        assert_eq!(cold_read.as_ref(), payload.as_slice());

        reopened.shutdown(Duration::from_secs(15)).await;
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn background_epoch_finalizer_keeps_non_publish_ready_epoch_local_after_restart() {
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_bucket = ScopedEnvVar::capture("STUDIOFS_S3_BUCKET");
    let _restore_access_key = ScopedEnvVar::capture("STUDIOFS_S3_ACCESS_KEY");
    let _restore_secret_key = ScopedEnvVar::capture("STUDIOFS_S3_SECRET_KEY");
    apply_test_online_s3_env();

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("restart-finalizer-no-pending-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let baseline_payload = vec![0x21u8; 512 * 1024];
    let replacement_payload = vec![0x91u8; 512 * 1024];
    let mut ino = 0u64;
    let mut committed_block = BlockRef::new_partial(0, 0);
    let mut replacement_object_id = 0u64;

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (seeded_ino, seeded_blocks) = seed_committed_file(
                &meta,
                &format_info,
                b"restart-finalizer-no-pending.bin",
                &baseline_payload,
            )
            .await;
            ino = seeded_ino;
            committed_block = seeded_blocks[0].1.clone();

            let mut staged_attr = meta.get_attr(ino).unwrap();
            staged_attr.size = replacement_payload.len() as u64;
            staged_attr.content_epoch = staged_attr.content_epoch.saturating_add(1);
            meta.set_attr(&staged_attr).unwrap();

            replacement_object_id = meta.next_block_ids(1).unwrap()[0];
            let store = DataStore::new(&data_config).unwrap();
            put_fragmented_object(
                &store,
                &format_info.data_prefix,
                replacement_object_id,
                Bytes::copy_from_slice(&replacement_payload),
            )
            .await
            .unwrap();

            let replacement_block =
                BlockRef::new_partial(replacement_object_id, replacement_payload.len() as u32);
            meta.stage_logical_payload_slice(ino, 0, &replacement_block)
                .unwrap();

            let unpublished_attr = meta.get_attr(ino).unwrap();
            assert_eq!(unpublished_attr.upload_state, UploadState::Updating);
            assert_ne!(
                unpublished_attr.committed_content_epoch,
                unpublished_attr.content_epoch
            );
            let lifecycle = meta.get_inode_lifecycle(ino).unwrap().unwrap();
            assert_eq!(
                lifecycle.local_content_epoch,
                unpublished_attr.content_epoch
            );
            assert_eq!(lifecycle.state, InodeLifecycleState::LocalOnly);
        });
        rt.shutdown_background();
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        assert!(
            vfs.pending_uploads_for_inode(ino).is_empty(),
            "recovery setup should keep the restart path free of upload journal entries"
        );

        tokio::time::sleep(Duration::from_secs(2)).await;

        let attr_after = meta.get_attr(ino).unwrap();
        assert_eq!(
            attr_after.upload_state,
            UploadState::Updating,
            "background finalizer must not publish epochs that never reached PublishReady"
        );
        assert_ne!(
            attr_after.committed_content_epoch,
            attr_after.content_epoch,
            "the staged epoch should remain unpublished after restart"
        );
        let lifecycle = meta.get_inode_lifecycle(ino).unwrap().unwrap();
        assert_eq!(
            lifecycle.state,
            InodeLifecycleState::LocalOnly,
            "without a durable PublishReady marker the restart path should keep the epoch local-only"
        );

        let local_data = vfs
            .read(ino, 0, replacement_payload.len() as u32)
            .await
            .unwrap();
        assert_eq!(
            local_data.as_ref(),
            replacement_payload.as_slice(),
            "the local mount should still recover and read its unpublished replacement payload"
        );

        let remote_db_path = tmp.path().join("remote-meta.redb");
        let remote_meta = RedbMetaEngine::open(&remote_db_path).unwrap();
        remote_meta.init_root(0, 0).unwrap();
        import_remote_snapshot(meta.as_ref(), &remote_meta, &format_info);

        let remote_attr = remote_meta.get_attr(ino).unwrap();
        assert_eq!(
            remote_attr.committed_content_epoch,
            attr_after.committed_content_epoch,
            "remote-visible state must remain on the last committed epoch"
        );
        assert_eq!(remote_attr.upload_state, UploadState::Updating);
        let remote_block = remote_meta.lookup_logical_payload_slice(ino, 0).unwrap().unwrap();
        assert_eq!(
            remote_block.object_id,
            committed_block.object_id,
            "remote-visible payload should stay on the previous committed payload slice"
        );
        assert_ne!(
            remote_block.object_id,
            replacement_object_id,
            "the unpublished replacement payload slice must not leak into published state"
        );
    });
    rt.shutdown_background();
}

#[test]
fn startup_recovery_replays_fragmented_regular_file_graph_without_quarantine() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "60000");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("restart-slice-replay-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let block_size_u32 = format_info.block_size;
    let payload = vec![0x5Au8; block_size];
    let mut ino = 0u64;
    let mut dirty_object_id = 0u64;

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let attr = vfs
                .create(1, b"startup-slice-replay.bin", 0, 0, 0o644)
                .unwrap();
            ino = attr.ino;

            vfs.write(ino, 0, &payload).await.unwrap();
            vfs.flush(ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(10), || {
                    vfs.pending_uploads_for_inode(ino).len() == 1
                })
                .await,
                "first boot should leave one pending upload for the local-only block"
            );

            dirty_object_id = vfs.pending_uploads_for_inode(ino)[0].1;

            let half = (block_size_u32 / 2) as u32;
            let slices = vec![
                SliceRecord::new(
                    SliceId(0),
                    SliceOrder(0),
                    SliceRange::new(0, half).unwrap(),
                    BlockRef::new_partial(dirty_object_id, half),
                ),
                SliceRecord::new(
                    SliceId(1),
                    SliceOrder(1),
                    SliceRange::new(half, block_size_u32).unwrap(),
                    BlockRef {
                        object_id: dirty_object_id,
                        offset: half,
                        length: half,
                        flags: BlockFlags::PARTIAL_BLOCK,
                    },
                ),
            ];
            let _ = meta
                .flush_regular_file_overlay_slices_with_attr(ino, &[(ChunkId(0), slices)], None)
                .unwrap();
        });
        rt.shutdown_background();
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        assert!(
            wait_until(Duration::from_secs(10), || {
                !vfs.pending_uploads_for_inode(ino).is_empty()
            })
            .await,
            "startup replay should restore upload queue entries for fragmented slice graphs"
        );
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "startup replay should keep live extents for fragmented slice graphs"
        );
        assert!(
            vfs.local_block_backend()
                .dirty_object_ids()
                .contains(&dirty_object_id),
            "startup replay must preserve the dirty local object while upload is pending"
        );

        vfs.fsync(ino, false).await.unwrap();
    });
    rt.shutdown_background();
}

#[test]
fn offline_s3_does_not_break_local_read_after_write() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr06-offline-local-read-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let mut payload = vec![0x61u8; block_size * 2];
    payload[block_size..].fill(0x7Cu8);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"offline-local-read.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                vfs.pending_uploads_for_inode(attr.ino).len() == 2
            })
            .await,
            "offline S3 scenario needs local materialization and queued uploads"
        );

        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            payload.as_slice(),
            "local read must succeed even while every upload is failing and requeuing"
        );

        tokio::time::sleep(Duration::from_millis(150)).await;
        let read_again = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(read_again.as_ref(), payload.as_slice());
    });
    rt.shutdown_background();
}

#[test]
fn cache_v3_restart_with_offline_s3_replays_dirty_uploads_from_index() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    provision_cache_runtime_layout(&cache_dir);
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let mut format_info = make_format_info_with_endpoint(
        &format!("local-block-runtime-replay-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    format_info.block_size = 512 * 1024;
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let mut payload = vec![0x3Au8; block_size * 2];
    payload[block_size..].fill(0x7Fu8);
    let mut ino = 0u64;
    let mut dirty_before = Vec::new();

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            assert!(
                vfs.local_block_backend()
                    .as_any()
                    .is::<PersistentLocalBlockBackend>(),
                "local-block-runtime marker should select local-block-runtime backend"
            );

            let attr = vfs.create(1, b"local-block-runtime-replay.bin", 0, 0, 0o644).unwrap();
            ino = attr.ino;
            vfs.write(attr.ino, 0, &payload).await.unwrap();
            vfs.flush(attr.ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(10), || {
                    vfs.pending_uploads_for_inode(attr.ino).len() == 2
                })
                .await,
                "offline local-block-runtime run should queue dirty uploads before restart"
            );
            dirty_before = vfs.local_block_backend().dirty_object_ids();
            assert!(
                !dirty_before.is_empty(),
                "offline local-block-runtime run should retain dirty replay candidates in local backend"
            );

            let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
            assert_eq!(read.as_ref(), payload.as_slice());
            vfs.shutdown(Duration::from_secs(10)).await;
        });
        rt.shutdown_background();
    }

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            assert!(
                vfs.local_block_backend()
                    .as_any()
                    .is::<PersistentLocalBlockBackend>(),
                "restart must keep using local-block-runtime backend contract"
            );

            let read = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
            assert_eq!(read.as_ref(), payload.as_slice());
            assert!(
                wait_until(Duration::from_secs(10), || {
                    !vfs.pending_uploads_for_inode(ino).is_empty()
                })
                .await,
                "local-block-runtime restart should replay pending uploads from persisted dirty index state"
            );
            let dirty_after = vfs.local_block_backend().dirty_object_ids();
            assert!(
                !dirty_after.is_empty(),
                "local-block-runtime restart should expose dirty replay candidates from index"
            );
            for object_id in &dirty_before {
                assert!(
                    dirty_after.contains(object_id),
                    "local-block-runtime replay lost dirty object {object_id}"
                );
            }
            vfs.shutdown(Duration::from_secs(10)).await;
        });
        rt.shutdown_background();
    }
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn upload_completion_batching_does_not_change_visibility_contract() {
    let _env = ScopedTestEnv::new();
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr06-upload-batching-{}", std::process::id());
    let online_info = make_format_info(&volume_suffix);
    let offline_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config_online = DataStoreConfig::from(&online_info);
    let data_config_offline = DataStoreConfig::from(&offline_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&online_info).unwrap();

    let block_size = online_info.block_size as usize;
    let baseline_payload = vec![0x10u8; block_size * 2];
    let mut replacement_payload = vec![0x90u8; block_size * 2];
    replacement_payload[block_size..].fill(0xA5u8);
    let mut ino = 0u64;

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (seeded_ino, _) = seed_committed_file(
                &meta,
                &online_info,
                b"upload-batching.bin",
                &baseline_payload,
            )
            .await;
            ino = seeded_ino;
        });
        rt.shutdown_background();
    }

    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
        std::env::remove_var("STUDIOFS_UPLOAD_DELAY_MS");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }
    meta.set_format_info(&offline_info).unwrap();

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            vfs.write(ino, 0, &replacement_payload).await.unwrap();
            vfs.flush(ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(10), || {
                    vfs.pending_uploads_for_inode(ino).len() == 2
                })
                .await,
                "offline phase should leave the whole replacement pending"
            );

            let read = vfs
                .read(ino, 0, replacement_payload.len() as u32)
                .await
                .unwrap();
            assert_eq!(read.as_ref(), replacement_payload.as_slice());
        });
        rt.shutdown_background();
    }

    unsafe {
        apply_test_online_s3_env();
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
        std::env::remove_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "75");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }
    meta.set_format_info(&online_info).unwrap();

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_online,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
            test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start().expect("failed to start StudioVfs background workers");

            assert!(
                wait_until(Duration::from_secs(20), || {
                    !vfs.pending_uploads_for_inode(ino).is_empty()
                })
                .await,
                "restart should recover the pending upload batch"
            );

            let deadline = Instant::now() + Duration::from_secs(90);
            loop {
                let read = vfs
                    .read(ino, 0, replacement_payload.len() as u32)
                    .await
                    .unwrap();
                if read.as_ref() == replacement_payload.as_slice() {
                    break;
                }
                let now = Instant::now();
                assert!(
                    now < deadline,
                    "upload completion batching must not change the local visible file contents; pending={:?} stats={:?} endpoint={:?}",
                    vfs.pending_uploads_for_inode(ino),
                    vfs.upload_stats(),
                    std::env::var("STUDIOFS_S3_ENDPOINT").ok(),
                );
                tokio::time::sleep(Duration::from_millis(75)).await;
            }

            loop {
                let read = vfs
                    .read(ino, 0, replacement_payload.len() as u32)
                    .await
                    .unwrap();
                if read.as_ref() != replacement_payload.as_slice() {
                    let now = Instant::now();
                    assert!(
                        now < deadline,
                        "upload completion batching must not change the local visible file contents; pending={:?} stats={:?} endpoint={:?}",
                        vfs.pending_uploads_for_inode(ino),
                        vfs.upload_stats(),
                        std::env::var("STUDIOFS_S3_ENDPOINT").ok(),
                    );
                    tokio::time::sleep(Duration::from_millis(75)).await;
                    continue;
                }
                if vfs.pending_uploads_for_inode(ino).is_empty() {
                    break;
                }
                let now = Instant::now();
                assert!(
                    now < deadline,
                    "pending uploads did not drain under sequential completion; pending={:?} stats={:?} endpoint={:?} meta_blocks={:?} live_extents={:?}",
                    vfs.pending_uploads_for_inode(ino),
                    vfs.upload_stats(),
                    std::env::var("STUDIOFS_S3_ENDPOINT").ok(),
                    vfs.meta().lookup_logical_payload_slices(ino).ok(),
                    vfs.live_extent_records_for_inode(ino),
                );
                tokio::time::sleep(Duration::from_millis(75)).await;
            }

            let final_read = vfs
                .read(ino, 0, replacement_payload.len() as u32)
                .await
                .unwrap();
            assert_eq!(final_read.as_ref(), replacement_payload.as_slice());
        });
        rt.shutdown_background();
    }
}

#[test]
fn slow_s3_does_not_stall_local_reads_and_writes() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    let _restore_qos_materialize = ScopedEnvVar::capture("STUDIOFS_QOS_MATERIALIZE_SLOTS");
    let _restore_qos_commit = ScopedEnvVar::capture("STUDIOFS_QOS_COMMIT_SLOTS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "8MB");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "128MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "0");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "2");
        std::env::set_var("STUDIOFS_QOS_MATERIALIZE_SLOTS", "1");
        std::env::set_var("STUDIOFS_QOS_COMMIT_SLOTS", "1");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr10-slow-s3-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let backlog_payload = vec![0x41u8; block_size * 3];
    let foreground_payload = vec![0x7Eu8; 256 * 1024];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let backlog = vfs.create(1, b"slow-backlog.bin", 0, 0, 0o644).unwrap();
        vfs.write(backlog.ino, 0, &backlog_payload).await.unwrap();
        vfs.flush(backlog.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                vfs.pending_uploads_for_inode(backlog.ino).len() == 3
            })
            .await,
            "backlog file should queue uploads even while S3 is offline"
        );

        let started = Instant::now();
        let foreground = vfs.create(1, b"foreground.bin", 0, 0, 0o644).unwrap();
        vfs.write(foreground.ino, 0, &foreground_payload)
            .await
            .unwrap();
        vfs.flush(foreground.ino).await.unwrap();
        let read = tokio::time::timeout(
            Duration::from_secs(2),
            vfs.read(foreground.ino, 0, foreground_payload.len() as u32),
        )
        .await
        .expect("foreground read timed out under slow/offline S3")
        .unwrap();
        assert_eq!(read.as_ref(), foreground_payload.as_slice());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "foreground write+flush+read should complete quickly despite failing uploads"
        );

        let stats = vfs.ingress_stats();
        assert_eq!(
            stats.write_backpressure_wait_ms, 0,
            "upload backlog alone should not create foreground backpressure waits"
        );
    });
    rt.shutdown_background();
}

#[test]
fn upload_backlog_does_not_create_local_backpressure_after_write_commit() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    let _restore_qos_materialize = ScopedEnvVar::capture("STUDIOFS_QOS_MATERIALIZE_SLOTS");
    let _restore_qos_commit = ScopedEnvVar::capture("STUDIOFS_QOS_COMMIT_SLOTS");

    let roomy_tmp = tempfile::tempdir().unwrap();
    let roomy_db = roomy_tmp.path().join("meta.redb");
    let roomy_cache = roomy_tmp.path().join("cache");
    let roomy_cache_str = roomy_cache.to_str().unwrap().to_string();
    let roomy_suffix = format!("pr10-backpressure-roomy-{}", std::process::id());
    let roomy_info = make_format_info_with_endpoint(&roomy_suffix, "http://127.0.0.1:59999");
    let roomy_data = DataStoreConfig::from(&roomy_info);
    let roomy_meta = Arc::new(RedbMetaEngine::open(&roomy_db).unwrap());
    roomy_meta.init_root(0, 0).unwrap();
    roomy_meta.set_format_info(&roomy_info).unwrap();
    let payload = vec![0x55u8; roomy_info.block_size as usize];

    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::remove_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
        std::env::set_var("STUDIOFS_QOS_MATERIALIZE_SLOTS", "1");
        std::env::set_var("STUDIOFS_QOS_COMMIT_SLOTS", "1");
    }

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(3)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                roomy_meta.clone(),
                &roomy_data,
                &roomy_cache_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            for idx in 0..4 {
                assert_eq!(
                    vfs.ingress_pressure_for(payload.len()).lane,
                    VfsIngressLane::DirtyMem,
                    "upload backlog alone should not force local-dirty backpressure"
                );
                let name = format!("spill-only-{idx}.bin");
                let attr = vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
                vfs.write(attr.ino, 0, &payload).await.unwrap();
                vfs.flush(attr.ino).await.unwrap();
            }

            let stats = vfs.ingress_stats();
            assert_eq!(
                stats.write_backpressure_wait_ms, 0,
                "slow/offline upload activity should not count as foreground backpressure"
            );
        });
        rt.shutdown_background();
    }

    let tight_tmp = tempfile::tempdir().unwrap();
    let tight_db = tight_tmp.path().join("meta.redb");
    let tight_cache = tight_tmp.path().join("cache");
    let tight_cache_str = tight_cache.to_str().unwrap().to_string();
    let tight_suffix = format!("pr10-backpressure-tight-{}", std::process::id());
    let tight_info = make_format_info_with_endpoint(&tight_suffix, "http://127.0.0.1:59999");
    let tight_data = DataStoreConfig::from(&tight_info);
    let tight_meta = Arc::new(RedbMetaEngine::open(&tight_db).unwrap());
    tight_meta.init_root(0, 0).unwrap();
    tight_meta.set_format_info(&tight_info).unwrap();
    let partial_payload = vec![0x44u8; (tight_info.block_size as usize) / 2];

    unsafe {
        std::env::set_var(
            "STUDIOFS_MAX_DIRTY_BYTES",
            partial_payload.len().to_string(),
        );
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "0");
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
        std::env::set_var("STUDIOFS_QOS_MATERIALIZE_SLOTS", "1");
        std::env::set_var("STUDIOFS_QOS_COMMIT_SLOTS", "1");
    }

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                tight_meta.clone(),
                &tight_data,
                &tight_cache_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
            test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start().expect("failed to start StudioVfs background workers");

            let attr = vfs.create(1, b"budget-full.bin", 0, 0, 0o644).unwrap();
            vfs.write(attr.ino, 0, &partial_payload).await.unwrap();
            let stats = vfs.ingress_stats();

            assert_eq!(
                vfs.ingress_pressure_for(partial_payload.len()).lane,
                VfsIngressLane::Backpressure,
                "buffered partial writes should continue consuming foreground dirty-memory budget until flush"
            );
            assert_eq!(
                stats.dirty_mem_bytes,
                partial_payload.len(),
                "partial writes should remain accounted as pending local-dirty bytes before flush"
            );
            vfs.flush(attr.ino).await.unwrap();
            let stats_after_flush = vfs.ingress_stats();
            assert_eq!(
                vfs.ingress_pressure_for(partial_payload.len()).lane,
                VfsIngressLane::DirtyMem,
                "explicit flush should clear the buffered dirty bytes so upload backlog alone does not create backpressure"
            );
            assert_eq!(
                stats_after_flush.dirty_mem_bytes, 0,
                "explicit flush should leave no pending local-dirty bytes behind"
            );
        });
        rt.shutdown_background();
    }
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn single_large_file_upload_does_not_starve_small_foreground_io() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    let _restore_qos_materialize = ScopedEnvVar::capture("STUDIOFS_QOS_MATERIALIZE_SLOTS");

    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "256MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "400");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
        std::env::set_var("STUDIOFS_QOS_MATERIALIZE_SLOTS", "1");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let format_info = make_format_info("pr10-fair-upload");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();
    let block_size = format_info.block_size as usize;
    let mut large_payload = vec![0x11u8; block_size * 8];
    for (index, chunk) in large_payload.chunks_mut(block_size).enumerate() {
        chunk.fill(0x11u8.saturating_add(index as u8));
    }
    let small_payload = vec![0xA4u8; 128 * 1024];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(3)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let large = vfs.create(1, b"large-upload.bin", 0, 0, 0o644).unwrap();
        vfs.write(large.ino, 0, &large_payload).await.unwrap();
        vfs.flush(large.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                vfs.pending_uploads_for_inode(large.ino).len() >= 6
            })
            .await,
            "large file should establish a backlog before the small file enters"
        );

        let small = vfs.create(1, b"small-upload.bin", 0, 0, 0o644).unwrap();
        vfs.write(small.ino, 0, &small_payload).await.unwrap();
        vfs.flush(small.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(20), || {
                vfs.meta()
                    .get_attr(small.ino)
                    .map(|attr| attr.upload_state == UploadState::Ready)
                    .unwrap_or(false)
                    && !vfs.pending_uploads_for_inode(large.ino).is_empty()
            })
            .await,
            "small file should finish while the large upload backlog is still draining"
        );

        let read = vfs
            .read(small.ino, 0, small_payload.len() as u32)
            .await
            .unwrap();
        assert_eq!(read.as_ref(), small_payload.as_slice());
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn recovery_after_offline_s3_preserves_local_accepted_view() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr10-recovery-offline-{}", std::process::id());
    let online_info = make_format_info(&volume_suffix);
    let offline_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config_offline = DataStoreConfig::from(&offline_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&online_info).unwrap();

    let block_size = online_info.block_size as usize;
    let baseline_payload = vec![0x20u8; block_size * 2];
    let mut replacement_payload = vec![0xB0u8; block_size * 2];
    replacement_payload[block_size..].fill(0xC7u8);
    let ino = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let ino = rt.block_on(async {
            let (seeded_ino, _) = seed_committed_file(
                &meta,
                &online_info,
                b"restart-offline.bin",
                &baseline_payload,
            )
            .await;
            seeded_ino
        });
        rt.shutdown_background();
        ino
    };

    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }
    meta.set_format_info(&offline_info).unwrap();

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            vfs.write(ino, 0, &replacement_payload).await.unwrap();
            vfs.flush(ino).await.unwrap();

            assert!(
                wait_until(Duration::from_secs(10), || {
                    vfs.pending_uploads_for_inode(ino).len() == 2
                })
                .await,
                "offline update should stay pending before restart"
            );

            let read = vfs
                .read(ino, 0, replacement_payload.len() as u32)
                .await
                .unwrap();
            assert_eq!(read.as_ref(), replacement_payload.as_slice());
        });
        rt.shutdown_background();
    }

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config_offline,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let read = vfs
                .read(ino, 0, replacement_payload.len() as u32)
                .await
                .unwrap();
            assert_eq!(
                read.as_ref(),
                replacement_payload.as_slice(),
                "restart with offline S3 must preserve the accepted local view"
            );
            assert!(
                wait_until(Duration::from_secs(10), || {
                    !vfs.pending_uploads_for_inode(ino).is_empty()
                })
                .await,
                "restart should recover the pending offline uploads"
            );
        });
        rt.shutdown_background();
    }
}

#[test]
#[ignore]
fn twenty_four_hour_soak_no_global_fs_stall() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    let _restore_qos_materialize = ScopedEnvVar::capture("STUDIOFS_QOS_MATERIALIZE_SLOTS");
    let _restore_qos_commit = ScopedEnvVar::capture("STUDIOFS_QOS_COMMIT_SLOTS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "256MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "2");
        std::env::set_var("STUDIOFS_QOS_MATERIALIZE_SLOTS", "1");
        std::env::set_var("STUDIOFS_QOS_COMMIT_SLOTS", "1");
    }

    let iterations = std::env::var("STUDIOFS_SOAK_ITERATIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(200);
    let max_op_ms = std::env::var("STUDIOFS_SOAK_MAX_OP_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1500);

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr10-soak-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let block_size = format_info.block_size as usize;
    let backlog_payload = vec![0x33u8; block_size * 6];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let backlog = vfs.create(1, b"soak-backlog.bin", 0, 0, 0o644).unwrap();
        vfs.write(backlog.ino, 0, &backlog_payload).await.unwrap();
        vfs.flush(backlog.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                vfs.pending_uploads_for_inode(backlog.ino).len() == 6
            })
            .await,
            "soak needs a standing upload backlog"
        );

        for idx in 0..iterations {
            let name = format!("soak-{idx:04}.bin");
            let payload = vec![(idx % 251) as u8; 256 * 1024];
            let read = tokio::time::timeout(Duration::from_millis(max_op_ms), async {
                let attr = vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
                vfs.write(attr.ino, 0, &payload).await.unwrap();
                vfs.flush(attr.ino).await.unwrap();

                loop {
                    match vfs.read(attr.ino, 0, payload.len() as u32).await {
                        Ok(read) => return read,
                        Err(VfsError::WouldBlock | VfsError::RemoteContentPending) => {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        Err(err) => {
                            panic!("unexpected soak read error on iteration {idx}: {err:?}")
                        }
                    }
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!("iteration {idx} exceeded the no-stall budget of {max_op_ms}ms")
            });
            assert_eq!(read.as_ref(), payload.as_slice());
        }

        let stats = vfs.ingress_stats();
        assert_eq!(
            stats.write_backpressure_wait_ms, 0,
            "soak should not accumulate foreground backpressure from upload stall alone"
        );
    });
    rt.shutdown_background();
}

#[test]
fn test_ingress_prefers_spill_before_backpressure() {
    type EnvRestore = ScopedEnvVar;

    let _restore_dirty = EnvRestore::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = EnvRestore::capture("STUDIOFS_MAX_SPILL_BYTES");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "16MB");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("spill-preferred-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
        test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start().expect("failed to start StudioVfs background workers");
        let attr = vfs.create(1, b"spill-first.bin", 0, 0, 0o644).unwrap();
        let payload = vec![0x33u8; 512 * 1024];

        assert_eq!(
            vfs.ingress_pressure_for(payload.len()).lane,
            VfsIngressLane::DirtyMem,
            "the first payload enters the write buffer and is then locally committed"
        );

        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert_eq!(
            vfs.ingress_pressure_for(1).lane,
            VfsIngressLane::DirtyMem,
            "after flush drains the write buffer, follow-up writes start from an empty local budget again"
        );
    });
    rt.shutdown_background();
}

#[test]
fn test_single_mode_ingress_write_path_materializes_local_blocks() {
    type EnvRestore = ScopedEnvVar;
    let _restore_dirty = EnvRestore::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = EnvRestore::capture("STUDIOFS_MAX_SPILL_BYTES");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "16MB");
    }

    fn build_vfs(tmp: &tempfile::TempDir, volume_suffix: &str) -> (Arc<StudioVfs>, Vec<u8>) {
        let db_path = tmp.path().join("meta.redb");
        let cache_dir = tmp.path().join("cache");
        let cache_dir_str = cache_dir.to_str().unwrap().to_string();
        let format_info = make_format_info(volume_suffix);
        let data_config = DataStoreConfig::from(&format_info);
        let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
        meta.init_root(0, 0).unwrap();
        meta.set_format_info(&format_info).unwrap();

        let vfs_config: VfsConfigRaw = test_vfs_config_from_env().into_inner().into();
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            vfs_config
                .validate()
                .expect("test VFS config should validate"),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");
        (vfs, vec![0x39u8; 512 * 1024])
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let (vfs, payload) = build_vfs(&tmp, &format!("single-mode-{}", std::process::id()));
        let attr = vfs.create(1, b"single-mode.bin", 0, 0, 0o644).unwrap();
        assert_eq!(
            vfs.ingress_pressure_for(payload.len()).lane,
            VfsIngressLane::DirtyMem
        );
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();
        let _stats = vfs.ingress_stats();
        assert_eq!(
            vfs.meta()
                .lookup_logical_payload_slices(attr.ino)
                .unwrap()
                .len(),
            1
        );
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn test_write_fast_mode_no_spill_artifacts() {
    fn segment_file_count(dir: &std::path::Path) -> usize {
        let dir = dir.join("segments");
        match std::fs::read_dir(dir) {
            Ok(entries) => entries
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "seg"))
                .count(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => panic!("failed to inspect ingress segments: {error}"),
        }
    }

    let _env = ScopedTestEnv::new();
    apply_test_online_s3_env();

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let spill_dir = cache_dir.join("ingress_spill");
    let volume_suffix = format!("fast-no-spill-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let payload = vec![0x5Au8; 512 * 1024];
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs_config = VfsConfigRaw {
            max_dirty_bytes: Some(32 * 1024 * 1024),
            ..VfsConfigRaw::default()
        };
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            vfs_config
                .validate()
                .expect("test VFS config should validate"),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs.create(1, b"fast-no-spill.bin", 0, 0, 0o644).unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert_eq!(
            segment_file_count(&spill_dir),
            0,
            "fast mode must not persist ingress segment payload files"
        );

        let mut object_id = None;
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            if let Some((_, id, _)) = vfs.pending_uploads_for_inode(attr.ino).into_iter().next() {
                object_id = Some(id);
                break;
            }
            if let Ok(blocks) = vfs.meta().lookup_logical_payload_slices(attr.ino)
                && let Some((_, block_ref)) = blocks.first()
            {
                object_id = Some(block_ref.object_id);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let object_id =
            object_id.expect("expected pending/committed object id for fast-mode write");
        let ds = DataStore::new(&data_config).unwrap();
        assert!(
            wait_until_object_exists(
                &ds,
                &format_info.data_prefix,
                object_id,
                Duration::from_secs(60)
            )
            .await,
            "fast-mode flush should still publish object to remote store"
        );

        vfs.shutdown(Duration::from_secs(15)).await;
    });
    rt.shutdown_background();
}

#[test]
fn test_ingress_pressure_reflects_buffered_partial_write_until_flush() {
    type EnvRestore = ScopedEnvVar;

    let _restore_dirty = EnvRestore::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = EnvRestore::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_flush = EnvRestore::capture("STUDIOFS_SLAB_FLUSH_INTERVAL_MS");
    let _restore_assist = EnvRestore::capture("STUDIOFS_FOREGROUND_DRAIN_ASSIST");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "512KB");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "512KB");
        std::env::set_var("STUDIOFS_SLAB_FLUSH_INTERVAL_MS", "200");
        std::env::set_var("STUDIOFS_FOREGROUND_DRAIN_ASSIST", "0");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("spill-backpressure-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");
        let attr = vfs
            .create(1, b"spill-backpressure.bin", 0, 0, 0o644)
            .unwrap();
        let payload = vec![0x44u8; 512 * 1024];

        assert_eq!(
            vfs.ingress_pressure_for(payload.len()).lane,
            VfsIngressLane::DirtyMem,
            "an empty write buffer accepts the first partial payload"
        );

        vfs.write(attr.ino, 0, &payload).await.unwrap();

        let stats = vfs.ingress_stats();
        assert_eq!(
            stats.dirty_mem_bytes,
            payload.len(),
            "the first partial payload should remain buffered in dirty memory until flush"
        );
        assert_eq!(
            vfs.ingress_pressure_for(payload.len()).lane,
            VfsIngressLane::Backpressure,
            "with the dirty budget fully consumed by the buffered partial payload, the next payload should see backpressure until flush"
        );

        vfs.flush(attr.ino).await.unwrap();

        let stats_after_flush = vfs.ingress_stats();
        assert_eq!(
            stats_after_flush.dirty_mem_bytes,
            0,
            "explicit flush should clear the buffered partial payload from dirty memory"
        );
        assert_eq!(
            vfs.ingress_pressure_for(payload.len()).lane,
            VfsIngressLane::DirtyMem,
            "once the buffered partial payload is flushed, the next payload should start from a clean local budget"
        );
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn test_ingress_backpressure_wait_time_accumulates_until_drain() {
    let _env = ScopedTestEnv::new();
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_flush_interval = ScopedEnvVar::capture("STUDIOFS_SLAB_FLUSH_INTERVAL_MS");
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    unsafe {
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "512KB");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "512KB");
        std::env::set_var("STUDIOFS_SLAB_FLUSH_INTERVAL_MS", "250");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("spill-wait-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        ensure_bucket().await;
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
        test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start().expect("failed to start StudioVfs background workers");

        let payload = vec![0x55u8; format_info.block_size as usize];

        let first = vfs.create(1, b"local-dirty-full.bin", 0, 0, 0o644).unwrap();
        vfs.write(first.ino, 0, &payload).await.unwrap();
        assert_eq!(
            vfs.ingress_pressure_for(payload.len()).lane,
            VfsIngressLane::DirtyMem,
            "once the first payload is durably committed locally, the next payload should start from a clean local budget"
        );

        let second = vfs.create(1, b"drained.bin", 0, 0, 0o644).unwrap();
        vfs.write(second.ino, 0, &payload).await.unwrap();
        match tokio::time::timeout(Duration::from_secs(30), vfs.flush(second.ino)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("flush failed: {e}"),
            Err(_) => {
                let stats = vfs.ingress_stats();
                panic!(
                    "flush timed out after 30s: dirty_mem={} wait_ms={}",
                    stats.dirty_mem_bytes, stats.write_backpressure_wait_ms
                );
            }
        }

        let stats = vfs.ingress_stats();
        assert!(
            stats.dirty_mem_bytes == 0,
            "foreground writes should leave no dirty-memory tail once each write crosses the local durable boundary"
        );
        assert_eq!(
            stats.write_backpressure_wait_ms, 0,
            "sequential locally committed writes must not accumulate ingress backpressure wait time"
        );
    });
    rt.shutdown_background();
}

#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn test_ingress_multi_writer_pressure_prefers_spill_without_wait() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("spill-multi-{}", std::process::id());
    let format_info = make_format_info(&volume_suffix);
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    rt.block_on(async {
            ensure_bucket().await;
            let vfs_config = VfsConfigRaw {
                max_dirty_bytes: Some(4 * 256 * 1024),
                slab_flush_interval_ms: Some(5000),
            ..VfsConfigRaw::default()
        };
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            vfs_config.validate().expect("test VFS config should validate"),
        )
        .unwrap();
        vfs.start().expect("failed to start StudioVfs background workers");
        let writer_count = 4usize;
        let payload = vec![0x66u8; 256 * 1024];

        let mut tasks = Vec::new();
        for idx in 0..writer_count {
            let writer_vfs = vfs.clone();
            let payload = payload.clone();
            tasks.push(tokio::spawn(async move {
                let name = format!("spill-writer-{idx}.bin");
                let attr = writer_vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
                writer_vfs.write(attr.ino, 0, &payload).await.unwrap();
                writer_vfs.flush(attr.ino).await.unwrap();
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        let stats = vfs.ingress_stats();
        assert_eq!(
            stats.write_backpressure_wait_ms, 0,
            "with sufficient local budget, concurrent foreground writers should not block"
        );
        assert_eq!(
            vfs.ingress_pressure_for(payload.len()).lane,
            VfsIngressLane::DirtyMem,
            "once flush drains the write buffer, new writes should start from the empty local budget again"
        );
    });
    rt.shutdown_background();
}

#[test]
fn test_ingress_multi_writer_spill_recovers_after_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("spill-multi-crash-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let writer_count = 3usize;
    let mut files: Vec<(u64, Vec<u8>)> = Vec::new();

    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs_config = VfsConfigRaw {
                max_dirty_bytes: Some(8 * 1024 * 1024),
                slab_flush_interval_ms: Some(5000),
                ..VfsConfigRaw::default()
            };
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                vfs_config
                    .validate()
                    .expect("test VFS config should validate"),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            let mut tasks = Vec::new();
            for idx in 0..writer_count {
                let writer_vfs = vfs.clone();
                tasks.push(tokio::spawn(async move {
                    let payload = vec![(0x70 + idx) as u8; 256 * 1024];
                    let name = format!("spill-crash-{idx}.bin");
                    let attr = writer_vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
                    writer_vfs.write(attr.ino, 0, &payload).await.unwrap();
                    writer_vfs.flush(attr.ino).await.unwrap();
                    (attr.ino, payload)
                }));
            }

            for task in tasks {
                files.push(task.await.unwrap());
            }
        });
        rt.shutdown_background();
    }
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");

            for (ino, payload) in &files {
                let data = vfs.read(*ino, 0, payload.len() as u32).await.unwrap();
                assert_eq!(
                    data.as_ref(),
                    payload.as_slice(),
                    "read-after-restart should return locally committed bytes for every file"
                );
            }
        });
        rt.shutdown_background();
    }
}

#[test]
#[ignore]
fn test_crash_recovery_replays_from_segments_when_spill_files_are_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let spill_dir = cache_dir.join("ingress_spill");

    let volume_suffix = format!("segment-crash-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let payload = vec![0x77u8; 512 * 1024];
    let mut ino = 0u64;

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs_config = VfsConfigRaw {
                max_dirty_bytes: Some(1),
                ..VfsConfigRaw::default()
            };
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                vfs_config
                    .validate()
                    .expect("test VFS config should validate"),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            let attr = vfs.create(1, b"segment-replay.bin", 0, 0, 0o644).unwrap();
            ino = attr.ino;
            tokio::time::timeout(Duration::from_secs(15), vfs.write(ino, 0, &payload))
                .await
                .expect("write timed out in crash-recovery regression")
                .unwrap();
            tokio::time::timeout(Duration::from_secs(15), vfs.flush(ino))
                .await
                .expect("flush timed out in crash-recovery regression")
                .unwrap();
        });
        rt.shutdown_background();
    }

    match std::fs::read_dir(&spill_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry.unwrap_or_else(|err| {
                    panic!(
                        "failed to read entry in spill dir {}: {err}",
                        spill_dir.display()
                    )
                });
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "spill") {
                    std::fs::remove_file(path).unwrap();
                }
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            panic!("failed to read spill dir {}: {err}", spill_dir.display())
        }
    }

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let vfs_config = VfsConfigRaw {
                max_dirty_bytes: Some(1),
                ..VfsConfigRaw::default()
            };
            let vfs = StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                vfs_config
                    .validate()
                    .expect("test VFS config should validate"),
            )
            .unwrap();
            vfs.start()
                .expect("failed to start StudioVfs background workers");
            let data = tokio::time::timeout(
                Duration::from_secs(20),
                vfs.read(ino, 0, payload.len() as u32),
            )
            .await
            .expect("read timed out in crash-recovery regression")
            .unwrap();
            assert_eq!(
                data.as_ref(),
                payload.as_slice(),
                "segment replay should recover accepted staged data even if spill files are gone"
            );
        });
        rt.shutdown_background();
    }
}

// =============================================================================
// Metadata Tests
// =============================================================================

#[tokio::test]
async fn test_block_ref_flags() {
    // Test BlockRef flags
    let full_block = BlockRef::new_full(123, 4 * 1024 * 1024);
    assert!(!full_block.is_compressed());
    assert!(!full_block.is_partial());
    assert_eq!(full_block.object_id, 123);
    assert_eq!(full_block.length, 4 * 1024 * 1024);

    let partial_block = BlockRef::new_partial(456, 1000);
    assert!(!partial_block.is_compressed());
    assert!(partial_block.is_partial());
    assert_eq!(partial_block.object_id, 456);
    assert_eq!(partial_block.length, 1000);
}

#[tokio::test]
async fn test_object_format() {
    let original_data = b"Test data for object format";

    // Create object
    let object = create_object_buffer(Bytes::copy_from_slice(original_data)).unwrap();

    // Verify we can read it back
    let decompressed = read_object(object).unwrap();
    assert_eq!(decompressed.as_ref(), original_data);
}

#[tokio::test]
async fn test_object_key_format() {
    let data_prefix = "disk-a/objects/";
    let object_id: u64 = 0x1234;

    let key = format_object_key(data_prefix, object_id);
    assert_eq!(key, "disk-a/objects/0000/0000000000001234");
}

// =============================================================================
// Performance Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_sequential_write_performance() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "perf");

    let attr = vfs.create(1, b"perf.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 100 MiB
    let size = 100 * 1024 * 1024;
    let block_size = 4 * 1024 * 1024; // 4 MiB blocks

    let start = std::time::Instant::now();

    for i in 0..(size / block_size) {
        let offset = (i * block_size) as u64;
        let payload: Vec<u8> = (0..block_size).map(|j| ((i + j) % 256) as u8).collect();
        vfs.write(ino, offset, &payload).await.unwrap();
    }

    let write_duration = start.elapsed();
    let write_mbps = (size as f64 / 1024.0 / 1024.0) / write_duration.as_secs_f64();

    println!(
        "Write: {:.2} MiB/s ({} bytes in {:?})",
        write_mbps, size, write_duration
    );

    // Flush
    let flush_start = std::time::Instant::now();
    vfs.flush(ino).await.unwrap();
    let flush_duration = flush_start.elapsed();
    println!("Flush: {:?}", flush_duration);

    // Read back
    let read_start = std::time::Instant::now();
    let data = vfs.read(ino, 0, size as u32).await.unwrap();
    let read_duration = read_start.elapsed();
    let read_mbps = (size as f64 / 1024.0 / 1024.0) / read_duration.as_secs_f64();

    println!(
        "Read: {:.2} MiB/s ({} bytes in {:?})",
        read_mbps, size, read_duration
    );

    assert_eq!(data.len(), size);
}
/// Test attribute cache with TTL and invalidation on writes.
#[tokio::test(flavor = "multi_thread")]
async fn test_attr_cache_ttl() {
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "attr_cache");

    // Create a file
    let file_attr = vfs.create(1, b"test.txt", 0, 0, 0o644).unwrap();
    let ino = file_attr.ino;

    // First getattr — loads from redb, populates cache
    let attr1 = vfs.getattr(ino).unwrap();
    assert_eq!(attr1.ino, ino);
    assert_eq!(attr1.size, 0);

    // Second getattr immediately — should hit read-through cache (no redb read)
    let attr2 = vfs.getattr(ino).unwrap();
    assert_eq!(attr2.ino, ino);
    assert_eq!(attr2.size, 0);

    // Write data via write() — should not immediately invalidate cache
    vfs.write(ino, 0, b"hello").await.unwrap();

    // Getattr should return value from dirty_attrs (written but not flushed)
    let attr3 = vfs.getattr(ino).unwrap();
    assert_eq!(attr3.size, 5); // size updated by write path

    // Flush the file
    vfs.flush(ino).await.unwrap();

    // After flush, read-through cache was invalidated, should re-fetch
    // (but dirty_attrs is cleared, so it comes from redb now)
    let attr4 = vfs.getattr(ino).unwrap();
    assert_eq!(attr4.size, 5);

    // Setattr should invalidate cache
    vfs.setattr(ino, SetattrArgs::size_only(10)).unwrap();

    // Next getattr should see the new size from dirty_attrs
    let attr5 = vfs.getattr(ino).unwrap();
    assert_eq!(attr5.size, 10);
}

// =============================================================================
// metadata sync Invalidation Tests
// =============================================================================

// =============================================================================
// Handle Tracking Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_open_file_and_release() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "handle1");

    let attr = vfs.create(1, b"handle.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Open a file handle
    let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
    assert_ne!(fh, 0);

    // Check open count
    assert_eq!(vfs.open_count(ino), 1);

    // Get the open file info
    let of = vfs.get_open_file(fh).unwrap();
    assert_eq!(of.ino, ino);

    // Write through handle
    vfs.write(ino, 0, b"via handle").await.unwrap();
    vfs.flush_fh(fh).await.unwrap();

    // Release the handle
    vfs.release_fh(fh).await.unwrap();
    assert_eq!(vfs.open_count(ino), 0);

    // Data should persist after release
    let data = vfs.read(ino, 0, 10).await.unwrap();
    assert_eq!(&data[..], b"via handle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_open_trunc() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "handle_trunc");

    let attr = vfs.create(1, b"trunc.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write some data
    vfs.write(ino, 0, b"existing data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Open with trunc=true
    let fh = vfs.open_file(ino, OpenFlags::RDWR, true).unwrap();

    // File should be truncated
    let attr = vfs.getattr(ino).unwrap();
    assert_eq!(attr.size, 0);

    vfs.release_fh(fh).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_open_unlink_deferred_cleanup() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "orphan");

    let attr = vfs.create(1, b"orphan.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write data
    vfs.write(ino, 0, b"orphan data").await.unwrap();
    vfs.flush(ino).await.unwrap();
    assert!(
        wait_until(Duration::from_secs(20), || {
            vfs.meta()
                .get_attr(ino)
                .map(|attr| attr.upload_state == UploadState::Ready)
                .unwrap_or(false)
        })
        .await,
        "orphan cleanup test should begin after the file reaches committed ready state"
    );

    // Open a handle
    let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

    // Unlink while handle is open — should defer cleanup
    vfs.unlink(1, b"orphan.txt").unwrap();

    // File is no longer findable by name
    assert!(vfs.lookup(1, b"orphan.txt").is_err());

    // But data is still accessible via ino (the handle keeps it alive)
    let data = vfs.read(ino, 0, 11).await.unwrap();
    assert_eq!(&data[..], b"orphan data");

    // Release the last handle — orphan cleanup should happen
    vfs.release_fh(fh).await.unwrap();

    // Inode should be gone now
    assert!(vfs.getattr(ino).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_release_fh_orphan_cleanup_is_idempotent_when_inode_already_missing() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "orphan-idempotent-cleanup");

    let attr = vfs
        .create(1, b"orphan-idempotent.txt", 0, 0, 0o644)
        .unwrap();
    let ino = attr.ino;
    vfs.write(ino, 0, b"orphan data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
    vfs.unlink(1, b"orphan-idempotent.txt").unwrap();

    vfs.meta().remove_attr(ino).unwrap();

    assert!(
        vfs.release_fh(fh).await.is_ok(),
        "release_fh must not fail if orphan inode was already removed"
    );
    assert!(vfs.meta().lookup(1, b"orphan-idempotent.txt").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_restart_recovers_deferred_open_unlink_cleanup() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let volume_suffix = "orphan-restart-recovery";

    let vfs = make_vfs(&tmp, volume_suffix);
    let attr = vfs.create(1, b"orphan-restart.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    vfs.write(ino, 0, b"orphan restart data").await.unwrap();
    vfs.flush(ino).await.unwrap();
    let _ = wait_until_n_payload_slices(&vfs, ino, 1, Duration::from_secs(15));

    let _fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
    vfs.unlink(1, b"orphan-restart.txt").unwrap();
    assert!(
        vfs.meta()
            .list_deferred_deletes()
            .unwrap()
            .iter()
            .any(|record| record.ino == ino),
        "open-unlink should persist deferred delete intent before restart"
    );

    vfs.shutdown(Duration::from_secs(5)).await;
    drop(vfs);

    let reopened = make_vfs(&tmp, volume_suffix);
    assert!(
        reopened.meta().lookup(1, b"orphan-restart.txt").is_err(),
        "name should stay removed after restart recovery"
    );
    assert!(
        reopened.meta().get_attr(ino).is_err(),
        "startup recovery should finalize orphan inode cleanup"
    );
    assert!(
        !reopened
            .meta()
            .list_deferred_deletes()
            .unwrap()
            .iter()
            .any(|record| record.ino == ino),
        "startup recovery should clear deferred delete record"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_multiple_handles_open_unlink() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "multi_handle");

    let attr = vfs.create(1, b"multi.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    vfs.write(ino, 0, b"multi-handle data").await.unwrap();
    vfs.flush(ino).await.unwrap();
    assert!(
        wait_until(Duration::from_secs(20), || {
            vfs.meta()
                .get_attr(ino)
                .map(|attr| attr.upload_state == UploadState::Ready)
                .unwrap_or(false)
        })
        .await,
        "multi-handle orphan test should begin after the file reaches committed ready state"
    );

    // Open two handles
    let fh1 = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
    let fh2 = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
    assert_eq!(vfs.open_count(ino), 2);

    // Unlink while both handles open
    vfs.unlink(1, b"multi.txt").unwrap();

    // Release first handle — inode should still exist (one handle left)
    vfs.release_fh(fh1).await.unwrap();
    assert_eq!(vfs.open_count(ino), 1);
    let data = vfs.read(ino, 0, 17).await.unwrap();
    assert_eq!(&data[..], b"multi-handle data");

    // Release second handle — now cleanup should happen
    vfs.release_fh(fh2).await.unwrap();
    assert!(vfs.getattr(ino).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_dangling_dentry_is_self_healed_on_lookup_and_unlink() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "dangling-dentry-heal");

    let dangling = vfs.create(1, b"dangling.txt", 0, 0, 0o644).unwrap();
    vfs.meta().remove_attr(dangling.ino).unwrap();
    assert!(
        vfs.meta().lookup(1, b"dangling.txt").is_ok(),
        "test setup requires a dentry that still points to the removed inode"
    );

    assert!(vfs.lookup(1, b"dangling.txt").is_err());
    assert!(
        vfs.meta().lookup(1, b"dangling.txt").is_err(),
        "lookup should self-heal the dangling dentry"
    );

    let dangling_unlink = vfs.create(1, b"dangling-unlink.txt", 0, 0, 0o644).unwrap();
    vfs.meta().remove_attr(dangling_unlink.ino).unwrap();
    assert!(
        vfs.unlink(1, b"dangling-unlink.txt").is_ok(),
        "unlink should succeed by removing the dangling dentry even if inode is missing"
    );
    assert!(vfs.meta().lookup(1, b"dangling-unlink.txt").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_dangling_dentry_is_self_healed_on_readdir() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "dangling-dentry-readdir-heal");

    let dangling = vfs.create(1, b"dangling-readdir.txt", 0, 0, 0o644).unwrap();
    vfs.meta().remove_attr(dangling.ino).unwrap();
    assert!(
        vfs.meta().lookup(1, b"dangling-readdir.txt").is_ok(),
        "test setup requires a dentry that still points to the removed inode"
    );

    let entries = vfs.readdir(1).unwrap();
    assert!(
        entries
            .iter()
            .any(|entry| entry.name.as_slice() == b"dangling-readdir.txt"),
        "readdir should not perform inode existence healing"
    );
    assert!(
        vfs.meta().lookup(1, b"dangling-readdir.txt").is_ok(),
        "dentry should still exist after readdir-only access"
    );
    assert!(
        vfs.lookup(1, b"dangling-readdir.txt").is_err(),
        "foreground lookup should trigger lazy dangling-dentry healing"
    );
    assert!(vfs.meta().lookup(1, b"dangling-readdir.txt").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_fsync() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "fsync");

    let attr = vfs.create(1, b"fsync.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    vfs.write(ino, 0, b"fsync test data").await.unwrap();

    // fsync should flush data
    vfs.fsync(ino, false).await.unwrap();

    // Data should be readable
    let data = vfs.read(ino, 0, 15).await.unwrap();
    assert_eq!(&data[..], b"fsync test data");

    // datasync=true should also work
    vfs.write(ino, 0, b"datasync test!!").await.unwrap();
    vfs.fsync(ino, true).await.unwrap();

    let data = vfs.read(ino, 0, 15).await.unwrap();
    assert_eq!(&data[..], b"datasync test!!");
}

// =============================================================================
// Phase 4: setattr Timestamps + Symlinks
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_setattr_timestamps() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "setattr_ts");

    let attr = vfs.create(1, b"ts.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    let _original_atime = attr.atime;
    let _original_mtime = attr.mtime;

    // Set specific atime and mtime
    let new_atime = 1_700_000_000_000_000_000i64; // some timestamp in nanos
    let new_mtime = 1_600_000_000_000_000_000i64;
    let args = SetattrArgs {
        atime: TimeSpec::Set(new_atime),
        mtime: TimeSpec::Set(new_mtime),
        ..SetattrArgs::default()
    };
    let updated = vfs.setattr(ino, args).unwrap();
    assert_eq!(updated.atime, new_atime);
    assert_eq!(updated.mtime, new_mtime);
    // ctime should be updated (any change touches ctime)
    assert!(updated.ctime >= attr.ctime);

    // TimeSpec::Now should set to current time
    let args2 = SetattrArgs {
        atime: TimeSpec::Now,
        ..SetattrArgs::default()
    };
    let updated2 = vfs.setattr(ino, args2).unwrap();
    assert!(updated2.atime > new_atime); // should be current time, which is much later
    assert_eq!(updated2.mtime, new_mtime); // mtime unchanged (Omit)

    // TimeSpec::Omit should leave value unchanged
    let args3 = SetattrArgs {
        atime: TimeSpec::Omit,
        mtime: TimeSpec::Omit,
        ..SetattrArgs::default()
    };
    let updated3 = vfs.setattr(ino, args3).unwrap();
    assert_eq!(updated3.atime, updated2.atime);
    assert_eq!(updated3.mtime, new_mtime);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_setattr_mode_uid_gid() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "setattr_mode");

    let attr = vfs.create(1, b"chmod.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Change mode
    let args = SetattrArgs {
        mode: Some(0o755),
        ..SetattrArgs::default()
    };
    let updated = vfs.setattr(ino, args).unwrap();
    assert_eq!(updated.mode, 0o755);

    // Change uid and gid
    let args2 = SetattrArgs {
        uid: Some(500),
        gid: Some(600),
        ..SetattrArgs::default()
    };
    let updated2 = vfs.setattr(ino, args2).unwrap();
    assert_eq!(updated2.uid, 500);
    assert_eq!(updated2.gid, 600);
    assert_eq!(updated2.mode, 0o755); // mode unchanged
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_symlink_create_and_readlink() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "symlink_basic");

    // Create a symlink
    let attr = vfs
        .symlink(1, b"mylink", b"/some/target/path", 0, 0)
        .unwrap();
    assert_eq!(attr.kind, studiofs_meta::FileType::Symlink);
    assert_eq!(attr.mode, 0o777);
    assert_eq!(attr.size, 17); // length of "/some/target/path"

    // Readlink
    let target = vfs.readlink(attr.ino).unwrap();
    assert_eq!(&target, b"/some/target/path");

    // Lookup should find it
    let looked_up = vfs.lookup(1, b"mylink").unwrap();
    assert_eq!(looked_up.ino, attr.ino);
    assert_eq!(looked_up.kind, studiofs_meta::FileType::Symlink);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_symlink_unlink() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "symlink_unlink");

    let attr = vfs.symlink(1, b"del_link", b"/target", 0, 0).unwrap();
    let ino = attr.ino;

    // Verify exists
    assert!(vfs.lookup(1, b"del_link").is_ok());

    // Unlink symlink
    vfs.unlink(1, b"del_link").unwrap();

    // Should be gone
    assert!(vfs.lookup(1, b"del_link").is_err());
    assert!(vfs.getattr(ino).is_err());
    assert!(vfs.readlink(ino).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_symlink_in_readdir() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "symlink_readdir");

    // Create a regular file and a symlink
    vfs.create(1, b"file.txt", 0, 0, 0o644).unwrap();
    vfs.symlink(1, b"link.txt", b"file.txt", 0, 0).unwrap();

    let entries = vfs.readdir(1).unwrap();
    assert_eq!(entries.len(), 2);

    let link_entry = entries.iter().find(|e| e.name == b"link.txt").unwrap();
    assert_eq!(link_entry.kind, studiofs_meta::FileType::Symlink);

    let file_entry = entries.iter().find(|e| e.name == b"file.txt").unwrap();
    assert_eq!(file_entry.kind, studiofs_meta::FileType::RegularFile);
}

// =============================================================================
// Phase 5 — Hardlink Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_hardlink_create_and_read() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hardlink_create");

    // Create a file and write data
    let attr = vfs.create(1, b"original.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    vfs.write(ino, 0, b"hardlink data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Create hardlink
    let linked = vfs.link(ino, 1, b"linked.txt").unwrap();
    assert_eq!(linked.ino, ino);
    assert_eq!(linked.nlink, 2);

    // Read data via original inode
    let data1 = vfs.read(ino, 0, 1024).await.unwrap();
    assert_eq!(&data1[..], b"hardlink data");

    // The linked file has the same ino
    let lookup = vfs.lookup(1, b"linked.txt").unwrap();
    assert_eq!(lookup.ino, ino);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_hardlink_unlink_keeps_data() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hardlink_unlink");

    // Create a file and write data
    let attr = vfs.create(1, b"src.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    vfs.write(ino, 0, b"survive unlink").await.unwrap();
    vfs.flush(ino).await.unwrap();
    assert!(
        wait_until(Duration::from_secs(20), || {
            vfs.meta()
                .get_attr(ino)
                .map(|attr| attr.upload_state == UploadState::Ready)
                .unwrap_or(false)
        })
        .await,
        "hardlink/unlink test should begin after the source file reaches committed ready state"
    );

    // Create hardlink, then remove the original
    vfs.link(ino, 1, b"backup.txt").unwrap();
    vfs.unlink(1, b"src.txt").unwrap();

    // Inode still accessible via hardlink name
    let lookup = vfs.lookup(1, b"backup.txt").unwrap();
    assert_eq!(lookup.ino, ino);
    assert_eq!(lookup.nlink, 1);

    // Data still readable
    let data = vfs.read(ino, 0, 1024).await.unwrap();
    assert_eq!(&data[..], b"survive unlink");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_hardlink_dir_fails() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hardlink_dir_fail");

    let dir = vfs.mkdir(1, b"mydir", 0, 0, 0o755).unwrap();
    let result = vfs.link(dir.ino, 1, b"dir_link");
    assert!(result.is_err());
}

// =============================================================================
// Phase 5 — Flock Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flock_exclusive_blocks() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flock_excl");

    let attr = vfs.create(1, b"locked.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Owner 1 takes exclusive lock
    vfs.flock(ino, LockKind::Exclusive, 1, false).await.unwrap();

    // Owner 2 non-blocking exclusive should fail with WouldBlock
    let result = vfs.flock(ino, LockKind::Exclusive, 2, false).await;
    assert!(matches!(result, Err(VfsError::WouldBlock)));

    // Owner 2 non-blocking shared should also fail
    let result2 = vfs.flock(ino, LockKind::Shared, 2, false).await;
    assert!(matches!(result2, Err(VfsError::WouldBlock)));

    // Owner 1 unlocks
    vfs.flock(ino, LockKind::Unlock, 1, false).await.unwrap();

    // Now owner 2 can take the lock
    vfs.flock(ino, LockKind::Exclusive, 2, false).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flock_shared_allows_multiple() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flock_shared");

    let attr = vfs.create(1, b"shared.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Multiple owners can hold shared locks
    vfs.flock(ino, LockKind::Shared, 1, false).await.unwrap();
    vfs.flock(ino, LockKind::Shared, 2, false).await.unwrap();
    vfs.flock(ino, LockKind::Shared, 3, false).await.unwrap();

    // Exclusive lock should be blocked
    let result = vfs.flock(ino, LockKind::Exclusive, 4, false).await;
    assert!(matches!(result, Err(VfsError::WouldBlock)));

    // Unlock all shared locks
    vfs.flock(ino, LockKind::Unlock, 1, false).await.unwrap();
    vfs.flock(ino, LockKind::Unlock, 2, false).await.unwrap();
    vfs.flock(ino, LockKind::Unlock, 3, false).await.unwrap();

    // Now exclusive should succeed
    vfs.flock(ino, LockKind::Exclusive, 4, false).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flock_unlock_and_relock() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flock_relock");

    let attr = vfs.create(1, b"relock.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Lock, unlock, then a different owner can lock
    vfs.flock(ino, LockKind::Exclusive, 10, false)
        .await
        .unwrap();
    vfs.flock(ino, LockKind::Unlock, 10, false).await.unwrap();
    vfs.flock(ino, LockKind::Exclusive, 20, false)
        .await
        .unwrap();

    // Re-entrant: same owner can re-lock
    vfs.flock(ino, LockKind::Exclusive, 20, false)
        .await
        .unwrap();
}

// =============================================================================
// Phase 6 — xattr Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_set_get_remove() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr_basic");

    let attr = vfs.create(1, b"file.txt", 0, 0, 0o644).unwrap();

    // Set xattr
    vfs.set_xattr(attr.ino, b"user.color", b"blue", 0).unwrap();

    // Get xattr
    let val = vfs.get_xattr(attr.ino, b"user.color").unwrap();
    assert_eq!(val, Some(b"blue".to_vec()));

    // List xattrs
    let names = vfs.list_xattrs(attr.ino).unwrap();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0], b"user.color");

    // Remove xattr
    vfs.remove_xattr(attr.ino, b"user.color").unwrap();
    let val2 = vfs.get_xattr(attr.ino, b"user.color").unwrap();
    assert!(val2.is_none());
    assert!(vfs.list_xattrs(attr.ino).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_cascade_on_unlink() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr_cascade");

    let attr = vfs.create(1, b"doomed.txt", 0, 0, 0o644).unwrap();
    vfs.set_xattr(attr.ino, b"user.tag", b"important", 0)
        .unwrap();

    vfs.unlink(1, b"doomed.txt").unwrap();

    // xattrs should be cleaned up after unlink
    let names = vfs.list_xattrs(attr.ino).unwrap();
    assert!(names.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_multiple_attrs() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr_multi");

    let attr = vfs.create(1, b"multi.txt", 0, 0, 0o644).unwrap();

    vfs.set_xattr(attr.ino, b"user.a", b"1", 0).unwrap();
    vfs.set_xattr(attr.ino, b"user.b", b"2", 0).unwrap();
    vfs.set_xattr(attr.ino, b"user.c", b"3", 0).unwrap();

    let names = vfs.list_xattrs(attr.ino).unwrap();
    assert_eq!(names.len(), 3);

    // Overwrite one
    vfs.set_xattr(attr.ino, b"user.b", b"updated", 0).unwrap();
    let val = vfs.get_xattr(attr.ino, b"user.b").unwrap();
    assert_eq!(val, Some(b"updated".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_limit_exact_and_exceeds_boundaries() {
    ensure_bucket().await;
    let _restore_xattr_limit = ScopedEnvVar::capture("STUDIOFS_XATTR_MAX_BYTES");
    unsafe {
        std::env::set_var("STUDIOFS_XATTR_MAX_BYTES", "16");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr_limit_boundary");
    let attr = vfs.create(1, b"xattr-limit.txt", 0, 0, 0o644).unwrap();

    let exact = vec![b'a'; 16];
    vfs.set_xattr(attr.ino, b"user.limit", &exact, 0).unwrap();
    assert_eq!(vfs.get_xattr(attr.ino, b"user.limit").unwrap(), Some(exact));

    let too_big = vec![b'b'; 17];
    let err = vfs
        .set_xattr(attr.ino, b"user.limit", &too_big, 0)
        .unwrap_err();
    assert!(matches!(
        err,
        VfsError::Meta(MetaError::XattrValueTooLarge {
            actual,
            limit
        }) if actual == 17 && limit == 16
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_resource_fork_honors_limit() {
    ensure_bucket().await;
    let _restore_xattr_limit = ScopedEnvVar::capture("STUDIOFS_XATTR_MAX_BYTES");
    unsafe {
        std::env::set_var("STUDIOFS_XATTR_MAX_BYTES", "8");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr_limit_resource_fork");
    let attr = vfs.create(1, b"fork.bin", 0, 0, 0o644).unwrap();
    let fork_name = b"com.apple.ResourceFork";

    let fits = vec![0x72, 0x73, 0x72, 0x63, 0x00, 0x00, 0x00, 0x00];
    vfs.set_xattr(attr.ino, fork_name, &fits, 0).unwrap();

    let err = vfs
        .set_xattr(attr.ino, fork_name, b"toolongforkvalue", 0)
        .unwrap_err();
    assert!(matches!(
        err,
        VfsError::Meta(MetaError::XattrValueTooLarge { .. })
    ));
}

// =============================================================================
// Phase 7 — Filename Validation Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_validate_name_create_rejects_invalid() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "validate_create");

    // Empty name
    assert!(vfs.create(1, b"", 0, 0, 0o644).is_err());

    // Name with slash
    assert!(vfs.create(1, b"a/b", 0, 0, 0o644).is_err());

    // Dot names
    assert!(vfs.create(1, b".", 0, 0, 0o644).is_err());
    assert!(vfs.create(1, b"..", 0, 0, 0o644).is_err());

    // Too long (256 bytes)
    let long = vec![b'x'; 256];
    assert!(vfs.create(1, &long, 0, 0, 0o644).is_err());

    // Valid name should work
    assert!(vfs.create(1, b"valid.txt", 0, 0, 0o644).is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_validate_name_mkdir_rejects_invalid() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "validate_mkdir");

    assert!(vfs.mkdir(1, b"", 0, 0, 0o755).is_err());
    assert!(vfs.mkdir(1, b"a/b", 0, 0, 0o755).is_err());
    assert!(vfs.mkdir(1, b"..", 0, 0, 0o755).is_err());
    assert!(vfs.mkdir(1, b"valid_dir", 0, 0, 0o755).is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_validate_name_rename_rejects_invalid() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "validate_rename");

    vfs.create(1, b"source.txt", 0, 0, 0o644).unwrap();

    // Rename to invalid name
    assert!(vfs.rename(1, b"source.txt", 1, b"").is_err());
    assert!(vfs.rename(1, b"source.txt", 1, b"a/b").is_err());
    assert!(vfs.rename(1, b"source.txt", 1, b"..").is_err());

    // Valid rename should work
    assert!(vfs.rename(1, b"source.txt", 1, b"dest.txt").is_ok());
}

// =============================================================================
// Phase 7 — statfs Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_statfs_initial() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "statfs_init");

    let st = vfs.statfs().unwrap();

    // Initial state: 1 inode (root), 0 used space
    assert_eq!(st.files, 1); // root inode
    assert!(st.bfree > 0);
    assert!(st.bavail > 0);
    assert_eq!(st.bsize, 4 * 1024 * 1024); // 4 MiB block size
    assert_eq!(st.namelen, 255);
    assert_eq!(st.blocks, st.bfree); // no used space yet
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_statfs_after_write() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "statfs_write");

    let st_before = vfs.statfs().unwrap();

    // Create a file and write data
    let attr = vfs.create(1, b"data.bin", 0, 0, 0o644).unwrap();
    let data = vec![0u8; 1024 * 1024]; // 1 MiB
    vfs.write(attr.ino, 0, &data).await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    let st_after = vfs.statfs().unwrap();

    // Should now report more inodes (root + file = 2)
    assert_eq!(st_after.files, st_before.files + 1);
    // USED_SPACE should have increased
    // allocated payload should show less free space (or equal if block size rounds down)
    assert!(st_after.bfree <= st_before.bfree);
}

// =============================================================================
// Phase 8 — Notes On PG Sync Coverage
// =============================================================================

// PG convergence and remote-visible metadata semantics are primarily covered in
// `studiofs-meta::pg_engine` ignored tests now:
// - checkpoint bootstrap
// - delta pull by batch version
// - committed-vs-staged payload visibility
// - overlay conflict handling
//
// The VFS ignored suite below focuses on end-to-end data path correctness:
// reads, writes, truncation, crash recovery, upload completion, and cache
// behavior on top of the local metadata serving model.

// =============================================================================
// Phase 9 — Deep Integration & Edge-Case Tests
// =============================================================================

// ─── 9.1 Block Boundary & Truncate Edge Cases ──────────────────────────────

/// Truncate a file to exactly a block boundary.
/// Verify the remaining logical payload range has full block_size length.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_truncate_to_exact_block_boundary() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "trunc-boundary");
    let block_size = vfs.meta().get_format_info().unwrap().block_size;

    // Write 12 MiB = 3 full blocks (block_size = 4 MiB).
    let size = 3 * block_size as usize;
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let attr = vfs.create(1, b"trunc-bound.bin", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, &payload).await.unwrap();
    vfs.flush(attr.ino).await.unwrap();
    let _ = wait_until_n_payload_slices(&vfs, attr.ino, 3, Duration::from_secs(15))
        .expect("Expected 3 payload slices");

    // Truncate to exactly 1 block boundary = block_size bytes.
    vfs.setattr(attr.ino, SetattrArgs::size_only(block_size as u64))
        .unwrap();
    let after = vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap();
    assert_eq!(after.len(), 1, "Only 1 payload slice should remain");

    let (idx, blk) = &after[0];
    assert_eq!(*idx, 0);
    assert_eq!(
        blk.length, block_size,
        "payload slice length should equal block_size"
    );

    // Verify read returns correct data.
    let data = vfs.read(attr.ino, 0, block_size).await.unwrap();
    assert_eq!(data.len(), block_size as usize);
    assert_eq!(&data[..], &payload[..block_size as usize]);
}

/// Extend a file beyond its current size (sparse hole).
/// Verify reading the hole returns zeroes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_truncate_extend_creates_sparse_hole() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "trunc-extend");

    let attr = vfs.create(1, b"sparse.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 1000 bytes.
    let payload = vec![0xABu8; 1000];
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Extend to 1 MiB via setattr — should NOT create blocks, only change size.
    let extended_size = 1024 * 1024u64;
    vfs.setattr(ino, SetattrArgs::size_only(extended_size))
        .unwrap();

    let a = vfs.getattr(ino).unwrap();
    assert_eq!(a.size, extended_size);

    // Read the first 1000 bytes — should be original data.
    let head = vfs.read(ino, 0, 1000).await.unwrap();
    assert_eq!(&head[..], &payload[..]);

    // Read into the sparse hole — should be zeroes.
    let hole = vfs.read(ino, 2000, 4096).await.unwrap();
    assert!(
        hole.iter().all(|&b| b == 0),
        "sparse hole should read as zeroes"
    );
}

/// Shrink inside a block, then extend by writing later in the same block.
/// Bytes between the truncated EOF and the new write offset must be zeroed,
/// not preserved from the old tail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_truncate_shrink_then_extend_zero_fills_old_tail() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "trunc-shrink-extend");

    let attr = vfs.create(1, b"tail-zero.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();
    let _ = wait_until_n_payload_slices(&vfs, ino, 1, Duration::from_secs(15));

    vfs.setattr(ino, SetattrArgs::size_only(1024)).unwrap();
    vfs.write(ino, 2048, &[0xEE]).await.unwrap();
    vfs.flush(ino).await.unwrap();

    let data = vfs.read(ino, 0, 2049).await.unwrap();
    assert_eq!(&data[..1024], &payload[..1024]);
    assert!(
        data[1024..2048].iter().all(|&b| b == 0),
        "gap after truncate must be zero-filled"
    );
    assert_eq!(data[2048], 0xEE);
}

/// Write exactly block_size bytes then 1 byte at the next block boundary.
/// Exercises the full-block eager flush path + new partial block creation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_write_at_exact_block_boundary_transition() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "boundary-trans");
    let block_size = vfs.meta().get_format_info().unwrap().block_size as usize;

    let attr = vfs.create(1, b"boundary.bin", 0, 0, 0o644).unwrap();

    // Write exactly one full block.
    let full_block: Vec<u8> = (0..block_size).map(|i| (i % 199) as u8).collect();
    vfs.write(attr.ino, 0, &full_block).await.unwrap();

    // Write 1 byte at the start of the next block.
    vfs.write(attr.ino, block_size as u64, &[0xFF])
        .await
        .unwrap();
    vfs.flush(attr.ino).await.unwrap();

    // Wait for payload slices to materialise.
    let blocks = wait_until_n_payload_slices(&vfs, attr.ino, 2, Duration::from_secs(15))
        .expect("Expected 2 payload slices");
    assert_eq!(blocks.len(), 2);

    // Verify file size.
    let a = vfs.getattr(attr.ino).unwrap();
    assert_eq!(a.size, (block_size + 1) as u64);

    // Verify data integrity.
    let data = vfs
        .read(attr.ino, 0, (block_size + 1) as u32)
        .await
        .unwrap();
    assert_eq!(&data[..block_size], &full_block[..]);
    assert_eq!(data[block_size], 0xFF);
}

#[test]
fn flush_with_offline_s3_keeps_data_locally_committed_and_queued() {
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1048576");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let mut format_info = make_format_info_with_endpoint(
        &format!("flush-failure-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    format_info.block_size = 512 * 1024;
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            &cache_dir_str,
            4096,
            16 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs.create(1, b"flush-failure.bin", 0, 0, 0o644).unwrap();
        let payload = vec![0xABu8; 1024];

        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            payload.as_slice(),
            "offline S3 must still leave flushed data readable from the local block backend"
        );
        assert!(
            !vfs.live_extent_records_for_inode(attr.ino).is_empty(),
            "flush should still materialize local live extents before remote upload completes"
        );
        assert!(
            !vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "offline S3 should leave the flush queued for later upload replay"
        );
        assert_eq!(
            vfs.meta()
                .lookup_logical_payload_slices(attr.ino)
                .unwrap()
                .len(),
            1
        );
        let state = vfs.inode_incarnation_state(attr.ino).await.unwrap();
        assert_eq!(state.upload_state, VfsUploadState::Updating);
        let committed = state
            .committed_incarnation
            .expect("offline flush should retain the zero-size committed base incarnation");
        assert_eq!(committed.size, 0);
        let pending = state.pending_incarnation.expect(
            "flushed offline file must retain a pending incarnation until upload completes",
        );
        assert_eq!(pending.size, payload.len() as u64);
        assert_eq!(pending.base_size, 0);
    });
    rt.shutdown_background();
}

fn make_local_zeroio_test_vfs(
    db_path: &std::path::Path,
    cache_dir_str: &str,
    format_info: &FormatInfo,
    vfs_config: VfsConfigValidated,
) -> (Arc<RedbMetaEngine>, DataStoreConfig, Arc<StudioVfs>) {
    let data_config = DataStoreConfig::from(format_info);
    let meta = Arc::new(RedbMetaEngine::open(db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(format_info).unwrap();
    let vfs = StudioVfs::new(
        meta.clone(),
        &data_config,
        cache_dir_str,
        16 * 1024 * 1024,
        16 * 1024 * 1024,
        vfs_config,
    )
    .unwrap();
    (meta, data_config, vfs)
}

#[test]
fn write_returns_after_small_new_file_is_locally_readable_until_flush() {
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let mut format_info = make_format_info_with_endpoint(
        &format!("zeroio-small-write-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    format_info.block_size = 512 * 1024;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let (meta, _data_config, vfs) = make_local_zeroio_test_vfs(
            &db_path,
            &cache_dir_str,
            &format_info,
            VfsConfigRaw::default()
                .validate()
                .expect("default VFS config should validate"),
        );
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"zeroio-small-write.bin", 0, 0, 0o644)
            .unwrap();
        let payload = b"small local durable payload";

        let written = vfs.write(attr.ino, 0, payload).await.unwrap();
        assert_eq!(written as usize, payload.len());

        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload);

        let persisted_attr = meta.get_attr(attr.ino).unwrap();
        assert_eq!(
            persisted_attr.size, 0,
            "small partial writes should leave durable metadata at the pre-barrier size"
        );
        let blocks = meta.lookup_logical_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            0,
            "small partial writes should stay buffered until flush"
        );

        vfs.flush(attr.ino).await.unwrap();

        let persisted_attr = meta.get_attr(attr.ino).unwrap();
        assert_eq!(persisted_attr.size, payload.len() as u64);
        let blocks = meta.lookup_logical_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            1,
            "flush should materialize the buffered partial block"
        );
        let block = &blocks[0].1;
        assert_eq!(block.length as usize, payload.len());
        assert!(
            block.flags.contains(BlockFlags::PARTIAL_BLOCK),
            "flush should persist the small write as a partial block"
        );
    });
    rt.shutdown_background();
}

#[test]
fn overwrite_of_published_file_is_locally_readable_without_flush() {
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "60000");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let format_info = make_format_info_with_endpoint(
        &format!("zeroio-overwrite-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    let baseline_payload = b"committed baseline payload".to_vec();
    let patch = b"draft".to_vec();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let (meta, _data_config, vfs) = make_local_zeroio_test_vfs(
            &db_path,
            &cache_dir_str,
            &format_info,
            VfsConfigRaw::default()
                .validate()
                .expect("default VFS config should validate"),
        );
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs.create(1, b"zeroio-overwrite.bin", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        vfs.write(ino, 0, &baseline_payload).await.unwrap();
        vfs.flush(ino).await.unwrap();
        wait_until_payload_slices(&vfs, ino, Duration::from_secs(15))
            .expect("baseline file must commit before overwrite test");

        let published_before = meta.get_attr(ino).unwrap();
        let published_epoch = published_before.committed_content_epoch;
        let published_size = published_before.committed_size;

        vfs.write(ino, 0, &patch).await.unwrap();

        let mut expected = baseline_payload.clone();
        expected[..patch.len()].copy_from_slice(&patch);
        let read = vfs.read(ino, 0, expected.len() as u32).await.unwrap();
        assert_eq!(read.as_ref(), expected.as_slice());

        let state = vfs.inode_incarnation_state(ino).await.unwrap();
        let committed = state
            .committed_incarnation
            .expect("published base must remain available during local overwrite");
        let pending = state
            .pending_incarnation
            .expect("local overwrite must create a pending unpublished epoch");
        assert_eq!(committed.content_epoch, published_epoch);
        assert_eq!(committed.size, published_size);
        assert!(pending.content_epoch > committed.content_epoch);
        assert_eq!(pending.base_size, committed.size);

        let persisted_after = meta.get_attr(ino).unwrap();
        assert_eq!(
            persisted_after.committed_content_epoch, published_epoch,
            "foreground overwrite must not advance globally published visibility"
        );
        assert_eq!(
            persisted_after.committed_size, published_size,
            "foreground overwrite must keep the last published size intact"
        );
    });
    rt.shutdown_background();
}

#[test]
fn truncate_of_published_file_keeps_committed_size_and_pending_incarnation() {
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "60000");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let format_info = make_format_info_with_endpoint(
        &format!("zeroio-published-truncate-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    let baseline_payload = b"committed baseline payload".to_vec();
    let truncated_size = 8u64;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let (meta, _data_config, vfs) = make_local_zeroio_test_vfs(
            &db_path,
            &cache_dir_str,
            &format_info,
            VfsConfigRaw::default()
                .validate()
                .expect("default VFS config should validate"),
        );
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"zeroio-published-truncate.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        vfs.write(ino, 0, &baseline_payload).await.unwrap();
        vfs.flush(ino).await.unwrap();
        wait_until_payload_slices(&vfs, ino, Duration::from_secs(15))
            .expect("baseline file must commit before truncate test");

        let published_before = meta.get_attr(ino).unwrap();
        let published_epoch = published_before.committed_content_epoch;
        let published_size = published_before.committed_size;
        let published_objects = meta
            .lookup_logical_payload_slices(ino)
            .unwrap()
            .into_iter()
            .map(|(_, block)| block.object_id)
            .collect::<Vec<_>>();

        vfs.setattr(ino, SetattrArgs::size_only(truncated_size))
            .unwrap();

        let read = vfs.read(ino, 0, truncated_size as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            &baseline_payload[..truncated_size as usize],
            "writing mount should immediately prefer the locally truncated unpublished view"
        );

        let state = vfs.inode_incarnation_state(ino).await.unwrap();
        let committed = state
            .committed_incarnation
            .expect("published base must remain available during local truncate");
        let pending = state
            .pending_incarnation
            .expect("local truncate of a published file must retain a pending incarnation");
        assert_eq!(committed.content_epoch, published_epoch);
        assert_eq!(committed.size, published_size);
        assert!(pending.content_epoch > committed.content_epoch);
        assert_eq!(pending.size, truncated_size);
        assert_eq!(pending.base_size, committed.size);

        let persisted_after = meta.get_attr(ino).unwrap();
        assert_eq!(
            persisted_after.size, truncated_size,
            "foreground truncate must persist the new local unpublished size"
        );
        assert_eq!(
            persisted_after.committed_content_epoch, published_epoch,
            "foreground truncate must not advance globally published visibility"
        );
        assert_eq!(
            persisted_after.committed_size, published_size,
            "foreground truncate must keep the last published size intact"
        );

        let pending_deletions = meta.get_pending_deletions().unwrap();
        for object_id in published_objects {
            assert!(
                !pending_deletions
                    .iter()
                    .any(|(pending_object_id, _)| *pending_object_id == object_id),
                "local truncate must not GC a still-published base object"
            );
        }
    });
    rt.shutdown_background();
}

#[test]
fn append_after_create_stays_buffered_until_flush() {
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let mut format_info = make_format_info_with_endpoint(
        &format!("zeroio-append-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    format_info.block_size = 512 * 1024;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let (meta, _data_config, vfs) = make_local_zeroio_test_vfs(
            &db_path,
            &cache_dir_str,
            &format_info,
            VfsConfigRaw::default()
                .validate()
                .expect("default VFS config should validate"),
        );
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs.create(1, b"zeroio-append.bin", 0, 0, 0o644).unwrap();
        let first = b"hello ";
        let second = b"world";

        vfs.write(attr.ino, 0, first).await.unwrap();
        vfs.write(attr.ino, first.len() as u64, second).await.unwrap();

        let expected = b"hello world";
        let read = vfs.read(attr.ino, 0, expected.len() as u32).await.unwrap();
        assert_eq!(read.as_ref(), expected);

        let persisted_attr = meta.get_attr(attr.ino).unwrap();
        assert_eq!(
            persisted_attr.size, 0,
            "ordinary append-before-flush should keep the durable local metadata at the pre-barrier size"
        );
        let blocks = meta.lookup_logical_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            0,
            "ordinary append-before-flush should not durably materialize a partial local block before the barrier"
        );

        vfs.flush(attr.ino).await.unwrap();

        let persisted_attr = meta.get_attr(attr.ino).unwrap();
        assert_eq!(persisted_attr.size, expected.len() as u64);
        let blocks = meta.lookup_logical_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            1,
            "flush should materialize the buffered append into one local block"
        );
        assert_eq!(blocks[0].1.length as usize, expected.len());
    });
    rt.shutdown_background();
}

#[test]
fn truncate_after_create_is_locally_durable_without_flush() {
    let _test_env = ScopedTestEnv::new();
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let mut format_info = make_format_info_with_endpoint(
        &format!("zeroio-truncate-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    format_info.block_size = 512 * 1024;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let (meta, _data_config, vfs) = make_local_zeroio_test_vfs(
            &db_path,
            &cache_dir_str,
            &format_info,
            VfsConfigRaw::default()
                .validate()
                .expect("default VFS config should validate"),
        );

        let attr = vfs.create(1, b"zeroio-truncate.bin", 0, 0, 0o644).unwrap();
        let payload = b"truncate-me-down";
        vfs.write(attr.ino, 0, payload).await.unwrap();

        vfs.setattr(attr.ino, SetattrArgs::size_only(8)).unwrap();

        let read = vfs.read(attr.ino, 0, 8).await.unwrap();
        assert_eq!(read.as_ref(), b"truncate");

        let persisted_attr = meta.get_attr(attr.ino).unwrap();
        assert_eq!(persisted_attr.size, 8);
        let blocks = meta.lookup_logical_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            1,
            "truncate must leave a durably persisted partial block for the unpublished local state"
        );
        assert_eq!(blocks[0].1.length, 8);
        assert!(
            blocks[0].1.flags.contains(BlockFlags::PARTIAL_BLOCK),
            "truncated block should remain marked partial"
        );
    });
    rt.shutdown_background();
}

#[test]
fn full_block_write_with_offline_s3_stays_local_until_flush_then_queues_upload() {
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_eager = ScopedEnvVar::capture("STUDIOFS_WRITE_EAGER_FLUSH_BATCH_BYTES");
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1048576");
        std::env::set_var("STUDIOFS_WRITE_EAGER_FLUSH_BATCH_BYTES", "512KB");
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let mut format_info = make_format_info_with_endpoint(
        &format!("write-failure-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    format_info.block_size = 512 * 1024;
    let block_size = format_info.block_size as usize;
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            &cache_dir_str,
            4096,
            16 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs.create(1, b"write-failure.bin", 0, 0, 0o644).unwrap();
        let payload = vec![0xCDu8; block_size];

        let written = vfs.write(attr.ino, 0, &payload).await.unwrap();
        assert_eq!(written as usize, payload.len());

        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            payload.as_slice(),
            "stage-first local acceptance must keep data readable while S3 is offline"
        );
        assert!(
            vfs.inode_has_local_dirty_state(attr.ino),
            "stage-first local acceptance must keep local dirty state until flush materializes metadata"
        );
        assert!(
            vfs.live_extent_records_for_inode(attr.ino).is_empty(),
            "live extents should only appear after flush commits unpublished metadata"
        );
        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "remote upload work should stay deferred until flush commits metadata"
        );
        assert_eq!(
            vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap().len(),
            0,
            "stage-first writes should not commit payload slices before flush"
        );

        vfs.flush(attr.ino).await.unwrap();

        assert!(
            !vfs.live_extent_records_for_inode(attr.ino).is_empty(),
            "flush should materialize live extents once the staged payload slice is committed"
        );
        assert!(
            !vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "flush should queue remote upload work even when S3 is offline"
        );
        assert_eq!(vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap().len(), 1);
        let state = vfs.inode_incarnation_state(attr.ino).await.unwrap();
        assert_eq!(state.upload_state, VfsUploadState::Updating);
        let committed = state.committed_incarnation.expect(
            "flush should retain the zero-size committed base incarnation while upload is pending",
        );
        assert_eq!(committed.size, 0);
        let pending = state.pending_incarnation.expect(
            "flush must retain a pending incarnation until upload completes",
        );
        assert_eq!(pending.size, payload.len() as u64);
        assert_eq!(pending.base_size, 0);
    });
    rt.shutdown_background();
}

#[test]
fn write_stages_successive_full_blocks_without_waiting_for_threshold() {
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    unsafe {
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let mut format_info = make_format_info_with_endpoint(
        &format!("write-eager-batch-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    format_info.block_size = 512 * 1024;
    let block_size = format_info.block_size as usize;
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs_config = VfsConfigRaw {
            max_dirty_bytes: Some(16 * 1024 * 1024),
            write_eager_flush_batch_bytes: Some(block_size * 4),
            ..VfsConfigRaw::default()
        };
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            vfs_config
                .validate()
                .expect("test VFS config should validate"),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"write-eager-batch.bin", 0, 0, 0o644)
            .unwrap();
        let first = vec![0x11u8; block_size];
        let second = vec![0x22u8; block_size];
        let third = vec![0x33u8; block_size];
        let fourth = vec![0x44u8; block_size];

        vfs.write(attr.ino, 0, &first).await.unwrap();
        assert!(
            vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap().is_empty(),
            "stage-first write should keep the first full block in local staging until flush"
        );
        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "upload work should stay deferred until the staged payload slices are flushed"
        );
        assert!(
            vfs.inode_has_local_dirty_state(attr.ino),
            "stage-first write should retain local dirty state for the staged full block"
        );
        let first_read = vfs.read(attr.ino, 0, block_size as u32).await.unwrap();
        assert_eq!(
            first_read.as_ref(),
            first.as_slice(),
            "staged full blocks must remain readable before metadata materialization"
        );

        vfs.write(attr.ino, block_size as u64, &second)
            .await
            .unwrap();
        vfs.write(attr.ino, (block_size * 2) as u64, &third)
            .await
            .unwrap();
        vfs.write(attr.ino, (block_size * 3) as u64, &fourth)
            .await
            .unwrap();
        let blocks = vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap();
        assert!(
            blocks.len() < 4,
            "foreground writes should not need to fully materialize every staged payload slice before the explicit flush barrier"
        );

        let combined = vfs
            .read(attr.ino, 0, (block_size * 4) as u32)
            .await
            .unwrap();
        assert_eq!(&combined[..block_size], first.as_slice());
        assert_eq!(&combined[block_size..(block_size * 2)], second.as_slice());
        assert_eq!(&combined[(block_size * 2)..(block_size * 3)], third.as_slice());
        assert_eq!(&combined[(block_size * 3)..], fourth.as_slice());
        assert!(
            vfs.pending_uploads_for_inode(attr.ino).len() < 4,
            "upload queue may grow opportunistically, but it should not require fully materializing every staged payload slice before flush"
        );
        assert_eq!(
            vfs.ingress_stats().dirty_mem_bytes,
            0,
            "full-block staging should spill payload out of dirty RAM even before flush"
        );

        vfs.flush(attr.ino).await.unwrap();
        let blocks = vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            4,
            "flush should materialize all staged full blocks in local metadata"
        );
        assert_eq!(
            vfs.pending_uploads_for_inode(attr.ino).len(),
            4,
            "flush should queue uploads for every materialized full block"
        );
    });
    rt.shutdown_background();
}

#[test]
fn write_stages_full_blocks_without_explicit_flush() {
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    unsafe {
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let mut format_info = make_format_info_with_endpoint(
        &format!("write-eager-flush-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    format_info.block_size = 512 * 1024;
    let block_size = format_info.block_size as usize;
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs_config = VfsConfigRaw {
            max_dirty_bytes: Some(16 * 1024 * 1024),
            write_eager_flush_batch_bytes: Some(block_size * 2),
            ..VfsConfigRaw::default()
        };
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            vfs_config
                .validate()
                .expect("test VFS config should validate"),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"write-eager-flush.bin", 0, 0, 0o644)
            .unwrap();
        let payload = vec![0x33u8; block_size];

        vfs.write(attr.ino, 0, &payload).await.unwrap();
        let blocks = vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            0,
            "stage-first write should not commit a full block to metadata before flush"
        );
        assert!(
            vfs.inode_has_local_dirty_state(attr.ino),
            "stage-first write should retain local dirty state for the staged full block"
        );
        let read = vfs.read(attr.ino, 0, block_size as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload.as_slice());

        vfs.flush(attr.ino).await.unwrap();
        let blocks = vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            1,
            "flush should materialize the staged full block"
        );
    });
    rt.shutdown_background();
}

#[test]
fn write_stages_full_block_prefix_and_buffers_tail_until_flush() {
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    unsafe {
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let mut format_info = make_format_info_with_endpoint(
        &format!("write-eager-tail-size-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    format_info.block_size = 512 * 1024;
    let block_size = format_info.block_size as usize;
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs_config = VfsConfigRaw {
            max_dirty_bytes: Some(16 * 1024 * 1024),
            write_eager_flush_batch_bytes: Some(block_size),
            ..VfsConfigRaw::default()
        };
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            vfs_config
                .validate()
                .expect("test VFS config should validate"),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"write-eager-tail-size.bin", 0, 0, 0o644)
            .unwrap();
        let tail = vec![0x44u8; block_size / 8];
        let mut payload = vec![0x33u8; block_size];
        payload.extend_from_slice(&tail);

        vfs.write(attr.ino, 0, &payload).await.unwrap();

        let live_attr = vfs.getattr(attr.ino).unwrap();
        assert_eq!(
            live_attr.size,
            payload.len() as u64,
            "the live inode view should expose the newly written tail bytes"
        );

        assert_eq!(
            vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap().len(),
            0,
            "stage-first write should keep the completed full-block prefix in local staging until flush"
        );
        assert!(
            vfs.ingress_stats().dirty_mem_bytes > 0,
            "the partial tail should remain buffered in local dirty state before flush"
        );
        let persisted_attr = vfs.meta().get_attr(attr.ino).unwrap();
        assert_eq!(
            persisted_attr.size,
            0,
            "durable metadata should remain unchanged until flush materializes the staged prefix and tail"
        );

        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload.as_slice());

        vfs.flush(attr.ino).await.unwrap();

        let persisted_attr = vfs.meta().get_attr(attr.ino).unwrap();
        assert_eq!(
            persisted_attr.size,
            payload.len() as u64,
            "flush should advance the durable local metadata to include the tail bytes"
        );
        assert_eq!(
            vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap().len(),
            2,
            "flush should materialize the buffered partial tail block"
        );
        assert_eq!(vfs.ingress_stats().dirty_mem_bytes, 0);
    });
    rt.shutdown_background();
}

#[test]
fn read_from_start_does_not_depend_on_metadata_materialization() {
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    unsafe {
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let format_info = make_format_info_with_endpoint(
        &format!("read-drains-dirty-tail-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    let block_size = format_info.block_size as usize;
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            VfsConfigRaw::default()
                .validate()
                .expect("default VFS config should validate"),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"read-drains-dirty-tail.bin", 0, 0, 0o644)
            .unwrap();
        let burst_blocks = 27usize;
        let payload = vec![0x5Au8; block_size * burst_blocks];

        vfs.write(attr.ino, 0, &payload).await.unwrap();

        let persisted_before = vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap().len();
        let ingress_before = vfs.ingress_stats();
        assert_eq!(
            persisted_before,
            0,
            "stage-first writes should not need metadata materialization before the first read"
        );
        assert_eq!(
            ingress_before.dirty_mem_bytes,
            0,
            "the first read should not need to drain any buffered dirty tail"
        );

        let read = vfs.read(attr.ino, 0, block_size as u32).await.unwrap();
        assert_eq!(read.as_ref(), &payload[..block_size]);

        let ingress_after = vfs.ingress_stats();
        assert_eq!(
            ingress_after.dirty_mem_blocks,
            ingress_before.dirty_mem_blocks,
            "the first read from offset 0 should not change how many locally staged payload slices are pending"
        );
        assert_eq!(
            ingress_after.dirty_mem_bytes,
            0,
            "the first read from offset 0 should not need to clear buffered full-block dirty bytes"
        );
        vfs.flush(attr.ino).await.unwrap();
        assert_eq!(
            vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap().len(),
            burst_blocks,
            "flush should materialize every staged full block after the read succeeds"
        );
    });
    rt.shutdown_background();
}

#[test]
fn write_stages_full_blocks_and_buffers_tail_without_buffered_relief() {
    let _test_env = ScopedTestEnv::new();
    let _restore_disable = ScopedEnvVar::capture("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
    unsafe {
        std::env::set_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "1");
    }
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let mut format_info = make_format_info_with_endpoint(
        &format!("write-backpressure-full-first-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    format_info.block_size = 512 * 1024;
    let block_size = format_info.block_size as usize;
    let tail_len = block_size / 8;
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs_config = VfsConfigRaw {
            max_dirty_bytes: Some(block_size + 1),
            write_eager_flush_batch_bytes: Some(block_size * 4),
            ..VfsConfigRaw::default()
        };
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            vfs_config
                .validate()
                .expect("test VFS config should validate"),
        )
        .unwrap();

        let attr = vfs
            .create(1, b"write-backpressure-full-first.bin", 0, 0, 0o644)
            .unwrap();
        let first = vec![0x11u8; block_size];
        let tail = vec![0x22u8; tail_len];

        vfs.write(attr.ino, 0, &first).await.unwrap();
        assert!(
            vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap().is_empty(),
            "stage-first writes should keep the initial full block in local staging until flush"
        );

        vfs.write(attr.ino, block_size as u64, &tail).await.unwrap();

        assert_eq!(
            vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap().len(),
            0,
            "the tail block should remain buffered while the initial full block stays locally staged"
        );
        assert_eq!(
            vfs.ingress_stats().dirty_mem_bytes,
            tail_len,
            "foreground success should keep the partial tail buffered until an explicit barrier"
        );

        let live_attr = vfs.getattr(attr.ino).unwrap();
        assert_eq!(
            live_attr.size,
            (block_size + tail_len) as u64,
            "the live inode view should still include the buffered tail after backpressure relief"
        );
        let persisted_attr = vfs.meta().get_attr(attr.ino).unwrap();
        assert_eq!(
            persisted_attr.size,
            0,
            "durable metadata should remain unchanged until flush commits the staged prefix and tail"
        );

        let read = vfs
            .read(attr.ino, 0, (block_size + tail_len) as u32)
            .await
            .unwrap();
        assert_eq!(&read[..block_size], first.as_slice());
        assert_eq!(&read[block_size..], tail.as_slice());

        vfs.flush(attr.ino).await.unwrap();

        assert_eq!(
            vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap().len(),
            2,
            "flush should durably persist the buffered tail block alongside the initial full block"
        );
        assert_eq!(
            vfs.ingress_stats().dirty_mem_bytes,
            0,
            "flush should clear the buffered tail block from dirty memory"
        );
        let persisted_attr = vfs.meta().get_attr(attr.ino).unwrap();
        assert_eq!(
            persisted_attr.size,
            (block_size + tail_len) as u64,
            "flush should advance the durable local metadata to the latest logical size"
        );
    });
    rt.shutdown_background();
}

/// Truncate to zero, then write again. Verify old blocks are GC'd and new data is correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_truncate_to_zero_then_rewrite() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "trunc-zero-rw");

    let attr = vfs.create(1, b"overwrite.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 5 MiB.
    let payload1: Vec<u8> = (0..5 * 1024 * 1024).map(|i| (i % 173) as u8).collect();
    vfs.write(ino, 0, &payload1).await.unwrap();
    vfs.flush(ino).await.unwrap();
    let _ = wait_until_n_payload_slices(&vfs, ino, 2, Duration::from_secs(15));

    // Get old block IDs.
    let old_blocks = vfs.meta().lookup_logical_payload_slices(ino).unwrap();
    let old_ids: Vec<u64> = old_blocks.iter().map(|(_, b)| b.object_id).collect();
    assert!(!old_ids.is_empty());

    // Truncate to 0.
    vfs.setattr(ino, SetattrArgs::size_only(0)).unwrap();
    assert_eq!(vfs.getattr(ino).unwrap().size, 0);

    // All payload slices should be removed when truncating to zero.
    let remaining = vfs.meta().lookup_logical_payload_slices(ino).unwrap();
    assert!(
        remaining.is_empty(),
        "all payload slices should be removed after truncate-to-zero, got {}",
        remaining.len()
    );

    // All old payload objects should be in pending deletions.
    let pending = vfs.meta().get_pending_deletions().unwrap();
    for id in &old_ids {
        assert!(
            pending.iter().any(|(pid, _)| pid == id),
            "old block {} should be pending deletion",
            id
        );
    }

    // Write new data.
    let payload2 = vec![0xCDu8; 1024];
    vfs.write(ino, 0, &payload2).await.unwrap();
    vfs.flush(ino).await.unwrap();
    assert_eq!(vfs.getattr(ino).unwrap().size, 1024);

    let data = vfs.read(ino, 0, 1024).await.unwrap();
    assert_eq!(&data[..], &payload2[..]);
}

// ─── 9.2 Read Edge Cases ────────────────────────────────────────────────────

/// Reading past EOF should return empty bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_read_beyond_eof_returns_empty() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "read-eof");

    let attr = vfs.create(1, b"small.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"hello").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    // Read at offset past file size.
    let data = vfs.read(attr.ino, 100, 4096).await.unwrap();
    assert!(data.is_empty(), "read past EOF should return empty bytes");

    // Read at exact EOF boundary.
    let data = vfs.read(attr.ino, 5, 4096).await.unwrap();
    assert!(data.is_empty(), "read at exact EOF should return empty");
}

/// Read spanning an allocated block and unallocated blocks (sparse file).
/// Allocated part returns data, unallocated returns zeroes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_read_spanning_allocated_and_sparse_blocks() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "read-sparse");
    let block_size = vfs.meta().get_format_info().unwrap().block_size as u64;

    let attr = vfs.create(1, b"sparse-read.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write data only in block 0.
    let payload: Vec<u8> = (0..1024).map(|i| (i % 200) as u8).collect();
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Extend file to cover 3 blocks without writing block 1 and 2.
    let extended = block_size * 3;
    vfs.setattr(ino, SetattrArgs::size_only(extended)).unwrap();

    // Read block 1 (entirely in sparse region) — should be all zeroes.
    let sparse_data = vfs.read(ino, block_size, block_size as u32).await.unwrap();
    assert_eq!(sparse_data.len(), block_size as usize);
    assert!(
        sparse_data.iter().all(|&b| b == 0),
        "sparse block should be all zeroes"
    );

    // Original block 0 data should still be there.
    let head = vfs.read(ino, 0, 1024).await.unwrap();
    assert_eq!(&head[..], &payload[..]);
}

/// Flush an empty file (no writes) — should be a no-op without errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flush_empty_file_is_noop() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flush-empty");

    let attr = vfs.create(1, b"empty.txt", 0, 0, 0o644).unwrap();
    // Flush without writing anything — should not panic or allocate blocks.
    vfs.flush(attr.ino).await.unwrap();
    assert_eq!(vfs.getattr(attr.ino).unwrap().size, 0);
    assert!(
        vfs.meta()
            .lookup_logical_payload_slices(attr.ino)
            .unwrap()
            .is_empty()
    );
}

/// Read zero bytes from a file — should return empty immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_read_zero_bytes() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "read-zero");

    let attr = vfs.create(1, b"content.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"data").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    let data = vfs.read(attr.ino, 0, 0).await.unwrap();
    assert!(data.is_empty());
}

/// Write zero bytes — should be a no-op and return 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_write_zero_bytes_is_noop() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "write-zero");

    let attr = vfs.create(1, b"noop.txt", 0, 0, 0o644).unwrap();
    let written = vfs.write(attr.ino, 0, &[]).await.unwrap();
    assert_eq!(written, 0);
    assert_eq!(vfs.getattr(attr.ino).unwrap().size, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn create_should_ignore_ds_store_without_persisting_metadata_or_data() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "ignore-ds-store");

    let attr = vfs.create(1, b".DS_Store", 1000, 1000, 0o644).unwrap();
    let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
    let written = vfs.write(attr.ino, 0, b"finder metadata").await.unwrap();
    assert_eq!(written, b"finder metadata".len() as u32);

    vfs.flush(attr.ino).await.unwrap();
    vfs.release_fh(fh).await.unwrap();

    assert!(vfs.wait_for_uploads_idle(Duration::from_millis(250)).await);
    let ingress = vfs.ingress_stats();
    assert_eq!(ingress.dirty_mem_bytes, 0);
    assert!(vfs.meta().lookup(1, b".DS_Store").is_err());
    assert!(vfs.lookup(1, b".DS_Store").is_err());
    assert!(vfs.readdir(1).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn create_should_ignore_appledouble_without_persisting_metadata_or_data() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "ignore-appledouble");

    let attr = vfs.create(1, b"._photo.jpg", 1000, 1000, 0o644).unwrap();
    let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
    let written = vfs.write(attr.ino, 0, b"appledouble").await.unwrap();
    assert_eq!(written, b"appledouble".len() as u32);

    vfs.flush(attr.ino).await.unwrap();
    vfs.release_fh(fh).await.unwrap();

    assert!(vfs.wait_for_uploads_idle(Duration::from_millis(250)).await);
    let ingress = vfs.ingress_stats();
    assert_eq!(ingress.dirty_mem_bytes, 0);
    assert!(vfs.meta().lookup(1, b"._photo.jpg").is_err());
    assert!(vfs.lookup(1, b"._photo.jpg").is_err());
    assert!(vfs.readdir(1).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn rename_should_drop_blocked_macos_metadata_target_without_persisting_it() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename-ignore-ds-store");

    let attr = vfs.create(1, b"normal.txt", 1000, 1000, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"payload").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    vfs.rename(1, b"normal.txt", 1, b".DS_Store").unwrap();

    assert!(vfs.lookup(1, b"normal.txt").is_err());
    assert!(vfs.lookup(1, b".DS_Store").is_err());
    assert!(vfs.meta().lookup(1, b".DS_Store").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn concurrent_create_should_reuse_single_ignored_file_inode_per_path() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "ignore-concurrent-create");

    let barrier = Arc::new(std::sync::Barrier::new(8));
    let inos = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();

    for _ in 0..8 {
        let vfs = vfs.clone();
        let barrier = barrier.clone();
        let inos = inos.clone();
        tasks.push(thread::spawn(move || {
            barrier.wait();
            let attr = vfs.create(1, b".DS_Store", 1000, 1000, 0o644).unwrap();
            inos.lock().push(attr.ino);
        }));
    }

    for task in tasks {
        task.join().unwrap();
    }

    let inos = inos.lock().clone();
    assert!(!inos.is_empty());
    let unique: std::collections::HashSet<_> = inos.into_iter().collect();
    assert_eq!(unique.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn mkdir_should_blackhole_root_macos_metadata_dirs_without_persisting_children() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "ignore-root-macos-dirs");

    let dir = vfs.mkdir(1, b".Spotlight-V100", 1000, 1000, 0o755).unwrap();
    assert_eq!(dir.kind, studiofs_meta::types::FileType::Directory);
    assert!(vfs.meta().lookup(1, b".Spotlight-V100").is_err());

    let looked_up_dir = vfs.lookup(1, b".Spotlight-V100").unwrap();
    assert_eq!(looked_up_dir.ino, dir.ino);

    let nested_dir = vfs.mkdir(dir.ino, b"Store-V2", 1000, 1000, 0o755).unwrap();
    let attr = vfs
        .create(nested_dir.ino, b"store.db", 1000, 1000, 0o644)
        .unwrap();
    let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
    let written = vfs.write(attr.ino, 0, b"spotlight").await.unwrap();
    assert_eq!(written, b"spotlight".len() as u32);
    vfs.flush(attr.ino).await.unwrap();

    let read_back = vfs.read(attr.ino, 0, written).await.unwrap();
    assert_eq!(read_back.len(), written as usize);
    assert!(read_back.iter().all(|byte| *byte == 0));

    vfs.release_fh(fh).await.unwrap();
    assert!(vfs.wait_for_uploads_idle(Duration::from_millis(250)).await);

    assert_eq!(vfs.readdir(1).unwrap().len(), 0);
    assert_eq!(vfs.readdir(dir.ino).unwrap().len(), 0);
    assert_eq!(vfs.readdir(nested_dir.ino).unwrap().len(), 0);
    assert!(vfs.lookup(1, b".Spotlight-V100").is_ok());
    assert!(vfs.meta().lookup(1, b".Spotlight-V100").is_err());
    assert!(vfs.meta().lookup(dir.ino, b"Store-V2").is_err());
    assert!(vfs.wait_for_uploads_idle(Duration::from_millis(250)).await);
    let ingress = vfs.ingress_stats();
    assert_eq!(ingress.dirty_mem_bytes, 0);

    vfs.rmdir(1, b".Spotlight-V100").unwrap();
    assert!(vfs.lookup(1, b".Spotlight-V100").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn blocked_root_metadata_names_should_be_ignored_for_file_and_symlink_creation() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "ignore-root-macos-metadata-names");

    let file = vfs.create(1, b".fseventsd", 1000, 1000, 0o644).unwrap();
    let fh = vfs.open_file(file.ino, OpenFlags::RDWR, false).unwrap();
    let written = vfs.write(file.ino, 0, b"fs-event-marker").await.unwrap();
    assert_eq!(written, b"fs-event-marker".len() as u32);
    vfs.flush(file.ino).await.unwrap();
    vfs.release_fh(fh).await.unwrap();

    let symlink = vfs
        .symlink(1, b".Spotlight-V100", b"/tmp/ignored", 1000, 1000)
        .unwrap();
    assert_eq!(symlink.kind, studiofs_meta::types::FileType::Symlink);
    assert_eq!(vfs.readlink(symlink.ino).unwrap(), Vec::<u8>::new());

    assert!(vfs.meta().lookup(1, b".Spotlight-V100").is_err());
    assert!(vfs.meta().lookup(1, b".fseventsd").is_err());
    assert!(vfs.readdir(1).unwrap().is_empty());

    vfs.unlink(1, b".Spotlight-V100").unwrap();
    assert!(vfs.meta().lookup(1, b".fseventsd").is_err());
    assert!(vfs.lookup(1, b".fseventsd").is_err());
    assert!(vfs.meta().lookup(1, b".Spotlight-V100").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn readdir_and_lookup_should_hide_legacy_root_macos_metadata_dirs() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hide-legacy-root-macos-dirs");

    vfs.meta()
        .create_dir(1, b".Spotlight-V100", 1000, 1000, 0o755)
        .unwrap();
    vfs.meta()
        .create_dir(1, b".Trashes", 1000, 1000, 0o755)
        .unwrap();
    vfs.meta()
        .create_dir(1, b".fseventsd", 1000, 1000, 0o755)
        .unwrap();
    vfs.meta()
        .create_dir(1, b"visible", 1000, 1000, 0o755)
        .unwrap();

    let entries = vfs.readdir(1).unwrap();
    let mut names: Vec<Vec<u8>> = entries.into_iter().map(|entry| entry.name).collect();
    names.sort();
    assert_eq!(names, vec![b".Trashes".to_vec(), b"visible".to_vec()]);

    assert!(vfs.lookup(1, b".Spotlight-V100").is_err());
    assert!(vfs.lookup(1, b".Trashes").is_ok());
    assert!(vfs.lookup(1, b".fseventsd").is_err());
    assert!(vfs.lookup(1, b"visible").is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn unlink_should_remove_legacy_persisted_root_blocked_regular_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "unlink-legacy-root-blocked-file");

    vfs.meta()
        .create_file(1, b".fseventsd", 1000, 1000, 0o644)
        .unwrap();

    assert!(vfs.lookup(1, b".fseventsd").is_err());
    vfs.unlink(1, b".fseventsd").unwrap();
    assert!(vfs.meta().lookup(1, b".fseventsd").is_err());
}

// ─── 9.3 Rename & Unlink Edge Cases ────────────────────────────────────────

/// Rename a file across directories.
/// Verify both parent caches are invalidated and the file is accessible at the new path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rename_cross_directory() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename-cross");

    // Set up directories.
    let dir_a = vfs.mkdir(1, b"dir_a", 0, 0, 0o755).unwrap();
    let dir_b = vfs.mkdir(1, b"dir_b", 0, 0, 0o755).unwrap();

    // Create file in dir_a.
    let attr = vfs.create(dir_a.ino, b"file.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"cross-dir data").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    // Rename from dir_a to dir_b.
    vfs.rename(dir_a.ino, b"file.txt", dir_b.ino, b"moved.txt")
        .unwrap();

    // Old path should fail.
    assert!(vfs.lookup(dir_a.ino, b"file.txt").is_err());

    // New path should succeed.
    let moved = vfs.lookup(dir_b.ino, b"moved.txt").unwrap();
    assert_eq!(moved.ino, attr.ino);

    // Data should be intact.
    let data = vfs.read(attr.ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"cross-dir data");

    // Both parent directories should reflect the change.
    let entries_a = vfs.readdir(dir_a.ino).unwrap();
    assert!(
        !entries_a.iter().any(|e| e.name == b"file.txt"),
        "dir_a should not contain file.txt"
    );
    let entries_b = vfs.readdir(dir_b.ino).unwrap();
    assert!(
        entries_b.iter().any(|e| e.name == b"moved.txt"),
        "dir_b should contain moved.txt"
    );
}

/// Rename a file over an existing file (overwrite).
/// Verify the displaced file's blocks are GC'd.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rename_overwrite_existing_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename-overwrite");

    // Create target file with data.
    let target = vfs.create(1, b"target.txt", 0, 0, 0o644).unwrap();
    vfs.write(target.ino, 0, b"old target data").await.unwrap();
    vfs.flush(target.ino).await.unwrap();
    let _ = wait_until_n_payload_slices(&vfs, target.ino, 1, Duration::from_secs(15));

    let target_blocks = vfs
        .meta()
        .lookup_logical_payload_slices(target.ino)
        .unwrap();
    let target_block_id = target_blocks[0].1.object_id;

    // Create source file with data.
    let source = vfs.create(1, b"source.txt", 0, 0, 0o644).unwrap();
    vfs.write(source.ino, 0, b"new source data").await.unwrap();
    vfs.flush(source.ino).await.unwrap();

    // Rename source → target (overwrite).
    vfs.rename(1, b"source.txt", 1, b"target.txt").unwrap();

    // source.txt should be gone.
    assert!(vfs.lookup(1, b"source.txt").is_err());

    // target.txt should resolve to source ino.
    let result = vfs.lookup(1, b"target.txt").unwrap();
    assert_eq!(result.ino, source.ino);

    // Displaced target's block should be in pending deletions.
    let pending = vfs.meta().get_pending_deletions().unwrap();
    assert!(
        pending.iter().any(|(id, _)| *id == target_block_id),
        "displaced file's block {} should be pending deletion",
        target_block_id
    );
}

/// Unlink a file while it has an open handle (open-unlink / orphan pattern).
/// Verify data is accessible until the last handle is released, then cleaned up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_unlink_with_open_handle_deferred_cleanup() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "open-unlink-gc");

    let attr = vfs.create(1, b"orphan.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    vfs.write(ino, 0, b"orphan data").await.unwrap();
    vfs.flush(ino).await.unwrap();
    let _ = wait_until_n_payload_slices(&vfs, ino, 1, Duration::from_secs(15));

    // Open the file.
    let fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();

    // Unlink while handle is open.
    vfs.unlink(1, b"orphan.txt").unwrap();

    // Lookup by name should fail.
    assert!(vfs.lookup(1, b"orphan.txt").is_err());

    // But read via the ino should still work (orphan).
    let data = vfs.read(ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"orphan data");

    // Blocks should still exist (not yet GC'd).
    let blocks = vfs.meta().lookup_logical_payload_slices(ino).unwrap();
    assert!(
        !blocks.is_empty(),
        "payload slices should survive until last handle closes"
    );
    let block_id = blocks[0].1.object_id;

    // Release the last handle — triggers orphan cleanup.
    vfs.release_fh(fh).await.unwrap();

    // Now payload slices should be removed and the object should be pending deletion.
    match vfs.meta().lookup_logical_payload_slices(ino) {
        Ok(blocks_after) => assert!(
            blocks_after.is_empty(),
            "payload graph should be removed after release_fh"
        ),
        Err(MetaError::InodeNotFound(missing)) if missing == ino => {}
        Err(error) => panic!("payload graph lookup after release_fh failed unexpectedly: {error}"),
    }
    let pending = vfs.meta().get_pending_deletions().unwrap();
    assert!(
        pending.iter().any(|(id, _)| *id == block_id),
        "block should be pending deletion after orphan cleanup"
    );
}

/// Rmdir on non-empty directory should fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rmdir_nonempty_fails() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rmdir-nonempty");

    let dir = vfs.mkdir(1, b"nonempty", 0, 0, 0o755).unwrap();
    vfs.create(dir.ino, b"child.txt", 0, 0, 0o644).unwrap();

    let result = vfs.rmdir(1, b"nonempty");
    assert!(result.is_err(), "rmdir on non-empty directory should fail");
}

/// Create many files in one directory and verify readdir returns all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_readdir_many_entries() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "readdir-many");

    let count = 100;
    for i in 0..count {
        let name = format!("file-{i:04}.txt");
        vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
    }

    let entries = vfs.readdir(1).unwrap();
    // readdir includes "." and ".." typically, but StudioFS may not.
    // Count only our created files:
    let our_files: Vec<_> = entries
        .iter()
        .filter(|e| e.name.starts_with(b"file-"))
        .collect();
    assert_eq!(
        our_files.len(),
        count,
        "readdir should return all {count} files"
    );
}

// ─── 9.4 Concurrent Access Patterns ────────────────────────────────────────

/// Concurrent reads and writes to the same file.
/// Verify reads see consistent data (no torn blocks).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_concurrent_read_write_same_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "conc-rw");
    let block_size = vfs.meta().get_format_info().unwrap().block_size as usize;

    let attr = vfs.create(1, b"conc.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write initial data.
    let initial: Vec<u8> = vec![0xAAu8; block_size * 2];
    vfs.write(ino, 0, &initial).await.unwrap();
    vfs.flush(ino).await.unwrap();
    // Wait for blocks to materialise.
    let _ = wait_until_n_payload_slices(&vfs, ino, 2, Duration::from_secs(15));

    let vfs1 = vfs.clone();
    let vfs2 = vfs.clone();

    // Spawn concurrent writer (overwrites block 0).
    let writer = tokio::spawn(async move {
        let new_data = vec![0xBBu8; block_size];
        for _ in 0..5 {
            vfs1.write(ino, 0, &new_data).await.unwrap();
            vfs1.flush(ino).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Spawn concurrent reader (reads block 0 repeatedly).
    let reader = tokio::spawn(async move {
        for _ in 0..20 {
            let data = vfs2.read(ino, 0, block_size as u32).await.unwrap();
            if !data.is_empty() {
                // Every byte should be either 0xAA (old) or 0xBB (new) — never mixed.
                let first = data[0];
                assert!(
                    first == 0xAA || first == 0xBB,
                    "unexpected byte value: {first:#x}"
                );
                // All bytes in block 0 should be the same value (atomic block reads).
                assert!(
                    data.iter().all(|&b| b == first),
                    "torn read detected in block 0"
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let (w, r) = tokio::join!(writer, reader);
    w.unwrap();
    r.unwrap();
}

/// Multiple concurrent flush() calls on the same inode should not deadlock or lose data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_concurrent_flush_does_not_deadlock() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "conc-flush");

    let attr = vfs.create(1, b"conc-flush.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write some data.
    vfs.write(ino, 0, &vec![0x42u8; 1024]).await.unwrap();

    // Spawn 5 concurrent flushes.
    let mut tasks = Vec::new();
    for _ in 0..5 {
        let v = vfs.clone();
        tasks.push(tokio::spawn(async move {
            v.flush(ino).await.unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // Data should be readable.
    let data = vfs.read(ino, 0, 1024).await.unwrap();
    assert_eq!(data.len(), 1024);
    assert!(data.iter().all(|&b| b == 0x42));
}

/// Race between create + write + unlink — verify no panic and consistent state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_create_write_unlink_race() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "create-unlink-race");

    // Create and immediately delete files in a tight loop.
    for i in 0..20 {
        let name = format!("race-{i}.txt");
        let attr = vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
        vfs.write(attr.ino, 0, b"race data").await.unwrap();
        vfs.flush(attr.ino).await.unwrap();
        vfs.unlink(1, name.as_bytes()).unwrap();
    }

    // Root dir should be clean.
    let entries = vfs.readdir(1).unwrap();
    let race_files: Vec<_> = entries
        .iter()
        .filter(|e| e.name.starts_with(b"race-"))
        .collect();
    assert!(
        race_files.is_empty(),
        "all race files should be deleted, found {}",
        race_files.len()
    );
}

// ─── 9.5 File Handle & Orphan Edge Cases ───────────────────────────────────

/// Multiple handles open, unlink, close all — verify cleanup only after last close.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_multiple_handles_orphan_cleanup_after_last_close() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "multi-fh-orphan");

    let attr = vfs.create(1, b"multi-fh.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    vfs.write(ino, 0, b"multi handle data").await.unwrap();
    vfs.flush(ino).await.unwrap();
    let _ = wait_until_n_payload_slices(&vfs, ino, 1, Duration::from_secs(15));

    // Open 3 handles.
    let fh1 = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
    let fh2 = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
    let fh3 = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();

    // Unlink.
    vfs.unlink(1, b"multi-fh.txt").unwrap();

    // Close first two — payload slices should still exist (orphan still alive).
    vfs.release_fh(fh1).await.unwrap();
    vfs.release_fh(fh2).await.unwrap();
    let blocks_mid = vfs.meta().lookup_logical_payload_slices(ino).unwrap();
    assert!(
        !blocks_mid.is_empty(),
        "payload slices should survive with 1 handle remaining"
    );

    // Close the last handle — orphan cleanup triggers.
    vfs.release_fh(fh3).await.unwrap();
    match vfs.meta().lookup_logical_payload_slices(ino) {
        Ok(blocks_final) => assert!(
            blocks_final.is_empty(),
            "payload graph should be cleaned up after last handle closes"
        ),
        Err(MetaError::InodeNotFound(missing)) if missing == ino => {}
        Err(error) => panic!("payload graph lookup after last handle failed unexpectedly: {error}"),
    }
}

/// open_file with O_TRUNC on a file with data — should truncate to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_open_trunc_clears_data() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "open-trunc-clear");

    let attr = vfs.create(1, b"trunc-open.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"original content").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();
    assert!(vfs.getattr(attr.ino).unwrap().size > 0);

    // Open with trunc flag.
    let fh = vfs.open_file(attr.ino, OpenFlags::WRONLY, true).unwrap();
    assert_eq!(
        vfs.getattr(attr.ino).unwrap().size,
        0,
        "file should be truncated on open"
    );
    vfs.release_fh(fh).await.unwrap();
}

// ─── 9.6 Multi-Client metadata sync Integration ─────────────────────────────────────

// Multi-client metadata convergence coverage now lives in `studiofs-meta`
// PostgreSQL tests. VFS integration below stays focused on local FS semantics.

// ─── 9.7 Attribute & Metadata Edge Cases ───────────────────────────────────

/// Setattr mode/uid/gid/times on a directory (not just files).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_setattr_on_directory() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "setattr-dir");

    let dir = vfs.mkdir(1, b"mydir", 0, 0, 0o755).unwrap();

    // Change mode.
    let args = SetattrArgs {
        mode: Some(0o700),
        ..SetattrArgs::default()
    };
    let updated = vfs.setattr(dir.ino, args).unwrap();
    assert_eq!(updated.mode & 0o777, 0o700);

    // Change uid/gid.
    let args = SetattrArgs {
        uid: Some(1000),
        gid: Some(1000),
        ..SetattrArgs::default()
    };
    let updated = vfs.setattr(dir.ino, args).unwrap();
    assert_eq!(updated.uid, 1000);
    assert_eq!(updated.gid, 1000);
}

/// Xattr operations on a directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_on_directory() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr-dir");

    let dir = vfs.mkdir(1, b"xattr-dir", 0, 0, 0o755).unwrap();
    vfs.set_xattr(dir.ino, b"user.tag", b"dir-value", 0)
        .unwrap();

    let val = vfs.get_xattr(dir.ino, b"user.tag").unwrap().unwrap();
    assert_eq!(&val[..], b"dir-value");

    let names = vfs.list_xattrs(dir.ino).unwrap();
    assert_eq!(names.len(), 1);
    assert_eq!(&names[0][..], b"user.tag");
}

// =============================================================================
// Local-First correctness and performance coverage (SFS-121)
//
// These tests encode the target user experience for the Local-First
// architecture on Windows and macOS: background upload must not break or
// heavily degrade foreground local reading, and the entire write→flush→upload
// finalization lifecycle must be transparent to readers.
//
// Correctness tests run without a real S3 backend where possible (unreachable
// endpoint forces the upload path to retry/backoff while reads stay local).
// Performance/lifecycle tests that require upload completion are #[ignore] and
// need S3 + PostgreSQL infra.
// =============================================================================

/// Verify that a large file with multiple full-size blocks plus a partial tail
/// can be read back correctly at every point in the upload lifecycle —
/// including the window between upload completion and the remaining local
/// block-backend cleanup/finalization work.
///
/// This extends `local_reads_through_upload_with_partial_tail_block` to a
/// larger file and validates sustained sequential reads across the complete
/// write→flush→upload→finalize path.
#[test]
#[ignore = "requires S3 backend and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 or valid .env"]
fn buffered_large_file_read_succeeds_across_full_upload_lifecycle() {
    let _env = setup_online_sfs121_env();
    unsafe {
        // Force immediate materialization and keep the upload window open.
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "40");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("sfs121-large-lifecycle-{}", std::process::id());
    let mut format_info = make_format_info(&volume_suffix);
    format_info.block_size = 512 * 1024; // 512 KiB blocks

    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();

    // 6 full blocks + one partial tail (3/4 of a block).
    let block_size = format_info.block_size as usize;
    let full_size = block_size * 6 + block_size * 3 / 4;
    let payload: Vec<u8> = (0..full_size)
        .map(|i| ((i as u64).wrapping_mul(6364136223846793005) >> 40) as u8)
        .collect();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        ensure_bucket().await;
        let vfs = make_vfs_with_meta(meta, &format_info, &cache_dir_str);

        let attr = vfs
            .create(1, b"sfs121-large-lifecycle.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
            })
            .await,
            "upload queue must be populated before the read loop begins"
        );

        // Continuously read the full file until the upload pipeline has
        // published the final epoch (UploadState::Ready).  No read must ever
        // fail; the data must equal the written payload on every iteration.
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut read_passes = 0usize;
        loop {
            let bytes = match vfs.read(attr.ino, 0, full_size as u32).await {
                Ok(b) => b,
                Err(VfsError::RemoteContentPending) => {
                    panic!(
                        "read pass {read_passes}: local read must not surface \
                         RemoteContentPending during background upload"
                    );
                }
                Err(err) => {
                    panic!("read pass {read_passes}: unexpected error: {err}");
                }
            };
            assert_eq!(
                bytes.len(),
                full_size,
                "read pass {read_passes}: wrong byte count"
            );
            assert_eq!(
                bytes.as_ref(),
                payload.as_slice(),
                "read pass {read_passes}: data mismatch during upload lifecycle"
            );
            read_passes += 1;

            let current_attr = vfs.getattr(attr.ino).unwrap();
            if vfs.pending_uploads_for_inode(attr.ino).is_empty()
                && current_attr.upload_state == UploadState::Ready
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "upload pipeline did not stabilize in time (after {read_passes} read passes)"
            );
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        // At least one read pass must have occurred during the upload window
        // (not just after finalization) to validate interleaving correctness.
        assert!(
            read_passes >= 2,
            "expected multiple read passes during upload lifecycle, got {read_passes}"
        );
    });
    rt.shutdown_background();
}

/// Measure and enforce a minimum sustained foreground sequential read
/// throughput while a background upload is concurrently active.
///
/// The test writes a multi-block file, waits for the upload queue to populate,
/// then times N sequential full-file reads.  The total elapsed time for the
/// read sweep must not exceed a generous upper bound, proving that the buffered
/// foreground path is not stalled by background upload activity.
///
/// Pass/fail threshold: all reads of a ~4 MiB file must complete in ≤ 10 s
/// regardless of the simulated upload delay.  The time-bounded assertion
/// (8 × 4 MiB in ≤ 10 s) implies ≥ 3.2 MiB/s; the explicit floor of
/// **4 MiB/s** hardens this expectation and matches the `assert!` below.
#[test]
#[ignore = "requires S3 backend; validates foreground throughput during background upload"]
fn sequential_foreground_read_throughput_during_upload_meets_minimum() {
    let _env = setup_online_sfs121_env();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        // Deliberately slow upload so the read sweep overlaps the active upload
        // window rather than racing to beat it.
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "200");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("sfs121-throughput-{}", std::process::id());
    let mut format_info = make_format_info(&volume_suffix);
    format_info.block_size = 1024 * 1024; // 1 MiB blocks

    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();

    // 4 MiB file (4 × 1 MiB blocks).
    let block_size = format_info.block_size as usize;
    let full_size = block_size * 4;
    let payload: Vec<u8> = (0..full_size).map(|i| (i as u8) ^ 0x37).collect();

    const READ_PASSES: usize = 8;
    const MAX_SWEEP_DURATION: Duration = Duration::from_secs(10);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        ensure_bucket().await;
        let vfs = make_vfs_with_meta(meta, &format_info, &cache_dir_str);

        let attr = vfs
            .create(1, b"sfs121-throughput.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        // Ensure upload is actively in progress before starting the read sweep.
        assert!(
            wait_until(Duration::from_secs(10), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
            })
            .await,
            "upload queue must be populated before throughput sweep begins"
        );

        let sweep_start = Instant::now();
        for pass in 0..READ_PASSES {
            let bytes =
                tokio::time::timeout(MAX_SWEEP_DURATION, vfs.read(attr.ino, 0, full_size as u32))
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "foreground read timed out on pass {pass} — \
                     upload pipeline must not stall local buffered reads"
                        )
                    })
                    .unwrap_or_else(|err| panic!("foreground read failed on pass {pass}: {err}"));
            assert_eq!(bytes.len(), full_size, "pass {pass}: wrong byte count");
        }
        let sweep_elapsed = sweep_start.elapsed();

        assert!(
            sweep_elapsed <= MAX_SWEEP_DURATION,
            "{READ_PASSES} sequential reads of {}B should complete within {MAX_SWEEP_DURATION:?} \
             during background upload; took {sweep_elapsed:?}",
            full_size
        );

        let throughput_mib_s =
            (READ_PASSES * full_size) as f64 / (1024.0 * 1024.0) / sweep_elapsed.as_secs_f64();
        assert!(
            throughput_mib_s >= 4.0,
            "foreground sequential read throughput during upload too low: \
             {throughput_mib_s:.2} MiB/s (minimum: 4.0 MiB/s)"
        );
    });
    rt.shutdown_background();
}

/// Compare buffered foreground-read performance across the three scheduler
/// phases that matter for Local-First regressions:
/// - steady-state read with no pending upload
/// - read while a new overwrite is uploading
/// - read again after that upload fully drains
///
/// The test keeps the same inode hot so the automated check exercises the
/// exact QoS path users hit after an overwrite on Windows/macOS.
#[test]
#[ignore = "requires S3 backend; compares buffered foreground read baseline/during/post upload"]
fn foreground_read_baseline_during_and_post_upload_are_comparable() {
    let _env = setup_online_sfs121_env();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "200");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("sfs121-phases-{}", std::process::id());
    let mut format_info = make_format_info(&volume_suffix);
    format_info.block_size = 1024 * 1024;

    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();

    let block_size = format_info.block_size as usize;
    let full_size = block_size * 4;
    let baseline_payload: Vec<u8> = (0..full_size).map(|i| (i as u8) ^ 0x37).collect();
    let updated_payload: Vec<u8> = (0..full_size)
        .map(|i| (i as u8).wrapping_mul(0x5B).wrapping_add(0x11))
        .collect();

    const READ_PASSES: usize = 8;
    const MAX_SWEEP_DURATION: Duration = Duration::from_secs(10);
    const PHASE_RESET_DELAY: Duration = Duration::from_millis(1700);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        ensure_bucket().await;
        let vfs = make_vfs_with_meta(meta, &format_info, &cache_dir_str);

        let attr = vfs.create(1, b"sfs121-phases.bin", 0, 0, 0o644).unwrap();

        vfs.write(attr.ino, 0, &baseline_payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(60), || {
                let current_attr = vfs.getattr(attr.ino).unwrap();
                vfs.pending_uploads_for_inode(attr.ino).is_empty()
                    && current_attr.upload_state == UploadState::Ready
            })
            .await,
            "initial upload did not drain before baseline sweep"
        );

        let (baseline_elapsed, baseline_throughput_mib_s, baseline_stats) =
            measure_foreground_read_phase(
                &vfs,
                attr.ino,
                &baseline_payload,
                READ_PASSES,
                MAX_SWEEP_DURATION,
                "baseline",
            )
            .await;
        assert!(
            baseline_stats.speedometer_samples > 0,
            "baseline foreground protection stats must contain samples"
        );

        tokio::time::sleep(PHASE_RESET_DELAY).await;

        vfs.write(attr.ino, 0, &updated_payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
            })
            .await,
            "upload queue must be populated before during-upload sweep begins"
        );

        let (during_elapsed, during_throughput_mib_s, during_stats) =
            measure_foreground_read_phase(
                &vfs,
                attr.ino,
                &updated_payload,
                READ_PASSES,
                MAX_SWEEP_DURATION,
                "during-upload",
            )
            .await;

        assert!(
            during_stats.speedometer_samples > 0,
            "during-upload foreground protection stats must contain samples"
        );
        assert!(
            during_stats.window_bytes > 0,
            "during-upload foreground protection stats must capture window bytes"
        );

        assert!(
            wait_until(Duration::from_secs(60), || {
                let current_attr = vfs.getattr(attr.ino).unwrap();
                vfs.pending_uploads_for_inode(attr.ino).is_empty()
                    && current_attr.upload_state == UploadState::Ready
            })
            .await,
            "overwrite upload did not drain before post-upload sweep"
        );

        tokio::time::sleep(PHASE_RESET_DELAY).await;

        let (post_elapsed, post_throughput_mib_s, post_stats) = measure_foreground_read_phase(
            &vfs,
            attr.ino,
            &updated_payload,
            READ_PASSES,
            MAX_SWEEP_DURATION,
            "post-upload",
        )
        .await;

        assert!(
            post_stats.speedometer_samples > 0,
            "post-upload foreground protection stats must contain samples"
        );
        assert!(
            during_elapsed <= MAX_SWEEP_DURATION,
            "during-upload sweep exceeded {MAX_SWEEP_DURATION:?}: baseline={baseline_elapsed:?}, during={during_elapsed:?}, post={post_elapsed:?}"
        );
        assert!(
            post_elapsed <= MAX_SWEEP_DURATION,
            "post-upload sweep exceeded {MAX_SWEEP_DURATION:?}: baseline={baseline_elapsed:?}, during={during_elapsed:?}, post={post_elapsed:?}"
        );
        // Keep the post-upload gate tolerant: on CI and slower Windows/macOS
        // hosts, either an absolute floor or recovery toward baseline is a
        // sufficient pass condition.
        assert!(
            during_throughput_mib_s >= 4.0
                || during_throughput_mib_s >= baseline_throughput_mib_s * 0.25,
            "foreground read degraded too far during upload: baseline={baseline_throughput_mib_s:.2} MiB/s, during={during_throughput_mib_s:.2} MiB/s, post={post_throughput_mib_s:.2} MiB/s, during_p90_ms={}, during_penalty_ms={}, during_cost={}",
            during_stats
                .p90_latency_ns
                .map(|ns| ns / 1_000_000)
                .unwrap_or(0),
            during_stats.upload_scheduler_penalty_ms,
            during_stats.upload_cache_io_foreground_cost,
        );
        assert!(
            post_throughput_mib_s >= 4.0 || post_throughput_mib_s >= baseline_throughput_mib_s * 0.8,
            "post-upload throughput too low: need >= 4.0 MiB/s or recovery to >= 80% of baseline; baseline={baseline_throughput_mib_s:.2} MiB/s, during={during_throughput_mib_s:.2} MiB/s, post={post_throughput_mib_s:.2} MiB/s"
        );
    });
    rt.shutdown_background();
}

/// Force an upload backlog and verify the uploader progresses via persisted
/// local payload sources instead of depending on transient in-memory state.
#[test]
fn upload_backlog_progresses_via_persisted_local_payload_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let _env = setup_offline_sfs121_env();
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "8MB");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let (format_info, vfs) =
            make_offline_vfs_sfs121(&tmp, "sfs124-stage-head", Some(64 * 1024));
        let block_size = format_info.block_size as usize;
        let full_size = block_size * 8;
        let payload: Vec<u8> = (0..full_size)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(7))
            .collect();

        let attr = vfs
            .create(1, b"sfs124-stage-head.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until(Duration::from_secs(10), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
            })
            .await,
            "upload queue must populate before staged-payload observation"
        );

        assert!(
            wait_until(Duration::from_secs(10), || {
                let stats = vfs.foreground_read_protection_stats();
                stats.upload_local_payload_reads > 0
            })
            .await,
            "expected uploader to consume persisted local payloads under backlog"
        );

        let stats = vfs.foreground_read_protection_stats();
        assert!(
            stats.upload_local_payload_reads > 0,
            "upload backlog should trigger persisted local payload reads; stats={stats:?}"
        );
        assert!(
            stats.upload_local_payload_read_bytes > 0,
            "persisted local payload reads should account transferred bytes; stats={stats:?}"
        );
    });
    rt.shutdown_background();
}

/// Validate warm-cache vs cold-cache buffered read behavior.
///
/// After a write + flush, data is committed into the local block backend while
/// remote upload remains pending. A first sequential read exercises the colder
/// local path; subsequent reads must stay within a consistent bounded latency.
///
/// This test uses an unreachable S3 endpoint so no actual upload completes;
/// all reads are served from local state without requiring remote fetch.
#[test]
fn warm_cache_sequential_read_bounded_latency_after_write() {
    let tmp = tempfile::tempdir().unwrap();
    let _env = setup_offline_sfs121_env();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let (format_info, vfs) = make_offline_vfs_sfs121(&tmp, "warm-cache", Some(512 * 1024));

        // 3 blocks — large enough to measure, small enough for deterministic timing.
        let block_size = format_info.block_size as usize;
        let full_size = block_size * 3;
        let payload: Vec<u8> = (0..full_size).map(|i| (i as u8).wrapping_mul(0xB7)).collect();

        const WARM_PASSES: usize = 5;
        // Cold + warm reads must each individually complete well within 5 s.
        const MAX_SINGLE_READ_DURATION: Duration = Duration::from_secs(5);

        let attr = vfs.create(1, b"sfs121-warm-cache.bin", 0, 0, 0o644).unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        // First read: colder local path before repeated reads warm the hot path.
        let t_cold_start = Instant::now();
        let bytes = tokio::time::timeout(
            MAX_SINGLE_READ_DURATION,
            vfs.read(attr.ino, 0, full_size as u32),
        )
        .await
        .expect("cold-path read timed out")
        .expect("cold-path read failed");
        let t_cold = t_cold_start.elapsed();
        assert_eq!(bytes.as_ref(), payload.as_slice(), "cold read: data mismatch");

        // Warm reads: data is now residing in L1 memory cache.
        let mut warm_durations = Vec::with_capacity(WARM_PASSES);
        for pass in 0..WARM_PASSES {
            let t_start = Instant::now();
            let bytes = tokio::time::timeout(
                MAX_SINGLE_READ_DURATION,
                vfs.read(attr.ino, 0, full_size as u32),
            )
            .await
            .unwrap_or_else(|_| panic!("warm read pass {pass} timed out"))
            .unwrap_or_else(|err| panic!("warm read pass {pass} failed: {err}"));
            let t_warm = t_start.elapsed();
            assert_eq!(bytes.as_ref(), payload.as_slice(), "warm read {pass}: data mismatch");
            warm_durations.push(t_warm);
        }

        // Each individual warm read must be bounded.
        for (pass, dur) in warm_durations.iter().enumerate() {
            assert!(
                *dur <= MAX_SINGLE_READ_DURATION,
                "warm read pass {pass} took {dur:?}, exceeds {MAX_SINGLE_READ_DURATION:?}"
            );
        }

        // Warm reads must not be dramatically slower than the cold read.
        // Allow up to 10× cold latency as a very generous regression gate.
        let cold_ceiling = t_cold.saturating_mul(10).max(Duration::from_millis(200));
        for (pass, dur) in warm_durations.iter().enumerate() {
            assert!(
                *dur <= cold_ceiling,
                "warm read pass {pass} took {dur:?} (cold was {t_cold:?}, ceiling {cold_ceiling:?}); \
                 warm cache path regressed vs cold"
            );
        }
    });
    rt.shutdown_background();
}

/// Verify that concurrent sequential, random-offset, and mixed readers all
/// observe correct data while a background upload is active.
///
/// This covers the "mixed workload classification" requirement from SFS-121:
/// sequential stream, random seeks, mixed patterns, and concurrent readers
/// must all work correctly simultaneously.
///
/// Uses an unreachable S3 endpoint so local buffered reads must succeed
/// independently of the upload outcome.
#[test]
fn mixed_workload_concurrent_readers_during_pending_upload_no_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let _env = setup_offline_sfs121_env();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    rt.block_on(async {
        let (format_info, vfs) = make_offline_vfs_sfs121(&tmp, "mixed-workload", None);

        let block_size = format_info.block_size as usize;
        // Use 4 blocks so random-offset reads land at block boundaries, midpoints,
        // and across block boundaries (cross-block sequential pattern).
        let full_size = block_size * 4;
        let payload: Vec<u8> = (0..full_size)
            .map(|i| ((i as u64).wrapping_mul(2654435761) >> 32) as u8)
            .collect();

        let attr = vfs
            .create(1, b"sfs121-mixed-workload.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        // Upload is queued but the S3 endpoint is unreachable — local reads
        // must be fully self-contained and not depend on upload success.
        assert!(
            wait_until(Duration::from_secs(10), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
            })
            .await,
            "upload queue must be populated for mixed workload test"
        );

        const PASSES_PER_READER: usize = 6;
        let deadline = Duration::from_secs(15);

        // — Sequential reader: reads the entire file from beginning to end.
        let vfs_seq = vfs.clone();
        let payload_seq = payload.clone();
        let seq_handle = tokio::spawn(async move {
            for pass in 0..PASSES_PER_READER {
                let bytes = tokio::time::timeout(
                    Duration::from_secs(5),
                    vfs_seq.read(attr.ino, 0, full_size as u32),
                )
                .await
                .unwrap_or_else(|_| panic!("sequential reader pass {pass} timed out"))
                .unwrap_or_else(|err| panic!("sequential reader pass {pass} failed: {err}"));
                assert_eq!(
                    bytes.len(),
                    full_size,
                    "sequential reader {pass}: wrong length"
                );
                assert_eq!(
                    bytes.as_ref(),
                    payload_seq.as_slice(),
                    "sequential reader {pass}: data mismatch"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        // — Random-offset reader: reads at block midpoints and cross-block offsets.
        let vfs_rand = vfs.clone();
        let payload_rand = payload.clone();
        let rand_handle = tokio::spawn(async move {
            // Offsets: block 0 midpoint, block 1 start, cross block 1→2, block 3 end.
            let read_specs: &[(u64, u32)] = &[
                (block_size as u64 / 2, block_size as u32),
                (block_size as u64, block_size as u32),
                (block_size as u64 + block_size as u64 / 2, block_size as u32),
                ((full_size - block_size / 4) as u64, block_size as u32 / 4),
            ];
            for pass in 0..PASSES_PER_READER {
                for &(offset, length) in read_specs {
                    let bytes = tokio::time::timeout(
                        Duration::from_secs(5),
                        vfs_rand.read(attr.ino, offset, length),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        panic!("random reader pass {pass} offset {offset} timed out")
                    })
                    .unwrap_or_else(|err| {
                        panic!("random reader pass {pass} offset {offset} failed: {err}")
                    });
                    let start = offset as usize;
                    let end = (start + length as usize).min(full_size);
                    assert_eq!(
                        bytes.as_ref(),
                        &payload_rand[start..end],
                        "random reader pass {pass} offset {offset}: data mismatch"
                    );
                }
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
        });

        // — Mixed-pattern reader: alternates between head, tail, and full reads.
        let vfs_mixed = vfs.clone();
        let payload_mixed = payload.clone();
        let mixed_handle = tokio::spawn(async move {
            let patterns: &[(u64, u32)] = &[
                (0, (block_size * 2) as u32),                         // head
                ((full_size - block_size) as u64, block_size as u32), // tail
                (0, full_size as u32),                                // full
                (block_size as u64, (block_size * 2) as u32),         // middle
            ];
            for pass in 0..PASSES_PER_READER {
                for &(offset, length) in patterns {
                    let bytes = tokio::time::timeout(
                        Duration::from_secs(5),
                        vfs_mixed.read(attr.ino, offset, length),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        panic!("mixed reader pass {pass} offset {offset} timed out")
                    })
                    .unwrap_or_else(|err| {
                        panic!("mixed reader pass {pass} offset {offset} failed: {err}")
                    });
                    let start = offset as usize;
                    let end = (start + length as usize).min(full_size);
                    assert_eq!(
                        bytes.as_ref(),
                        &payload_mixed[start..end],
                        "mixed reader pass {pass} offset {offset}: data mismatch"
                    );
                }
                tokio::time::sleep(Duration::from_millis(4)).await;
            }
        });

        tokio::time::timeout(deadline, seq_handle)
            .await
            .expect("sequential reader task timed out")
            .expect("sequential reader task panicked");
        tokio::time::timeout(deadline, rand_handle)
            .await
            .expect("random-offset reader task timed out")
            .expect("random-offset reader task panicked");
        tokio::time::timeout(deadline, mixed_handle)
            .await
            .expect("mixed-pattern reader task timed out")
            .expect("mixed-pattern reader task panicked");
    });
    rt.shutdown_background();
}

#[test]
fn handle_aware_read_policy_keeps_sequential_and_random_streams_separate() {
    let tmp = tempfile::tempdir().unwrap();
    let _env = setup_offline_sfs121_env();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let (format_info, vfs) = make_offline_vfs_sfs121(&tmp, "sfs125-streams", Some(128 * 1024));
        let block_size = format_info.block_size as usize;
        let full_size = block_size * 8;
        let payload: Vec<u8> = (0..full_size)
            .map(|idx| ((idx as u64).wrapping_mul(1_103_515_245) >> 24) as u8)
            .collect();

        let attr = vfs
            .create(1, b"sfs125-handle-aware.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        let fh_seq = vfs.open_file(attr.ino, OpenFlags::RDONLY, false).unwrap();
        let fh_rand = vfs.open_file(attr.ino, OpenFlags::RDONLY, false).unwrap();

        let mut seq_buf = vec![0u8; block_size];
        for step in 0..6u64 {
            let offset = step * block_size as u64;
            let read = vfs
                .read_into_fh(fh_seq, attr.ino, offset, &mut seq_buf)
                .await
                .unwrap();
            assert_eq!(read as usize, block_size);
            let start = offset as usize;
            assert_eq!(&seq_buf[..block_size], &payload[start..start + block_size]);
        }

        let mut rand_buf = vec![0u8; block_size / 2];
        for offset in [0u64, (block_size * 6) as u64, (block_size * 2) as u64] {
            let read = vfs
                .read_into_fh(fh_rand, attr.ino, offset, &mut rand_buf)
                .await
                .unwrap();
            let start = offset as usize;
            let end = start + read as usize;
            assert_eq!(&rand_buf[..read as usize], &payload[start..end]);
        }

        let seq_open = vfs.get_open_file(fh_seq).unwrap();
        let rand_open = vfs.get_open_file(fh_rand).unwrap();
        assert_eq!(seq_open.access_class_name(), "sequential_media_read");
        assert_eq!(rand_open.access_class_name(), "random_interactive_read");

        let stats = vfs.foreground_read_protection_stats();
        assert_eq!(
            stats.dominant_access_class.as_deref(),
            Some("sequential_media_read")
        );
        assert!(stats.dominant_prefetch_enabled);
        assert!(!stats.dominant_cache_promotion_on_read);
        assert!(stats.dominant_readahead_max_width >= stats.dominant_readahead_base_width);
        assert_eq!(stats.active_sequential_media_reads, 1);
        assert_eq!(stats.active_random_interactive_reads, 1);

        vfs.release_fh(fh_seq).await.unwrap();
        vfs.release_fh(fh_rand).await.unwrap();

        let closed_stats = vfs.foreground_read_protection_stats();
        assert_eq!(closed_stats.active_sequential_media_reads, 0);
        assert_eq!(closed_stats.active_random_interactive_reads, 0);
    });
    rt.shutdown_background();
}

/// Verify that per-file flush ordering is deterministic: a flush followed by
/// a read must always observe the last flushed state, and interleaved writes
/// to different inodes must not cross-contaminate.
///
/// This encodes the "deterministic file-session ordering" requirement from
/// SFS-121: write/flush/finalize transitions must produce a consistent
/// observable state for readers on the same client.
///
/// Does not require a real S3 backend.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_session_flush_ordering_is_deterministic() {
    let tmp = tempfile::tempdir().unwrap();
    let _env = setup_offline_sfs121_env();
    // Use a modest block size so the test runs quickly on any machine.
    let block_size: u32 = 128 * 1024;
    let (_format_info, vfs) = make_offline_vfs_sfs121(&tmp, "session-order", Some(block_size));

    let payload_a1: Vec<u8> = (0..block_size as usize).map(|i| (i as u8) ^ 0xAA).collect();
    let payload_a2: Vec<u8> = (0..block_size as usize).map(|i| (i as u8) ^ 0xBB).collect();
    let payload_b: Vec<u8> = (0..block_size as usize).map(|i| (i as u8) ^ 0xCC).collect();
    let attr_a = vfs.create(1, b"session-a.bin", 0, 0, 0o644).unwrap();
    let attr_b = vfs.create(1, b"session-b.bin", 0, 0, 0o644).unwrap();

    // ── First write session ───────────────────────────────────────────────
    vfs.write(attr_a.ino, 0, &payload_a1).await.unwrap();
    vfs.flush(attr_a.ino).await.unwrap();

    vfs.write(attr_b.ino, 0, &payload_b).await.unwrap();
    vfs.flush(attr_b.ino).await.unwrap();

    // A post-flush read must reflect the flushed content.
    let read_a = vfs.read(attr_a.ino, 0, block_size).await.unwrap();
    assert_eq!(
        read_a.as_ref(),
        payload_a1.as_slice(),
        "file A must reflect first session write after flush"
    );

    // B must not contain any bytes from A's write.
    let read_b = vfs.read(attr_b.ino, 0, block_size).await.unwrap();
    assert_eq!(
        read_b.as_ref(),
        payload_b.as_slice(),
        "file B must not be contaminated by writes to file A"
    );

    // ── Second write session on A ─────────────────────────────────────────
    vfs.write(attr_a.ino, 0, &payload_a2).await.unwrap();
    vfs.flush(attr_a.ino).await.unwrap();

    // A must now reflect the updated payload.
    let read_a2 = vfs.read(attr_a.ino, 0, block_size).await.unwrap();
    assert_eq!(
        read_a2.as_ref(),
        payload_a2.as_slice(),
        "file A must reflect second session write after second flush"
    );

    // B must still reflect the original payload; no cross-contamination.
    let read_b2 = vfs.read(attr_b.ino, 0, block_size).await.unwrap();
    assert_eq!(
        read_b2.as_ref(),
        payload_b.as_slice(),
        "file B must not be contaminated by second write session to file A"
    );

    // ── Stability: repeated reads return the same data ────────────────────
    for pass in 0..3 {
        let ra = vfs.read(attr_a.ino, 0, block_size).await.unwrap();
        let rb = vfs.read(attr_b.ino, 0, block_size).await.unwrap();
        assert_eq!(
            ra.as_ref(),
            payload_a2.as_slice(),
            "A stability pass {pass}: data changed without a write"
        );
        assert_eq!(
            rb.as_ref(),
            payload_b.as_slice(),
            "B stability pass {pass}: data changed without a write"
        );
    }
}

/// Getattr on non-existent inode should return an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_getattr_nonexistent_inode() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "getattr-noent");

    let result = vfs.getattr(999999);
    assert!(result.is_err(), "getattr on non-existent inode should fail");
}

/// Lookup non-existent entry in a directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_lookup_nonexistent_entry() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "lookup-noent");

    let result = vfs.lookup(1, b"does-not-exist.txt");
    assert!(
        result.is_err(),
        "lookup non-existent entry should return error"
    );
}

/// Create a file, verify counter TOTAL_INODES increments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_create_increments_total_inodes() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "inode-count");

    let before = vfs
        .meta()
        .get_counter(studiofs_meta::types::counters::TOTAL_INODES)
        .unwrap_or(0);

    vfs.create(1, b"counted.txt", 0, 0, 0o644).unwrap();

    let after = vfs
        .meta()
        .get_counter(studiofs_meta::types::counters::TOTAL_INODES)
        .unwrap_or(0);
    assert_eq!(after, before + 1, "TOTAL_INODES should increment by 1");
}

// ─── 9.8 Symlink & Hardlink Edge Cases ─────────────────────────────────────

/// Create a symlink pointing to a non-existent target. readlink should return
/// the target path even though the target doesn't exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_symlink_dangling_target() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "sym-dangle");

    let attr = vfs
        .symlink(1, b"dangling-link", b"/nonexistent/path", 0, 0)
        .unwrap();
    let target = vfs.readlink(attr.ino).unwrap();
    assert_eq!(&target[..], b"/nonexistent/path");
}

/// Hardlink increases nlink and can be read from both paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_hardlink_nlink_increments() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hardlink-nlink");

    let attr = vfs.create(1, b"original.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"shared data").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    let before_nlink = vfs.getattr(attr.ino).unwrap().nlink;

    // Create hardlink.
    vfs.link(attr.ino, 1, b"link.txt").unwrap();

    let after_nlink = vfs.getattr(attr.ino).unwrap().nlink;
    assert_eq!(after_nlink, before_nlink + 1, "nlink should increment");

    // Both names should resolve to the same ino.
    let a = vfs.lookup(1, b"original.txt").unwrap();
    let b = vfs.lookup(1, b"link.txt").unwrap();
    assert_eq!(a.ino, b.ino);

    // Read from the link should return the same data.
    let data = vfs.read(b.ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"shared data");
}

/// Unlink one hardlink — file should survive because nlink > 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_hardlink_unlink_one_keeps_other() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hardlink-unlink");

    let attr = vfs.create(1, b"hl-orig.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"persist").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();
    let _ = wait_until_n_payload_slices(&vfs, attr.ino, 1, Duration::from_secs(15));

    vfs.link(attr.ino, 1, b"hl-link.txt").unwrap();

    // Unlink the original name.
    vfs.unlink(1, b"hl-orig.txt").unwrap();

    // The link should still be alive.
    let a = vfs.lookup(1, b"hl-link.txt").unwrap();
    assert_eq!(a.ino, attr.ino);

    // Blocks should NOT be removed (nlink still > 0).
    let blocks = vfs.meta().lookup_logical_payload_slices(attr.ino).unwrap();
    assert!(
        !blocks.is_empty(),
        "payload slices should survive when hardlink remains"
    );

    // Data should still be readable.
    let data = vfs.read(attr.ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"persist");
}

// ─── 9.9 Flock Edge Cases ──────────────────────────────────────────────────

/// Exclusive flock on a file, try exclusive from second owner — should fail non-blocking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flock_exclusive_nonblocking_conflict() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flock-conflict");

    let attr = vfs.create(1, b"flock.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Owner 1 acquires exclusive.
    vfs.flock(ino, LockKind::Exclusive, 1, false).await.unwrap();

    // Owner 2 tries non-blocking exclusive — should fail.
    let result = vfs.flock(ino, LockKind::Exclusive, 2, false).await;
    assert!(
        matches!(result, Err(VfsError::WouldBlock)),
        "non-blocking exclusive should fail with WouldBlock"
    );

    // Unlock owner 1.
    vfs.flock(ino, LockKind::Unlock, 1, false).await.unwrap();

    // Now owner 2 should succeed.
    vfs.flock(ino, LockKind::Exclusive, 2, false).await.unwrap();
    vfs.flock(ino, LockKind::Unlock, 2, false).await.unwrap();
}

/// Shared lock + try shared from another owner → should succeed.
/// Then try exclusive → should fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flock_shared_then_exclusive_conflict() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flock-sh-ex");

    let attr = vfs.create(1, b"flock-share.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Two readers.
    vfs.flock(ino, LockKind::Shared, 1, false).await.unwrap();
    vfs.flock(ino, LockKind::Shared, 2, false).await.unwrap();

    // Exclusive should fail.
    let result = vfs.flock(ino, LockKind::Exclusive, 3, false).await;
    assert!(
        matches!(result, Err(VfsError::WouldBlock)),
        "exclusive should fail when shared locks held"
    );

    // Unlock both shared.
    vfs.flock(ino, LockKind::Unlock, 1, false).await.unwrap();
    vfs.flock(ino, LockKind::Unlock, 2, false).await.unwrap();

    // Now exclusive should succeed.
    vfs.flock(ino, LockKind::Exclusive, 3, false).await.unwrap();
    vfs.flock(ino, LockKind::Unlock, 3, false).await.unwrap();
}

// ─── 9.10 Multi-Client Visibility Smoke Tests ──────────────────────────────

// Writer creates a deeply nested directory tree, reader can traverse the full path.
// Writer creates a large file (multi-block), reader reads and verifies integrity.
// Writer does setattr (chmod, chown), reader sees updated attributes via metadata sync.
// Writer creates symlink, reader resolves it.
// Writer creates hardlink, reader sees it and reads shared data.

// ─── 9.11 Shutdown & Drain Tests ────────────────────────────────────────────

/// Shutdown after writes should drain local write buffers and persist metadata.
/// After shutdown, payload objects should be uploaded to S3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_with_pending_uploads_completes_within_bound() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr12-bounded-shutdown-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let vfs = make_vfs_with_meta(meta, &format_info, &cache_dir_str);
    let payload = vec![0x5Au8; format_info.block_size as usize * 2];
    let attr = vfs.create(1, b"bounded-shutdown.bin", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, &payload).await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    assert!(
        wait_until(Duration::from_secs(10), || {
            !vfs.pending_uploads_for_inode(attr.ino).is_empty()
        })
        .await,
        "offline upload scenario should queue pending uploads before shutdown"
    );

    tokio::time::timeout(Duration::from_secs(5), vfs.shutdown(Duration::ZERO))
        .await
        .expect("shutdown should stay bounded even with offline pending uploads");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zeroio_shutdown_snapshot_uses_runtime_dir_without_ingress_spill() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("zeroio-runtime-layout-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let vfs = make_vfs_with_meta(meta, &format_info, &cache_dir_str);
    let payload = vec![0x33u8; format_info.block_size as usize];
    let attr = vfs
        .create(1, b"zeroio-runtime-layout.bin", 0, 0, 0o644)
        .unwrap();
    vfs.write(attr.ino, 0, &payload).await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    assert!(
        wait_until(Duration::from_secs(10), || {
            !vfs.pending_uploads_for_inode(attr.ino).is_empty()
        })
        .await,
        "offline upload scenario should queue pending uploads before shutdown"
    );

    tokio::time::timeout(Duration::from_secs(5), vfs.shutdown(Duration::ZERO))
        .await
        .expect("shutdown should stay bounded even with offline pending uploads");

    let runtime_snapshot_path = cache_dir.join("runtime").join("zeroio-last-shutdown.json");
    assert!(
        runtime_snapshot_path.exists(),
        "zeroio shutdown snapshot should be persisted under cache/runtime"
    );
    assert!(
        !cache_dir.join("ingress_spill").exists(),
        "runtime cleanup should not recreate legacy ingress_spill"
    );

    drop(vfs);
    let reopened = reopen_vfs(&db_path, &cache_dir);
    let runtime_snapshot = reopened.zeroio_runtime_snapshot();
    let last_graceful_shutdown = runtime_snapshot
        .last_graceful_shutdown
        .expect("reopened zeroio snapshot should include persisted graceful shutdown state");
    assert!(
        last_graceful_shutdown.local_payload_sync_ok,
        "graceful shutdown snapshot should record successful local payload sync"
    );
}

#[test]
fn zeroio_startup_removes_retired_ingress_spill_on_startup() {
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let format_info = make_format_info_with_endpoint(
        &format!("zeroio-legacy-fast-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    let legacy_spill_dir = cache_dir.join("ingress_spill");
    std::fs::create_dir_all(legacy_spill_dir.join("segments")).unwrap();
    std::fs::write(
        legacy_spill_dir
            .join("segments")
            .join("0000000000000001.seg"),
        b"legacy-segment",
    )
    .unwrap();
    let vfs = make_local_zeroio_test_vfs(
        &db_path,
        &cache_dir_str,
        &format_info,
        VfsConfigRaw::default()
            .validate()
            .expect("default config should validate"),
    )
    .2;

    assert!(
        !legacy_spill_dir.exists(),
        "startup should delete unsupported ingress_spill layouts instead of leaving disk garbage behind"
    );

    drop(vfs);
}

#[test]
fn zeroio_direct_studiovfs_new_removes_retired_ingress_spill_on_startup() {
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    unsafe {
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let format_info = make_format_info_with_endpoint(
        &format!("zeroio-legacy-durable-{}", std::process::id()),
        "http://127.0.0.1:59999",
    );
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let legacy_spill_dir = cache_dir.join("ingress_spill");
    std::fs::create_dir_all(legacy_spill_dir.join("segments")).unwrap();
    std::fs::write(
        legacy_spill_dir
            .join("segments")
            .join("0000000000000001.seg"),
        b"legacy-segment",
    )
    .unwrap();

    let vfs = StudioVfs::new(
        meta,
        &DataStoreConfig::from(&format_info),
        &cache_dir_str,
        16 * 1024 * 1024,
        16 * 1024 * 1024,
        VfsConfigRaw::default()
            .validate()
            .expect("default config should validate"),
    )
    .expect(
        "startup should delete unsupported ingress_spill layouts instead of refusing the mount",
    );

    assert!(
        !legacy_spill_dir.exists(),
        "startup should remove retired ingress_spill directories after provisioning the active runtime"
    );

    drop(vfs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ctrl_c_during_small_file_churn_does_not_hang() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr12-ctrlc-small-churn-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let vfs = make_vfs_with_meta(meta, &format_info, &cache_dir_str);
    let churn_vfs = vfs.clone();
    let churn = tokio::spawn(async move {
        let payload = vec![0xABu8; 16 * 1024];
        let mut idx = 0u64;
        loop {
            if churn_vfs.is_shutting_down() {
                break;
            }
            let name = format!("ctrlc-churn-{idx}.txt");
            if let Ok(attr) = churn_vfs.create(1, name.as_bytes(), 0, 0, 0o644) {
                let _ = churn_vfs.write(attr.ino, 0, &payload).await;
                let _ = churn_vfs.flush(attr.ino).await;
                let _ = churn_vfs.unlink(1, name.as_bytes());
            }
            idx = idx.saturating_add(1);
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    tokio::time::timeout(Duration::from_secs(5), vfs.shutdown(Duration::ZERO))
        .await
        .expect("shutdown should stay bounded during small-file churn");
    tokio::time::timeout(Duration::from_secs(2), churn)
        .await
        .expect("foreground churn task should observe shutdown and exit")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remount_after_delete_shutdown_does_not_resurrect_ghost_entry() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr12-remount-delete-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");

    {
        let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
        meta.init_root(0, 0).unwrap();
        meta.set_format_info(&format_info).unwrap();
        let vfs = make_vfs_with_meta(meta, &format_info, &cache_dir_str);

        let dir = vfs.mkdir(1, b"ghost-dir", 0, 0, 0o755).unwrap();
        let attr = vfs.create(dir.ino, b"ghost.txt", 0, 0, 0o644).unwrap();
        vfs.write(attr.ino, 0, b"ghost-bytes").await.unwrap();
        vfs.flush(attr.ino).await.unwrap();
        vfs.unlink(dir.ino, b"ghost.txt").unwrap();
        vfs.rmdir(1, b"ghost-dir").unwrap();

        tokio::time::timeout(Duration::from_secs(5), vfs.shutdown(Duration::ZERO))
            .await
            .expect("shutdown after delete should stay bounded");
    }

    let reopened = reopen_vfs(&db_path, &cache_dir);
    assert!(reopened.lookup(1, b"ghost-dir").is_err());
    let entries = reopened.readdir(1).unwrap();
    assert!(
        !entries.iter().any(|entry| {
            entry.name.as_slice() == b"ghost-dir" || entry.name.as_slice() == b"ghost.txt"
        }),
        "deleted directory must not reappear as a ghost root entry after remount"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unmount_after_recursive_delete_preserves_consistent_namespace() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr12-recursive-delete-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");

    {
        let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
        meta.init_root(0, 0).unwrap();
        meta.set_format_info(&format_info).unwrap();
        let vfs = make_vfs_with_meta(meta, &format_info, &cache_dir_str);

        let root = vfs.mkdir(1, b"delete-tree", 0, 0, 0o755).unwrap();
        let nested = vfs.mkdir(root.ino, b"nested", 0, 0, 0o755).unwrap();
        let a = vfs.create(root.ino, b"a.txt", 0, 0, 0o644).unwrap();
        let b = vfs.create(nested.ino, b"b.txt", 0, 0, 0o644).unwrap();
        vfs.write(a.ino, 0, b"root-data").await.unwrap();
        vfs.flush(a.ino).await.unwrap();
        vfs.write(b.ino, 0, b"nested-data").await.unwrap();
        vfs.flush(b.ino).await.unwrap();
        vfs.unlink(root.ino, b"a.txt").unwrap();
        vfs.unlink(nested.ino, b"b.txt").unwrap();
        vfs.rmdir(root.ino, b"nested").unwrap();
        vfs.rmdir(1, b"delete-tree").unwrap();

        tokio::time::timeout(Duration::from_secs(5), vfs.shutdown(Duration::ZERO))
            .await
            .expect("shutdown after recursive delete should stay bounded");
    }

    let reopened = reopen_vfs(&db_path, &cache_dir);
    assert!(reopened.lookup(1, b"delete-tree").is_err());
    let root_entries = reopened.readdir(1).unwrap();
    assert!(
        !root_entries.iter().any(|entry| {
            let name = entry.name.as_slice();
            name == b"delete-tree" || name == b"nested" || name == b"a.txt" || name == b"b.txt"
        }),
        "recursive delete should not leave ghost entries in the root namespace after remount"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_upload_helpers_use_live_index_without_directory_scan() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:59999");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "60000");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "1");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", "100");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("pr13-live-upload-index-{}", std::process::id());
    let format_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    let vfs = make_vfs_with_meta(meta, &format_info, &cache_dir_str);
    let payload = vec![0x22u8; format_info.block_size as usize * 2];
    let attr = vfs.create(1, b"live-index.bin", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, &payload).await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    assert!(
        wait_until(Duration::from_secs(10), || {
            vfs.pending_uploads_for_inode(attr.ino).len() == 2
        })
        .await,
        "offline upload scenario should surface pending uploads before the upload dir is hidden"
    );

    let upload_dir = cache_dir.join("upload");
    std::fs::create_dir_all(&upload_dir).unwrap();
    let hidden_dir = cache_dir.join("upload-hidden");
    std::fs::rename(&upload_dir, &hidden_dir).unwrap();

    let pending = vfs.pending_uploads_for_inode(attr.ino);
    assert_eq!(
        pending.len(),
        2,
        "pending upload helpers must use the live in-memory index instead of scanning upload/"
    );
    assert_eq!(
        vfs.upload_stats().pending_uploads,
        2,
        "pending upload count should stay available even if upload/ is temporarily unavailable"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_shutdown_drains_dirty_buffers() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "shutdown-drain");
    let block_size = vfs.meta().get_format_info().unwrap().block_size as usize;

    let attr = vfs.create(1, b"pre-shutdown.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 2,5 blocks of data.
    let total = block_size * 2 + block_size / 2;
    let payload: Vec<u8> = (0..total).map(|i| (i % 197) as u8).collect();
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Before shutdown — blocks may or may not be uploaded yet.
    // Call shutdown which should drain everything.
    vfs.shutdown(Duration::from_secs(30)).await;

    // After shutdown, all 3 payload slices should exist in metadata.
    let blocks = vfs.meta().lookup_logical_payload_slices(ino).unwrap();
    assert_eq!(
        blocks.len(),
        3,
        "after shutdown, all 3 payload slices should be persisted"
    );
}

/// Double shutdown should not panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_double_shutdown_does_not_panic() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "double-shutdown");

    vfs.create(1, b"file.txt", 0, 0, 0o644).unwrap();
    vfs.shutdown(Duration::from_secs(5)).await;
    vfs.shutdown(Duration::from_secs(5)).await;
    // No panic = pass.
}

// ─── 9.12 More Error & Edge Cases ──────────────────────────────────────────

/// Double unlink of the same file should fail on the second call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_double_unlink_fails() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "double-unlink");

    vfs.create(1, b"once.txt", 0, 0, 0o644).unwrap();
    vfs.unlink(1, b"once.txt").unwrap();
    let result = vfs.unlink(1, b"once.txt");
    assert!(result.is_err(), "second unlink should fail");
}

/// Rename a file to itself should be a no-op (or succeed without error).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rename_to_self_is_noop() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename-self");

    let attr = vfs.create(1, b"self.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"self data").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    // Rename to itself.
    vfs.rename(1, b"self.txt", 1, b"self.txt").unwrap();

    // File should still be accessible with original data.
    let after = vfs.lookup(1, b"self.txt").unwrap();
    assert_eq!(after.ino, attr.ino);
    let data = vfs.read(attr.ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"self data");
}

/// Write at a high offset (sparse) then read back — gap should be zero-filled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_sparse_write_at_high_offset() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "sparse-high");
    let block_size = vfs.meta().get_format_info().unwrap().block_size as u64;

    let attr = vfs.create(1, b"sparse-high.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write at offset = block_size * 3 (skipping blocks 0-2).
    let high_offset = block_size * 3;
    let payload = b"high offset data";
    vfs.write(ino, high_offset, payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    assert_eq!(
        vfs.getattr(ino).unwrap().size,
        high_offset + payload.len() as u64
    );

    // Read from the gap — should be zero-filled.
    let gap = vfs.read(ino, 0, 1024).await.unwrap();
    assert!(
        gap.iter().all(|&b| b == 0),
        "gap before high offset should be zero-filled"
    );

    // Read the actual data.
    let data = vfs
        .read(ino, high_offset, payload.len() as u32)
        .await
        .unwrap();
    assert_eq!(&data[..], payload);
}

/// Create a file in a subdirectory, unlink it, rmdir the parent.
/// Verify the directory can be removed after all children are gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_unlink_child_then_rmdir_parent() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rmdir-after-unlink");

    let dir = vfs.mkdir(1, b"parent", 0, 0, 0o755).unwrap();
    let child = vfs.create(dir.ino, b"child.txt", 0, 0, 0o644).unwrap();
    vfs.write(child.ino, 0, b"data").await.unwrap();
    vfs.flush(child.ino).await.unwrap();

    // Remove child.
    vfs.unlink(dir.ino, b"child.txt").unwrap();

    // Now rmdir should succeed.
    vfs.rmdir(1, b"parent").unwrap();

    // Parent dir should be gone.
    assert!(vfs.lookup(1, b"parent").is_err());
}

/// mkdir with duplicate name should fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_mkdir_duplicate_name_fails() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "mkdir-dup");

    vfs.mkdir(1, b"dup-dir", 0, 0, 0o755).unwrap();
    let result = vfs.mkdir(1, b"dup-dir", 0, 0, 0o755);
    assert!(result.is_err(), "creating duplicate directory should fail");
}

/// Create a file with duplicate name should fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_create_duplicate_name_fails() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "create-dup");

    vfs.create(1, b"dup.txt", 0, 0, 0o644).unwrap();
    let result = vfs.create(1, b"dup.txt", 0, 0, 0o644);
    assert!(result.is_err(), "creating duplicate file should fail");
}

/// Overwrite a partial block within an existing block (mid-block write).
/// Verify only the written region changes and the rest is preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_mid_block_overwrite_preserves_surrounding_data() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "mid-block-overwrite");

    let attr = vfs.create(1, b"mid.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 4096 bytes of 0xAA.
    let initial = vec![0xAAu8; 4096];
    vfs.write(ino, 0, &initial).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Overwrite bytes 1000..1500 with 0xBB.
    let patch = vec![0xBBu8; 500];
    vfs.write(ino, 1000, &patch).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Read entire file.
    let data = vfs.read(ino, 0, 4096).await.unwrap();
    assert_eq!(data.len(), 4096);

    // Before patch.
    assert!(
        data[..1000].iter().all(|&b| b == 0xAA),
        "bytes before patch should be 0xAA"
    );
    // Patch region.
    assert!(
        data[1000..1500].iter().all(|&b| b == 0xBB),
        "patch region should be 0xBB"
    );
    // After patch.
    assert!(
        data[1500..4096].iter().all(|&b| b == 0xAA),
        "bytes after patch should be 0xAA"
    );
}

/// Multiple xattrs set, then remove one — remaining should persist.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_remove_one_preserves_others() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr-remove-one");

    let attr = vfs.create(1, b"xattr-rm.txt", 0, 0, 0o644).unwrap();
    vfs.set_xattr(attr.ino, b"user.a", b"val-a", 0).unwrap();
    vfs.set_xattr(attr.ino, b"user.b", b"val-b", 0).unwrap();
    vfs.set_xattr(attr.ino, b"user.c", b"val-c", 0).unwrap();

    // Remove b.
    vfs.remove_xattr(attr.ino, b"user.b").unwrap();

    // a and c should survive.
    let names = vfs.list_xattrs(attr.ino).unwrap();
    assert_eq!(names.len(), 2);
    assert!(vfs.get_xattr(attr.ino, b"user.a").unwrap().is_some());
    assert!(vfs.get_xattr(attr.ino, b"user.b").unwrap().is_none());
    assert!(vfs.get_xattr(attr.ino, b"user.c").unwrap().is_some());
}

/// Rename a directory with children — children should follow.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rename_directory_with_children() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename-dir-tree");

    let dir = vfs.mkdir(1, b"old-name", 0, 0, 0o755).unwrap();
    let child = vfs.create(dir.ino, b"child.txt", 0, 0, 0o644).unwrap();
    vfs.write(child.ino, 0, b"child data").await.unwrap();
    vfs.flush(child.ino).await.unwrap();

    // Rename the directory.
    vfs.rename(1, b"old-name", 1, b"new-name").unwrap();

    // Old name should be gone.
    assert!(vfs.lookup(1, b"old-name").is_err());

    // New name should resolve to the same ino.
    let renamed = vfs.lookup(1, b"new-name").unwrap();
    assert_eq!(renamed.ino, dir.ino);

    // Child should still be accessible through the renamed dir.
    let found_child = vfs.lookup(renamed.ino, b"child.txt").unwrap();
    assert_eq!(found_child.ino, child.ino);
    let data = vfs.read(child.ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"child data");
}

#[tokio::test]
async fn acl_no_access_hides_directory_from_lookup_and_readdir() {
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "acl-hide");

    let secret_dir = vfs.mkdir(1, b"secret", 0, 0, 0o755).unwrap();
    let secret_file = vfs
        .create(secret_dir.ino, b"flag.txt", 0, 0, 0o644)
        .unwrap();
    vfs.write(secret_file.ino, 0, b"top-secret").await.unwrap();
    vfs.flush(secret_file.ino).await.unwrap();

    vfs.set_acl_rule(secret_dir.ino, "*", AccessLevel::NoAccess)
        .unwrap();

    let root_entries = vfs.readdir(1).unwrap();
    assert!(!root_entries.iter().any(|e| e.name == b"secret"));
    assert!(vfs.lookup(1, b"secret").is_err());
}

#[tokio::test]
async fn acl_read_only_denies_writes_but_allows_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "acl-ro");

    let file = vfs.create(1, b"ro.txt", 0, 0, 0o644).unwrap();
    vfs.write(file.ino, 0, b"hello").await.unwrap();
    vfs.flush(file.ino).await.unwrap();

    vfs.set_acl_rule(file.ino, "*", AccessLevel::ReadOnly)
        .unwrap();

    let data = vfs.read(file.ino, 0, 5).await.unwrap();
    assert_eq!(&data[..], b"hello");

    let write_err = vfs.write(file.ino, 0, b"X").await.unwrap_err();
    assert!(matches!(
        write_err,
        VfsError::Meta(studiofs_meta::MetaError::AccessDenied { .. })
    ));
}

#[tokio::test]
async fn acl_wildcard_applies_as_fallback_for_specific_principal() {
    let _restore_principal = ScopedEnvVar::capture("STUDIOFS_PRINCIPAL");
    unsafe {
        std::env::set_var("STUDIOFS_PRINCIPAL", "alice");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "acl-wildcard-fallback");

    let secret_dir = vfs.mkdir(1, b"secret", 0, 0, 0o755).unwrap();
    vfs.set_acl_rule(secret_dir.ino, "*", AccessLevel::NoAccess)
        .unwrap();

    let root_entries = vfs.readdir(1).unwrap();
    assert!(!root_entries.iter().any(|e| e.name == b"secret"));
}

// =============================================================================
// SFS-123: completion-aware local read semantics during background upload
// =============================================================================

/// Verify that local reads return correct data through the full upload pipeline
/// when the last block is a partial (non–block-size-aligned) tail.
///
/// Covers the transient window in which a block is present in `live_extent_records`
/// while upload completion and local block-backend cleanup are interleaving.
#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn local_reads_through_upload_with_partial_tail_block() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_bucket = ScopedEnvVar::capture("STUDIOFS_S3_BUCKET");
    let _restore_access_key = ScopedEnvVar::capture("STUDIOFS_S3_ACCESS_KEY");
    let _restore_secret_key = ScopedEnvVar::capture("STUDIOFS_S3_SECRET_KEY");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "50");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("sfs123-partial-tail-{}", std::process::id());
    let mut format_info = make_format_info(&volume_suffix);
    format_info.block_size = 512 * 1024; // 512 KiB blocks
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    // 4 full blocks + a partial tail (block_size / 2 bytes).
    let block_size = format_info.block_size as usize;
    let full_size = block_size * 4 + block_size / 2;
    let payload: Vec<u8> = (0..full_size)
        .map(|i| (i as u8).wrapping_mul(3) ^ 0xA7)
        .collect();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            test_vfs_config_from_env(),
        )
        .unwrap();
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"sfs123-partial-tail.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until_with_interval(Duration::from_secs(10), Duration::from_millis(5), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
            })
            .await,
            "test setup: queued uploads must appear for the new file"
        );

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match vfs.read(attr.ino, 0, full_size as u32).await {
                Ok(bytes) => {
                    assert_eq!(
                        bytes.len(),
                        full_size,
                        "read returned wrong byte count (including partial tail)"
                    );
                    assert_eq!(
                        bytes.as_ref(),
                        payload.as_slice(),
                        "read data mismatch — partial tail corrupted during upload pipeline"
                    );
                }
                Err(VfsError::RemoteContentPending) => {
                    panic!("local read with partial tail must not surface RemoteContentPending");
                }
                Err(err) => {
                    panic!("local read failed during upload finalization: {err}");
                }
            }

            let current_attr = vfs.getattr(attr.ino).unwrap();
            if vfs.pending_uploads_for_inode(attr.ino).is_empty()
                && current_attr.upload_state == UploadState::Ready
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "upload pipeline did not stabilize before timeout"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    rt.shutdown_background();
}

/// Verify that a concurrent reader observes correct data while the upload
/// pipeline finalizes a multi-block file that includes a partial tail block.
///
/// A background reader task continuously polls the full file while the writer
/// waits for epoch finalization.  Neither task must ever see `RemoteContentPending`
/// or incorrect data.
///
/// This is part of the SFS-121 / SFS-123 Local-First coverage suite.
#[test]
#[ignore = "requires live S3 backend; run with --ignored and valid .env"]
fn concurrent_reads_through_upload_finalization() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_bucket = ScopedEnvVar::capture("STUDIOFS_S3_BUCKET");
    let _restore_access_key = ScopedEnvVar::capture("STUDIOFS_S3_ACCESS_KEY");
    let _restore_secret_key = ScopedEnvVar::capture("STUDIOFS_S3_SECRET_KEY");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");
    apply_test_online_s3_env();
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "64MB");
        std::env::set_var("STUDIOFS_UPLOAD_DELAY_MS", "30");
        std::env::set_var("STUDIOFS_UPLOAD_WORKER_COUNT", "1");
    }

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());
    boot_rt.shutdown_background();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();
    let volume_suffix = format!("sfs123-concurrent-{}", std::process::id());
    let mut format_info = make_format_info(&volume_suffix);
    format_info.block_size = 512 * 1024; // 512 KiB blocks
    let data_config = DataStoreConfig::from(&format_info);
    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&format_info).unwrap();

    // 3 full blocks + partial tail.
    let block_size = format_info.block_size as usize;
    let full_size = block_size * 3 + block_size / 3;
    let payload: Vec<u8> = (0..full_size)
        .map(|i| (i as u8).wrapping_add(0x5B))
        .collect();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    rt.block_on(async {
        let vfs = Arc::new(
            StudioVfs::new(
                meta.clone(),
                &data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
                test_vfs_config_from_env(),
            )
            .unwrap(),
        );
        vfs.start()
            .expect("failed to start StudioVfs background workers");

        let attr = vfs
            .create(1, b"sfs123-concurrent.bin", 0, 0, 0o644)
            .unwrap();
        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        assert!(
            wait_until_with_interval(Duration::from_secs(10), Duration::from_millis(5), || {
                !vfs.pending_uploads_for_inode(attr.ino).is_empty()
            })
            .await,
            "test setup: queued uploads must appear for the new file"
        );

        // Spawn a concurrent reader that polls the full file until signalled to stop.
        //
        // The reader uses an adaptive sleep: normal reads sleep 5 ms, but once
        // `pending_uploads_for_inode` starts shrinking (uploads are completing) it
        // switches to yield-only polling to maximize the chance of overlapping the
        // finalization window while the background upload pipeline is still settling.
        let vfs_reader = vfs.clone();
        let payload_reader = payload.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_reader = stop.clone();
        let initial_upload_count = vfs.pending_uploads_for_inode(attr.ino).len();
        let initial_upload_count_shared = Arc::new(initial_upload_count);
        let initial_uploads_for_reader = initial_upload_count_shared.clone();
        let reader_handle = tokio::spawn(async move {
            while !stop_reader.load(std::sync::atomic::Ordering::Relaxed) {
                match vfs_reader.read(attr.ino, 0, full_size as u32).await {
                    Ok(bytes) => {
                        assert_eq!(
                            bytes.len(),
                            full_size,
                            "concurrent reader: wrong byte count"
                        );
                        assert_eq!(
                            bytes.as_ref(),
                            payload_reader.as_slice(),
                            "concurrent reader: data mismatch during upload finalization"
                        );
                    }
                    Err(VfsError::RemoteContentPending) => {
                        panic!(
                            "concurrent local reader must not see RemoteContentPending during upload"
                        );
                    }
                    Err(err) => {
                        panic!("concurrent reader failed: {err}");
                    }
                }
                // Switch to aggressive (yield-only) polling once uploads start completing
                // so the reader is likely to interleave with upload finalization.
                let current_pending = vfs_reader.pending_uploads_for_inode(attr.ino).len();
                if current_pending < *initial_uploads_for_reader && current_pending > 0 {
                    tokio::task::yield_now().await;
                } else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });

        // Wait for epoch finalization on the main task.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let current_attr = vfs.getattr(attr.ino).unwrap();
            if vfs.pending_uploads_for_inode(attr.ino).is_empty()
                && current_attr.upload_state == UploadState::Ready
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "upload pipeline did not stabilize before timeout"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Signal the concurrent reader to stop and wait for it.
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        reader_handle.await.expect("concurrent reader task panicked");
    });
    rt.shutdown_background();
}
