//! File-per-object payload storage for the current local payload runtime.
//!
//! The runtime persists payloads directly under the UUID-scoped cache root.

use std::array;
use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use tracing::{debug, warn};
use uuid::Uuid;

use super::layout::{
    LOCAL_PAYLOAD_BATCH_INTENT_DIR, LOCAL_PAYLOAD_BATCH_INTENT_FILE_EXTENSION, LocalPayloadLayout,
};
use super::platform;
use super::{LocalPayloadReadMode, LocalPayloadSyncMode, LocalPayloadWriteMode};

const TEMP_FILE_CREATE_ATTEMPTS: usize = 32;
const TEMP_FILE_SUFFIX: &str = ".tmp";
const READ_FD_CACHE_MAX_CAPACITY: usize = 2_048;
#[cfg(any(test, not(windows)))]
const READ_FD_CACHE_MIN_CAPACITY: usize = READ_FD_CACHE_SHARDS;
const READ_FD_CACHE_FAMILIES: usize = 2;
#[cfg(any(test, not(windows)))]
const READ_FD_CACHE_RESERVED_MIN: usize = 256;
const READ_FD_CACHE_SHARDS: usize = 16;
const READ_HINT_MIN_LEN: usize = 512 * 1024;
const FAST_BATCH_STAGING_MAX_CONCURRENCY: usize = 4;

static TEMP_FILE_SEQ: AtomicU64 = AtomicU64::new(0);
static BATCH_INTENT_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
/// A single payload entry for a batched [`FilePayloadStore::put_batch`] operation.
pub struct PayloadBatchEntry<'a> {
    /// Object id for the payload.
    pub object_id: u64,
    /// Payload bytes to persist.
    pub data: &'a [u8],
}

#[derive(Debug)]
#[must_use = "batch payload commit intents must be completed after the index commit succeeds"]
/// A handle that completes the payload-side intent for a batched write.
pub struct PayloadBatchCommit {
    intent_path: PathBuf,
}

impl PayloadBatchCommit {
    /// Completes the payload-side intent by removing the staged intent marker.
    pub fn complete(self) {
        remove_batch_payload_intent(&self.intent_path);
    }
}

#[derive(Debug, thiserror::Error)]
/// Errors returned by [`FilePayloadStore`] operations.
pub enum FilePayloadStoreError {
    /// An underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// Exhausted attempts to create a unique temp file for the payload write.
    #[error("failed to create a unique temp payload file for object_id={object_id} in '{dir}'")]
    TempFileCreateExhausted {
        /// The object id for which temp-file creation failed.
        object_id: u64,
        /// The directory where temp-file creation attempts were performed.
        dir: PathBuf,
    },
}

/// File-per-object payload store rooted under the current local payload runtime
/// layout.
#[derive(Debug, Clone)]
pub struct FilePayloadStore {
    layout: LocalPayloadLayout,
    sync_mode: LocalPayloadSyncMode,
    write_mode: LocalPayloadWriteMode,
    read_mode: LocalPayloadReadMode,
    shared: Arc<PayloadStoreSharedState>,
}

#[derive(Debug)]
struct PayloadStoreSharedState {
    read_handles: ReadHandleCache,
    buffered_read_fallback_handles: ReadHandleCache,
    #[cfg(windows)]
    unbuffered_read_scratch: Mutex<platform::UnbufferedReadBuffer>,
    pending_grouped_sync: PendingGroupedSync,
    #[cfg(test)]
    write_counters: PayloadWriteCounters,
    #[cfg(test)]
    read_counters: PayloadReadCounters,
    #[cfg(test)]
    fail_directory_sync: Mutex<Option<PathBuf>>,
    barrier_counters: SyncBarrierCounters,
}

impl FilePayloadStore {
    /// Opens a payload store rooted at `layout` using durable sync mode and buffered writes.
    pub fn open(layout: LocalPayloadLayout) -> Result<Self, FilePayloadStoreError> {
        Self::open_with_sync_mode(layout, LocalPayloadSyncMode::Durable)
    }

    /// Opens a payload store rooted at `layout` using the provided sync mode and buffered writes.
    pub fn open_with_sync_mode(
        layout: LocalPayloadLayout,
        sync_mode: LocalPayloadSyncMode,
    ) -> Result<Self, FilePayloadStoreError> {
        Self::open_with_sync_and_write_mode(layout, sync_mode, LocalPayloadWriteMode::Buffered)
    }

    /// Opens a payload store rooted at `layout` using the provided sync and write modes.
    pub fn open_with_sync_and_write_mode(
        layout: LocalPayloadLayout,
        sync_mode: LocalPayloadSyncMode,
        write_mode: LocalPayloadWriteMode,
    ) -> Result<Self, FilePayloadStoreError> {
        Self::open_with_modes(
            layout,
            sync_mode,
            write_mode,
            LocalPayloadReadMode::default_for_mount_path(),
        )
    }

    fn open_with_modes(
        layout: LocalPayloadLayout,
        sync_mode: LocalPayloadSyncMode,
        write_mode: LocalPayloadWriteMode,
        read_mode: LocalPayloadReadMode,
    ) -> Result<Self, FilePayloadStoreError> {
        std::fs::create_dir_all(&layout.payload_objects_dir)?;
        std::fs::create_dir_all(&layout.tmp_dir)?;
        std::fs::create_dir_all(layout.tmp_dir.join(LOCAL_PAYLOAD_BATCH_INTENT_DIR))?;
        if matches!(sync_mode, LocalPayloadSyncMode::Durable) {
            // Persist the intent directory before any payload can be committed.
            platform::sync_directory(&layout.tmp_dir)?;
        }
        let read_fd_family_capacity = read_fd_cache_capacity();
        let total_read_fd_capacity = read_fd_family_capacity * READ_FD_CACHE_FAMILIES;
        debug!(
            total_read_fd_capacity,
            read_fd_family_capacity,
            "local payload runtime read descriptor cache capacity selected"
        );
        Ok(Self {
            layout,
            sync_mode,
            write_mode,
            read_mode,
            shared: Arc::new(PayloadStoreSharedState {
                read_handles: ReadHandleCache::new(read_fd_family_capacity),
                buffered_read_fallback_handles: ReadHandleCache::new(read_fd_family_capacity),
                #[cfg(windows)]
                unbuffered_read_scratch: Mutex::new(platform::UnbufferedReadBuffer::new()),
                pending_grouped_sync: PendingGroupedSync::default(),
                #[cfg(test)]
                write_counters: PayloadWriteCounters::default(),
                #[cfg(test)]
                read_counters: PayloadReadCounters::default(),
                #[cfg(test)]
                fail_directory_sync: Mutex::new(None),
                barrier_counters: SyncBarrierCounters::default(),
            }),
        })
    }

    /// Loads the full payload for `object_id` into memory.
    ///
    /// Returns `None` if the payload is missing or if an error occurs while reading it.
    pub fn get(&self, object_id: u64) -> Option<Bytes> {
        let path = self.layout.payload_object_path(object_id);
        if self.read_mode == LocalPayloadReadMode::Unbuffered {
            let file = match platform::open_read_only(&path) {
                Ok(file) => file,
                Err(error) => {
                    warn!(
                        object_id,
                        path = %path.display(),
                        error = ?error,
                        "local payload runtime payload get failed to open buffered handle"
                    );
                    return None;
                }
            };
            return self.get_with_file(object_id, &path, &file);
        }
        let file = self.cached_read_handle(object_id, &path).ok()?;
        self.get_with_file(object_id, &path, file.as_ref())
    }

    fn get_with_file(&self, object_id: u64, path: &Path, file: &File) -> Option<Bytes> {
        let metadata = match file.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                warn!(
                    object_id,
                    path = %path.display(),
                    error = ?error,
                    "local payload runtime payload get metadata failed"
                );
                return None;
            }
        };
        let len = match usize::try_from(metadata.len()) {
            Ok(len) => len,
            Err(_) => {
                warn!(
                    object_id,
                    path = %path.display(),
                    payload_len = metadata.len(),
                    "local payload runtime payload get length exceeds addressable usize"
                );
                return None;
            }
        };
        let mut payload = vec![0_u8; len];
        if len > 0
            && let Err(error) = platform::pread_exact(file, &mut payload, 0)
        {
            warn!(
                object_id,
                path = %path.display(),
                error = ?error,
                "local payload runtime payload get failed"
            );
            return None;
        }
        Some(Bytes::from(payload))
    }

    /// Reads up to `out.len()` bytes from the payload for `object_id` starting at `data_offset`.
    ///
    /// Returns `None` if the payload is missing or if an error occurs while reading it.
    pub fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
        if out.is_empty() {
            return Some(0);
        }
        let start = u64::try_from(data_offset).ok()?;

        let path = self.layout.payload_object_path(object_id);
        if self.read_mode == LocalPayloadReadMode::Unbuffered {
            let alignment = platform::unbuffered_read_alignment_for_path(&path);
            if !platform::can_read_unbuffered_range(start, out.len(), alignment) {
                return self.read_into_buffered_fallback(object_id, data_offset, &path, start, out);
            }

            let file = self.cached_read_handle(object_id, &path).ok()?;
            match self.try_read_into_unbuffered(file.as_ref(), alignment, start, out) {
                Ok(Some(read)) => return Some(read),
                Ok(None) => {
                    return self.read_into_buffered_fallback(
                        object_id,
                        data_offset,
                        &path,
                        start,
                        out,
                    );
                }
                Err(error) => {
                    warn!(
                        object_id,
                        data_offset,
                        path = %path.display(),
                        error = ?error,
                        "local payload runtime payload unbuffered read_into failed"
                    );
                    return None;
                }
            }
        }
        let file = self.cached_read_handle(object_id, &path).ok()?;
        if out.len() >= READ_HINT_MIN_LEN {
            let hint_len = u64::try_from(out.len()).unwrap_or(u64::MAX);
            if let Err(error) = platform::advise_will_need(file.as_ref(), start, hint_len) {
                warn!(
                    object_id,
                    data_offset,
                    path = %path.display(),
                    error = ?error,
                    "local payload runtime payload read_into advise_will_need failed"
                );
            }
        }

        match platform::pread(file.as_ref(), out, start) {
            Ok(read) => Some(read),
            Err(error) => {
                warn!(
                    object_id,
                    data_offset,
                    path = %path.display(),
                    error = ?error,
                    "local payload runtime payload read_into failed"
                );
                None
            }
        }
    }

    /// Hints that the payload for `object_id` will be read soon.
    ///
    /// On supported platforms, this may warm OS caches to reduce subsequent read latency.
    /// Windows advice is a no-op and does not open or read the payload.
    pub fn advise_will_need(&self, object_id: u64) {
        if cfg!(windows) || self.read_mode == LocalPayloadReadMode::Unbuffered {
            return;
        }
        let path = self.layout.payload_object_path(object_id);
        let Ok(file) = self.cached_read_handle(object_id, &path) else {
            return;
        };
        let Ok(metadata) = file.metadata() else {
            return;
        };
        let len = metadata.len();
        if len == 0 {
            return;
        }
        if let Err(error) = platform::advise_will_need(file.as_ref(), 0, len) {
            warn!(
                object_id,
                path = %path.display(),
                error = ?error,
                "local payload runtime payload advise_will_need failed"
            );
        }
    }

    /// Returns `true` if a payload exists for `object_id`.
    pub fn contains(&self, object_id: u64) -> bool {
        match self.payload_exists(object_id) {
            Ok(exists) => exists,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "local payload runtime payload contains check failed"
                );
                false
            }
        }
    }

    /// Checks whether a payload exists for `object_id`.
    pub fn payload_exists(&self, object_id: u64) -> io::Result<bool> {
        self.layout.payload_object_path(object_id).try_exists()
    }

    /// Persists `data` as the payload for `object_id`.
    pub fn put(&self, object_id: u64, data: &[u8]) -> Result<(), FilePayloadStoreError> {
        self.put_with_intent(object_id, data)
            .map(PayloadBatchCommit::complete)
    }

    /// Keeps the payload intent until the caller commits the corresponding index row.
    pub(crate) fn put_with_intent(
        &self,
        object_id: u64,
        data: &[u8],
    ) -> Result<PayloadBatchCommit, FilePayloadStoreError> {
        let durable_sync = matches!(self.sync_mode, LocalPayloadSyncMode::Durable);
        let payload_path = self.layout.payload_object_path(object_id);
        let payload_dir = payload_path.parent().ok_or_else(|| {
            io::Error::other(format!(
                "payload path has no parent directory: {}",
                payload_path.display()
            ))
        })?;
        std::fs::create_dir_all(payload_dir)?;

        let temp_path =
            self.write_temp_payload_file(&self.layout.tmp_dir, object_id, data, durable_sync)?;
        let staged = StagedBatchPayload {
            entry: PayloadBatchEntry { object_id, data },
            payload_path: payload_path.clone(),
            payload_dir: payload_dir.to_path_buf(),
            temp_path,
        };
        let intent_path =
            self.write_batch_payload_intent(std::slice::from_ref(&staged), durable_sync)?;
        self.commit_staged_batch_payload(&staged, durable_sync)?;
        self.invalidate_cached_read_handle(object_id);
        if durable_sync {
            self.sync_directory_put_barrier(&self.layout.tmp_dir)?;
            self.sync_directory_put_barrier(payload_dir)?;
        } else {
            self.record_grouped_sync_targets(object_id, payload_dir);
        }
        Ok(PayloadBatchCommit { intent_path })
    }

    /// Persists a batch of payloads and returns a commit handle for the payload-side intent.
    ///
    /// Call [`PayloadBatchCommit::complete`] after the corresponding index/metadata commit succeeds.
    pub fn put_batch(
        &self,
        entries: &[PayloadBatchEntry<'_>],
    ) -> Result<PayloadBatchCommit, FilePayloadStoreError> {
        if entries.is_empty() {
            return Ok(PayloadBatchCommit {
                intent_path: PathBuf::new(),
            });
        }

        let durable_sync = matches!(self.sync_mode, LocalPayloadSyncMode::Durable);
        let mut prepared_dirs = HashSet::new();
        let mut prepared = Vec::with_capacity(entries.len());
        for entry in entries {
            let payload_path = self.layout.payload_object_path(entry.object_id);
            let payload_dir = payload_path
                .parent()
                .ok_or_else(|| {
                    io::Error::other(format!(
                        "payload path has no parent directory: {}",
                        payload_path.display()
                    ))
                })?
                .to_path_buf();
            if prepared_dirs.insert(payload_dir.clone()) {
                std::fs::create_dir_all(&payload_dir)?;
            }
            prepared.push(PreparedBatchPayload {
                entry: *entry,
                payload_path,
                payload_dir,
            });
        }
        let staged = self.stage_batch_payloads(&prepared, durable_sync)?;

        let mut synced_dirs = HashSet::new();
        let intent = self.write_batch_payload_intent(&staged, durable_sync)?;
        for idx in 0..staged.len() {
            let staged_entry = &staged[idx];
            if let Err(error) = self.commit_staged_batch_payload(staged_entry, durable_sync) {
                self.cleanup_staged_batch_payloads(&staged[idx + 1..]);
                // The intent lets startup preserve indexed files and remove orphan outputs.
                return Err(error);
            }
            self.invalidate_cached_read_handle(staged_entry.entry.object_id);
            synced_dirs.insert(staged_entry.payload_dir.clone());
        }

        if durable_sync {
            self.sync_directory_put_barrier(&self.layout.tmp_dir)?;
            for dir in synced_dirs {
                self.sync_directory_put_barrier(&dir)?;
            }
        } else {
            for staged_entry in &staged {
                self.record_grouped_sync_targets(
                    staged_entry.entry.object_id,
                    &staged_entry.payload_dir,
                );
            }
        }
        Ok(PayloadBatchCommit {
            intent_path: intent,
        })
    }

    fn stage_batch_payloads<'a>(
        &self,
        prepared: &[PreparedBatchPayload<'a>],
        durable_sync: bool,
    ) -> Result<Vec<StagedBatchPayload<'a>>, FilePayloadStoreError> {
        let concurrency = batch_staging_concurrency(prepared.len(), durable_sync);
        if concurrency <= 1 {
            return prepared
                .iter()
                .map(|entry| self.stage_one_batch_payload(entry, durable_sync))
                .collect();
        }

        let mut staged = Vec::with_capacity(prepared.len());
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..concurrency)
                .map(|worker_idx| {
                    scope.spawn(move || {
                        #[cfg(test)]
                        self.shared
                            .write_counters
                            .staging_worker_starts
                            .fetch_add(1, Ordering::Relaxed);
                        let mut unbuffered_scratch = platform::UnbufferedWriteBuffer::new();
                        let mut worker_staged = Vec::new();
                        for idx in (worker_idx..prepared.len()).step_by(concurrency) {
                            worker_staged.push((
                                idx,
                                self.stage_one_batch_payload_with_unbuffered_buffer(
                                    &prepared[idx],
                                    durable_sync,
                                    &mut unbuffered_scratch,
                                )?,
                            ));
                        }
                        Ok::<_, FilePayloadStoreError>(worker_staged)
                    })
                })
                .collect();
            let mut first_error = None;
            for handle in handles {
                match handle.join() {
                    Ok(Ok(mut worker_staged)) => staged.append(&mut worker_staged),
                    Ok(Err(error)) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    Err(_) => {
                        if first_error.is_none() {
                            first_error = Some(FilePayloadStoreError::Io(io::Error::other(
                                "payload batch staging worker panicked",
                            )));
                        }
                    }
                }
            }
            match first_error {
                Some(error) => Err(error),
                None => Ok(()),
            }
        })?;
        staged.sort_by_key(|(idx, _)| *idx);
        Ok(staged
            .into_iter()
            .map(|(_, staged_entry)| staged_entry)
            .collect())
    }

    fn stage_one_batch_payload<'a>(
        &self,
        entry: &PreparedBatchPayload<'a>,
        durable_sync: bool,
    ) -> Result<StagedBatchPayload<'a>, FilePayloadStoreError> {
        let mut unbuffered_scratch = platform::UnbufferedWriteBuffer::new();
        self.stage_one_batch_payload_with_unbuffered_buffer(
            entry,
            durable_sync,
            &mut unbuffered_scratch,
        )
    }

    fn stage_one_batch_payload_with_unbuffered_buffer<'a>(
        &self,
        entry: &PreparedBatchPayload<'a>,
        durable_sync: bool,
        unbuffered_scratch: &mut platform::UnbufferedWriteBuffer,
    ) -> Result<StagedBatchPayload<'a>, FilePayloadStoreError> {
        let temp_dir = if durable_sync {
            self.layout.tmp_dir.as_path()
        } else {
            entry.payload_dir.as_path()
        };
        let temp_path = self.write_temp_payload_file_with_unbuffered_buffer(
            temp_dir,
            entry.entry.object_id,
            entry.entry.data,
            durable_sync,
            unbuffered_scratch,
        )?;
        Ok(StagedBatchPayload {
            entry: entry.entry,
            payload_path: entry.payload_path.clone(),
            payload_dir: entry.payload_dir.clone(),
            temp_path,
        })
    }

    /// Removes the payload for `object_id`, if present.
    pub fn remove(&self, object_id: u64) {
        self.invalidate_cached_read_handle(object_id);
        self.shared
            .pending_grouped_sync
            .object_ids
            .lock()
            .remove(&object_id);
        let payload_path = self.layout.payload_object_path(object_id);
        if let Err(error) = std::fs::remove_file(&payload_path)
            && error.kind() != io::ErrorKind::NotFound
        {
            warn!(
                object_id,
                path = %payload_path.display(),
                error = ?error,
                "local payload runtime payload remove failed"
            );
        }
    }

    /// Removes payloads for all `object_ids`.
    pub fn remove_batch(&self, object_ids: &[u64]) {
        for object_id in object_ids {
            self.remove(*object_id);
        }
    }

    /// Removes failed puts' payloads before retiring their shared recovery intent.
    pub(crate) fn rollback_batch(
        &self,
        object_ids: &[u64],
        commit: PayloadBatchCommit,
    ) -> Result<(), FilePayloadStoreError> {
        self.remove_batch(object_ids);
        let mut synced_dirs = HashSet::new();
        let mut first_error: Option<FilePayloadStoreError> = None;
        for &object_id in object_ids {
            match self.payload_exists(object_id) {
                Ok(true) => {
                    first_error.get_or_insert_with(|| {
                        io::Error::other(format!("payload {object_id} remains after rollback"))
                            .into()
                    });
                }
                Err(error) => {
                    first_error.get_or_insert(error.into());
                }
                Ok(false) => {}
            }
            if !matches!(self.sync_mode, LocalPayloadSyncMode::Durable) {
                continue;
            }
            let payload_path = self.layout.payload_object_path(object_id);
            let Some(payload_dir) = payload_path.parent() else {
                first_error.get_or_insert_with(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "payload path has no parent").into()
                });
                continue;
            };
            // Persist all attempted deletions even when another removal fails.
            if synced_dirs.insert(payload_dir.to_path_buf())
                && let Err(error) = self.sync_directory_put_barrier(payload_dir)
            {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        commit.complete();
        Ok(())
    }

    /// Flushes and syncs pending payload writes according to the configured sync mode.
    pub fn sync_data(&self) -> Result<(), FilePayloadStoreError> {
        if matches!(self.sync_mode, LocalPayloadSyncMode::Fast) {
            // ponytail: Fast mode is throughput-first; use Durable mode when
            // crash-safe payload fsync/dirsync is required.
            self.clear_grouped_sync_targets();
            return Ok(());
        }

        for dir in [
            self.layout.payload_objects_dir.as_path(),
            self.layout.payload_root.as_path(),
            self.layout.runtime_root.as_path(),
        ] {
            self.sync_directory_grouped_barrier(dir)?;
        }
        Ok(())
    }

    fn write_temp_payload_file(
        &self,
        temp_dir: &Path,
        object_id: u64,
        data: &[u8],
        sync_file_data: bool,
    ) -> Result<PathBuf, FilePayloadStoreError> {
        let mut unbuffered_scratch = platform::UnbufferedWriteBuffer::new();
        self.write_temp_payload_file_with_unbuffered_buffer(
            temp_dir,
            object_id,
            data,
            sync_file_data,
            &mut unbuffered_scratch,
        )
    }

    fn write_temp_payload_file_with_unbuffered_buffer(
        &self,
        temp_dir: &Path,
        object_id: u64,
        data: &[u8],
        sync_file_data: bool,
        unbuffered_scratch: &mut platform::UnbufferedWriteBuffer,
    ) -> Result<PathBuf, FilePayloadStoreError> {
        #[cfg(test)]
        let _staging_probe = self.enter_test_staging_probe();

        if self.write_mode == LocalPayloadWriteMode::UnbufferedFast {
            if let Some(temp_path) = self.try_write_temp_payload_file_unbuffered(
                temp_dir,
                object_id,
                data,
                sync_file_data,
                unbuffered_scratch,
            )? {
                return Ok(temp_path);
            }
            #[cfg(test)]
            self.shared
                .write_counters
                .unbuffered_write_fallbacks
                .fetch_add(1, Ordering::Relaxed);
        }

        let (mut temp_file, temp_path) = self.create_temp_file(temp_dir, object_id)?;
        platform::write_all(&mut temp_file, data)?;
        if sync_file_data {
            platform::sync_data(&temp_file)?;
            self.shared
                .barrier_counters
                .per_put_file_sync_calls
                .fetch_add(1, Ordering::Relaxed);
        }
        drop(temp_file);
        Ok(temp_path)
    }

    fn try_write_temp_payload_file_unbuffered(
        &self,
        temp_dir: &Path,
        object_id: u64,
        data: &[u8],
        sync_file_data: bool,
        unbuffered_scratch: &mut platform::UnbufferedWriteBuffer,
    ) -> Result<Option<PathBuf>, FilePayloadStoreError> {
        let unbuffered_alignment = platform::unbuffered_write_alignment_for_path(temp_dir);
        if !platform::can_write_unbuffered_payload(data, unbuffered_alignment) {
            return Ok(None);
        }

        #[cfg(test)]
        self.shared
            .write_counters
            .unbuffered_write_attempts
            .fetch_add(1, Ordering::Relaxed);

        let (mut temp_file, temp_path) = match self.create_temp_file_with_opener(
            temp_dir,
            object_id,
            platform::create_new_unbuffered_write,
        ) {
            Ok(opened) => opened,
            Err(FilePayloadStoreError::Io(error))
                if platform::is_unbuffered_io_unsupported_error(&error) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };

        if let Err(error) = platform::write_all_unbuffered_with_buffer(
            &mut temp_file,
            data,
            unbuffered_scratch,
            unbuffered_alignment,
        ) {
            drop(temp_file);
            let _ = std::fs::remove_file(&temp_path);
            if platform::is_unbuffered_io_unsupported_error(&error) {
                return Ok(None);
            }
            return Err(FilePayloadStoreError::Io(error));
        }

        if sync_file_data {
            if let Err(error) = platform::sync_data(&temp_file) {
                drop(temp_file);
                let _ = std::fs::remove_file(&temp_path);
                return Err(FilePayloadStoreError::Io(error));
            }
            self.shared
                .barrier_counters
                .per_put_file_sync_calls
                .fetch_add(1, Ordering::Relaxed);
        }
        drop(temp_file);
        #[cfg(test)]
        self.shared
            .write_counters
            .unbuffered_write_successes
            .fetch_add(1, Ordering::Relaxed);
        Ok(Some(temp_path))
    }

    fn commit_staged_batch_payload(
        &self,
        staged: &StagedBatchPayload<'_>,
        durable_sync: bool,
    ) -> Result<(), FilePayloadStoreError> {
        match platform::rename_atomic(&staged.temp_path, &staged.payload_path) {
            Ok(()) => Ok(()),
            Err(error) if platform::is_cross_device_link_error(&error) => {
                let _ = std::fs::remove_file(&staged.temp_path);
                let payload_local_temp = self.write_temp_payload_file(
                    &staged.payload_dir,
                    staged.entry.object_id,
                    staged.entry.data,
                    durable_sync,
                )?;
                if let Err(rename_error) =
                    platform::rename_atomic(&payload_local_temp, &staged.payload_path)
                {
                    let _ = std::fs::remove_file(&payload_local_temp);
                    return Err(FilePayloadStoreError::Io(rename_error));
                }
                Ok(())
            }
            Err(error) => {
                let _ = std::fs::remove_file(&staged.temp_path);
                Err(FilePayloadStoreError::Io(error))
            }
        }
    }

    fn cleanup_staged_batch_payloads(&self, staged: &[StagedBatchPayload<'_>]) {
        for entry in staged {
            let _ = std::fs::remove_file(&entry.temp_path);
        }
    }

    fn write_batch_payload_intent(
        &self,
        staged: &[StagedBatchPayload<'_>],
        durable_sync: bool,
    ) -> Result<PathBuf, FilePayloadStoreError> {
        let intent_dir = self.layout.tmp_dir.join(LOCAL_PAYLOAD_BATCH_INTENT_DIR);
        let path = intent_dir.join(format!(
            "batch-{}.{}",
            next_batch_intent_token(),
            LOCAL_PAYLOAD_BATCH_INTENT_FILE_EXTENSION
        ));
        if let Err(error) = self.write_batch_payload_intent_file(&path, staged, durable_sync) {
            self.cleanup_staged_batch_payloads(staged);
            return Err(error);
        }
        Ok(path)
    }

    fn write_batch_payload_intent_file(
        &self,
        path: &Path,
        staged: &[StagedBatchPayload<'_>],
        durable_sync: bool,
    ) -> Result<(), FilePayloadStoreError> {
        let mut content = String::new();
        // Retain the v1 phase header for compatibility; recovery uses index presence.
        content.push_str("version=1\nphase=committing\n");
        for entry in staged {
            content.push_str("object=");
            content.push_str(&entry.entry.object_id.to_string());
            content.push_str("|temp_hex=");
            content.push_str(&encode_batch_intent_path_hex(
                &self.intent_relative_path(&entry.temp_path)?,
            ));
            content.push_str("|final_hex=");
            content.push_str(&encode_batch_intent_path_hex(
                &self.intent_relative_path(&entry.payload_path)?,
            ));
            content.push('\n');
        }

        let parent = path.parent().ok_or_else(|| {
            io::Error::other(format!(
                "batch intent path has no parent directory: {}",
                path.display()
            ))
        })?;
        let temp_path = parent.join(format!("{}.tmp", next_batch_intent_token()));
        let result = (|| {
            let mut temp_file = File::create(&temp_path)?;
            platform::write_all(&mut temp_file, content.as_bytes())?;
            if durable_sync {
                platform::sync_data(&temp_file)?;
                self.shared
                    .barrier_counters
                    .per_put_file_sync_calls
                    .fetch_add(1, Ordering::Relaxed);
            }
            drop(temp_file);
            platform::rename_atomic(&temp_path, path)?;
            if durable_sync && let Err(error) = self.sync_directory_put_barrier(parent) {
                // Payload rename has not started, so this marker owns no final output.
                let _ = std::fs::remove_file(path);
                return Err(error);
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(temp_path);
        }
        result
    }

    fn intent_relative_path(&self, path: &Path) -> Result<String, FilePayloadStoreError> {
        let relative = path.strip_prefix(&self.layout.runtime_root).map_err(|_| {
            io::Error::other(format!(
                "batch intent path is outside runtime root: {}",
                path.display()
            ))
        })?;
        Ok(relative.to_string_lossy().replace('\\', "/"))
    }

    #[cfg(test)]
    fn write_mode_snapshot(&self) -> PayloadWriteSnapshot {
        PayloadWriteSnapshot {
            unbuffered_write_attempts: self
                .shared
                .write_counters
                .unbuffered_write_attempts
                .load(Ordering::Relaxed),
            unbuffered_write_successes: self
                .shared
                .write_counters
                .unbuffered_write_successes
                .load(Ordering::Relaxed),
            unbuffered_write_fallbacks: self
                .shared
                .write_counters
                .unbuffered_write_fallbacks
                .load(Ordering::Relaxed),
            max_staging_writes: self
                .shared
                .write_counters
                .max_staging_writes
                .load(Ordering::Relaxed),
            staging_worker_starts: self
                .shared
                .write_counters
                .staging_worker_starts
                .load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    fn set_test_staging_delay_ms(&self, delay_ms: u64) {
        self.shared
            .write_counters
            .staging_delay_ms
            .store(delay_ms, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn enter_test_staging_probe(&self) -> TestStagingProbeGuard<'_> {
        let active = self
            .shared
            .write_counters
            .active_staging_writes
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        update_max_atomic(&self.shared.write_counters.max_staging_writes, active);
        let delay_ms = self
            .shared
            .write_counters
            .staging_delay_ms
            .load(Ordering::Relaxed);
        if delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        }
        TestStagingProbeGuard {
            counters: &self.shared.write_counters,
        }
    }

    #[cfg(test)]
    pub(crate) fn sync_barrier_snapshot(&self) -> SyncBarrierSnapshot {
        SyncBarrierSnapshot {
            per_put_file_sync_calls: self
                .shared
                .barrier_counters
                .per_put_file_sync_calls
                .load(Ordering::Relaxed),
            per_put_dir_sync_calls: self
                .shared
                .barrier_counters
                .per_put_dir_sync_calls
                .load(Ordering::Relaxed),
            grouped_file_sync_calls: self
                .shared
                .barrier_counters
                .grouped_file_sync_calls
                .load(Ordering::Relaxed),
            grouped_dir_sync_calls: self
                .shared
                .barrier_counters
                .grouped_dir_sync_calls
                .load(Ordering::Relaxed),
            pending_grouped_file_syncs: self.shared.pending_grouped_sync.object_ids.lock().len(),
            pending_grouped_dir_syncs: self.shared.pending_grouped_sync.directories.lock().len(),
        }
    }

    fn create_temp_file(
        &self,
        temp_dir: &Path,
        object_id: u64,
    ) -> Result<(File, PathBuf), FilePayloadStoreError> {
        self.create_temp_file_with_opener(temp_dir, object_id, platform::create_new_buffered_write)
    }

    fn create_temp_file_with_opener(
        &self,
        temp_dir: &Path,
        object_id: u64,
        mut opener: impl FnMut(&Path) -> io::Result<File>,
    ) -> Result<(File, PathBuf), FilePayloadStoreError> {
        let hex = format!("{object_id:016x}");
        for _ in 0..TEMP_FILE_CREATE_ATTEMPTS {
            let candidate =
                temp_dir.join(format!("{hex}-{}{}", next_temp_token(), TEMP_FILE_SUFFIX));
            match opener(&candidate) {
                Ok(file) => return Ok((file, candidate)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }

        Err(FilePayloadStoreError::TempFileCreateExhausted {
            object_id,
            dir: temp_dir.to_path_buf(),
        })
    }

    #[cfg(test)]
    fn read_mode_snapshot(&self) -> PayloadReadSnapshot {
        PayloadReadSnapshot {
            unbuffered_read_attempts: self
                .shared
                .read_counters
                .unbuffered_read_attempts
                .load(Ordering::Relaxed),
            unbuffered_read_successes: self
                .shared
                .read_counters
                .unbuffered_read_successes
                .load(Ordering::Relaxed),
            unbuffered_read_fallbacks: self
                .shared
                .read_counters
                .unbuffered_read_fallbacks
                .load(Ordering::Relaxed),
        }
    }

    fn read_into_buffered_fallback(
        &self,
        object_id: u64,
        data_offset: usize,
        path: &Path,
        start: u64,
        out: &mut [u8],
    ) -> Option<usize> {
        #[cfg(test)]
        self.shared
            .read_counters
            .unbuffered_read_fallbacks
            .fetch_add(1, Ordering::Relaxed);

        let file = match self.cached_buffered_read_fallback_handle(object_id, path) {
            Ok(file) => file,
            Err(error) => {
                warn!(
                    object_id,
                    data_offset,
                    path = %path.display(),
                    error = ?error,
                    "local payload runtime payload buffered read fallback open failed"
                );
                return None;
            }
        };
        match platform::pread(file.as_ref(), out, start) {
            Ok(read) => Some(read),
            Err(error) => {
                warn!(
                    object_id,
                    data_offset,
                    path = %path.display(),
                    error = ?error,
                    "local payload runtime payload buffered read fallback failed"
                );
                None
            }
        }
    }

    fn try_read_into_unbuffered(
        &self,
        file: &File,
        alignment: usize,
        start: u64,
        out: &mut [u8],
    ) -> io::Result<Option<usize>> {
        if !platform::can_read_unbuffered_range(start, out.len(), alignment) {
            return Ok(None);
        }

        #[cfg(test)]
        self.shared
            .read_counters
            .unbuffered_read_attempts
            .fetch_add(1, Ordering::Relaxed);

        let result = {
            #[cfg(windows)]
            let mut scratch = self.shared.unbuffered_read_scratch.lock();
            #[cfg(not(windows))]
            let mut scratch = platform::UnbufferedReadBuffer::new();
            platform::read_unbuffered_with_buffer(file, out, start, &mut scratch, alignment)
        };
        match result {
            Ok(read) => {
                #[cfg(test)]
                self.shared
                    .read_counters
                    .unbuffered_read_successes
                    .fetch_add(1, Ordering::Relaxed);
                Ok(Some(read))
            }
            Err(error) if platform::is_unbuffered_io_unsupported_error(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn open_for_read(&self, path: &Path) -> io::Result<File> {
        let file = if self.read_mode == LocalPayloadReadMode::Unbuffered {
            match platform::open_read_only_unbuffered(path) {
                Ok(file) => Ok(file),
                Err(error) if platform::is_unbuffered_io_unsupported_error(&error) => {
                    platform::open_read_only(path)
                }
                Err(error) => Err(error),
            }
        } else {
            platform::open_read_only(path)
        };

        match file {
            Ok(file) => Ok(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Err(error),
            Err(error) => Err(error),
        }
    }

    fn cached_read_handle(&self, object_id: u64, path: &Path) -> io::Result<Arc<File>> {
        self.cached_read_handle_from_cache(
            object_id,
            path,
            &self.shared.read_handles,
            |this, path| this.open_for_read(path),
        )
    }

    fn cached_buffered_read_fallback_handle(
        &self,
        object_id: u64,
        path: &Path,
    ) -> io::Result<Arc<File>> {
        self.cached_read_handle_from_cache(
            object_id,
            path,
            &self.shared.buffered_read_fallback_handles,
            |_this, path| platform::open_read_only(path),
        )
    }

    fn cached_read_handle_from_cache(
        &self,
        object_id: u64,
        path: &Path,
        cache: &ReadHandleCache,
        mut opener: impl FnMut(&Self, &Path) -> io::Result<File>,
    ) -> io::Result<Arc<File>> {
        let shard = read_fd_shard_idx(object_id);
        if let Some(file) = cache.shards[shard].lock().get(&object_id).cloned() {
            cache.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(file);
        }

        cache.misses.fetch_add(1, Ordering::Relaxed);
        let file = match opener(self, path) {
            Ok(file) => file,
            Err(first_error) if platform::is_too_many_open_files_error(&first_error) => {
                let evicted = self.evict_cached_read_handles_under_pressure();
                debug!(
                    object_id,
                    path = %path.display(),
                    evicted,
                    error = ?first_error,
                    "local payload runtime read descriptor cache evicted entries after descriptor exhaustion"
                );
                if evicted == 0 {
                    return Err(first_error);
                }
                opener(self, path)?
            }
            Err(error) => return Err(error),
        };
        let file = Arc::new(file);
        if let Err(error) = platform::advise_random(file.as_ref()) {
            warn!(
                object_id,
                path = %path.display(),
                error = ?error,
                "local payload runtime payload open advise_random failed"
            );
        }

        let mut handles = cache.shards[shard].lock();
        if let Some(existing) = handles.get(&object_id).cloned() {
            cache.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(existing);
        }
        let evicted_lru = handles.len() == handles.cap().get() && !handles.contains(&object_id);
        let _ = handles.put(object_id, Arc::clone(&file));
        if evicted_lru {
            cache.evictions.fetch_add(1, Ordering::Relaxed);
        }
        Ok(file)
    }

    fn evict_cached_read_handles_under_pressure(&self) -> usize {
        self.shared.read_handles.evict_under_pressure()
            + self
                .shared
                .buffered_read_fallback_handles
                .evict_under_pressure()
    }

    fn invalidate_cached_read_handle(&self, object_id: u64) {
        self.shared.read_handles.shards[read_fd_shard_idx(object_id)]
            .lock()
            .pop(&object_id);
        self.shared.buffered_read_fallback_handles.shards[read_fd_shard_idx(object_id)]
            .lock()
            .pop(&object_id);
    }

    fn record_grouped_sync_targets(&self, object_id: u64, payload_dir: &Path) {
        let mut object_ids = self.shared.pending_grouped_sync.object_ids.lock();
        object_ids.insert(object_id);
        drop(object_ids);

        let mut directories = self.shared.pending_grouped_sync.directories.lock();
        directories.insert(self.layout.tmp_dir.clone());
        directories.insert(payload_dir.to_path_buf());
        directories.insert(self.layout.payload_objects_dir.clone());
        directories.insert(self.layout.payload_root.clone());
        directories.insert(self.layout.runtime_root.clone());
    }

    fn clear_grouped_sync_targets(&self) {
        self.shared.pending_grouped_sync.object_ids.lock().clear();
        self.shared.pending_grouped_sync.directories.lock().clear();
    }

    #[cfg(test)]
    fn take_grouped_sync_targets(&self) -> (Vec<u64>, Vec<PathBuf>) {
        let object_ids = {
            let mut object_ids = self.shared.pending_grouped_sync.object_ids.lock();
            object_ids.drain().collect()
        };
        let directories = {
            let mut directories = self.shared.pending_grouped_sync.directories.lock();
            directories.drain().collect()
        };
        (object_ids, directories)
    }

    #[cfg(test)]
    fn requeue_grouped_sync_targets(&self, object_ids: &[u64], directories: &[PathBuf]) {
        if !object_ids.is_empty() {
            let mut pending_object_ids = self.shared.pending_grouped_sync.object_ids.lock();
            pending_object_ids.extend(object_ids.iter().copied());
        }
        if !directories.is_empty() {
            let mut pending_directories = self.shared.pending_grouped_sync.directories.lock();
            pending_directories.extend(directories.iter().cloned());
        }
    }

    #[cfg(test)]
    fn sync_pending_grouped_targets<SyncFile, SyncDir>(
        &self,
        pending_object_ids: Vec<u64>,
        pending_dirs: Vec<PathBuf>,
        mut sync_file: SyncFile,
        mut sync_dir: SyncDir,
    ) -> Result<(), FilePayloadStoreError>
    where
        SyncFile: FnMut(u64) -> Result<(), FilePayloadStoreError>,
        SyncDir: FnMut(&Path) -> Result<(), FilePayloadStoreError>,
    {
        for (index, &object_id) in pending_object_ids.iter().enumerate() {
            if let Err(error) = sync_file(object_id) {
                self.requeue_grouped_sync_targets(&pending_object_ids[index..], &pending_dirs);
                return Err(error);
            }
        }
        for (index, dir) in pending_dirs.iter().enumerate() {
            if let Err(error) = sync_dir(dir) {
                self.requeue_grouped_sync_targets(&[], &pending_dirs[index..]);
                return Err(error);
            }
        }
        Ok(())
    }

    fn sync_directory_put_barrier(&self, dir: &Path) -> Result<(), FilePayloadStoreError> {
        #[cfg(test)]
        {
            let mut failure = self.shared.fail_directory_sync.lock();
            if failure.as_deref() == Some(dir) {
                failure.take();
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected directory sync failure",
                )
                .into());
            }
        }
        platform::sync_directory(dir)?;
        self.shared
            .barrier_counters
            .per_put_dir_sync_calls
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fail_next_directory_sync_for_test(&self, dir: &Path) {
        *self.shared.fail_directory_sync.lock() = Some(dir.to_path_buf());
    }

    fn sync_directory_grouped_barrier(&self, dir: &Path) -> Result<(), FilePayloadStoreError> {
        match platform::sync_directory(dir) {
            Ok(()) => {
                self.shared
                    .barrier_counters
                    .grouped_dir_sync_calls
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(test)]
    fn read_handle_stats(&self) -> ReadHandleStats {
        ReadHandleStats {
            hits: self.shared.read_handles.hits.load(Ordering::Relaxed)
                + self
                    .shared
                    .buffered_read_fallback_handles
                    .hits
                    .load(Ordering::Relaxed),
            misses: self.shared.read_handles.misses.load(Ordering::Relaxed)
                + self
                    .shared
                    .buffered_read_fallback_handles
                    .misses
                    .load(Ordering::Relaxed),
            evictions: self.shared.read_handles.evictions.load(Ordering::Relaxed)
                + self
                    .shared
                    .buffered_read_fallback_handles
                    .evictions
                    .load(Ordering::Relaxed),
            cached: self
                .shared
                .read_handles
                .shards
                .iter()
                .map(|shard| shard.lock().len())
                .sum::<usize>()
                + self
                    .shared
                    .buffered_read_fallback_handles
                    .shards
                    .iter()
                    .map(|shard| shard.lock().len())
                    .sum::<usize>(),
        }
    }

    #[cfg(test)]
    fn read_handle_family_capacity_for_test(&self) -> usize {
        self.shared.read_handles.capacity()
    }

    #[cfg(test)]
    fn shared_state_arc_count_for_test(&self) -> usize {
        Arc::strong_count(&self.shared)
    }
}

#[repr(align(128))]
#[derive(Debug)]
struct ReadHandleCache {
    shards: [Mutex<LruCache<u64, Arc<File>>>; READ_FD_CACHE_SHARDS],
    hits: CachePaddedAtomicU64,
    misses: CachePaddedAtomicU64,
    evictions: CachePaddedAtomicU64,
}

impl ReadHandleCache {
    fn new(capacity: usize) -> Self {
        let shard_cap = read_fd_cache_shard_cap(capacity);
        Self {
            shards: array::from_fn(|_| Mutex::new(LruCache::new(shard_cap))),
            hits: CachePaddedAtomicU64::new(0),
            misses: CachePaddedAtomicU64::new(0),
            evictions: CachePaddedAtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn capacity(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().cap().get())
            .sum()
    }

    fn evict_under_pressure(&self) -> usize {
        let mut evicted = 0usize;
        for shard in &self.shards {
            let mut evicted_handles = Vec::new();
            let mut handles = shard.lock();
            if handles.is_empty() {
                continue;
            }
            let target = (handles.len() / 2).max(1);
            for _ in 0..target {
                if let Some((_object_id, file)) = handles.pop_lru() {
                    evicted_handles.push(file);
                    evicted += 1;
                } else {
                    break;
                }
            }
            drop(handles);
            drop(evicted_handles);
        }
        if evicted > 0 {
            self.evictions.fetch_add(evicted as u64, Ordering::Relaxed);
        }
        evicted
    }
}

#[derive(Debug, Default)]
struct PendingGroupedSync {
    object_ids: Mutex<HashSet<u64>>,
    directories: Mutex<HashSet<PathBuf>>,
}

#[cfg(test)]
#[repr(align(128))]
#[derive(Debug, Default)]
struct PayloadWriteCounters {
    unbuffered_write_attempts: CachePaddedAtomicU64,
    unbuffered_write_successes: CachePaddedAtomicU64,
    unbuffered_write_fallbacks: CachePaddedAtomicU64,
    active_staging_writes: CachePaddedAtomicU64,
    max_staging_writes: CachePaddedAtomicU64,
    staging_worker_starts: CachePaddedAtomicU64,
    staging_delay_ms: CachePaddedAtomicU64,
}

#[cfg(test)]
#[repr(align(128))]
#[derive(Debug, Default)]
struct PayloadReadCounters {
    unbuffered_read_attempts: CachePaddedAtomicU64,
    unbuffered_read_successes: CachePaddedAtomicU64,
    unbuffered_read_fallbacks: CachePaddedAtomicU64,
}

#[repr(align(128))]
#[derive(Debug, Default)]
struct SyncBarrierCounters {
    per_put_file_sync_calls: CachePaddedAtomicU64,
    per_put_dir_sync_calls: CachePaddedAtomicU64,
    #[cfg(test)]
    grouped_file_sync_calls: CachePaddedAtomicU64,
    grouped_dir_sync_calls: CachePaddedAtomicU64,
}

#[repr(align(128))]
#[derive(Debug, Default)]
struct CachePaddedAtomicU64(AtomicU64);

impl CachePaddedAtomicU64 {
    fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }
}

impl Deref for CachePaddedAtomicU64 {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CachePaddedAtomicU64 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SyncBarrierSnapshot {
    pub(crate) per_put_file_sync_calls: u64,
    pub(crate) per_put_dir_sync_calls: u64,
    pub(crate) grouped_file_sync_calls: u64,
    pub(crate) grouped_dir_sync_calls: u64,
    pub(crate) pending_grouped_file_syncs: usize,
    pub(crate) pending_grouped_dir_syncs: usize,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PayloadWriteSnapshot {
    unbuffered_write_attempts: u64,
    unbuffered_write_successes: u64,
    unbuffered_write_fallbacks: u64,
    max_staging_writes: u64,
    staging_worker_starts: u64,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PayloadReadSnapshot {
    unbuffered_read_attempts: u64,
    unbuffered_read_successes: u64,
    unbuffered_read_fallbacks: u64,
}

#[cfg(test)]
struct TestStagingProbeGuard<'a> {
    counters: &'a PayloadWriteCounters,
}

#[cfg(test)]
impl Drop for TestStagingProbeGuard<'_> {
    fn drop(&mut self) {
        self.counters
            .active_staging_writes
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
fn update_max_atomic(max: &AtomicU64, value: u64) {
    let mut current = max.load(Ordering::Relaxed);
    while value > current {
        match max.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadHandleStats {
    hits: u64,
    misses: u64,
    evictions: u64,
    cached: usize,
}

fn read_fd_cache_capacity() -> usize {
    #[cfg(test)]
    {
        READ_FD_CACHE_MIN_CAPACITY
    }

    #[cfg(all(not(test), windows))]
    {
        READ_FD_CACHE_MAX_CAPACITY
    }

    #[cfg(all(not(test), not(windows)))]
    {
        read_fd_cache_capacity_for_limit(platform::open_file_descriptor_limit())
    }
}

#[cfg(any(test, not(windows)))]
fn read_fd_cache_capacity_for_limit(limit: Option<usize>) -> usize {
    let raw_capacity = match limit {
        Some(limit) => {
            let reserve = READ_FD_CACHE_RESERVED_MIN.max(limit / 4);
            limit.saturating_sub(reserve) / READ_FD_CACHE_FAMILIES
        }
        None => READ_FD_CACHE_MAX_CAPACITY,
    }
    .clamp(READ_FD_CACHE_MIN_CAPACITY, READ_FD_CACHE_MAX_CAPACITY);
    read_fd_cache_round_family_capacity(raw_capacity)
}

#[cfg(any(test, not(windows)))]
fn read_fd_cache_round_family_capacity(family_capacity: usize) -> usize {
    let shard_units = family_capacity / READ_FD_CACHE_SHARDS;
    shard_units.max(1) * READ_FD_CACHE_SHARDS
}

fn read_fd_cache_shard_cap(family_capacity: usize) -> NonZeroUsize {
    let capacity = family_capacity / READ_FD_CACHE_SHARDS;
    NonZeroUsize::new(capacity.max(1)).expect("read FD cache shard capacity must be non-zero")
}

fn read_fd_shard_idx(object_id: u64) -> usize {
    let shard_count = u64::try_from(READ_FD_CACHE_SHARDS).unwrap_or(u64::MAX);
    let shard = object_id % shard_count;
    usize::try_from(shard).unwrap_or(0)
}

fn batch_staging_concurrency(entry_count: usize, durable_sync: bool) -> usize {
    if durable_sync {
        1
    } else {
        entry_count.clamp(1, FAST_BATCH_STAGING_MAX_CONCURRENCY)
    }
}

fn next_temp_token() -> String {
    let seq = TEMP_FILE_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}-{}", seq, Uuid::new_v4().simple())
}

fn next_batch_intent_token() -> String {
    let seq = BATCH_INTENT_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}-{}", seq, Uuid::new_v4().simple())
}

fn remove_batch_payload_intent(path: &Path) {
    if path.as_os_str().is_empty() {
        return;
    }
    if let Err(error) = std::fs::remove_file(path)
        && error.kind() != io::ErrorKind::NotFound
    {
        warn!(
            path = %path.display(),
            error = ?error,
            "local payload runtime batch intent remove failed"
        );
    }
}

fn encode_batch_intent_path_hex(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_cold_reopen_roundtrip() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout.clone()).unwrap();
        store.put(42, b"payload").unwrap();
        drop(store);

        let reopened = FilePayloadStore::open(layout).unwrap();
        assert_eq!(reopened.get(42).unwrap().as_ref(), b"payload");
    }

    #[test]
    fn payload_store_uses_one_shared_state_arc() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();

        assert_eq!(store.shared_state_arc_count_for_test(), 1);
    }

    #[test]
    fn payload_store_shared_state_count_reflects_live_store_clones() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();
        let _clone = store.clone();

        assert_eq!(store.shared_state_arc_count_for_test(), 2);
    }

    #[test]
    fn hot_atomic_counter_groups_are_cache_line_aligned() {
        assert!(std::mem::align_of::<ReadHandleCache>() >= 128);
        assert!(std::mem::align_of::<PayloadWriteCounters>() >= 128);
        assert!(std::mem::align_of::<PayloadReadCounters>() >= 128);
        assert!(std::mem::align_of::<SyncBarrierCounters>() >= 128);
        assert_eq!(std::mem::align_of::<CachePaddedAtomicU64>(), 128);
        assert_eq!(std::mem::size_of::<CachePaddedAtomicU64>(), 128);
    }

    #[test]
    fn cache_padded_atomic_allows_exclusive_mutable_access() {
        let mut counter = CachePaddedAtomicU64::new(7);

        *counter.get_mut() = 11;

        assert_eq!(counter.load(Ordering::Relaxed), 11);
    }

    #[test]
    fn read_fd_cache_capacity_leaves_reserved_descriptors_at_default_unix_limit() {
        let capacity = read_fd_cache_capacity_for_limit(Some(1024));

        assert!(capacity <= 768);
        assert_eq!(capacity % READ_FD_CACHE_SHARDS, 0);
    }

    #[test]
    fn read_fd_cache_capacity_caps_high_limits() {
        assert_eq!(
            read_fd_cache_capacity_for_limit(Some(65_536)),
            READ_FD_CACHE_MAX_CAPACITY
        );
    }

    #[test]
    fn read_fd_cache_capacity_keeps_minimum_for_low_limits() {
        assert_eq!(
            read_fd_cache_capacity_for_limit(Some(64)),
            READ_FD_CACHE_MIN_CAPACITY
        );
    }

    #[test]
    fn unbuffered_fast_mode_roundtrips_aligned_payload() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open_with_sync_and_write_mode(
            layout,
            LocalPayloadSyncMode::Fast,
            LocalPayloadWriteMode::UnbufferedFast,
        )
        .unwrap();
        let payload = vec![0xAB; 4096];

        store.put(4200, &payload).unwrap();

        assert_eq!(store.get(4200).unwrap().as_ref(), payload.as_slice());
        let writes = store.write_mode_snapshot();
        if cfg!(windows) {
            assert_eq!(writes.unbuffered_write_attempts, 1);
            assert_eq!(
                writes.unbuffered_write_successes + writes.unbuffered_write_fallbacks,
                1
            );
        } else {
            assert_eq!(writes.unbuffered_write_attempts, 0);
            assert_eq!(writes.unbuffered_write_fallbacks, 1);
        }
    }

    #[test]
    fn unbuffered_fast_mode_falls_back_for_unaligned_payload() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open_with_sync_and_write_mode(
            layout,
            LocalPayloadSyncMode::Fast,
            LocalPayloadWriteMode::UnbufferedFast,
        )
        .unwrap();

        store.put(4201, b"payload").unwrap();

        assert_eq!(store.get(4201).unwrap().as_ref(), b"payload");
        let writes = store.write_mode_snapshot();
        assert_eq!(writes.unbuffered_write_attempts, 0);
        assert_eq!(writes.unbuffered_write_successes, 0);
        assert_eq!(writes.unbuffered_write_fallbacks, 1);
    }

    #[test]
    fn default_mount_write_mode_keeps_payload_writes_cacheable() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open_with_sync_and_write_mode(
            layout,
            LocalPayloadSyncMode::Fast,
            LocalPayloadWriteMode::default_for_mount_path(),
        )
        .unwrap();
        let payload = vec![0xAB; 4096];

        store.put(4298, &payload).unwrap();

        assert_eq!(store.get(4298).unwrap().as_ref(), payload.as_slice());
        let writes = store.write_mode_snapshot();
        assert_eq!(writes.unbuffered_write_attempts, 0);
        assert_eq!(writes.unbuffered_write_successes, 0);
        assert_eq!(writes.unbuffered_write_fallbacks, 0);
    }

    #[test]
    fn default_mount_read_mode_avoids_duplicate_macos_page_cache() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open_with_sync_and_write_mode(
            layout,
            LocalPayloadSyncMode::Fast,
            LocalPayloadWriteMode::Buffered,
        )
        .unwrap();
        let payload = vec![0xCD; 4096];

        store.put(4299, &payload).unwrap();

        let mut out = vec![0_u8; 4096];
        assert_eq!(store.read_into(4299, 0, &mut out), Some(4096));
        assert_eq!(out, payload);
        let reads = store.read_mode_snapshot();
        if cfg!(target_os = "macos") {
            assert_eq!(reads.unbuffered_read_attempts, 1);
            assert_eq!(reads.unbuffered_read_successes, 1);
            assert_eq!(reads.unbuffered_read_fallbacks, 0);
        } else {
            assert_eq!(reads.unbuffered_read_attempts, 0);
            assert_eq!(reads.unbuffered_read_successes, 0);
            assert_eq!(reads.unbuffered_read_fallbacks, 0);
        }
    }

    #[test]
    fn unbuffered_read_mode_roundtrips_aligned_read_into() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open_with_modes(
            layout,
            LocalPayloadSyncMode::Fast,
            LocalPayloadWriteMode::Buffered,
            LocalPayloadReadMode::Unbuffered,
        )
        .unwrap();
        let payload = vec![0xCD; 4096];

        store.put(4300, &payload).unwrap();

        let mut out = vec![0_u8; 4096];
        assert_eq!(store.read_into(4300, 0, &mut out), Some(4096));
        assert_eq!(out, payload);
        let reads = store.read_mode_snapshot();
        if cfg!(any(windows, target_os = "macos")) {
            assert_eq!(reads.unbuffered_read_attempts, 1);
            assert_eq!(
                reads.unbuffered_read_successes + reads.unbuffered_read_fallbacks,
                1
            );
        } else {
            assert_eq!(reads.unbuffered_read_attempts, 0);
            assert_eq!(reads.unbuffered_read_fallbacks, 1);
        }
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "current_thread")]
    async fn windows_read_advice_does_not_read_large_payload_on_executor() {
        use std::io::Seek;

        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open_with_modes(
            layout.clone(),
            LocalPayloadSyncMode::Fast,
            LocalPayloadWriteMode::Buffered,
            LocalPayloadReadMode::Buffered,
        )
        .unwrap();
        store.put(748, b"payload").unwrap();
        let path = layout.payload_object_path(748);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        let file = store.cached_read_handle(748, &path).unwrap();
        let mut cursor = file.as_ref();
        assert_eq!(cursor.stream_position().unwrap(), 0);

        store.advise_will_need(748);

        // Windows seek_read moves the cursor to the end of each actual read.
        assert_eq!(
            cursor.stream_position().unwrap(),
            0,
            "read advice must not synchronously read payload bytes on the executor"
        );
        store.invalidate_cached_read_handle(748);
        store.advise_will_need(748);
        assert_eq!(
            store.read_handle_stats().misses,
            1,
            "advice must not open a payload"
        );
    }

    // macOS enables F_NOCACHE; other non-Windows platforms use buffered fallback.
    #[cfg(target_os = "macos")]
    #[test]
    fn unbuffered_reads_through_clones_overlap_at_platform_read() {
        use std::sync::mpsc;
        use std::time::Duration;

        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open_with_modes(
            layout,
            LocalPayloadSyncMode::Fast,
            LocalPayloadWriteMode::Buffered,
            LocalPayloadReadMode::Unbuffered,
        )
        .unwrap();
        store.put(4300, b"abcdef").unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();

        std::thread::scope(|scope| {
            let mut releases = Vec::new();
            for offset in [0, 3] {
                let store = store.clone();
                let entered = entered_tx.clone();
                let (release_tx, release_rx) = mpsc::channel();
                releases.push(release_tx);
                scope.spawn(move || {
                    platform::PREAD_TEST_GATE.with(|gate| {
                        *gate.borrow_mut() = Some((entered, release_rx));
                    });
                    let mut out = [0; 3];
                    assert_eq!(store.read_into(4300, offset, &mut out), Some(3));
                    assert_eq!(&out, if offset == 0 { b"abc" } else { b"def" });
                });
            }
            let first = entered_rx.recv_timeout(Duration::from_secs(5));
            let second = entered_rx.recv_timeout(Duration::from_secs(5));
            // Release even on failure; the scope joins all readers.
            for release in releases {
                let _ = release.send(());
            }
            assert!(
                first.is_ok() && second.is_ok(),
                "both reads must enter pread before either returns"
            );
        });
        let reads = store.read_mode_snapshot();
        assert_eq!(reads.unbuffered_read_successes, 2);
        assert_eq!(reads.unbuffered_read_fallbacks, 0);
    }

    #[test]
    fn unbuffered_read_mode_handles_platform_alignment_rules() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open_with_modes(
            layout,
            LocalPayloadSyncMode::Fast,
            LocalPayloadWriteMode::Buffered,
            LocalPayloadReadMode::Unbuffered,
        )
        .unwrap();

        store.put(4301, b"abcdef").unwrap();

        let mut out = [0_u8; 3];
        assert_eq!(store.read_into(4301, 1, &mut out), Some(3));
        assert_eq!(&out, b"bcd");
        let reads = store.read_mode_snapshot();
        if cfg!(target_os = "macos") {
            assert_eq!(reads.unbuffered_read_attempts, 1);
            assert_eq!(reads.unbuffered_read_successes, 1);
            assert_eq!(reads.unbuffered_read_fallbacks, 0);
        } else {
            assert_eq!(reads.unbuffered_read_attempts, 0);
            assert_eq!(reads.unbuffered_read_successes, 0);
            assert_eq!(reads.unbuffered_read_fallbacks, 1);
            assert_eq!(
                store.read_handle_stats().cached,
                1,
                "unaligned fallback should cache a separate buffered read handle"
            );
        }
    }

    #[test]
    fn read_into_supports_partial_ranges() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();
        store.put(7, b"abcdef").unwrap();

        let mut out = [0_u8; 3];
        assert_eq!(store.read_into(7, 2, &mut out), Some(3));
        assert_eq!(&out, b"cde");
        assert_eq!(store.read_into(7, 99, &mut out), Some(0));
    }

    #[test]
    fn staged_temp_file_does_not_override_committed_payload_without_rename() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout.clone()).unwrap();
        store.put(9001, b"committed").unwrap();

        // Simulate a crash after temp-file fsync but before atomic rename.
        let (mut temp_file, temp_path) = store.create_temp_file(&layout.tmp_dir, 9001).unwrap();
        platform::write_all(&mut temp_file, b"staged-new-value").unwrap();
        platform::sync_data(&temp_file).unwrap();
        drop(temp_file);
        drop(store);

        let reopened = FilePayloadStore::open(layout).unwrap();
        assert_eq!(reopened.get(9001).unwrap().as_ref(), b"committed");
        assert!(temp_path.exists());
    }

    #[test]
    fn hot_read_into_reuses_cached_descriptors() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();
        store.put(700, b"abcdef").unwrap();

        let mut out = [0_u8; 3];
        assert_eq!(store.read_into(700, 0, &mut out), Some(3));
        assert_eq!(store.read_into(700, 1, &mut out), Some(3));
        assert_eq!(store.read_into(700, 2, &mut out), Some(3));

        let stats = store.read_handle_stats();
        assert_eq!(stats.misses, 1);
        assert!(
            stats.hits >= 2,
            "expected repeated reads to hit descriptor cache"
        );
        assert_eq!(stats.cached, 1);
    }

    #[test]
    fn overwrite_invalidates_cached_descriptor() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();
        store.put(1337, b"old").unwrap();

        let mut out = [0_u8; 3];
        assert_eq!(store.read_into(1337, 0, &mut out), Some(3));
        assert_eq!(&out, b"old");

        store.put(1337, b"new").unwrap();
        assert_eq!(store.read_into(1337, 0, &mut out), Some(3));
        assert_eq!(&out, b"new");
    }

    #[test]
    fn descriptor_cache_is_bounded() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();
        let capacity = store.read_handle_family_capacity_for_test();
        let capacity_to_fill = capacity.saturating_add(4);

        for object_id in 0..u64::try_from(capacity_to_fill).expect("cache capacity fits u64") {
            store.put(object_id, b"x").unwrap();
            let mut out = [0_u8; 1];
            assert_eq!(store.read_into(object_id, 0, &mut out), Some(1));
        }

        let stats = store.read_handle_stats();
        assert_eq!(stats.cached, capacity);
        assert!(stats.evictions >= 1);
    }

    #[test]
    fn emfile_retry_evicts_cached_descriptors_before_reopening() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();

        for object_id in 0..4 {
            store.put(object_id, b"x").unwrap();
            let mut out = [0_u8; 1];
            assert_eq!(store.read_into(object_id, 0, &mut out), Some(1));
        }
        let cached_before = store.read_handle_stats().cached;
        assert!(cached_before >= 4);

        store.put(99, b"y").unwrap();
        let path = store.layout.payload_object_path(99);
        let mut attempts = 0usize;
        let file = store
            .cached_read_handle_from_cache(99, &path, &store.shared.read_handles, |_this, path| {
                attempts += 1;
                if attempts == 1 {
                    return Err(platform::too_many_open_files_error_for_test());
                }
                platform::open_read_only(path)
            })
            .unwrap();
        drop(file);

        assert_eq!(attempts, 2);
        assert!(store.read_handle_stats().cached < cached_before + 1);
    }

    #[test]
    fn descriptor_pressure_retry_skips_reopen_when_cache_is_empty() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();

        store.put(99, b"y").unwrap();
        let path = store.layout.payload_object_path(99);
        let mut attempts = 0usize;
        let error = store
            .cached_read_handle_from_cache(99, &path, &store.shared.read_handles, |_this, _path| {
                attempts += 1;
                Err(platform::too_many_open_files_error_for_test())
            })
            .unwrap_err();

        assert!(platform::is_too_many_open_files_error(&error));
        assert_eq!(attempts, 1);
    }

    #[test]
    fn descriptor_pressure_retry_returns_second_open_error() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();

        for object_id in 0..4 {
            store.put(object_id, b"x").unwrap();
            let mut out = [0_u8; 1];
            assert_eq!(store.read_into(object_id, 0, &mut out), Some(1));
        }

        store.put(99, b"y").unwrap();
        let path = store.layout.payload_object_path(99);
        let mut attempts = 0usize;
        let error = store
            .cached_read_handle_from_cache(99, &path, &store.shared.read_handles, |_this, _path| {
                attempts += 1;
                if attempts == 1 {
                    return Err(platform::too_many_open_files_error_for_test());
                }
                Err(io::Error::from(io::ErrorKind::NotFound))
            })
            .unwrap_err();

        assert_eq!(attempts, 2);
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn sfs753_failed_intent_write_removes_uncommitted_staged_payloads() {
        for (sync_mode, fail_intent_barrier) in [
            (LocalPayloadSyncMode::Durable, false),
            (LocalPayloadSyncMode::Durable, true),
            (LocalPayloadSyncMode::Fast, false),
        ] {
            for batch in [false, true] {
                let temp = studiofs_test_support::workspace_target_tempdir("test-");
                let layout = LocalPayloadLayout::from_cache_root(temp.path());
                let store =
                    FilePayloadStore::open_with_sync_mode(layout.clone(), sync_mode).unwrap();
                store.put(1, b"kept").unwrap();
                let intent_dir = layout.tmp_dir.join(LOCAL_PAYLOAD_BATCH_INTENT_DIR);
                if fail_intent_barrier {
                    store.fail_next_directory_sync_for_test(&intent_dir);
                } else {
                    std::fs::remove_dir(&intent_dir).unwrap();
                    std::fs::write(&intent_dir, b"blocked directory").unwrap();
                }
                let entries = [
                    PayloadBatchEntry {
                        object_id: 80,
                        data: b"new",
                    },
                    PayloadBatchEntry {
                        object_id: 81,
                        data: b"new",
                    },
                ];
                assert!(
                    if batch {
                        store.put_batch(&entries).map(PayloadBatchCommit::complete)
                    } else {
                        store.put(80, b"new")
                    }
                    .is_err()
                );
                for entry in &entries {
                    assert!(!layout.payload_object_path(entry.object_id).exists());
                }
                let mut dirs = vec![layout.tmp_dir.clone()];
                dirs.extend(entries.iter().map(|entry| {
                    layout
                        .payload_object_path(entry.object_id)
                        .parent()
                        .unwrap()
                        .to_path_buf()
                }));
                for dir in dirs {
                    if !dir.exists() {
                        continue;
                    }
                    assert!(
                        std::fs::read_dir(dir).unwrap().all(|entry| {
                            !entry
                                .unwrap()
                                .file_name()
                                .to_string_lossy()
                                .ends_with(".tmp")
                        }),
                        "failed intent write leaked staged payload bytes"
                    );
                }
                if fail_intent_barrier {
                    assert!(
                        std::fs::read_dir(&intent_dir).unwrap().next().is_none(),
                        "failed intent barrier left an unowned marker before payload rename"
                    );
                }
                assert_eq!(store.get(1).unwrap().as_ref(), b"kept");
            }
        }
    }

    #[test]
    fn sfs753_failed_payload_removal_retains_intent_and_reports_error() {
        for sync_mode in [LocalPayloadSyncMode::Durable, LocalPayloadSyncMode::Fast] {
            let temp = studiofs_test_support::workspace_target_tempdir("test-");
            let layout = LocalPayloadLayout::from_cache_root(temp.path());
            let store = FilePayloadStore::open_with_sync_mode(layout.clone(), sync_mode).unwrap();
            let commit = store
                .put_batch(&[
                    PayloadBatchEntry {
                        object_id: 90,
                        data: b"blocked",
                    },
                    PayloadBatchEntry {
                        object_id: 91,
                        data: b"removed",
                    },
                ])
                .unwrap();
            let intent_path = commit.intent_path.clone();
            let blocked_path = layout.payload_object_path(90);
            std::fs::remove_file(&blocked_path).unwrap();
            std::fs::create_dir(&blocked_path).unwrap();
            let before = store.sync_barrier_snapshot().per_put_dir_sync_calls;
            assert!(store.rollback_batch(&[90, 91], commit).is_err());
            assert!(
                intent_path.exists(),
                "failed rollback discarded recovery evidence"
            );
            assert!(blocked_path.is_dir());
            assert!(!layout.payload_object_path(91).exists());
            let shard_count = [90, 91]
                .into_iter()
                .map(|id| {
                    layout
                        .payload_object_path(id)
                        .parent()
                        .unwrap()
                        .to_path_buf()
                })
                .collect::<HashSet<_>>()
                .len() as u64;
            assert_eq!(
                store.sync_barrier_snapshot().per_put_dir_sync_calls - before,
                if sync_mode == LocalPayloadSyncMode::Durable {
                    shard_count
                } else {
                    0
                },
                "one failed unlink must not suppress successful deletion barriers"
            );
        }
    }

    #[test]
    fn durable_mode_keeps_per_put_sync_barriers() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store =
            FilePayloadStore::open_with_sync_mode(layout, LocalPayloadSyncMode::Durable).unwrap();
        store.put(7001, b"payload").unwrap();

        let stats = store.sync_barrier_snapshot();
        assert_eq!(
            stats.per_put_file_sync_calls, 2,
            "durable mode must sync both payload and intent temp bytes before rename"
        );
        assert_eq!(
            stats.per_put_dir_sync_calls, 3,
            "durable puts need intent + tmp + payload directory barriers"
        );
        assert_eq!(stats.pending_grouped_file_syncs, 0);
        assert_eq!(stats.pending_grouped_dir_syncs, 0);
        store.put(7002, b"second payload").unwrap();
        assert_eq!(store.sync_barrier_snapshot().per_put_dir_sync_calls, 6);
    }

    #[test]
    fn fast_mode_skips_put_barriers_on_grouped_sync_data() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store =
            FilePayloadStore::open_with_sync_mode(layout, LocalPayloadSyncMode::Fast).unwrap();
        store.put(8001, b"one").unwrap();
        store.put(8002, b"two").unwrap();

        let before_sync = store.sync_barrier_snapshot();
        assert_eq!(before_sync.per_put_file_sync_calls, 0);
        assert_eq!(before_sync.per_put_dir_sync_calls, 0);
        assert!(
            before_sync.pending_grouped_file_syncs >= 2,
            "fast mode should queue payload files for grouped sync_data barriers"
        );
        assert!(
            before_sync.pending_grouped_dir_syncs >= 1,
            "fast mode should queue directories for grouped sync_data barriers"
        );
        let object_capacity_before_sync = store
            .shared
            .pending_grouped_sync
            .object_ids
            .lock()
            .capacity();
        let dir_capacity_before_sync = store
            .shared
            .pending_grouped_sync
            .directories
            .lock()
            .capacity();

        store.sync_data().unwrap();
        let after_sync = store.sync_barrier_snapshot();
        assert_eq!(after_sync.pending_grouped_file_syncs, 0);
        assert_eq!(after_sync.pending_grouped_dir_syncs, 0);
        assert_eq!(after_sync.grouped_file_sync_calls, 0);
        assert_eq!(after_sync.grouped_dir_sync_calls, 0);
        assert!(
            store
                .shared
                .pending_grouped_sync
                .object_ids
                .lock()
                .capacity()
                >= object_capacity_before_sync
        );
        assert!(
            store
                .shared
                .pending_grouped_sync
                .directories
                .lock()
                .capacity()
                >= dir_capacity_before_sync
        );
    }

    #[test]
    fn fast_mode_requeues_only_unsynced_grouped_file_targets_after_grouped_sync_failure() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store =
            FilePayloadStore::open_with_sync_mode(layout, LocalPayloadSyncMode::Fast).unwrap();
        store.put(8101, b"one").unwrap();
        store.put(8102, b"two").unwrap();

        let (pending_object_ids, pending_dirs) = store.take_grouped_sync_targets();
        assert_eq!(store.sync_barrier_snapshot().pending_grouped_file_syncs, 0);
        assert_eq!(store.sync_barrier_snapshot().pending_grouped_dir_syncs, 0);

        let mut succeeded_once = false;
        let error = store
            .sync_pending_grouped_targets(
                pending_object_ids.clone(),
                pending_dirs.clone(),
                |_object_id| {
                    if !succeeded_once {
                        succeeded_once = true;
                        return Ok(());
                    }
                    Err(FilePayloadStoreError::Io(io::Error::other(
                        "simulated grouped sync failure",
                    )))
                },
                |_dir| Ok(()),
            )
            .unwrap_err();
        assert!(matches!(error, FilePayloadStoreError::Io(_)));

        let after_failure = store.sync_barrier_snapshot();
        assert_eq!(
            after_failure.pending_grouped_file_syncs,
            pending_object_ids.len().saturating_sub(1)
        );
        assert_eq!(after_failure.pending_grouped_dir_syncs, pending_dirs.len());
    }

    #[test]
    fn fast_mode_requeues_only_unsynced_grouped_dirs_after_grouped_dir_sync_failure() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store =
            FilePayloadStore::open_with_sync_mode(layout, LocalPayloadSyncMode::Fast).unwrap();
        store.put(8151, b"one").unwrap();
        store.put(8152, b"two").unwrap();

        let (pending_object_ids, pending_dirs) = store.take_grouped_sync_targets();
        assert!(pending_dirs.len() >= 2);

        let mut synced_dirs = 0usize;
        let error = store
            .sync_pending_grouped_targets(
                pending_object_ids.clone(),
                pending_dirs.clone(),
                |_object_id| Ok(()),
                |_dir| {
                    if synced_dirs == 0 {
                        synced_dirs += 1;
                        return Ok(());
                    }
                    Err(FilePayloadStoreError::Io(io::Error::other(
                        "simulated grouped dir sync failure",
                    )))
                },
            )
            .unwrap_err();
        assert!(matches!(error, FilePayloadStoreError::Io(_)));

        let after_failure = store.sync_barrier_snapshot();
        assert_eq!(after_failure.pending_grouped_file_syncs, 0);
        assert_eq!(
            after_failure.pending_grouped_dir_syncs,
            pending_dirs.len().saturating_sub(1)
        );
    }

    #[test]
    fn fast_mode_put_batch_skips_barriers_on_grouped_sync_data() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store =
            FilePayloadStore::open_with_sync_mode(layout, LocalPayloadSyncMode::Fast).unwrap();
        let entries = [
            PayloadBatchEntry {
                object_id: 8201,
                data: b"one",
            },
            PayloadBatchEntry {
                object_id: 8202,
                data: b"two",
            },
        ];

        store.put_batch(&entries).unwrap().complete();

        let before_sync = store.sync_barrier_snapshot();
        assert_eq!(before_sync.per_put_file_sync_calls, 0);
        assert_eq!(before_sync.per_put_dir_sync_calls, 0);
        assert!(
            before_sync.pending_grouped_file_syncs >= entries.len(),
            "fast mode batched put should queue every payload for grouped sync_data barriers"
        );
        assert!(
            before_sync.pending_grouped_dir_syncs >= 1,
            "fast mode batched put should queue directories for grouped sync_data barriers"
        );
        assert_eq!(store.get(8201).unwrap().as_ref(), b"one");
        assert_eq!(store.get(8202).unwrap().as_ref(), b"two");

        store.sync_data().unwrap();

        let after_sync = store.sync_barrier_snapshot();
        assert_eq!(after_sync.pending_grouped_file_syncs, 0);
        assert_eq!(after_sync.pending_grouped_dir_syncs, 0);
        assert_eq!(after_sync.grouped_file_sync_calls, 0);
        assert_eq!(after_sync.grouped_dir_sync_calls, 0);
    }

    #[test]
    fn fast_mode_put_batch_stages_payload_files_concurrently() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store =
            FilePayloadStore::open_with_sync_mode(layout, LocalPayloadSyncMode::Fast).unwrap();
        store.set_test_staging_delay_ms(50);
        let entries = [
            PayloadBatchEntry {
                object_id: 8301,
                data: b"one",
            },
            PayloadBatchEntry {
                object_id: 8302,
                data: b"two",
            },
            PayloadBatchEntry {
                object_id: 8303,
                data: b"three",
            },
            PayloadBatchEntry {
                object_id: 8304,
                data: b"four",
            },
        ];

        store.put_batch(&entries).unwrap().complete();

        let writes = store.write_mode_snapshot();
        assert!(
            writes.max_staging_writes > 1,
            "batched payload staging should allow multiple temp-file writes in flight"
        );
        assert_eq!(store.get(8301).unwrap().as_ref(), b"one");
        assert_eq!(store.get(8304).unwrap().as_ref(), b"four");
    }

    #[test]
    fn fast_mode_put_batch_uses_bounded_staging_workers_for_large_batches() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store =
            FilePayloadStore::open_with_sync_mode(layout, LocalPayloadSyncMode::Fast).unwrap();
        let payloads = [
            b"payload-0".as_slice(),
            b"payload-1".as_slice(),
            b"payload-2".as_slice(),
            b"payload-3".as_slice(),
            b"payload-4".as_slice(),
            b"payload-5".as_slice(),
            b"payload-6".as_slice(),
            b"payload-7".as_slice(),
        ];
        let entries: Vec<_> = payloads
            .iter()
            .enumerate()
            .map(|(idx, data)| PayloadBatchEntry {
                object_id: 8350 + idx as u64,
                data,
            })
            .collect();

        store.put_batch(&entries).unwrap().complete();

        let writes = store.write_mode_snapshot();
        assert!(
            writes.staging_worker_starts <= FAST_BATCH_STAGING_MAX_CONCURRENCY as u64,
            "large batches should reuse a bounded worker set instead of spawning one thread per payload"
        );
        assert_eq!(store.get(8350).unwrap().as_ref(), b"payload-0");
        assert_eq!(store.get(8357).unwrap().as_ref(), b"payload-7");
    }

    #[test]
    fn fast_mode_put_batch_stages_temp_files_in_payload_shards() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store =
            FilePayloadStore::open_with_sync_mode(layout.clone(), LocalPayloadSyncMode::Fast)
                .unwrap();
        let prepared = PreparedBatchPayload {
            entry: PayloadBatchEntry {
                object_id: 8401,
                data: b"payload",
            },
            payload_path: layout.payload_object_path(8401),
            payload_dir: layout
                .payload_object_path(8401)
                .parent()
                .unwrap()
                .to_path_buf(),
        };
        std::fs::create_dir_all(&prepared.payload_dir).unwrap();

        let staged = store.stage_one_batch_payload(&prepared, false).unwrap();

        assert_eq!(
            staged.temp_path.parent(),
            Some(prepared.payload_dir.as_path())
        );
        assert!(staged.temp_path.exists());
    }
}

#[derive(Debug)]
struct PreparedBatchPayload<'a> {
    entry: PayloadBatchEntry<'a>,
    payload_path: PathBuf,
    payload_dir: PathBuf,
}

#[derive(Debug)]
struct StagedBatchPayload<'a> {
    entry: PayloadBatchEntry<'a>,
    payload_path: PathBuf,
    payload_dir: PathBuf,
    temp_path: PathBuf,
}
