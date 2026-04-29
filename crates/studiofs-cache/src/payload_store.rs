//! File-per-object payload storage for the current local payload runtime.
//!
//! The runtime persists payloads directly under the UUID-scoped cache root.

use std::array;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use tracing::warn;
use uuid::Uuid;

use super::layout::{
    LOCAL_PAYLOAD_BATCH_INTENT_DIR, LOCAL_PAYLOAD_BATCH_INTENT_FILE_EXTENSION, LocalPayloadLayout,
};
use super::platform;
use super::{LocalPayloadSyncMode, LocalPayloadWriteMode};

const TEMP_FILE_CREATE_ATTEMPTS: usize = 32;
const TEMP_FILE_SUFFIX: &str = ".tmp";
#[cfg(not(test))]
const READ_FD_CACHE_CAPACITY: usize = 2_048;
#[cfg(test)]
const READ_FD_CACHE_CAPACITY: usize = 16;
const READ_FD_CACHE_SHARDS: usize = 16;
const READ_HINT_MIN_LEN: usize = 512 * 1024;
const FAST_BATCH_STAGING_MAX_CONCURRENCY: usize = 4;

static TEMP_FILE_SEQ: AtomicU64 = AtomicU64::new(0);
static BATCH_INTENT_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
pub struct PayloadBatchEntry<'a> {
    pub object_id: u64,
    pub data: &'a [u8],
}

#[derive(Debug)]
#[must_use = "batch payload commit intents must be completed after the index commit succeeds"]
pub struct PayloadBatchCommit {
    intent_path: PathBuf,
}

impl PayloadBatchCommit {
    pub fn complete(self) {
        remove_batch_payload_intent(&self.intent_path);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FilePayloadStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("failed to create a unique temp payload file for object_id={object_id} in '{dir}'")]
    TempFileCreateExhausted { object_id: u64, dir: PathBuf },
}

/// File-per-object payload store rooted under the current local payload runtime
/// layout.
#[derive(Debug, Clone)]
pub struct FilePayloadStore {
    layout: LocalPayloadLayout,
    sync_mode: LocalPayloadSyncMode,
    write_mode: LocalPayloadWriteMode,
    read_handles: Arc<ReadHandleCache>,
    pending_grouped_sync: Arc<PendingGroupedSync>,
    #[cfg(test)]
    write_counters: Arc<PayloadWriteCounters>,
    barrier_counters: Arc<SyncBarrierCounters>,
}

impl FilePayloadStore {
    pub fn open(layout: LocalPayloadLayout) -> Result<Self, FilePayloadStoreError> {
        Self::open_with_sync_mode(layout, LocalPayloadSyncMode::Durable)
    }

    pub fn open_with_sync_mode(
        layout: LocalPayloadLayout,
        sync_mode: LocalPayloadSyncMode,
    ) -> Result<Self, FilePayloadStoreError> {
        Self::open_with_sync_and_write_mode(layout, sync_mode, LocalPayloadWriteMode::Buffered)
    }

    pub fn open_with_sync_and_write_mode(
        layout: LocalPayloadLayout,
        sync_mode: LocalPayloadSyncMode,
        write_mode: LocalPayloadWriteMode,
    ) -> Result<Self, FilePayloadStoreError> {
        std::fs::create_dir_all(&layout.payload_objects_dir)?;
        std::fs::create_dir_all(&layout.tmp_dir)?;
        Ok(Self {
            layout,
            sync_mode,
            write_mode,
            read_handles: Arc::new(ReadHandleCache {
                shards: array::from_fn(|_| Mutex::new(LruCache::new(read_fd_cache_shard_cap()))),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
            }),
            pending_grouped_sync: Arc::new(PendingGroupedSync::default()),
            #[cfg(test)]
            write_counters: Arc::new(PayloadWriteCounters::default()),
            barrier_counters: Arc::new(SyncBarrierCounters::default()),
        })
    }

    pub fn get(&self, object_id: u64) -> Option<Bytes> {
        let path = self.layout.payload_object_path(object_id);
        let file = self.cached_read_handle(object_id, &path).ok()?;
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
        if len > 0 {
            if let Err(error) = platform::pread_exact(file.as_ref(), &mut payload, 0) {
                warn!(
                    object_id,
                    path = %path.display(),
                    error = ?error,
                    "local payload runtime payload get failed"
                );
                return None;
            }
        }
        Some(Bytes::from(payload))
    }

    pub fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
        if out.is_empty() {
            return Some(0);
        }
        let start = u64::try_from(data_offset).ok()?;

        let path = self.layout.payload_object_path(object_id);
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

    pub fn payload_exists(&self, object_id: u64) -> io::Result<bool> {
        self.layout.payload_object_path(object_id).try_exists()
    }

    pub fn put(&self, object_id: u64, data: &[u8]) -> Result<(), FilePayloadStoreError> {
        let payload_path = self.layout.payload_object_path(object_id);
        let payload_dir = payload_path.parent().ok_or_else(|| {
            io::Error::other(format!(
                "payload path has no parent directory: {}",
                payload_path.display()
            ))
        })?;
        std::fs::create_dir_all(payload_dir)?;

        let temp_path = self.write_temp_payload_file(
            &self.layout.tmp_dir,
            object_id,
            data,
            matches!(self.sync_mode, LocalPayloadSyncMode::Durable),
        )?;
        match platform::rename_atomic(&temp_path, &payload_path) {
            Ok(()) => {}
            Err(error) if platform::is_cross_device_link_error(&error) => {
                let _ = std::fs::remove_file(&temp_path);
                // Keep rename atomic even with unusual split-mount layouts by
                // staging in the destination directory.
                let payload_local_temp = self.write_temp_payload_file(
                    payload_dir,
                    object_id,
                    data,
                    matches!(self.sync_mode, LocalPayloadSyncMode::Durable),
                )?;
                if let Err(rename_error) =
                    platform::rename_atomic(&payload_local_temp, &payload_path)
                {
                    let _ = std::fs::remove_file(&payload_local_temp);
                    return Err(FilePayloadStoreError::Io(rename_error));
                }
            }
            Err(error) => {
                let _ = std::fs::remove_file(&temp_path);
                return Err(FilePayloadStoreError::Io(error));
            }
        }
        self.invalidate_cached_read_handle(object_id);
        if matches!(self.sync_mode, LocalPayloadSyncMode::Durable) {
            self.sync_directory_put_barrier(&self.layout.tmp_dir)?;
            self.sync_directory_put_barrier(payload_dir)?;
        } else {
            self.record_grouped_sync_targets(object_id, payload_dir);
        }
        Ok(())
    }

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

        let mut renamed_object_ids = Vec::with_capacity(staged.len());
        let mut synced_dirs = HashSet::new();
        let intent = self.write_batch_payload_intent("committing", &staged, durable_sync)?;
        for idx in 0..staged.len() {
            let staged_entry = &staged[idx];
            if let Err(error) = self.commit_staged_batch_payload(staged_entry, durable_sync) {
                self.cleanup_staged_batch_payloads(&staged[idx + 1..]);
                self.remove_batch(&renamed_object_ids);
                return Err(error);
            }
            renamed_object_ids.push(staged_entry.entry.object_id);
            self.invalidate_cached_read_handle(staged_entry.entry.object_id);
            synced_dirs.insert(staged_entry.payload_dir.clone());
        }

        if durable_sync {
            if let Err(error) = self.sync_directory_put_barrier(&self.layout.tmp_dir) {
                self.remove_batch(&renamed_object_ids);
                return Err(error);
            }
            for dir in synced_dirs {
                if let Err(error) = self.sync_directory_put_barrier(&dir) {
                    self.remove_batch(&renamed_object_ids);
                    return Err(error);
                }
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
                        self.write_counters
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

    pub fn remove(&self, object_id: u64) {
        self.invalidate_cached_read_handle(object_id);
        self.pending_grouped_sync
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

    pub fn remove_batch(&self, object_ids: &[u64]) {
        for object_id in object_ids {
            self.remove(*object_id);
        }
    }

    pub fn sync_data(&self) -> Result<(), FilePayloadStoreError> {
        if matches!(self.sync_mode, LocalPayloadSyncMode::Fast) {
            let (pending_object_ids, pending_dirs) = self.take_grouped_sync_targets();
            return self.sync_pending_grouped_targets(
                pending_object_ids,
                pending_dirs,
                |object_id| self.sync_payload_file_grouped(object_id),
                |dir| self.sync_directory_grouped_barrier(dir),
            );
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
            self.write_counters
                .unbuffered_write_fallbacks
                .fetch_add(1, Ordering::Relaxed);
        }

        let (mut temp_file, temp_path) = self.create_temp_file(temp_dir, object_id)?;
        platform::write_all(&mut temp_file, data)?;
        if sync_file_data {
            platform::sync_data(&temp_file)?;
            self.barrier_counters
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
        if !platform::can_write_unbuffered_payload(data) {
            return Ok(None);
        }

        #[cfg(test)]
        self.write_counters
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

        if let Err(error) =
            platform::write_all_unbuffered_with_buffer(&mut temp_file, data, unbuffered_scratch)
        {
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
            self.barrier_counters
                .per_put_file_sync_calls
                .fetch_add(1, Ordering::Relaxed);
        }
        drop(temp_file);
        #[cfg(test)]
        self.write_counters
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
        phase: &str,
        staged: &[StagedBatchPayload<'_>],
        durable_sync: bool,
    ) -> Result<PathBuf, FilePayloadStoreError> {
        let intent_dir = self.layout.tmp_dir.join(LOCAL_PAYLOAD_BATCH_INTENT_DIR);
        std::fs::create_dir_all(&intent_dir)?;
        if durable_sync {
            self.sync_directory_put_barrier(&self.layout.tmp_dir)?;
        }
        let path = intent_dir.join(format!(
            "batch-{}.{}",
            next_batch_intent_token(),
            LOCAL_PAYLOAD_BATCH_INTENT_FILE_EXTENSION
        ));
        self.write_batch_payload_intent_phase(&path, phase, staged, durable_sync)?;
        Ok(path)
    }

    fn write_batch_payload_intent_phase(
        &self,
        path: &Path,
        phase: &str,
        staged: &[StagedBatchPayload<'_>],
        durable_sync: bool,
    ) -> Result<(), FilePayloadStoreError> {
        let mut content = String::new();
        content.push_str("version=1\n");
        content.push_str("phase=");
        content.push_str(phase);
        content.push('\n');
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
        let mut temp_file = File::create(&temp_path)?;
        platform::write_all(&mut temp_file, content.as_bytes())?;
        if durable_sync {
            platform::sync_data(&temp_file)?;
        }
        drop(temp_file);
        platform::rename_atomic(&temp_path, path)?;
        if durable_sync {
            self.sync_directory_put_barrier(parent)?;
        }
        Ok(())
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
                .write_counters
                .unbuffered_write_attempts
                .load(Ordering::Relaxed),
            unbuffered_write_successes: self
                .write_counters
                .unbuffered_write_successes
                .load(Ordering::Relaxed),
            unbuffered_write_fallbacks: self
                .write_counters
                .unbuffered_write_fallbacks
                .load(Ordering::Relaxed),
            max_staging_writes: self
                .write_counters
                .max_staging_writes
                .load(Ordering::Relaxed),
            staging_worker_starts: self
                .write_counters
                .staging_worker_starts
                .load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    fn set_test_staging_delay_ms(&self, delay_ms: u64) {
        self.write_counters
            .staging_delay_ms
            .store(delay_ms, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn enter_test_staging_probe(&self) -> TestStagingProbeGuard {
        let active = self
            .write_counters
            .active_staging_writes
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        update_max_atomic(&self.write_counters.max_staging_writes, active);
        let delay_ms = self.write_counters.staging_delay_ms.load(Ordering::Relaxed);
        if delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        }
        TestStagingProbeGuard {
            counters: Arc::clone(&self.write_counters),
        }
    }

    #[cfg(test)]
    pub(crate) fn sync_barrier_snapshot(&self) -> SyncBarrierSnapshot {
        SyncBarrierSnapshot {
            per_put_file_sync_calls: self
                .barrier_counters
                .per_put_file_sync_calls
                .load(Ordering::Relaxed),
            per_put_dir_sync_calls: self
                .barrier_counters
                .per_put_dir_sync_calls
                .load(Ordering::Relaxed),
            grouped_file_sync_calls: self
                .barrier_counters
                .grouped_file_sync_calls
                .load(Ordering::Relaxed),
            grouped_dir_sync_calls: self
                .barrier_counters
                .grouped_dir_sync_calls
                .load(Ordering::Relaxed),
            pending_grouped_file_syncs: self.pending_grouped_sync.object_ids.lock().len(),
            pending_grouped_dir_syncs: self.pending_grouped_sync.directories.lock().len(),
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

    fn open_for_read(path: &Path) -> io::Result<File> {
        match platform::open_read_only(path) {
            Ok(file) => Ok(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Err(error),
            Err(error) => Err(error),
        }
    }

    fn cached_read_handle(&self, object_id: u64, path: &Path) -> io::Result<Arc<File>> {
        let shard = read_fd_shard_idx(object_id);
        if let Some(file) = self.read_handles.shards[shard]
            .lock()
            .get(&object_id)
            .cloned()
        {
            self.read_handles.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(file);
        }

        self.read_handles.misses.fetch_add(1, Ordering::Relaxed);
        let file = Arc::new(Self::open_for_read(path)?);
        if let Err(error) = platform::advise_random(file.as_ref()) {
            warn!(
                object_id,
                path = %path.display(),
                error = ?error,
                "local payload runtime payload open advise_random failed"
            );
        }

        let mut handles = self.read_handles.shards[shard].lock();
        if let Some(existing) = handles.get(&object_id).cloned() {
            self.read_handles.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(existing);
        }
        let evicted_lru = handles.len() == handles.cap().get() && !handles.contains(&object_id);
        let _ = handles.put(object_id, Arc::clone(&file));
        if evicted_lru {
            self.read_handles.evictions.fetch_add(1, Ordering::Relaxed);
        }
        Ok(file)
    }

    fn invalidate_cached_read_handle(&self, object_id: u64) {
        self.read_handles.shards[read_fd_shard_idx(object_id)]
            .lock()
            .pop(&object_id);
    }

    fn record_grouped_sync_targets(&self, object_id: u64, payload_dir: &Path) {
        let mut object_ids = self.pending_grouped_sync.object_ids.lock();
        object_ids.insert(object_id);
        drop(object_ids);

        let mut directories = self.pending_grouped_sync.directories.lock();
        directories.insert(self.layout.tmp_dir.clone());
        directories.insert(payload_dir.to_path_buf());
        directories.insert(self.layout.payload_objects_dir.clone());
        directories.insert(self.layout.payload_root.clone());
        directories.insert(self.layout.runtime_root.clone());
    }

    fn take_grouped_sync_targets(&self) -> (Vec<u64>, Vec<PathBuf>) {
        let object_ids = {
            let mut object_ids = self.pending_grouped_sync.object_ids.lock();
            object_ids.drain().collect()
        };
        let directories = {
            let mut directories = self.pending_grouped_sync.directories.lock();
            directories.drain().collect()
        };
        (object_ids, directories)
    }

    fn requeue_grouped_sync_targets(&self, object_ids: &[u64], directories: &[PathBuf]) {
        if !object_ids.is_empty() {
            let mut pending_object_ids = self.pending_grouped_sync.object_ids.lock();
            pending_object_ids.extend(object_ids.iter().copied());
        }
        if !directories.is_empty() {
            let mut pending_directories = self.pending_grouped_sync.directories.lock();
            pending_directories.extend(directories.iter().cloned());
        }
    }

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

    fn sync_payload_file_grouped(&self, object_id: u64) -> Result<(), FilePayloadStoreError> {
        let path = self.layout.payload_object_path(object_id);
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                warn!(
                    object_id,
                    path = %path.display(),
                    "local payload runtime grouped payload sync skipped because file could not be opened for writing"
                );
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        platform::sync_data(&file)?;
        self.barrier_counters
            .grouped_file_sync_calls
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn sync_directory_put_barrier(&self, dir: &Path) -> Result<(), FilePayloadStoreError> {
        platform::sync_directory(dir)?;
        self.barrier_counters
            .per_put_dir_sync_calls
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn sync_directory_grouped_barrier(&self, dir: &Path) -> Result<(), FilePayloadStoreError> {
        match platform::sync_directory(dir) {
            Ok(()) => {
                self.barrier_counters
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
            hits: self.read_handles.hits.load(Ordering::Relaxed),
            misses: self.read_handles.misses.load(Ordering::Relaxed),
            evictions: self.read_handles.evictions.load(Ordering::Relaxed),
            cached: self
                .read_handles
                .shards
                .iter()
                .map(|shard| shard.lock().len())
                .sum(),
        }
    }
}

#[derive(Debug)]
struct ReadHandleCache {
    shards: [Mutex<LruCache<u64, Arc<File>>>; READ_FD_CACHE_SHARDS],
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct PendingGroupedSync {
    object_ids: Mutex<HashSet<u64>>,
    directories: Mutex<HashSet<PathBuf>>,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct PayloadWriteCounters {
    unbuffered_write_attempts: AtomicU64,
    unbuffered_write_successes: AtomicU64,
    unbuffered_write_fallbacks: AtomicU64,
    active_staging_writes: AtomicU64,
    max_staging_writes: AtomicU64,
    staging_worker_starts: AtomicU64,
    staging_delay_ms: AtomicU64,
}

#[derive(Debug, Default)]
struct SyncBarrierCounters {
    per_put_file_sync_calls: AtomicU64,
    per_put_dir_sync_calls: AtomicU64,
    grouped_file_sync_calls: AtomicU64,
    grouped_dir_sync_calls: AtomicU64,
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
struct TestStagingProbeGuard {
    counters: Arc<PayloadWriteCounters>,
}

#[cfg(test)]
impl Drop for TestStagingProbeGuard {
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

fn read_fd_cache_shard_cap() -> NonZeroUsize {
    let capacity = READ_FD_CACHE_CAPACITY.div_ceil(READ_FD_CACHE_SHARDS);
    NonZeroUsize::new(capacity.max(1)).expect("READ_FD_CACHE_CAPACITY must be non-zero")
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
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout.clone()).unwrap();
        store.put(42, b"payload").unwrap();
        drop(store);

        let reopened = FilePayloadStore::open(layout).unwrap();
        assert_eq!(reopened.get(42).unwrap().as_ref(), b"payload");
    }

    #[test]
    fn unbuffered_fast_mode_roundtrips_aligned_payload() {
        let temp = tempfile::tempdir().unwrap();
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
        let temp = tempfile::tempdir().unwrap();
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
    fn read_into_supports_partial_ranges() {
        let temp = tempfile::tempdir().unwrap();
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
        let temp = tempfile::tempdir().unwrap();
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
        let temp = tempfile::tempdir().unwrap();
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
        let temp = tempfile::tempdir().unwrap();
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
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();
        let capacity_to_fill = READ_FD_CACHE_CAPACITY.saturating_add(4);

        for object_id in 0..u64::try_from(capacity_to_fill).expect("cache capacity fits u64") {
            store.put(object_id, b"x").unwrap();
            let mut out = [0_u8; 1];
            assert_eq!(store.read_into(object_id, 0, &mut out), Some(1));
        }

        let stats = store.read_handle_stats();
        assert_eq!(stats.cached, READ_FD_CACHE_CAPACITY);
        assert!(stats.evictions >= 1);
    }

    #[test]
    fn durable_mode_keeps_per_put_sync_barriers() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        let store =
            FilePayloadStore::open_with_sync_mode(layout, LocalPayloadSyncMode::Durable).unwrap();
        store.put(7001, b"payload").unwrap();

        let stats = store.sync_barrier_snapshot();
        assert!(
            stats.per_put_file_sync_calls >= 1,
            "durable mode should sync temp payload bytes before rename"
        );
        assert!(
            stats.per_put_dir_sync_calls >= 2,
            "durable mode should sync tmp + payload directories per put"
        );
        assert_eq!(stats.pending_grouped_file_syncs, 0);
        assert_eq!(stats.pending_grouped_dir_syncs, 0);
    }

    #[test]
    fn fast_mode_defers_put_barriers_until_grouped_sync_data() {
        let temp = tempfile::tempdir().unwrap();
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

        store.sync_data().unwrap();
        let after_sync = store.sync_barrier_snapshot();
        assert_eq!(after_sync.pending_grouped_file_syncs, 0);
        assert_eq!(after_sync.pending_grouped_dir_syncs, 0);
        assert!(
            after_sync.grouped_file_sync_calls >= 2,
            "grouped sync_data should sync queued payload files"
        );
        assert!(
            after_sync.grouped_dir_sync_calls >= 1,
            "grouped sync_data should sync queued directories"
        );
    }

    #[test]
    fn fast_mode_requeues_only_unsynced_grouped_file_targets_after_grouped_sync_failure() {
        let temp = tempfile::tempdir().unwrap();
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
        let temp = tempfile::tempdir().unwrap();
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
    fn fast_mode_put_batch_defers_barriers_until_grouped_sync_data() {
        let temp = tempfile::tempdir().unwrap();
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
        assert!(
            after_sync.grouped_file_sync_calls >= entries.len() as u64,
            "grouped sync_data should sync all queued payload files from batched put"
        );
    }

    #[test]
    fn fast_mode_put_batch_stages_payload_files_concurrently() {
        let temp = tempfile::tempdir().unwrap();
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
        let temp = tempfile::tempdir().unwrap();
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
        let temp = tempfile::tempdir().unwrap();
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
