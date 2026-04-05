//! Active raw block file backend.
//!
//! This backend keeps the `LocalBlockBackend` contract while using a simple
//! append-only record file instead of the allocator/free-list/LRU machinery
//! used by the retired cache backend.
//!
//! Trade-offs:
//! - append-only log writes are simpler, and periodic maintenance can compact
//!   dead records, but there is still no in-place allocator or free-list reuse;
//! - the implementation is easier to reason about, but it does not provide
//!   eviction ordering or background reclamation;
//! - recovery replays a small log instead of rebuilding a richer allocator and
//!   index layout.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;

use bytes::Bytes;
use parking_lot::Mutex;
use tracing::warn;

use super::{LocalBlockBackend, LocalBlockBackendError, LocalBlockLifecycleState};
use crate::v2::platform;

const FILE_MAGIC: [u8; 8] = *b"STFSRAWF";
const RECORD_MAGIC: [u8; 8] = *b"STFSRAWR";
const FILE_VERSION: u32 = 1;
const FILE_HEADER_SIZE: usize = 32;
const RECORD_HEADER_SIZE: usize = 32;
const DATA_FILE_NAME: &str = "blocks.raw";

const RECORD_KIND_PUT: u8 = 1;
const RECORD_KIND_STATE: u8 = 2;
const RECORD_KIND_REMOVE: u8 = 3;

const STATE_DIRTY: u8 = 1;
const STATE_UPLOADING: u8 = 2;
const STATE_UPLOADED: u8 = 3;
const COMPACTION_MIN_DEAD_BYTES: u64 = 4 * 1024 * 1024;
const COMPACTION_MIN_FILE_BYTES: u64 = 8 * 1024 * 1024;
const COMPACTION_RATIO: u64 = 2;

/// Configuration for opening a [`RawBlockFileBackend`].
#[derive(Debug, Clone)]
pub struct RawBlockFileBackendConfig {
    /// Directory where the raw block file will be stored.
    pub cache_dir: PathBuf,
    /// Volume UUID used to prevent accidental reuse across mounts.
    pub volume_uuid: [u8; 16],
}

/// Errors returned when opening or repairing the raw backend file.
#[derive(Debug, thiserror::Error)]
pub enum RawBlockFileBackendError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("corrupt raw backend header: {0}")]
    CorruptHeader(String),
    #[error("volume UUID mismatch")]
    VolumeUuidMismatch,
}

#[derive(Debug, Clone, Copy)]
struct RawBlockEntry {
    data_offset: u64,
    data_len: u64,
    lifecycle: LocalBlockLifecycleState,
    evictable: bool,
}

#[derive(Debug)]
struct RawBlockFileState {
    file_len: u64,
    entries: HashMap<u64, RawBlockEntry>,
    dirty_order: VecDeque<u64>,
}

impl RawBlockFileState {
    fn new(file_len: u64) -> Self {
        Self {
            file_len,
            entries: HashMap::new(),
            dirty_order: VecDeque::new(),
        }
    }

    fn enter_dirty(&mut self, object_id: u64) {
        if let Some(entry) = self.entries.get_mut(&object_id) {
            entry.lifecycle = LocalBlockLifecycleState::Dirty;
            entry.evictable = false;
        }
        self.dirty_order.retain(|current| *current != object_id);
        if self.entries.contains_key(&object_id) {
            self.dirty_order.push_back(object_id);
        }
    }

    fn enter_uploaded(&mut self, object_id: u64) {
        if let Some(entry) = self.entries.get_mut(&object_id) {
            entry.lifecycle = LocalBlockLifecycleState::Uploaded;
            entry.evictable = true;
        }
        self.dirty_order.retain(|current| *current != object_id);
    }

    fn enter_uploading(&mut self, object_id: u64) {
        if let Some(entry) = self.entries.get_mut(&object_id) {
            entry.lifecycle = LocalBlockLifecycleState::Uploading;
            entry.evictable = false;
        }
        if self.entries.contains_key(&object_id)
            && !self.dirty_order.iter().any(|current| *current == object_id)
        {
            self.dirty_order.push_back(object_id);
        }
    }

    fn remove_entry(&mut self, object_id: u64) {
        self.entries.remove(&object_id);
        self.dirty_order.retain(|current| *current != object_id);
    }

    fn compacted_len(&self) -> u64 {
        let live_records = self
            .entries
            .values()
            .map(|entry| RECORD_HEADER_SIZE as u64 + entry.data_len)
            .sum::<u64>();
        FILE_HEADER_SIZE as u64 + live_records
    }

    fn should_compact(&self) -> bool {
        let compacted_len = self.compacted_len();
        let dead_bytes = self.file_len.saturating_sub(compacted_len);
        self.file_len >= COMPACTION_MIN_FILE_BYTES
            && dead_bytes >= COMPACTION_MIN_DEAD_BYTES
            && self.file_len >= compacted_len.saturating_mul(COMPACTION_RATIO)
    }
}

/// Simple append-only raw block file backend.
///
/// This backend intentionally favors straightforward durability and replay over
/// sophisticated local-space management.
#[derive(Debug)]
pub struct RawBlockFileBackend {
    path: PathBuf,
    volume_uuid: [u8; 16],
    file: Mutex<File>,
    state: Mutex<RawBlockFileState>,
}

impl RawBlockFileBackend {
    /// Open or create the backend file.
    pub fn open(config: RawBlockFileBackendConfig) -> Result<Self, RawBlockFileBackendError> {
        std::fs::create_dir_all(&config.cache_dir)?;
        let path = config.cache_dir.join(DATA_FILE_NAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let file_len = file.metadata()?.len();
        if file_len == 0 {
            write_file_header(&file, config.volume_uuid)?;
            platform::fdatasync(&file)?;
            let state = RawBlockFileState::new(FILE_HEADER_SIZE as u64);
            return Ok(Self {
                path,
                volume_uuid: config.volume_uuid,
                file: Mutex::new(file),
                state: Mutex::new(state),
            });
        }

        let volume_uuid = read_file_header(&file, file_len)?;
        if volume_uuid != config.volume_uuid {
            return Err(RawBlockFileBackendError::VolumeUuidMismatch);
        }

        let state = load_state(&file, file_len)?;
        Ok(Self {
            path,
            volume_uuid: config.volume_uuid,
            file: Mutex::new(file),
            state: Mutex::new(state),
        })
    }

    fn append_record(
        &self,
        state: &mut RawBlockFileState,
        header: [u8; RECORD_HEADER_SIZE],
        body: Option<&[u8]>,
    ) -> Result<u64, LocalBlockBackendError> {
        let offset = state.file_len;
        let file = self.file.lock();
        if let Some(body) = body {
            platform::pwrite(&file, body, offset + RECORD_HEADER_SIZE as u64)?;
            platform::fdatasync(&file)?;
        }
        platform::pwrite(&file, &header, offset)?;
        let body_len = body.map_or(0_u64, |bytes| bytes.len() as u64);
        let new_len = offset + RECORD_HEADER_SIZE as u64 + body_len;
        file.set_len(new_len)?;
        platform::fdatasync(&file)?;
        state.file_len = new_len;
        Ok(offset)
    }

    fn read_payload(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
        let state = self.state.lock();
        let entry = *state.entries.get(&object_id)?;
        let start = u64::try_from(data_offset).ok()?;
        if start >= entry.data_len {
            return Some(0);
        }
        let available = entry.data_len - start;
        let to_read = available.min(out.len() as u64) as usize;
        if to_read == 0 {
            return Some(0);
        }
        let offset = entry.data_offset + start;
        let file = self.file.lock();
        if let Err(error) = platform::pread(&file, &mut out[..to_read], offset) {
            warn!(
                object_id,
                data_offset,
                error = ?error,
                "raw backend read failed"
            );
            return None;
        }
        Some(to_read)
    }

    fn compact_path(&self) -> PathBuf {
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| format!("{name}.compact"))
            .unwrap_or_else(|| "blocks.raw.compact".to_string());
        self.path.with_file_name(file_name)
    }

    fn prune_dirty_order(&self, state: &mut RawBlockFileState) {
        let mut seen = HashSet::new();
        let mut pruned = VecDeque::with_capacity(state.dirty_order.len());
        for object_id in state.dirty_order.drain(..) {
            if !seen.insert(object_id) {
                continue;
            }
            if matches!(
                state.entries.get(&object_id).map(|entry| entry.lifecycle),
                Some(LocalBlockLifecycleState::Dirty | LocalBlockLifecycleState::Uploading)
            ) {
                pruned.push_back(object_id);
            }
        }
        state.dirty_order = pruned;
    }

    fn compact_locked(&self, state: &mut RawBlockFileState) -> Result<(), LocalBlockBackendError> {
        self.prune_dirty_order(state);

        let mut ordered_object_ids = Vec::with_capacity(state.entries.len());
        let mut seen = HashSet::new();
        for object_id in &state.dirty_order {
            if state.entries.contains_key(object_id) && seen.insert(*object_id) {
                ordered_object_ids.push(*object_id);
            }
        }

        let mut remaining: Vec<u64> = state
            .entries
            .keys()
            .copied()
            .filter(|object_id| seen.insert(*object_id))
            .collect();
        remaining.sort_unstable();
        ordered_object_ids.extend(remaining);

        let temp_path = self.compact_path();
        let temp_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        write_file_header(&temp_file, self.volume_uuid)
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;

        let file = self.file.lock();
        let mut new_state = RawBlockFileState::new(FILE_HEADER_SIZE as u64);
        for object_id in ordered_object_ids {
            let entry = *state.entries.get(&object_id).ok_or_else(|| {
                LocalBlockBackendError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    "entry disappeared during raw backend compaction",
                ))
            })?;
            let payload_len = usize::try_from(entry.data_len).map_err(|_| {
                LocalBlockBackendError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raw backend entry too large to compact on this platform",
                ))
            })?;
            let mut payload = vec![0_u8; payload_len];
            if !payload.is_empty() {
                platform::pread(&file, &mut payload, entry.data_offset)?;
            }

            let offset = new_state.file_len;
            let data_offset = offset + RECORD_HEADER_SIZE as u64;
            let header = encode_record_header(
                RECORD_KIND_PUT,
                entry.lifecycle,
                entry.evictable,
                object_id,
                entry.data_len,
            );
            if !payload.is_empty() {
                platform::pwrite(&temp_file, &payload, data_offset)?;
            }
            platform::pwrite(&temp_file, &header, offset)?;
            new_state.file_len = data_offset + entry.data_len;
            new_state.entries.insert(
                object_id,
                RawBlockEntry {
                    data_offset,
                    data_len: entry.data_len,
                    lifecycle: entry.lifecycle,
                    evictable: entry.evictable,
                },
            );
            if matches!(
                entry.lifecycle,
                LocalBlockLifecycleState::Dirty | LocalBlockLifecycleState::Uploading
            ) {
                new_state.dirty_order.push_back(object_id);
            }
        }
        drop(file);

        temp_file.set_len(new_state.file_len)?;
        platform::fdatasync(&temp_file)?;

        let mut current_file = self.file.lock();
        let old_file = std::mem::replace(&mut *current_file, temp_file);
        drop(current_file);
        drop(old_file);

        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        std::fs::rename(&temp_path, &self.path)?;

        *state = new_state;
        Ok(())
    }
}

impl LocalBlockBackend for RawBlockFileBackend {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get(&self, object_id: u64) -> Option<Bytes> {
        let state = self.state.lock();
        let entry = *state.entries.get(&object_id)?;
        let len = usize::try_from(entry.data_len).ok()?;
        let mut buf = vec![0_u8; len];
        if len > 0 {
            let file = self.file.lock();
            if let Err(error) = platform::pread(&file, &mut buf, entry.data_offset) {
                warn!(object_id, error = ?error, "raw backend get failed");
                return None;
            }
        }
        Some(Bytes::from(buf))
    }

    fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
        self.read_payload(object_id, data_offset, out)
    }

    fn get_for_upload(&self, object_id: u64) -> Option<Bytes> {
        self.get(object_id)
    }

    fn contains(&self, object_id: u64) -> bool {
        self.state.lock().entries.contains_key(&object_id)
    }

    fn advise_will_need(&self, _object_id: u64) {}

    fn put(
        &self,
        object_id: u64,
        data: &[u8],
        evictable: bool,
    ) -> Result<(), LocalBlockBackendError> {
        let data_len = u64::try_from(data.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "block too large for raw backend format",
            )
        })?;
        let mut state = self.state.lock();
        let offset = state.file_len;
        let data_offset = offset + RECORD_HEADER_SIZE as u64;
        let lifecycle = if evictable {
            LocalBlockLifecycleState::Uploaded
        } else {
            LocalBlockLifecycleState::Dirty
        };
        let header =
            encode_record_header(RECORD_KIND_PUT, lifecycle, evictable, object_id, data_len);
        self.append_record(&mut state, header, Some(data))?;

        let entry = RawBlockEntry {
            data_offset,
            data_len,
            lifecycle,
            evictable,
        };
        state.entries.insert(object_id, entry);
        match lifecycle {
            LocalBlockLifecycleState::Dirty => state.enter_dirty(object_id),
            LocalBlockLifecycleState::Uploading => state.enter_uploading(object_id),
            LocalBlockLifecycleState::Uploaded => state.enter_uploaded(object_id),
            LocalBlockLifecycleState::Missing => {}
        }
        Ok(())
    }

    fn sync_data(&self) -> Result<(), LocalBlockBackendError> {
        self.file.lock().sync_all()?;
        Ok(())
    }

    fn run_pressure_maintenance(&self) -> Result<(), LocalBlockBackendError> {
        let mut state = self.state.lock();
        if state.should_compact() {
            self.compact_locked(&mut state)?;
        } else {
            self.prune_dirty_order(&mut state);
        }
        Ok(())
    }

    fn mark_uploading(&self, object_id: u64) {
        let mut state = self.state.lock();
        let should_mark = state
            .entries
            .get(&object_id)
            .is_some_and(|entry| entry.lifecycle == LocalBlockLifecycleState::Dirty);
        if should_mark {
            state.enter_uploading(object_id);
        }
    }

    fn mark_upload_failed(&self, object_id: u64) {
        let mut state = self.state.lock();
        let should_requeue = {
            let Some(entry) = state.entries.get_mut(&object_id) else {
                return;
            };
            if entry.lifecycle == LocalBlockLifecycleState::Uploading {
                entry.lifecycle = LocalBlockLifecycleState::Dirty;
                entry.evictable = false;
                true
            } else {
                false
            }
        };
        if should_requeue {
            state.enter_dirty(object_id);
        }
    }

    fn mark_uploaded(&self, object_id: u64) {
        let mut state = self.state.lock();
        if !state.entries.contains_key(&object_id) {
            return;
        }
        let header = encode_record_header(
            RECORD_KIND_STATE,
            LocalBlockLifecycleState::Uploaded,
            true,
            object_id,
            0,
        );
        if let Err(error) = self.append_record(&mut state, header, None) {
            warn!(object_id, error = ?error, "raw backend mark_uploaded write failed");
            return;
        }
        state.enter_uploaded(object_id);
    }

    fn remove(&self, object_id: u64) {
        let mut state = self.state.lock();
        if !state.entries.contains_key(&object_id) {
            return;
        }
        let header = encode_record_header(
            RECORD_KIND_REMOVE,
            LocalBlockLifecycleState::Missing,
            false,
            object_id,
            0,
        );
        if let Err(error) = self.append_record(&mut state, header, None) {
            warn!(object_id, error = ?error, "raw backend remove write failed");
            return;
        }
        state.remove_entry(object_id);
    }

    fn remove_batch(&self, object_ids: &[u64]) {
        let mut state = self.state.lock();
        for object_id in object_ids {
            if !state.entries.contains_key(object_id) {
                continue;
            }
            let header = encode_record_header(
                RECORD_KIND_REMOVE,
                LocalBlockLifecycleState::Missing,
                false,
                *object_id,
                0,
            );
            if let Err(error) = self.append_record(&mut state, header, None) {
                warn!(object_id, error = ?error, "raw backend remove_batch write failed");
                continue;
            }
            state.remove_entry(*object_id);
        }
    }

    fn dirty_object_ids(&self) -> Vec<u64> {
        let state = self.state.lock();
        let mut dirty_ids = Vec::new();
        for object_id in &state.dirty_order {
            if matches!(
                state.entries.get(object_id).map(|entry| entry.lifecycle),
                Some(LocalBlockLifecycleState::Dirty | LocalBlockLifecycleState::Uploading)
            ) {
                dirty_ids.push(*object_id);
            }
        }
        dirty_ids
    }

    fn lifecycle_state(&self, object_id: u64) -> LocalBlockLifecycleState {
        self.state
            .lock()
            .entries
            .get(&object_id)
            .map(|entry| entry.lifecycle)
            .unwrap_or(LocalBlockLifecycleState::Missing)
    }

    fn is_evictable(&self, object_id: u64) -> bool {
        self.state
            .lock()
            .entries
            .get(&object_id)
            .is_some_and(|entry| {
                entry.lifecycle == LocalBlockLifecycleState::Uploaded && entry.evictable
            })
    }
}

impl Drop for RawBlockFileBackend {
    fn drop(&mut self) {
        if let Err(error) = self.file.lock().sync_all() {
            warn!(error = ?error, "raw backend drop: sync_all failed");
        }
    }
}

fn load_state(file: &File, file_len: u64) -> Result<RawBlockFileState, RawBlockFileBackendError> {
    let mut state = RawBlockFileState::new(file_len);
    let mut cursor = FILE_HEADER_SIZE as u64;
    let mut header = [0_u8; RECORD_HEADER_SIZE];

    while cursor + RECORD_HEADER_SIZE as u64 <= file_len {
        if let Err(error) = platform::pread(file, &mut header, cursor) {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(error.into());
        }
        let Some(record) = decode_record_header(&header) else {
            warn!(
                offset = cursor,
                "raw backend scan stopped at invalid tail record"
            );
            break;
        };

        match record.kind {
            RECORD_KIND_PUT => {
                let data_offset = cursor + RECORD_HEADER_SIZE as u64;
                if data_offset + record.data_len > file_len {
                    warn!(
                        offset = cursor,
                        object_id = record.object_id,
                        "raw backend scan stopped at truncated put record"
                    );
                    break;
                }
                let lifecycle =
                    decode_state(record.state).unwrap_or(LocalBlockLifecycleState::Dirty);
                let entry = RawBlockEntry {
                    data_offset,
                    data_len: record.data_len,
                    lifecycle,
                    evictable: record.evictable,
                };
                state.entries.insert(record.object_id, entry);
                match lifecycle {
                    LocalBlockLifecycleState::Dirty => state.enter_dirty(record.object_id),
                    LocalBlockLifecycleState::Uploading => state.enter_uploading(record.object_id),
                    LocalBlockLifecycleState::Uploaded => state.enter_uploaded(record.object_id),
                    LocalBlockLifecycleState::Missing => {}
                }
                cursor = data_offset + record.data_len;
            }
            RECORD_KIND_STATE => {
                let Some(lifecycle) = decode_state(record.state) else {
                    warn!(
                        offset = cursor,
                        "raw backend scan stopped at invalid state record"
                    );
                    break;
                };
                if state.entries.contains_key(&record.object_id) {
                    match lifecycle {
                        LocalBlockLifecycleState::Dirty => state.enter_dirty(record.object_id),
                        LocalBlockLifecycleState::Uploading => {
                            state.enter_uploading(record.object_id)
                        }
                        LocalBlockLifecycleState::Uploaded => {
                            state.enter_uploaded(record.object_id)
                        }
                        LocalBlockLifecycleState::Missing => {}
                    }
                }
                cursor += RECORD_HEADER_SIZE as u64;
            }
            RECORD_KIND_REMOVE => {
                state.remove_entry(record.object_id);
                cursor += RECORD_HEADER_SIZE as u64;
            }
            _ => {
                warn!(
                    offset = cursor,
                    kind = record.kind,
                    "raw backend scan stopped at unknown record kind"
                );
                break;
            }
        }
    }

    Ok(state)
}

fn write_file_header(file: &File, volume_uuid: [u8; 16]) -> Result<(), RawBlockFileBackendError> {
    let mut header = [0_u8; FILE_HEADER_SIZE];
    header[0..8].copy_from_slice(&FILE_MAGIC);
    header[8..12].copy_from_slice(&FILE_VERSION.to_le_bytes());
    header[12..28].copy_from_slice(&volume_uuid);
    platform::pwrite(file, &header, 0)?;
    platform::fdatasync(file)?;
    Ok(())
}

fn read_file_header(file: &File, file_len: u64) -> Result<[u8; 16], RawBlockFileBackendError> {
    if file_len < FILE_HEADER_SIZE as u64 {
        return Err(RawBlockFileBackendError::CorruptHeader(
            "file shorter than raw backend header".to_string(),
        ));
    }
    let mut header = [0_u8; FILE_HEADER_SIZE];
    platform::pread(file, &mut header, 0)?;
    if header[0..8] != FILE_MAGIC {
        return Err(RawBlockFileBackendError::CorruptHeader(
            "magic mismatch".to_string(),
        ));
    }
    let mut version_bytes = [0_u8; 4];
    version_bytes.copy_from_slice(&header[8..12]);
    let version = u32::from_le_bytes(version_bytes);
    if version != FILE_VERSION {
        return Err(RawBlockFileBackendError::CorruptHeader(format!(
            "version mismatch: expected {FILE_VERSION}, found {version}"
        )));
    }
    let mut volume_uuid = [0_u8; 16];
    volume_uuid.copy_from_slice(&header[12..28]);
    Ok(volume_uuid)
}

#[derive(Debug, Clone, Copy)]
struct RecordHeader {
    kind: u8,
    state: u8,
    evictable: bool,
    object_id: u64,
    data_len: u64,
}

fn encode_record_header(
    kind: u8,
    state: LocalBlockLifecycleState,
    evictable: bool,
    object_id: u64,
    data_len: u64,
) -> [u8; RECORD_HEADER_SIZE] {
    let mut header = [0_u8; RECORD_HEADER_SIZE];
    header[0..8].copy_from_slice(&RECORD_MAGIC);
    header[8..12].copy_from_slice(&FILE_VERSION.to_le_bytes());
    header[12] = kind;
    header[13] = encode_state(state);
    header[14] = u8::from(evictable);
    header[15] = 0;
    header[16..24].copy_from_slice(&object_id.to_le_bytes());
    header[24..32].copy_from_slice(&data_len.to_le_bytes());
    header
}

fn decode_record_header(bytes: &[u8; RECORD_HEADER_SIZE]) -> Option<RecordHeader> {
    if bytes[0..8] != RECORD_MAGIC {
        return None;
    }
    let mut version_bytes = [0_u8; 4];
    version_bytes.copy_from_slice(&bytes[8..12]);
    let version = u32::from_le_bytes(version_bytes);
    if version != FILE_VERSION {
        return None;
    }
    let mut object_id_bytes = [0_u8; 8];
    object_id_bytes.copy_from_slice(&bytes[16..24]);
    let mut data_len_bytes = [0_u8; 8];
    data_len_bytes.copy_from_slice(&bytes[24..32]);
    Some(RecordHeader {
        kind: bytes[12],
        state: bytes[13],
        evictable: bytes[14] != 0,
        object_id: u64::from_le_bytes(object_id_bytes),
        data_len: u64::from_le_bytes(data_len_bytes),
    })
}

fn encode_state(state: LocalBlockLifecycleState) -> u8 {
    match state {
        LocalBlockLifecycleState::Missing => 0,
        LocalBlockLifecycleState::Dirty => STATE_DIRTY,
        LocalBlockLifecycleState::Uploading => STATE_UPLOADING,
        LocalBlockLifecycleState::Uploaded => STATE_UPLOADED,
    }
}

fn decode_state(state: u8) -> Option<LocalBlockLifecycleState> {
    match state {
        STATE_DIRTY => Some(LocalBlockLifecycleState::Dirty),
        STATE_UPLOADING => Some(LocalBlockLifecycleState::Uploading),
        STATE_UPLOADED => Some(LocalBlockLifecycleState::Uploaded),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::v2::local_block_backend::contract_tests::assert_local_block_backend_contract;

    fn test_config(dir: &TempDir) -> RawBlockFileBackendConfig {
        RawBlockFileBackendConfig {
            cache_dir: dir.path().to_path_buf(),
            volume_uuid: [0xAB; 16],
        }
    }

    #[test]
    fn local_block_backend_contract() {
        assert_local_block_backend_contract(|| {
            let dir = TempDir::new().unwrap();
            let backend = RawBlockFileBackend::open(test_config(&dir)).unwrap();
            (backend, dir)
        });
    }

    #[test]
    fn reopen_preserves_uploaded_state() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        {
            let backend = RawBlockFileBackend::open(config.clone()).unwrap();
            backend.put(7, b"payload", false).unwrap();
            backend.mark_uploaded(7);
            backend.sync_data().unwrap();
        }

        let backend = RawBlockFileBackend::open(config).unwrap();
        assert_eq!(
            backend.lifecycle_state(7),
            LocalBlockLifecycleState::Uploaded
        );
        assert!(backend.is_evictable(7));
        assert_eq!(backend.get(7).unwrap().as_ref(), b"payload");
    }

    #[test]
    fn dirty_object_ids_dedupes_repeated_dirty_updates() {
        let dir = TempDir::new().unwrap();
        let backend = RawBlockFileBackend::open(test_config(&dir)).unwrap();

        backend.put(1, b"one", false).unwrap();
        backend.put(2, b"two", false).unwrap();
        backend.put(1, b"one-again", false).unwrap();

        assert_eq!(backend.dirty_object_ids(), vec![2, 1]);
    }

    #[test]
    fn pressure_maintenance_compacts_dead_records() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = RawBlockFileBackend::open(config.clone()).unwrap();
        let payload = vec![0xAB; 256 * 1024];
        let path = dir.path().join(DATA_FILE_NAME);

        for _ in 0..40 {
            backend.put(42, &payload, false).unwrap();
            backend.mark_uploaded(42);
            backend.remove(42);
        }

        let before = std::fs::metadata(&path).unwrap().len();
        assert!(before >= COMPACTION_MIN_FILE_BYTES);

        backend.run_pressure_maintenance().unwrap();

        let after = std::fs::metadata(&path).unwrap().len();
        assert!(
            after < before,
            "maintenance should compact the raw backend log"
        );

        let reopened = RawBlockFileBackend::open(config).unwrap();
        assert_eq!(
            reopened.lifecycle_state(42),
            LocalBlockLifecycleState::Missing
        );
    }

    #[test]
    fn rejects_mismatched_volume_uuid() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = RawBlockFileBackend::open(config).unwrap();
        drop(backend);

        let err = RawBlockFileBackend::open(RawBlockFileBackendConfig {
            cache_dir: dir.path().to_path_buf(),
            volume_uuid: [0xCD; 16],
        })
        .unwrap_err();
        assert!(matches!(err, RawBlockFileBackendError::VolumeUuidMismatch));
    }
}
