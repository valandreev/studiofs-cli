use crate::vfs::{VfsError, VfsResult};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use studiofs_meta::Ino;

pub(crate) const INITIAL_INGRESS_SEGMENT_ID: u64 = 1;
const INGRESS_SEGMENT_MAGIC: [u8; 4] = *b"ISG1";
const INGRESS_STATE_MAGIC: [u8; 4] = *b"IWS1";
const INGRESS_CHECKPOINT_MAGIC: [u8; 4] = *b"IWC1";
const INGRESS_RECORD_PUT: u8 = 1;
const INGRESS_RECORD_INVALIDATE: u8 = 2;
const INGRESS_RECORD_HEADER_LEN: usize = 37;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IngressRecordKind {
    Put = INGRESS_RECORD_PUT,
    Invalidate = INGRESS_RECORD_INVALIDATE,
}

impl TryFrom<u8> for IngressRecordKind {
    type Error = VfsError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            INGRESS_RECORD_PUT => Ok(Self::Put),
            INGRESS_RECORD_INVALIDATE => Ok(Self::Invalidate),
            _ => Err(VfsError::Other(
                "unknown ingress segment record kind".into(),
            )),
        }
    }
}

impl IngressRecordKind {
    fn as_byte(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpillEntry {
    pub(crate) seq: u64,
    pub(crate) path: PathBuf,
    pub(crate) offset: u64,
    pub(crate) len: usize,
    pub(crate) created_unix_ms: u64,
}

#[derive(Debug, Default)]
pub(crate) struct IngressLiveIndex {
    entries: HashMap<(Ino, u32), SpillEntry>,
    by_ino_counts: HashMap<Ino, usize>,
    live_segment_counts: HashMap<u64, usize>,
}

impl IngressLiveIndex {
    pub(crate) fn from_entries(entries: HashMap<(Ino, u32), SpillEntry>) -> Self {
        let mut by_ino_counts = HashMap::new();
        let mut live_segment_counts = HashMap::new();
        for (ino, _) in entries.keys() {
            *by_ino_counts.entry(*ino).or_insert(0) += 1;
        }
        for entry in entries.values() {
            if let Some(segment_id) = parse_ingress_segment_id(&entry.path) {
                *live_segment_counts.entry(segment_id).or_insert(0) += 1;
            }
        }
        Self {
            entries,
            by_ino_counts,
            live_segment_counts,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn get_cloned(&self, key: &(Ino, u32)) -> Option<SpillEntry> {
        self.entries.get(key).cloned()
    }

    pub(crate) fn insert(&mut self, key: (Ino, u32), entry: SpillEntry) -> Option<SpillEntry> {
        let new_segment_id = parse_ingress_segment_id(&entry.path);
        let old = self.entries.insert(key, entry);
        if old.is_none() {
            *self.by_ino_counts.entry(key.0).or_insert(0) += 1;
        }
        if let Some(old) = old.as_ref() {
            self.dec_segment(old);
        }
        if let Some(segment_id) = new_segment_id {
            *self.live_segment_counts.entry(segment_id).or_insert(0) += 1;
        }
        old
    }

    pub(crate) fn remove(&mut self, key: &(Ino, u32)) -> Option<SpillEntry> {
        let removed = self.entries.remove(key);
        if removed.is_some() {
            let ino = key.0;
            let count = self.by_ino_counts.get(&ino).copied().unwrap_or(0);
            if count <= 1 {
                self.by_ino_counts.remove(&ino);
            } else {
                self.by_ino_counts.insert(ino, count - 1);
            }
            if let Some(entry) = removed.as_ref() {
                self.dec_segment(entry);
            }
        }
        removed
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &(Ino, u32)> {
        self.entries.keys()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&(Ino, u32), &SpillEntry)> {
        self.entries.iter()
    }

    pub(crate) fn live_segment_count(&self) -> u64 {
        self.live_segment_counts.len() as u64
    }

    pub(crate) fn has_ino(&self, ino: Ino) -> bool {
        self.by_ino_counts.contains_key(&ino)
    }

    fn dec_segment(&mut self, entry: &SpillEntry) {
        let Some(segment_id) = parse_ingress_segment_id(&entry.path) else {
            return;
        };
        let count = self
            .live_segment_counts
            .get(&segment_id)
            .copied()
            .unwrap_or(0);
        if count <= 1 {
            self.live_segment_counts.remove(&segment_id);
        } else {
            self.live_segment_counts.insert(segment_id, count - 1);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IngressWal {
    spill_dir: PathBuf,
    segment_dir: PathBuf,
    checkpoint_dir: PathBuf,
    max_segment_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct AppendOutcome {
    pub(crate) offset: u64,
    pub(crate) path: PathBuf,
    pub(crate) record_bytes: u64,
}

impl IngressWal {
    pub(crate) fn new(spill_dir: PathBuf, max_segment_bytes: u64) -> Self {
        Self {
            segment_dir: spill_dir.join("segments"),
            checkpoint_dir: spill_dir.join("checkpoints"),
            spill_dir,
            max_segment_bytes,
        }
    }

    pub(crate) fn spill_dir(&self) -> &Path {
        &self.spill_dir
    }

    pub(crate) fn segment_dir(&self) -> &Path {
        &self.segment_dir
    }

    pub(crate) fn checkpoint_dir(&self) -> &Path {
        &self.checkpoint_dir
    }

    pub(crate) fn max_segment_bytes(&self) -> u64 {
        self.max_segment_bytes
    }

    pub(crate) fn append(
        &self,
        active_segment_id: &AtomicU64,
        key: (Ino, u32),
        seq: u64,
        created_unix_ms: u64,
        kind: IngressRecordKind,
        payload: &[u8],
    ) -> VfsResult<AppendOutcome> {
        let record_bytes = INGRESS_RECORD_HEADER_LEN as u64 + payload.len() as u64;
        let mut segment_id = active_segment_id.load(Ordering::Relaxed);
        let active_path = active_ingress_segment_path(self.segment_dir(), segment_id);
        let active_len = std::fs::metadata(&active_path)
            .map(|meta| meta.len())
            .unwrap_or(0);
        if active_len > 0 && active_len.saturating_add(record_bytes) > self.max_segment_bytes() {
            segment_id = segment_id.saturating_add(1);
            active_segment_id.store(segment_id, Ordering::Relaxed);
        }
        let offset = append_ingress_record(
            self.segment_dir(),
            segment_id,
            key,
            seq,
            created_unix_ms,
            kind,
            payload,
        )?;
        Ok(AppendOutcome {
            offset,
            path: ingress_segment_path(self.segment_dir(), segment_id),
            record_bytes,
        })
    }
}

#[derive(Debug, Clone)]
struct RecoveredIngressRecord {
    seq: u64,
    path: PathBuf,
    offset: u64,
    len: usize,
    created_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngressCheckpointEntry {
    ino: Ino,
    block_index: u32,
    seq: u64,
    segment_id: u64,
    offset: u64,
    len: usize,
    created_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngressCheckpointFile {
    version: u32,
    covered_segment_id: u64,
    covered_offset: u64,
    next_seq: u64,
    entries: Vec<IngressCheckpointEntry>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IngressCheckpointMeta {
    pub(crate) covered_segment_id: u64,
    pub(crate) covered_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngressStateFile {
    version: u32,
    active_segment_id: u64,
    next_seq: u64,
}

type RecoveredIngressMap = HashMap<(Ino, u32), RecoveredIngressRecord>;
pub(crate) type RecoveredSpillMap = HashMap<(Ino, u32), SpillEntry>;
pub(crate) type RecoveredSpillState = (RecoveredSpillMap, usize, u64, u64);

pub(crate) fn read_ingress_entry_bytes(entry: &SpillEntry) -> VfsResult<Bytes> {
    let mut file = std::fs::File::open(&entry.path)?;
    file.seek(SeekFrom::Start(entry.offset))?;
    let mut buf = vec![0u8; entry.len];
    file.read_exact(&mut buf)?;
    Ok(Bytes::from(buf))
}

pub(crate) fn ingress_segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("{segment_id:016x}.seg"))
}

pub(crate) fn list_ingress_segment_files(segment_dir: &Path) -> VfsResult<Vec<(u64, PathBuf)>> {
    let mut files: Vec<(u64, PathBuf)> = match std::fs::read_dir(segment_dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                if !path.is_file() || path.extension().is_none_or(|ext| ext != "seg") {
                    return None;
                }
                let id = parse_ingress_segment_id(&path)?;
                Some((id, path))
            })
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(VfsError::Io(e)),
    };
    files.sort_unstable_by_key(|(id, _)| *id);
    Ok(files)
}

pub(crate) fn current_active_ingress_segment_id(segment_dir: &Path) -> VfsResult<u64> {
    let files = list_ingress_segment_files(segment_dir)?;
    Ok(files
        .last()
        .map(|(id, _)| *id)
        .unwrap_or(INITIAL_INGRESS_SEGMENT_ID))
}

pub(crate) fn active_ingress_segment_path(dir: &Path, active_segment_id: u64) -> PathBuf {
    ingress_segment_path(dir, active_segment_id)
}

pub(crate) fn latest_ingress_checkpoint_path(dir: &Path) -> PathBuf {
    dir.join("latest.json")
}

pub(crate) fn ingress_state_path(spill_dir: &Path) -> PathBuf {
    spill_dir.join("state.json")
}

pub(crate) fn parse_ingress_segment_id(path: &Path) -> Option<u64> {
    let stem = path.file_stem()?.to_str()?;
    u64::from_str_radix(stem, 16).ok()
}

fn load_latest_ingress_checkpoint(
    checkpoint_dir: &Path,
    segment_dir: &Path,
) -> VfsResult<Option<IngressCheckpointFile>> {
    let path = latest_ingress_checkpoint_path(checkpoint_dir);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(VfsError::Io(e)),
    };
    let checkpoint: IngressCheckpointFile = if bytes.starts_with(&INGRESS_CHECKPOINT_MAGIC) {
        postcard::from_bytes(&bytes[INGRESS_CHECKPOINT_MAGIC.len()..])
            .map_err(|e| VfsError::Other(format!("invalid ingress checkpoint: {e}")))?
    } else {
        serde_json::from_slice(&bytes)
            .map_err(|e| VfsError::Other(format!("invalid ingress checkpoint: {e}")))?
    };
    if checkpoint.version != 1 {
        return Err(VfsError::Other(format!(
            "unsupported ingress checkpoint version {}",
            checkpoint.version
        )));
    }
    for entry in &checkpoint.entries {
        let path = ingress_segment_path(segment_dir, entry.segment_id);
        if !path.exists() {
            return Err(VfsError::Other(format!(
                "missing ingress segment {:016x} referenced by checkpoint",
                entry.segment_id
            )));
        }
    }
    Ok(Some(checkpoint))
}

pub(crate) fn load_latest_ingress_checkpoint_meta(
    checkpoint_dir: &Path,
    segment_dir: &Path,
) -> VfsResult<Option<IngressCheckpointMeta>> {
    let checkpoint = load_latest_ingress_checkpoint(checkpoint_dir, segment_dir)?;
    Ok(checkpoint.map(|checkpoint| IngressCheckpointMeta {
        covered_segment_id: checkpoint.covered_segment_id,
        covered_offset: checkpoint.covered_offset,
    }))
}

fn load_ingress_state(spill_dir: &Path) -> VfsResult<Option<IngressStateFile>> {
    let path = ingress_state_path(spill_dir);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(VfsError::Io(e)),
    };
    let state: IngressStateFile = if bytes.starts_with(&INGRESS_STATE_MAGIC) {
        postcard::from_bytes(&bytes[INGRESS_STATE_MAGIC.len()..])
            .map_err(|e| VfsError::Other(format!("invalid ingress state: {e}")))?
    } else {
        serde_json::from_slice(&bytes)
            .map_err(|e| VfsError::Other(format!("invalid ingress state: {e}")))?
    };
    if state.version != 1 {
        return Err(VfsError::Other(format!(
            "unsupported ingress state version {}",
            state.version
        )));
    }
    Ok(Some(state))
}

pub(crate) fn write_ingress_state(
    spill_dir: &Path,
    active_segment_id: u64,
    next_seq: u64,
) -> VfsResult<()> {
    std::fs::create_dir_all(spill_dir)?;
    let state = IngressStateFile {
        version: 1,
        active_segment_id,
        next_seq,
    };
    let tmp = spill_dir.join("state.json.tmp");
    let path = ingress_state_path(spill_dir);
    let mut bytes = Vec::from(INGRESS_STATE_MAGIC);
    let payload = postcard::to_stdvec(&state)
        .map_err(|e| VfsError::Other(format!("encode ingress state failed: {e}")))?;
    bytes.extend_from_slice(&payload);
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

pub(crate) fn write_ingress_checkpoint(
    checkpoint_dir: &Path,
    ingress_live: &IngressLiveIndex,
    segment_dir: &Path,
    active_segment_id: u64,
    next_seq: u64,
) -> VfsResult<IngressCheckpointMeta> {
    std::fs::create_dir_all(checkpoint_dir)?;
    let active_segment_path = active_ingress_segment_path(segment_dir, active_segment_id);
    let covered_offset = std::fs::metadata(&active_segment_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let mut entries = Vec::with_capacity(ingress_live.len());
    for (&(ino, block_index), entry) in ingress_live.iter() {
        let Some(segment_id) = parse_ingress_segment_id(&entry.path) else {
            continue;
        };
        entries.push(IngressCheckpointEntry {
            ino,
            block_index,
            seq: entry.seq,
            segment_id,
            offset: entry.offset,
            len: entry.len,
            created_unix_ms: entry.created_unix_ms,
        });
    }
    let checkpoint = IngressCheckpointFile {
        version: 1,
        covered_segment_id: active_segment_id,
        covered_offset,
        next_seq,
        entries,
    };
    let tmp = checkpoint_dir.join("latest.json.tmp");
    let path = latest_ingress_checkpoint_path(checkpoint_dir);
    let mut bytes = Vec::from(INGRESS_CHECKPOINT_MAGIC);
    let payload = postcard::to_stdvec(&checkpoint)
        .map_err(|e| VfsError::Other(format!("encode ingress checkpoint failed: {e}")))?;
    bytes.extend_from_slice(&payload);
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, path)?;
    let live_segment_ids: HashSet<u64> = checkpoint
        .entries
        .iter()
        .map(|entry| entry.segment_id)
        .collect();
    for (segment_id, path) in list_ingress_segment_files(segment_dir)? {
        if segment_id < checkpoint.covered_segment_id && !live_segment_ids.contains(&segment_id) {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(IngressCheckpointMeta {
        covered_segment_id: checkpoint.covered_segment_id,
        covered_offset: checkpoint.covered_offset,
    })
}

pub(crate) fn append_ingress_record(
    segment_dir: &Path,
    segment_id: u64,
    key: (Ino, u32),
    seq: u64,
    created_unix_ms: u64,
    kind: IngressRecordKind,
    payload: &[u8],
) -> VfsResult<u64> {
    std::fs::create_dir_all(segment_dir)?;
    let path = ingress_segment_path(segment_dir, segment_id);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?;
    let start = file.seek(SeekFrom::End(0))?;
    file.write_all(&INGRESS_SEGMENT_MAGIC)?;
    file.write_all(&[kind.as_byte()])?;
    file.write_all(&key.0.to_le_bytes())?;
    file.write_all(&key.1.to_le_bytes())?;
    file.write_all(&seq.to_le_bytes())?;
    file.write_all(&created_unix_ms.to_le_bytes())?;
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    if !payload.is_empty() {
        file.write_all(payload)?;
    }
    // Durable mode relies on PUT records surviving power loss once
    // `flush_fh()` observes ingress drain completion. Invalidate records are
    // recovery optimizations and can be checkpointed asynchronously.
    if kind == IngressRecordKind::Put {
        file.sync_data()?;
    }
    Ok(start + INGRESS_RECORD_HEADER_LEN as u64)
}

fn replay_ingress_segment_records(
    segment_dir: &Path,
    recovered: &mut RecoveredIngressMap,
    next_seq: &mut u64,
    resume_after: Option<(u64, u64)>,
) -> VfsResult<()> {
    for (id, path) in list_ingress_segment_files(segment_dir)? {
        if let Some((covered_segment_id, covered_offset)) = resume_after {
            if id < covered_segment_id {
                continue;
            }
            if id == covered_segment_id && covered_offset > 0 {
                let file_len = std::fs::metadata(&path)?.len();
                if covered_offset >= file_len {
                    continue;
                }
            }
        }
        let mut file = std::fs::File::open(&path)?;
        if let Some((covered_segment_id, covered_offset)) = resume_after
            && id == covered_segment_id
            && covered_offset > 0
        {
            file.seek(SeekFrom::Start(covered_offset))?;
        }
        loop {
            let record_start = file.stream_position()?;
            let mut header = [0u8; INGRESS_RECORD_HEADER_LEN];
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(VfsError::Io(e)),
            }
            if header[0..4] != INGRESS_SEGMENT_MAGIC {
                return Err(VfsError::Other("invalid ingress segment magic".into()));
            }
            let kind = IngressRecordKind::try_from(header[4])?;
            let ino = u64::from_le_bytes(header[5..13].try_into().unwrap());
            let block_index = u32::from_le_bytes(header[13..17].try_into().unwrap());
            let seq = u64::from_le_bytes(header[17..25].try_into().unwrap());
            let created_unix_ms = u64::from_le_bytes(header[25..33].try_into().unwrap());
            let payload_len = u32::from_le_bytes(header[33..37].try_into().unwrap()) as usize;
            *next_seq = (*next_seq).max(seq.saturating_add(1));
            let payload_offset = record_start + INGRESS_RECORD_HEADER_LEN as u64;
            if payload_len > 0 {
                file.seek(SeekFrom::Current(payload_len as i64))?;
            }
            let key = (ino, block_index);
            match kind {
                IngressRecordKind::Put => {
                    let should_replace = recovered
                        .get(&key)
                        .map(|current| seq >= current.seq)
                        .unwrap_or(true);
                    if should_replace {
                        recovered.insert(
                            key,
                            RecoveredIngressRecord {
                                seq,
                                path: path.clone(),
                                offset: payload_offset,
                                len: payload_len,
                                created_unix_ms,
                            },
                        );
                    }
                }
                IngressRecordKind::Invalidate => {
                    let should_replace = recovered
                        .get(&key)
                        .map(|current| seq >= current.seq)
                        .unwrap_or(true);
                    if should_replace {
                        recovered.insert(
                            key,
                            RecoveredIngressRecord {
                                seq,
                                path: path.clone(),
                                offset: payload_offset,
                                len: 0,
                                created_unix_ms,
                            },
                        );
                    }
                }
            }
        }
    }

    recovered.retain(|_, record| record.len > 0);
    Ok(())
}

#[cfg(test)]
fn recover_ingress_segment_records(segment_dir: &Path) -> VfsResult<(RecoveredIngressMap, u64)> {
    let mut recovered = RecoveredIngressMap::new();
    let mut next_seq = 1u64;
    replay_ingress_segment_records(segment_dir, &mut recovered, &mut next_seq, None)?;
    Ok((recovered, next_seq))
}

pub(crate) fn recover_spill_dir(spill_dir: &Path) -> VfsResult<RecoveredSpillState> {
    let segment_dir = spill_dir.join("segments");
    let checkpoint_dir = spill_dir.join("checkpoints");
    let mut recovered = RecoveredIngressMap::new();
    let mut next_seq = 1u64;
    let mut active_segment_id = INITIAL_INGRESS_SEGMENT_ID;
    let mut resume_after = None;

    if let Some(state) = load_ingress_state(spill_dir)? {
        next_seq = next_seq.max(state.next_seq);
        active_segment_id = active_segment_id.max(state.active_segment_id);
    }

    if let Some(checkpoint) = load_latest_ingress_checkpoint(&checkpoint_dir, &segment_dir)? {
        next_seq = next_seq.max(checkpoint.next_seq);
        resume_after = Some((checkpoint.covered_segment_id, checkpoint.covered_offset));
        for entry in checkpoint.entries {
            next_seq = next_seq.max(entry.seq.saturating_add(1));
            recovered.insert(
                (entry.ino, entry.block_index),
                RecoveredIngressRecord {
                    seq: entry.seq,
                    path: ingress_segment_path(&segment_dir, entry.segment_id),
                    offset: entry.offset,
                    len: entry.len,
                    created_unix_ms: entry.created_unix_ms,
                },
            );
        }
    }
    replay_ingress_segment_records(&segment_dir, &mut recovered, &mut next_seq, resume_after)?;

    let mut total_bytes = 0usize;
    let mut spill_entries = RecoveredSpillMap::new();
    for (key, record) in recovered {
        next_seq = next_seq.max(record.seq.saturating_add(1));
        let spill = SpillEntry {
            seq: record.seq,
            path: record.path,
            offset: record.offset,
            len: record.len,
            created_unix_ms: record.created_unix_ms,
        };
        total_bytes = total_bytes.saturating_add(spill.len);
        spill_entries.insert(key, spill);
    }

    active_segment_id = active_segment_id.max(current_active_ingress_segment_id(&segment_dir)?);
    Ok((spill_entries, total_bytes, next_seq, active_segment_id))
}

pub(crate) fn reset_ingress_artifacts_for_fast_mode(spill_dir: &Path) -> VfsResult<()> {
    std::fs::create_dir_all(spill_dir)?;
    let segment_dir = spill_dir.join("segments");
    let checkpoint_dir = spill_dir.join("checkpoints");

    if let Err(e) = std::fs::remove_dir_all(&segment_dir)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(VfsError::Io(e));
    }
    if let Err(e) = std::fs::remove_dir_all(&checkpoint_dir)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(VfsError::Io(e));
    }
    std::fs::create_dir_all(&segment_dir)?;
    std::fs::create_dir_all(&checkpoint_dir)?;

    if let Err(e) = std::fs::remove_file(ingress_state_path(spill_dir))
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(VfsError::Io(e));
    }

    for entry in std::fs::read_dir(spill_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "spill") {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(())
}

pub(crate) fn scan_ingress_artifacts(spill_dir: &Path) -> VfsResult<(usize, usize, bool)> {
    let segment_dir = spill_dir.join("segments");
    let checkpoint_dir = spill_dir.join("checkpoints");
    let segment_count = list_ingress_segment_files(&segment_dir)?.len();
    let checkpoint_present = latest_ingress_checkpoint_path(&checkpoint_dir).exists();
    let state_present = ingress_state_path(spill_dir).exists();
    Ok((
        segment_count,
        usize::from(checkpoint_present),
        state_present,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};

    #[test]
    fn test_recover_spill_dir_reads_segment_backed_entries() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            1,
            100,
            IngressRecordKind::Put,
            b"newer-data",
        )
        .unwrap();
        append_ingress_record(
            &segment_dir,
            1,
            (42, 8),
            3,
            300,
            IngressRecordKind::Put,
            b"other",
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();

        assert_eq!(recovered.len(), 2);
        let first = recovered.get(&(42, 7)).unwrap();
        let second = recovered.get(&(42, 8)).unwrap();
        let mut file = std::fs::File::open(&first.path).unwrap();
        file.seek(SeekFrom::Start(first.offset)).unwrap();
        let mut buf = vec![0u8; first.len];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"newer-data");
        let mut file = std::fs::File::open(&second.path).unwrap();
        file.seek(SeekFrom::Start(second.offset)).unwrap();
        let mut buf = vec![0u8; second.len];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"other");
        assert_eq!(total_bytes, "newer-data".len() + "other".len());
        assert_eq!(next_seq, 4);
        assert_eq!(active_segment_id, 1);
        assert!(recovered.get(&(42, 7)).unwrap().created_unix_ms > 0);
    }

    #[test]
    fn test_read_ingress_entry_bytes_reads_segment_slice() {
        let dir = tempfile::tempdir().unwrap();
        let segment_dir = dir.path().join("segments");
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            1,
            100,
            IngressRecordKind::Put,
            b"first",
        )
        .unwrap();
        let second_offset = append_ingress_record(
            &segment_dir,
            1,
            (42, 8),
            2,
            200,
            IngressRecordKind::Put,
            b"second-payload",
        )
        .unwrap();
        let segment_path = active_ingress_segment_path(&segment_dir, 1);

        let bytes = read_ingress_entry_bytes(&SpillEntry {
            seq: 2,
            path: segment_path,
            offset: second_offset,
            len: "second-payload".len(),
            created_unix_ms: 200,
        })
        .unwrap();

        assert_eq!(bytes.as_ref(), b"second-payload");
    }

    #[test]
    fn test_recover_ingress_segment_records_keeps_latest_put() {
        let dir = tempfile::tempdir().unwrap();
        let segment_dir = dir.path().join("segments");
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            1,
            100,
            IngressRecordKind::Put,
            b"old",
        )
        .unwrap();
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            2,
            200,
            IngressRecordKind::Put,
            b"newer",
        )
        .unwrap();
        append_ingress_record(
            &segment_dir,
            1,
            (42, 8),
            3,
            300,
            IngressRecordKind::Put,
            b"other",
        )
        .unwrap();

        let (recovered, next_seq) = recover_ingress_segment_records(&segment_dir).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(next_seq, 4);
        let newer = recovered.get(&(42, 7)).unwrap();
        let other = recovered.get(&(42, 8)).unwrap();
        let mut file = std::fs::File::open(&newer.path).unwrap();
        file.seek(SeekFrom::Start(newer.offset)).unwrap();
        let mut buf = vec![0u8; newer.len];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"newer");
        let mut file = std::fs::File::open(&other.path).unwrap();
        file.seek(SeekFrom::Start(other.offset)).unwrap();
        let mut buf = vec![0u8; other.len];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"other");
    }

    #[test]
    fn test_recover_ingress_segment_records_applies_invalidate() {
        let dir = tempfile::tempdir().unwrap();
        let segment_dir = dir.path().join("segments");
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            1,
            100,
            IngressRecordKind::Put,
            b"payload",
        )
        .unwrap();
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            2,
            200,
            IngressRecordKind::Invalidate,
            &[],
        )
        .unwrap();

        let (recovered, next_seq) = recover_ingress_segment_records(&segment_dir).unwrap();
        assert_eq!(next_seq, 3);
        assert!(
            !recovered.contains_key(&(42, 7)),
            "invalidate should remove the latest accepted record from replay view"
        );
    }

    #[test]
    fn test_recover_spill_dir_rebuilds_from_segments_without_spill_files() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            5,
            123,
            IngressRecordKind::Put,
            b"segment-data",
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(total_bytes, "segment-data".len());
        assert_eq!(next_seq, 6);
        assert_eq!(active_segment_id, 1);
        let entry = recovered.get(&(42, 7)).unwrap();
        let mut file = std::fs::File::open(&entry.path).unwrap();
        file.seek(SeekFrom::Start(entry.offset)).unwrap();
        let mut buf = vec![0u8; entry.len];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"segment-data");
    }

    #[test]
    fn test_recover_spill_dir_replays_only_segment_tail_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        let checkpoint_dir = spill_dir.join("checkpoints");
        let first_offset = append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            1,
            100,
            IngressRecordKind::Put,
            b"first",
        )
        .unwrap();
        let mut ingress_live = IngressLiveIndex::default();
        ingress_live.insert(
            (42, 7),
            SpillEntry {
                seq: 1,
                path: active_ingress_segment_path(&segment_dir, 1),
                offset: first_offset,
                len: "first".len(),
                created_unix_ms: 100,
            },
        );
        write_ingress_checkpoint(&checkpoint_dir, &ingress_live, &segment_dir, 1, 2).unwrap();

        append_ingress_record(
            &segment_dir,
            1,
            (42, 8),
            2,
            200,
            IngressRecordKind::Put,
            b"second",
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(total_bytes, "first".len() + "second".len());
        assert_eq!(next_seq, 3);
        assert_eq!(active_segment_id, 1);
        assert_eq!(
            read_ingress_entry_bytes(recovered.get(&(42, 7)).unwrap()).unwrap(),
            "first"
        );
        assert_eq!(
            read_ingress_entry_bytes(recovered.get(&(42, 8)).unwrap()).unwrap(),
            "second"
        );
    }

    #[test]
    fn test_recover_spill_dir_applies_tail_invalidate_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        let checkpoint_dir = spill_dir.join("checkpoints");
        let first_offset = append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            7,
            100,
            IngressRecordKind::Put,
            b"payload",
        )
        .unwrap();
        let mut ingress_live = IngressLiveIndex::default();
        ingress_live.insert(
            (42, 7),
            SpillEntry {
                seq: 7,
                path: active_ingress_segment_path(&segment_dir, 1),
                offset: first_offset,
                len: "payload".len(),
                created_unix_ms: 100,
            },
        );
        write_ingress_checkpoint(&checkpoint_dir, &ingress_live, &segment_dir, 1, 8).unwrap();
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            8,
            200,
            IngressRecordKind::Invalidate,
            &[],
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();
        assert!(recovered.is_empty());
        assert_eq!(total_bytes, 0);
        assert_eq!(next_seq, 9);
        assert_eq!(active_segment_id, 1);
    }

    #[test]
    fn test_write_ingress_checkpoint_prunes_fully_covered_old_segments() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        let checkpoint_dir = spill_dir.join("checkpoints");
        append_ingress_record(
            &segment_dir,
            1,
            (1, 0),
            1,
            100,
            IngressRecordKind::Put,
            b"old",
        )
        .unwrap();
        let new_offset = append_ingress_record(
            &segment_dir,
            2,
            (1, 1),
            2,
            200,
            IngressRecordKind::Put,
            b"new",
        )
        .unwrap();
        let mut ingress_live = IngressLiveIndex::default();
        ingress_live.insert(
            (1, 1),
            SpillEntry {
                seq: 2,
                path: ingress_segment_path(&segment_dir, 2),
                offset: new_offset,
                len: 3,
                created_unix_ms: 200,
            },
        );

        write_ingress_checkpoint(&checkpoint_dir, &ingress_live, &segment_dir, 2, 3).unwrap();

        assert!(!ingress_segment_path(&segment_dir, 1).exists());
        assert!(ingress_segment_path(&segment_dir, 2).exists());
    }

    #[test]
    fn test_recover_spill_dir_uses_state_file_for_writer_head() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        write_ingress_state(spill_dir, 5, 11).unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();

        assert!(recovered.is_empty());
        assert_eq!(total_bytes, 0);
        assert_eq!(next_seq, 11);
        assert_eq!(active_segment_id, 5);
    }

    #[test]
    fn test_recover_spill_dir_accepts_legacy_json_state_file() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        std::fs::create_dir_all(spill_dir).unwrap();
        std::fs::write(
            ingress_state_path(spill_dir),
            br#"{"version":1,"active_segment_id":7,"next_seq":13}"#,
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();

        assert!(recovered.is_empty());
        assert_eq!(total_bytes, 0);
        assert_eq!(next_seq, 13);
        assert_eq!(active_segment_id, 7);
    }

    #[test]
    fn test_recover_spill_dir_prefers_replayed_seq_over_stale_state_file() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        write_ingress_state(spill_dir, 1, 2).unwrap();
        append_ingress_record(
            &segment_dir,
            3,
            (42, 7),
            9,
            100,
            IngressRecordKind::Put,
            b"payload",
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();

        assert_eq!(recovered.len(), 1);
        assert_eq!(total_bytes, "payload".len());
        assert_eq!(next_seq, 10);
        assert_eq!(active_segment_id, 3);
    }

    #[test]
    fn test_recover_spill_dir_uses_checkpoint_entry_seq_when_head_is_stale() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        let checkpoint_dir = spill_dir.join("checkpoints");

        let offset = append_ingress_record(
            &segment_dir,
            2,
            (42, 7),
            9,
            100,
            IngressRecordKind::Put,
            b"payload",
        )
        .unwrap();
        let mut ingress_live = IngressLiveIndex::default();
        ingress_live.insert(
            (42, 7),
            SpillEntry {
                seq: 9,
                path: ingress_segment_path(&segment_dir, 2),
                offset,
                len: "payload".len(),
                created_unix_ms: 100,
            },
        );
        // Intentionally stale next_seq to simulate an inconsistent checkpoint head.
        write_ingress_checkpoint(&checkpoint_dir, &ingress_live, &segment_dir, 2, 2).unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();

        assert_eq!(recovered.len(), 1);
        assert_eq!(total_bytes, "payload".len());
        assert_eq!(next_seq, 10);
        assert_eq!(active_segment_id, 2);
    }

    #[test]
    fn test_reset_ingress_artifacts_for_fast_mode_cleans_recovery_files() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        let checkpoint_dir = spill_dir.join("checkpoints");

        append_ingress_record(
            &segment_dir,
            1,
            (7, 1),
            1,
            42,
            IngressRecordKind::Put,
            b"payload",
        )
        .unwrap();
        write_ingress_state(spill_dir, 1, 2).unwrap();
        std::fs::create_dir_all(&checkpoint_dir).unwrap();
        std::fs::write(checkpoint_dir.join("latest.json"), br#"{"version":1}"#).unwrap();
        std::fs::write(spill_dir.join("legacy.spill"), b"legacy").unwrap();

        reset_ingress_artifacts_for_fast_mode(spill_dir).unwrap();

        assert!(segment_dir.exists());
        assert!(checkpoint_dir.exists());
        assert!(list_ingress_segment_files(&segment_dir).unwrap().is_empty());
        assert!(!ingress_state_path(spill_dir).exists());
        assert!(!spill_dir.join("legacy.spill").exists());
    }

    #[test]
    fn test_live_segment_count_is_updated_incrementally() {
        let mut idx = IngressLiveIndex::default();
        let base = tempfile::tempdir().unwrap();
        let seg1 = ingress_segment_path(base.path(), 1);
        let seg2 = ingress_segment_path(base.path(), 2);
        let seg3 = ingress_segment_path(base.path(), 3);

        idx.insert(
            (10, 0),
            SpillEntry {
                seq: 1,
                path: seg1.clone(),
                offset: 0,
                len: 1,
                created_unix_ms: 1,
            },
        );
        idx.insert(
            (10, 1),
            SpillEntry {
                seq: 2,
                path: seg1,
                offset: 0,
                len: 1,
                created_unix_ms: 2,
            },
        );
        assert_eq!(idx.live_segment_count(), 1);

        idx.insert(
            (11, 0),
            SpillEntry {
                seq: 3,
                path: seg2,
                offset: 0,
                len: 1,
                created_unix_ms: 3,
            },
        );
        assert_eq!(idx.live_segment_count(), 2);

        idx.insert(
            (11, 0),
            SpillEntry {
                seq: 4,
                path: seg3,
                offset: 0,
                len: 1,
                created_unix_ms: 4,
            },
        );
        assert_eq!(idx.live_segment_count(), 2);

        idx.remove(&(10, 0));
        assert_eq!(idx.live_segment_count(), 2);
        idx.remove(&(10, 1));
        assert_eq!(idx.live_segment_count(), 1);
        idx.remove(&(11, 0));
        assert_eq!(idx.live_segment_count(), 0);
    }
}
