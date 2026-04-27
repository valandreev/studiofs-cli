use bytes::Bytes;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const DATA_FILE: &str = "landing.data";
const MANIFEST_FILE: &str = "landing.manifest";
const MANIFEST_TMP_FILE: &str = "landing.manifest.tmp";
const MANIFEST_BACKUP_FILE: &str = "landing.manifest.bak";
const DEFAULT_MAX_QUEUED_WRITES: usize = 1024;
const DEFAULT_MAX_QUEUED_BYTES: usize = 256 * 1024 * 1024;
#[cfg(windows)]
const LANDING_FILE_SHARE_MODE: u32 = 0x00000001 | 0x00000002 | 0x00000004;

#[allow(dead_code)]
pub(super) struct LandingWrite {
    pub(super) ino: u64,
    pub(super) fh: u64,
    pub(super) offset: u64,
    pub(super) data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RecoveredLandingSegment {
    pub(super) ino: u64,
    pub(super) fh: u64,
    pub(super) offset: u64,
    pub(super) len: u64,
    pub(super) data_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LandingRecoverySegment {
    pub(super) segment: RecoveredLandingSegment,
    pub(super) data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct LandingZoneStats {
    pub(super) accepted_writes: u64,
    pub(super) accepted_bytes: u64,
    pub(super) recoverable_segments: usize,
    pub(super) accept_total_ns: u64,
    pub(super) accept_max_ns: u64,
    pub(super) data_syncs: u64,
    pub(super) manifest_syncs: u64,
    pub(super) manifest_rewrites: u64,
    pub(super) sync_total_ns: u64,
    pub(super) sync_max_ns: u64,
    pub(super) background_write_total_ns: u64,
    pub(super) background_write_max_ns: u64,
}

#[derive(Debug, Clone, Copy)]
struct LandingSegment {
    ino: u64,
    fh: u64,
    offset: u64,
    len: u64,
    data_offset: u64,
}

impl LandingSegment {
    fn as_manifest_record(self) -> String {
        format!(
            "{} {} {} {} {}\n",
            self.ino, self.fh, self.offset, self.len, self.data_offset
        )
    }

    fn parse(record: &str) -> io::Result<Self> {
        let mut fields = record.split_whitespace();
        let ino = parse_manifest_u64(fields.next(), "ino")?;
        let fh = parse_manifest_u64(fields.next(), "fh")?;
        let offset = parse_manifest_u64(fields.next(), "offset")?;
        let len = parse_manifest_u64(fields.next(), "len")?;
        let data_offset = parse_manifest_u64(fields.next(), "data_offset")?;
        if fields.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "landing manifest record has extra fields",
            ));
        }
        Ok(Self {
            ino,
            fh,
            offset,
            len,
            data_offset,
        })
    }
}

fn parse_manifest_u64(value: Option<&str>, field: &str) -> io::Result<u64> {
    value
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("landing manifest record missing {field}"),
            )
        })?
        .parse::<u64>()
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("landing manifest {field} is not a u64: {err}"),
            )
        })
}

pub(super) struct LandingZone {
    data_path: PathBuf,
    manifest_path: PathBuf,
    manifest_tmp_path: PathBuf,
    manifest_backup_path: PathBuf,
    files: Arc<Mutex<LandingFiles>>,
    writer: Mutex<Option<JoinHandle<()>>>,
    writer_state: Arc<LandingWriterShared>,
    counters: Arc<LandingZoneCounters>,
    #[allow(dead_code)]
    queue_limits: LandingQueueLimits,
}

#[derive(Default)]
struct LandingZoneCounters {
    accepted_writes: AtomicU64,
    accepted_bytes: AtomicU64,
    recoverable_segments: AtomicUsize,
    accept_total_ns: AtomicU64,
    accept_max_ns: AtomicU64,
    data_syncs: AtomicU64,
    manifest_syncs: AtomicU64,
    manifest_rewrites: AtomicU64,
    sync_total_ns: AtomicU64,
    sync_max_ns: AtomicU64,
    background_write_total_ns: AtomicU64,
    background_write_max_ns: AtomicU64,
}

struct LandingFiles {
    data: File,
    manifest: Option<File>,
    segments: Vec<LandingSegment>,
}

#[derive(Debug, Clone, Copy)]
struct LandingQueueLimits {
    #[allow(dead_code)]
    max_writes: usize,
    #[allow(dead_code)]
    max_bytes: usize,
}

impl Default for LandingQueueLimits {
    fn default() -> Self {
        Self {
            max_writes: DEFAULT_MAX_QUEUED_WRITES,
            max_bytes: DEFAULT_MAX_QUEUED_BYTES,
        }
    }
}

struct QueuedLandingWrite {
    ino: u64,
    fh: u64,
    offset: u64,
    data: Bytes,
}

impl QueuedLandingWrite {
    fn len(&self) -> usize {
        self.data.len()
    }
}

#[derive(Default)]
struct LandingWriterState {
    queue: VecDeque<QueuedLandingWrite>,
    queued_bytes: usize,
    active: bool,
    shutdown: bool,
    error: Option<String>,
}

impl LandingWriterState {
    #[allow(dead_code)]
    fn can_accept(&self, write_len: usize, limits: LandingQueueLimits) -> bool {
        if self.queue.len() >= limits.max_writes.max(1) {
            return false;
        }
        self.queued_bytes
            .checked_add(write_len)
            .is_some_and(|next| next <= limits.max_bytes)
            || self.queue.is_empty()
    }

    #[allow(dead_code)]
    fn push_queued(&mut self, write: QueuedLandingWrite) {
        self.queued_bytes = self.queued_bytes.saturating_add(write.len());
        self.queue.push_back(write);
    }

    fn pop_batch(&mut self) -> Option<Vec<QueuedLandingWrite>> {
        let queued = self.queue.pop_front()?;
        self.queued_bytes = self.queued_bytes.saturating_sub(queued.len());
        let mut batch = Vec::with_capacity(self.queue.len() + 1);
        batch.push(queued);
        for queued in self.queue.drain(..) {
            self.queued_bytes = self.queued_bytes.saturating_sub(queued.len());
            batch.push(queued);
        }
        self.active = true;
        Some(batch)
    }

    fn restore_failed_batch(&mut self, failed_batch: Vec<QueuedLandingWrite>, error: String) {
        for queued in failed_batch.into_iter().rev() {
            self.queued_bytes = self.queued_bytes.saturating_add(queued.len());
            self.queue.push_front(queued);
        }
        self.error = Some(error);
    }
}

#[derive(Default)]
struct LandingWriterShared {
    state: Mutex<LandingWriterState>,
    changed: Condvar,
}

impl std::fmt::Debug for LandingZone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LandingZone")
            .field("data_path", &self.data_path)
            .field("manifest_path", &self.manifest_path)
            .finish_non_exhaustive()
    }
}

impl LandingZone {
    pub(super) fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_queue_limits(root, LandingQueueLimits::default())
    }

    fn open_with_queue_limits(
        root: impl AsRef<Path>,
        queue_limits: LandingQueueLimits,
    ) -> io::Result<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let data_path = root.join(DATA_FILE);
        let manifest_path = root.join(MANIFEST_FILE);
        let manifest_tmp_path = root.join(MANIFEST_TMP_FILE);
        let manifest_backup_path = root.join(MANIFEST_BACKUP_FILE);
        Self::repair_manifest_swap_files(
            &manifest_path,
            &manifest_tmp_path,
            &manifest_backup_path,
        )?;
        let data_file = open_landing_append_file(&data_path)?;
        let manifest_file = open_landing_append_file(&manifest_path)?;
        let initial_segments = Self::read_manifest_segments(&manifest_path)?;
        let recoverable_segments = initial_segments.len();
        let files = Arc::new(Mutex::new(LandingFiles {
            data: data_file,
            manifest: Some(manifest_file),
            segments: initial_segments,
        }));
        let writer_state = Arc::new(LandingWriterShared::default());
        let counters = Arc::new(LandingZoneCounters {
            recoverable_segments: AtomicUsize::new(recoverable_segments),
            ..LandingZoneCounters::default()
        });
        let writer = Some(Self::spawn_writer(
            manifest_path.clone(),
            files.clone(),
            writer_state.clone(),
            counters.clone(),
        )?);
        Ok(Self {
            data_path,
            manifest_path,
            manifest_tmp_path,
            manifest_backup_path,
            files,
            writer: Mutex::new(writer),
            writer_state,
            counters,
            queue_limits,
        })
    }

    #[allow(dead_code)]
    pub(super) fn accept(&self, write: LandingWrite) -> io::Result<()> {
        let accept_started = Instant::now();
        let write_len = write.data.len();
        let len = u64::try_from(write.data.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "landing write length exceeds u64::MAX",
            )
        })?;
        {
            let mut state = self
                .writer_state
                .state
                .lock()
                .map_err(|_| io::Error::other("landing writer lock poisoned"))?;
            if let Some(error) = state.error.as_ref() {
                return Err(io::Error::other(format!(
                    "landing writer previously failed: {error}"
                )));
            }
            while !state.can_accept(write_len, self.queue_limits) {
                state = self
                    .writer_state
                    .changed
                    .wait(state)
                    .map_err(|_| io::Error::other("landing writer lock poisoned"))?;
                if let Some(error) = state.error.as_ref() {
                    return Err(io::Error::other(format!(
                        "landing writer previously failed: {error}"
                    )));
                }
            }
            state.push_queued(QueuedLandingWrite {
                ino: write.ino,
                fh: write.fh,
                offset: write.offset,
                data: write.data,
            });
            self.writer_state.changed.notify_one();
        }

        self.counters
            .accepted_writes
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .accepted_bytes
            .fetch_add(len, Ordering::Relaxed);
        self.record_accept_latency(accept_started.elapsed());
        Ok(())
    }

    #[allow(dead_code)]
    pub(super) fn read(&self, ino: u64, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        self.drain_pending()?;
        let requested_end = offset.saturating_add(len as u64);
        let mut result = vec![0; len];
        let segments = self
            .files
            .lock()
            .map_err(|_| io::Error::other("landing files lock poisoned"))?
            .segments
            .clone();
        let mut data = open_landing_read_file(&self.data_path)?;
        for segment in segments {
            if segment.ino != ino {
                continue;
            }
            let segment_end = segment.offset.saturating_add(segment.len);
            let overlap_start = offset.max(segment.offset);
            let overlap_end = requested_end.min(segment_end);
            if overlap_start >= overlap_end {
                continue;
            }
            let dst_start =
                usize::try_from(overlap_start.saturating_sub(offset)).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "landing read dst offset overflow",
                    )
                })?;
            let copy_len =
                usize::try_from(overlap_end.saturating_sub(overlap_start)).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "landing read length overflow")
                })?;
            let source_offset = segment
                .data_offset
                .saturating_add(overlap_start.saturating_sub(segment.offset));
            data.seek(SeekFrom::Start(source_offset))?;
            data.read_exact(&mut result[dst_start..dst_start + copy_len])?;
        }
        Ok(result)
    }

    #[cfg(test)]
    pub(super) fn recover(&self) -> io::Result<Vec<RecoveredLandingSegment>> {
        self.drain_pending()?;
        let segments = self
            .files
            .lock()
            .map_err(|_| io::Error::other("landing files lock poisoned"))?
            .segments
            .clone();
        Ok(segments.into_iter().map(Into::into).collect())
    }

    pub(super) fn recover_with_data(&self) -> io::Result<Vec<LandingRecoverySegment>> {
        self.drain_pending()?;
        let segments = self
            .files
            .lock()
            .map_err(|_| io::Error::other("landing files lock poisoned"))?
            .segments
            .clone();
        let mut data = open_landing_read_file(&self.data_path)?;
        segments
            .into_iter()
            .map(|segment| {
                let len = usize::try_from(segment.len).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "landing segment length overflow",
                    )
                })?;
                let mut payload = vec![0; len];
                data.seek(SeekFrom::Start(segment.data_offset))?;
                data.read_exact(&mut payload)?;
                Ok(LandingRecoverySegment {
                    segment: segment.into(),
                    data: payload,
                })
            })
            .collect()
    }

    pub(super) fn clear_inode(&self, ino: u64) -> io::Result<()> {
        self.drain_pending()?;
        let mut files = self
            .files
            .lock()
            .map_err(|_| io::Error::other("landing files lock poisoned"))?;
        let segments = files.segments.clone();
        let retained: Vec<_> = segments
            .iter()
            .copied()
            .filter(|segment| segment.ino != ino)
            .collect();
        let retained_count = retained.len();
        if retained_count == segments.len() {
            self.counters
                .recoverable_segments
                .store(retained_count, Ordering::Relaxed);
            return Ok(());
        }
        let retained_empty = retained.is_empty();
        self.sync_landing_unlocked(&files).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!("landing sync before retire failed: {err}"),
            )
        })?;
        self.rewrite_manifest_unlocked(&retained, &mut files.manifest)
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("landing manifest rewrite failed: {err}"),
                )
            })?;
        files.segments = retained;
        if retained_empty {
            files.data.set_len(0).map_err(|err| {
                io::Error::new(err.kind(), format!("landing data truncate failed: {err}"))
            })?;
        }
        self.counters
            .recoverable_segments
            .store(retained_count, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn clear_inodes(&self, inos: &std::collections::BTreeSet<u64>) -> io::Result<()> {
        if inos.is_empty() {
            return Ok(());
        }
        self.drain_pending()?;
        let mut files = self
            .files
            .lock()
            .map_err(|_| io::Error::other("landing files lock poisoned"))?;
        let segments = files.segments.clone();
        let retained: Vec<_> = segments
            .iter()
            .copied()
            .filter(|segment| !inos.contains(&segment.ino))
            .collect();
        let retained_count = retained.len();
        if retained_count == segments.len() {
            self.counters
                .recoverable_segments
                .store(retained_count, Ordering::Relaxed);
            return Ok(());
        }
        let retained_empty = retained.is_empty();
        self.sync_landing_unlocked(&files).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!("landing sync before retire failed: {err}"),
            )
        })?;
        self.rewrite_manifest_unlocked(&retained, &mut files.manifest)
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("landing manifest rewrite failed: {err}"),
                )
            })?;
        files.segments = retained;
        if retained_empty {
            files.data.set_len(0).map_err(|err| {
                io::Error::new(err.kind(), format!("landing data truncate failed: {err}"))
            })?;
        }
        self.counters
            .recoverable_segments
            .store(retained_count, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn stats(&self) -> LandingZoneStats {
        LandingZoneStats {
            accepted_writes: self.counters.accepted_writes.load(Ordering::Relaxed),
            accepted_bytes: self.counters.accepted_bytes.load(Ordering::Relaxed),
            recoverable_segments: self.counters.recoverable_segments.load(Ordering::Relaxed),
            accept_total_ns: self.counters.accept_total_ns.load(Ordering::Relaxed),
            accept_max_ns: self.counters.accept_max_ns.load(Ordering::Relaxed),
            data_syncs: self.counters.data_syncs.load(Ordering::Relaxed),
            manifest_syncs: self.counters.manifest_syncs.load(Ordering::Relaxed),
            manifest_rewrites: self.counters.manifest_rewrites.load(Ordering::Relaxed),
            sync_total_ns: self.counters.sync_total_ns.load(Ordering::Relaxed),
            sync_max_ns: self.counters.sync_max_ns.load(Ordering::Relaxed),
            background_write_total_ns: self
                .counters
                .background_write_total_ns
                .load(Ordering::Relaxed),
            background_write_max_ns: self
                .counters
                .background_write_max_ns
                .load(Ordering::Relaxed),
        }
    }

    fn ensure_manifest_file<'a>(
        manifest_path: &Path,
        manifest_file: &'a mut Option<File>,
    ) -> io::Result<&'a mut File> {
        if manifest_file.is_none() {
            *manifest_file = Some(open_landing_append_file(manifest_path)?);
        }
        manifest_file
            .as_mut()
            .ok_or_else(|| io::Error::other("landing manifest handle unavailable"))
    }

    fn spawn_writer(
        manifest_path: PathBuf,
        files: Arc<Mutex<LandingFiles>>,
        shared: Arc<LandingWriterShared>,
        counters: Arc<LandingZoneCounters>,
    ) -> io::Result<JoinHandle<()>> {
        thread::Builder::new()
            .name("studiofs-landing-writer".to_string())
            .spawn(move || {
                loop {
                    let batch = {
                        let mut state = match shared.state.lock() {
                            Ok(state) => state,
                            Err(_) => return,
                        };
                        loop {
                            if let Some(batch) = state.pop_batch() {
                                break batch;
                            }
                            if state.shutdown {
                                return;
                            }
                            state = match shared.changed.wait(state) {
                                Ok(state) => state,
                                Err(_) => return,
                            };
                        }
                    };
                    shared.changed.notify_all();

                    let result =
                        Self::append_queued_writes(&manifest_path, &files, &counters, &batch);
                    let mut state = match shared.state.lock() {
                        Ok(state) => state,
                        Err(_) => return,
                    };
                    state.active = false;
                    if let Err(error) = result {
                        state.restore_failed_batch(batch, error.to_string());
                    }
                    shared.changed.notify_all();
                }
            })
            .map_err(|err| io::Error::new(err.kind(), format!("spawn landing writer: {err}")))
    }

    fn append_queued_writes(
        manifest_path: &Path,
        files: &Mutex<LandingFiles>,
        counters: &LandingZoneCounters,
        queued_writes: &[QueuedLandingWrite],
    ) -> io::Result<()> {
        let append_started = Instant::now();
        let result =
            Self::append_queued_writes_inner(manifest_path, files, counters, queued_writes);
        record_latency(
            &counters.background_write_total_ns,
            &counters.background_write_max_ns,
            append_started.elapsed(),
        );
        result
    }

    fn append_queued_writes_inner(
        manifest_path: &Path,
        files: &Mutex<LandingFiles>,
        counters: &LandingZoneCounters,
        queued_writes: &[QueuedLandingWrite],
    ) -> io::Result<()> {
        let mut files = files
            .lock()
            .map_err(|_| io::Error::other("landing files lock poisoned"))?;
        let mut manifest_records = String::new();
        let mut appended_segments = Vec::with_capacity(queued_writes.len());
        let mut appended_count = 0usize;
        for queued in queued_writes {
            let data_offset = files.data.seek(SeekFrom::End(0))?;
            files.data.write_all(queued.data.as_ref())?;
            let len = u64::try_from(queued.data.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "landing write length exceeds u64::MAX",
                )
            })?;
            let segment = LandingSegment {
                ino: queued.ino,
                fh: queued.fh,
                offset: queued.offset,
                len,
                data_offset,
            };
            manifest_records.push_str(&segment.as_manifest_record());
            appended_segments.push(segment);
            appended_count = appended_count.saturating_add(1);
        }
        let manifest = Self::ensure_manifest_file(manifest_path, &mut files.manifest)?;
        manifest.seek(SeekFrom::End(0))?;
        manifest.write_all(manifest_records.as_bytes())?;
        files.segments.extend(appended_segments);
        counters
            .recoverable_segments
            .fetch_add(appended_count, Ordering::Relaxed);
        Ok(())
    }

    fn drain_pending(&self) -> io::Result<()> {
        let mut state = self
            .writer_state
            .state
            .lock()
            .map_err(|_| io::Error::other("landing writer lock poisoned"))?;
        loop {
            if let Some(error) = state.error.as_ref() {
                return Err(io::Error::other(format!(
                    "landing writer previously failed: {error}"
                )));
            }
            if state.queue.is_empty() && !state.active {
                return Ok(());
            }
            state = self
                .writer_state
                .changed
                .wait(state)
                .map_err(|_| io::Error::other("landing writer lock poisoned"))?;
        }
    }

    fn sync_landing_unlocked(&self, files: &LandingFiles) -> io::Result<()> {
        let data_sync_started = Instant::now();
        files.data.sync_data()?;
        self.counters.data_syncs.fetch_add(1, Ordering::Relaxed);
        self.record_sync_latency(data_sync_started.elapsed());
        if let Some(manifest) = files.manifest.as_ref() {
            let manifest_sync_started = Instant::now();
            manifest.sync_data()?;
            self.counters.manifest_syncs.fetch_add(1, Ordering::Relaxed);
            self.record_sync_latency(manifest_sync_started.elapsed());
        }
        Ok(())
    }

    fn read_manifest_segments(manifest_path: &Path) -> io::Result<Vec<LandingSegment>> {
        let manifest = open_landing_read_file(manifest_path)?;
        let mut reader = BufReader::new(manifest);
        let mut segments = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if line.trim().is_empty() {
                continue;
            }
            match LandingSegment::parse(line) {
                Ok(segment) => segments.push(segment),
                Err(_) => break,
            }
        }
        Ok(segments)
    }

    fn rewrite_manifest_unlocked(
        &self,
        segments: &[LandingSegment],
        manifest_file: &mut Option<File>,
    ) -> io::Result<()> {
        *manifest_file = None;
        remove_file_if_exists(&self.manifest_tmp_path)?;
        let mut manifest = open_landing_truncate_file(&self.manifest_tmp_path)?;
        for segment in segments {
            manifest.write_all(segment.as_manifest_record().as_bytes())?;
        }
        manifest.flush()?;
        let sync_started = Instant::now();
        manifest.sync_data()?;
        self.counters.manifest_syncs.fetch_add(1, Ordering::Relaxed);
        self.record_sync_latency(sync_started.elapsed());
        drop(manifest);
        remove_file_if_exists(&self.manifest_backup_path)?;
        if self.manifest_path.exists() {
            std::fs::rename(&self.manifest_path, &self.manifest_backup_path)?;
        }
        if let Err(error) = std::fs::rename(&self.manifest_tmp_path, &self.manifest_path) {
            if self.manifest_backup_path.exists() && !self.manifest_path.exists() {
                let _ = std::fs::rename(&self.manifest_backup_path, &self.manifest_path);
            }
            return Err(error);
        }
        remove_file_if_exists(&self.manifest_backup_path)?;
        *manifest_file = Some(open_landing_append_file(&self.manifest_path)?);
        self.counters
            .manifest_rewrites
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[allow(dead_code)]
    fn record_accept_latency(&self, duration: Duration) {
        let elapsed_ns = duration_ns_u64(duration);
        self.counters
            .accept_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.counters
            .accept_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    fn record_sync_latency(&self, duration: Duration) {
        let elapsed_ns = duration_ns_u64(duration);
        self.counters
            .sync_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.counters
            .sync_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    fn repair_manifest_swap_files(
        manifest_path: &Path,
        manifest_tmp_path: &Path,
        manifest_backup_path: &Path,
    ) -> io::Result<()> {
        if !manifest_path.exists() && manifest_backup_path.exists() {
            std::fs::rename(manifest_backup_path, manifest_path)?;
        } else if manifest_path.exists() {
            remove_file_if_exists(manifest_backup_path)?;
        }
        remove_file_if_exists(manifest_tmp_path)
    }
}

fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

impl Drop for LandingZone {
    fn drop(&mut self) {
        if let Ok(mut state) = self.writer_state.state.lock() {
            state.shutdown = true;
            self.writer_state.changed.notify_all();
        }
        if let Ok(mut writer) = self.writer.lock()
            && let Some(handle) = writer.take()
        {
            let _ = handle.join();
        }
    }
}

fn open_landing_append_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    set_landing_share_mode(&mut options);
    options.open(path)
}

fn open_landing_read_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    set_landing_share_mode(&mut options);
    options.open(path)
}

fn open_landing_truncate_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).write(true).truncate(true);
    set_landing_share_mode(&mut options);
    options.open(path)
}

#[cfg(windows)]
fn set_landing_share_mode(options: &mut OpenOptions) {
    options.share_mode(LANDING_FILE_SHARE_MODE);
}

#[cfg(not(windows))]
fn set_landing_share_mode(_options: &mut OpenOptions) {}

fn duration_ns_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn record_latency(total: &AtomicU64, max: &AtomicU64, duration: Duration) {
    let elapsed_ns = duration_ns_u64(duration);
    total.fetch_add(elapsed_ns, Ordering::Relaxed);
    max.fetch_max(elapsed_ns, Ordering::Relaxed);
}

impl From<LandingSegment> for RecoveredLandingSegment {
    fn from(value: LandingSegment) -> Self {
        Self {
            ino: value.ino,
            fh: value.fh,
            offset: value.offset,
            len: value.len,
            data_offset: value.data_offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn landing_zone_accepts_sequential_segments_and_reopens_manifest() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let zone = LandingZone::open(tmp.path()).expect("open landing zone");

        zone.accept(LandingWrite {
            ino: 42,
            fh: 7,
            offset: 0,
            data: Bytes::from_static(b"hello"),
        })
        .expect("accept first segment");
        zone.accept(LandingWrite {
            ino: 42,
            fh: 7,
            offset: 5,
            data: Bytes::from_static(b" world"),
        })
        .expect("accept second segment");

        assert_eq!(
            zone.read(42, 0, 11).expect("read landed bytes"),
            b"hello world"
        );
        assert_eq!(zone.stats().accepted_writes, 2);
        assert_eq!(zone.stats().accepted_bytes, 11);

        let reopened = LandingZone::open(tmp.path()).expect("reopen landing zone");
        let recovered = reopened.recover().expect("recover landed segments");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].ino, 42);
        assert_eq!(recovered[0].offset, 0);
        assert_eq!(recovered[0].len, 5);
        assert_eq!(recovered[1].offset, 5);
        assert_eq!(recovered[1].len, 6);
    }

    #[test]
    fn landing_zone_clear_inode_retires_recovered_segments() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let zone = LandingZone::open(tmp.path()).expect("open landing zone");

        zone.accept(LandingWrite {
            ino: 42,
            fh: 7,
            offset: 0,
            data: Bytes::from_static(b"retire"),
        })
        .expect("accept segment");
        zone.clear_inode(42).expect("clear inode");

        let reopened = LandingZone::open(tmp.path()).expect("reopen landing zone");
        assert!(
            reopened
                .recover()
                .expect("recover landed segments")
                .is_empty(),
            "cleared landed writes must not look recoverable after normal drain"
        );
    }

    #[test]
    fn landing_zone_open_recovers_manifest_backup_after_interrupted_swap() {
        let tmp = tempfile::tempdir().expect("tempdir");
        {
            let zone = LandingZone::open(tmp.path()).expect("open landing zone");
            zone.accept(LandingWrite {
                ino: 42,
                fh: 7,
                offset: 0,
                data: Bytes::from_static(b"survives"),
            })
            .expect("accept segment");
            std::fs::rename(&zone.manifest_path, &zone.manifest_backup_path)
                .expect("simulate crash after manifest moved to backup");
        }

        let reopened = LandingZone::open(tmp.path()).expect("reopen landing zone");
        let recovered = reopened.recover().expect("recover landed segments");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].ino, 42);
        assert_eq!(
            reopened.read(42, 0, 8).expect("read recovered"),
            b"survives"
        );
    }

    #[test]
    fn landing_zone_clear_inode_preserves_other_inode_manifest_records() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let zone = LandingZone::open(tmp.path()).expect("open landing zone");
        zone.accept(LandingWrite {
            ino: 1,
            fh: 10,
            offset: 0,
            data: Bytes::from_static(b"clear-me"),
        })
        .expect("accept cleared segment");
        zone.accept(LandingWrite {
            ino: 2,
            fh: 11,
            offset: 0,
            data: Bytes::from_static(b"keep-me"),
        })
        .expect("accept retained segment");

        zone.clear_inode(1).expect("clear one inode");

        let recovered = zone.recover().expect("recover landed segments");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].ino, 2);
        assert_eq!(zone.read(2, 0, 7).expect("read retained"), b"keep-me");
        assert!(!zone.manifest_tmp_path.exists());
        assert!(!zone.manifest_backup_path.exists());
    }

    #[test]
    fn landing_zone_recovers_valid_prefix_before_torn_manifest_tail() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let zone = LandingZone::open(tmp.path()).expect("open landing zone");
        zone.accept(LandingWrite {
            ino: 1,
            fh: 10,
            offset: 0,
            data: Bytes::from_static(b"valid"),
        })
        .expect("accept valid segment");
        zone.drain_pending().expect("durably append valid segment");
        {
            let mut manifest = OpenOptions::new()
                .append(true)
                .open(&zone.manifest_path)
                .expect("open manifest");
            manifest
                .write_all(b"2 11 0")
                .expect("append torn manifest tail");
        }

        let recovered = zone.recover().expect("recover valid prefix");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].ino, 1);
    }

    #[test]
    fn landing_zone_stats_track_recoverable_segments_without_reparse() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let zone = LandingZone::open(tmp.path()).expect("open landing zone");
        assert_eq!(zone.stats().recoverable_segments, 0);
        zone.accept(LandingWrite {
            ino: 1,
            fh: 10,
            offset: 0,
            data: Bytes::from_static(b"one"),
        })
        .expect("accept first");
        zone.accept(LandingWrite {
            ino: 2,
            fh: 11,
            offset: 0,
            data: Bytes::from_static(b"two"),
        })
        .expect("accept second");
        zone.drain_pending()
            .expect("durably append accepted segments");
        assert_eq!(zone.stats().recoverable_segments, 2);
        zone.clear_inode(1).expect("clear one inode");
        assert_eq!(zone.stats().recoverable_segments, 1);
        zone.clear_inode(2).expect("clear second inode");
        assert_eq!(zone.stats().recoverable_segments, 0);
    }

    #[test]
    fn opportunistic_accept_defers_sync_until_landing_records_retire() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let zone = LandingZone::open(tmp.path()).expect("open landing zone");

        zone.accept(LandingWrite {
            ino: 1,
            fh: 10,
            offset: 0,
            data: Bytes::from_static(b"writeback"),
        })
        .expect("accept segment");
        zone.accept(LandingWrite {
            ino: 1,
            fh: 10,
            offset: 9,
            data: Bytes::from_static(b"-mode"),
        })
        .expect("accept second segment");

        let accepted = zone.stats();
        assert_eq!(accepted.data_syncs, 0);
        assert_eq!(accepted.manifest_syncs, 0);
        assert_eq!(accepted.manifest_rewrites, 0);
        assert_eq!(
            zone.read(1, 0, 14).expect("read landed bytes"),
            b"writeback-mode"
        );

        zone.clear_inode(1).expect("retire landed records");

        let retired = zone.stats();
        assert!(retired.data_syncs > 0);
        assert!(retired.manifest_syncs > 0);
        assert_eq!(retired.manifest_rewrites, 1);
        assert_eq!(retired.recoverable_segments, 0);
    }

    #[test]
    fn landing_zone_accept_enqueues_without_waiting_for_writer_file_lock() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let zone = LandingZone::open(tmp.path()).expect("open landing zone");
        let files_guard = zone.files.lock().expect("hold landing files lock");

        zone.accept(LandingWrite {
            ino: 7,
            fh: 70,
            offset: 0,
            data: Bytes::from_static(b"queued"),
        })
        .expect("accept should only enqueue");

        let accepted = zone.stats();
        assert_eq!(accepted.accepted_writes, 1);
        assert_eq!(accepted.accepted_bytes, 6);
        assert_eq!(
            accepted.recoverable_segments, 0,
            "queued writes become recoverable only after the background writer appends manifest"
        );

        drop(files_guard);
        assert_eq!(
            zone.read(7, 0, 6).expect("read drained landing bytes"),
            b"queued"
        );
        assert_eq!(zone.stats().recoverable_segments, 1);
        assert!(zone.stats().background_write_total_ns > 0);
        assert!(zone.stats().background_write_max_ns > 0);
    }

    #[test]
    fn landing_zone_read_uses_cached_manifest_segments() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let zone = LandingZone::open(tmp.path()).expect("open landing zone");
        zone.accept(LandingWrite {
            ino: 9,
            fh: 90,
            offset: 0,
            data: Bytes::from_static(b"cached"),
        })
        .expect("accept segment");
        zone.drain_pending().expect("drain segment");
        {
            let mut files = zone.files.lock().expect("lock files");
            files.manifest = None;
        }
        std::fs::remove_file(&zone.manifest_path).expect("remove manifest path");

        assert_eq!(
            zone.read(9, 0, 6)
                .expect("read should use cached manifest state"),
            b"cached"
        );
    }

    #[test]
    fn landing_writer_state_restores_failed_batch_ahead_of_later_writes() {
        let mut state = LandingWriterState::default();
        state.push_queued(QueuedLandingWrite {
            ino: 3,
            fh: 30,
            offset: 0,
            data: Bytes::from_static(b"later"),
        });

        state.restore_failed_batch(
            vec![
                QueuedLandingWrite {
                    ino: 1,
                    fh: 10,
                    offset: 0,
                    data: Bytes::from_static(b"first"),
                },
                QueuedLandingWrite {
                    ino: 2,
                    fh: 20,
                    offset: 0,
                    data: Bytes::from_static(b"second"),
                },
            ],
            "disk failed".to_string(),
        );

        let inos: Vec<_> = state.queue.iter().map(|write| write.ino).collect();
        assert_eq!(inos, vec![1, 2, 3]);
        assert_eq!(state.queued_bytes, 16);
        assert_eq!(state.error.as_deref(), Some("disk failed"));
    }

    #[test]
    fn landing_zone_accept_blocks_when_queue_limit_is_reached() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let zone = Arc::new(
            LandingZone::open_with_queue_limits(
                tmp.path(),
                LandingQueueLimits {
                    max_writes: 2,
                    max_bytes: usize::MAX,
                },
            )
            .expect("open landing zone"),
        );
        let files_guard = zone.files.lock().expect("hold landing files lock");

        zone.accept(LandingWrite {
            ino: 7,
            fh: 70,
            offset: 0,
            data: Bytes::from_static(b"active"),
        })
        .expect("start active writer batch");
        wait_for_active_writer(&zone);

        zone.accept(LandingWrite {
            ino: 7,
            fh: 70,
            offset: 10,
            data: Bytes::from_static(b"a"),
        })
        .expect("queue first pending write");
        zone.accept(LandingWrite {
            ino: 7,
            fh: 70,
            offset: 11,
            data: Bytes::from_static(b"b"),
        })
        .expect("queue second pending write");

        let (tx, rx) = std::sync::mpsc::channel();
        let blocked_zone = Arc::clone(&zone);
        let handle = thread::spawn(move || {
            let result = blocked_zone.accept(LandingWrite {
                ino: 7,
                fh: 70,
                offset: 12,
                data: Bytes::from_static(b"c"),
            });
            tx.send(result.is_ok()).expect("send accept result");
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "accept should apply backpressure while queued writes are at the limit"
        );
        drop(files_guard);
        assert!(
            rx.recv_timeout(Duration::from_secs(5))
                .expect("accept eventually finishes")
        );
        handle.join().expect("join accept thread");
    }

    fn wait_for_active_writer(zone: &LandingZone) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            {
                let state = zone.writer_state.state.lock().expect("lock writer state");
                if state.active {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "writer did not become active before timeout"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }
}
