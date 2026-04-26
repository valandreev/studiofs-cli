use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const DATA_FILE: &str = "landing.data";
const MANIFEST_FILE: &str = "landing.manifest";
const MANIFEST_TMP_FILE: &str = "landing.manifest.tmp";
const MANIFEST_BACKUP_FILE: &str = "landing.manifest.bak";
#[cfg(windows)]
const LANDING_FILE_SHARE_MODE: u32 = 0x00000001 | 0x00000002 | 0x00000004;

pub(super) struct LandingWrite<'a> {
    pub(super) ino: u64,
    pub(super) fh: u64,
    pub(super) offset: u64,
    pub(super) data: &'a [u8],
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

#[derive(Debug)]
pub(super) struct LandingZone {
    data_path: PathBuf,
    manifest_path: PathBuf,
    manifest_tmp_path: PathBuf,
    manifest_backup_path: PathBuf,
    data_file: Mutex<File>,
    manifest_file: Mutex<Option<File>>,
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
}

impl LandingZone {
    pub(super) fn open(root: impl AsRef<Path>) -> io::Result<Self> {
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
        let recoverable_segments = Self::count_manifest_segments(&manifest_path)?;
        Ok(Self {
            data_path,
            manifest_path,
            manifest_tmp_path,
            manifest_backup_path,
            data_file: Mutex::new(data_file),
            manifest_file: Mutex::new(Some(manifest_file)),
            accepted_writes: AtomicU64::new(0),
            accepted_bytes: AtomicU64::new(0),
            recoverable_segments: AtomicUsize::new(recoverable_segments),
            accept_total_ns: AtomicU64::new(0),
            accept_max_ns: AtomicU64::new(0),
            data_syncs: AtomicU64::new(0),
            manifest_syncs: AtomicU64::new(0),
            manifest_rewrites: AtomicU64::new(0),
            sync_total_ns: AtomicU64::new(0),
            sync_max_ns: AtomicU64::new(0),
        })
    }

    pub(super) fn accept(&self, write: LandingWrite<'_>) -> io::Result<RecoveredLandingSegment> {
        let accept_started = Instant::now();
        let mut data = self
            .data_file
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let mut manifest_guard = self
            .manifest_file
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let data_offset = data.seek(SeekFrom::End(0))?;
        data.write_all(write.data)?;

        let len = u64::try_from(write.data.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "landing write length exceeds u64::MAX",
            )
        })?;
        let segment = LandingSegment {
            ino: write.ino,
            fh: write.fh,
            offset: write.offset,
            len,
            data_offset,
        };
        let manifest = Self::ensure_manifest_file(&self.manifest_path, &mut manifest_guard)?;
        manifest.seek(SeekFrom::End(0))?;
        manifest.write_all(segment.as_manifest_record().as_bytes())?;

        self.accepted_writes.fetch_add(1, Ordering::Relaxed);
        self.accepted_bytes.fetch_add(len, Ordering::Relaxed);
        self.recoverable_segments.fetch_add(1, Ordering::Relaxed);
        self.record_accept_latency(accept_started.elapsed());
        Ok(segment.into())
    }

    #[allow(dead_code)]
    pub(super) fn read(&self, ino: u64, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let requested_end = offset.saturating_add(len as u64);
        let mut result = vec![0; len];
        let _data_guard = self
            .data_file
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let _manifest_guard = self
            .manifest_file
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let mut data = open_landing_read_file(&self.data_path)?;
        for segment in self.recover_segments_unlocked()? {
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
        let _manifest_guard = self
            .manifest_file
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        self.recover_segments_unlocked()
            .map(|segments| segments.into_iter().map(Into::into).collect())
    }

    pub(super) fn recover_with_data(&self) -> io::Result<Vec<LandingRecoverySegment>> {
        let _data_guard = self
            .data_file
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let _manifest_guard = self
            .manifest_file
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let segments = self.recover_segments_unlocked()?;
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
        let data = self
            .data_file
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let mut manifest_guard = self
            .manifest_file
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let segments = self.recover_segments_unlocked()?;
        let retained: Vec<_> = segments
            .iter()
            .copied()
            .filter(|segment| segment.ino != ino)
            .collect();
        let retained_count = retained.len();
        if retained_count == segments.len() {
            self.recoverable_segments
                .store(retained_count, Ordering::Relaxed);
            return Ok(());
        }
        self.sync_landing_unlocked(&data, manifest_guard.as_ref())
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("landing sync before retire failed: {err}"),
                )
            })?;
        self.rewrite_manifest_unlocked(&retained, &mut manifest_guard)
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("landing manifest rewrite failed: {err}"),
                )
            })?;
        if retained.is_empty() {
            data.set_len(0).map_err(|err| {
                io::Error::new(err.kind(), format!("landing data truncate failed: {err}"))
            })?;
        }
        self.recoverable_segments
            .store(retained_count, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn clear_inodes(&self, inos: &std::collections::BTreeSet<u64>) -> io::Result<()> {
        if inos.is_empty() {
            return Ok(());
        }
        let data = self
            .data_file
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let mut manifest_guard = self
            .manifest_file
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let segments = self.recover_segments_unlocked()?;
        let retained: Vec<_> = segments
            .iter()
            .copied()
            .filter(|segment| !inos.contains(&segment.ino))
            .collect();
        let retained_count = retained.len();
        if retained_count == segments.len() {
            self.recoverable_segments
                .store(retained_count, Ordering::Relaxed);
            return Ok(());
        }
        self.sync_landing_unlocked(&data, manifest_guard.as_ref())
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("landing sync before retire failed: {err}"),
                )
            })?;
        self.rewrite_manifest_unlocked(&retained, &mut manifest_guard)
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("landing manifest rewrite failed: {err}"),
                )
            })?;
        if retained.is_empty() {
            data.set_len(0).map_err(|err| {
                io::Error::new(err.kind(), format!("landing data truncate failed: {err}"))
            })?;
        }
        self.recoverable_segments
            .store(retained_count, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn stats(&self) -> LandingZoneStats {
        LandingZoneStats {
            accepted_writes: self.accepted_writes.load(Ordering::Relaxed),
            accepted_bytes: self.accepted_bytes.load(Ordering::Relaxed),
            recoverable_segments: self.recoverable_segments.load(Ordering::Relaxed),
            accept_total_ns: self.accept_total_ns.load(Ordering::Relaxed),
            accept_max_ns: self.accept_max_ns.load(Ordering::Relaxed),
            data_syncs: self.data_syncs.load(Ordering::Relaxed),
            manifest_syncs: self.manifest_syncs.load(Ordering::Relaxed),
            manifest_rewrites: self.manifest_rewrites.load(Ordering::Relaxed),
            sync_total_ns: self.sync_total_ns.load(Ordering::Relaxed),
            sync_max_ns: self.sync_max_ns.load(Ordering::Relaxed),
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

    fn sync_landing_unlocked(&self, data: &File, manifest: Option<&File>) -> io::Result<()> {
        let data_sync_started = Instant::now();
        data.sync_data()?;
        self.data_syncs.fetch_add(1, Ordering::Relaxed);
        self.record_sync_latency(data_sync_started.elapsed());
        if let Some(manifest) = manifest {
            let manifest_sync_started = Instant::now();
            manifest.sync_data()?;
            self.manifest_syncs.fetch_add(1, Ordering::Relaxed);
            self.record_sync_latency(manifest_sync_started.elapsed());
        }
        Ok(())
    }

    fn recover_segments_unlocked(&self) -> io::Result<Vec<LandingSegment>> {
        let manifest = open_landing_read_file(&self.manifest_path)?;
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
        self.manifest_syncs.fetch_add(1, Ordering::Relaxed);
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
        self.manifest_rewrites.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn record_accept_latency(&self, duration: Duration) {
        let elapsed_ns = duration_ns_u64(duration);
        self.accept_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.accept_max_ns.fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    fn record_sync_latency(&self, duration: Duration) {
        let elapsed_ns = duration_ns_u64(duration);
        self.sync_total_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        self.sync_max_ns.fetch_max(elapsed_ns, Ordering::Relaxed);
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

    fn count_manifest_segments(manifest_path: &Path) -> io::Result<usize> {
        let manifest = open_landing_read_file(manifest_path)?;
        let mut reader = BufReader::new(manifest);
        let mut count = 0usize;
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
            if LandingSegment::parse(line).is_err() {
                break;
            }
            count = count.saturating_add(1);
        }
        Ok(count)
    }
}

fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
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
            data: b"hello".as_slice(),
        })
        .expect("accept first segment");
        zone.accept(LandingWrite {
            ino: 42,
            fh: 7,
            offset: 5,
            data: b" world".as_slice(),
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
            data: b"retire".as_slice(),
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
                data: b"survives".as_slice(),
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
            data: b"clear-me".as_slice(),
        })
        .expect("accept cleared segment");
        zone.accept(LandingWrite {
            ino: 2,
            fh: 11,
            offset: 0,
            data: b"keep-me".as_slice(),
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
            data: b"valid".as_slice(),
        })
        .expect("accept valid segment");
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
            data: b"one".as_slice(),
        })
        .expect("accept first");
        zone.accept(LandingWrite {
            ino: 2,
            fh: 11,
            offset: 0,
            data: b"two".as_slice(),
        })
        .expect("accept second");
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
            data: b"writeback".as_slice(),
        })
        .expect("accept segment");
        zone.accept(LandingWrite {
            ino: 1,
            fh: 10,
            offset: 9,
            data: b"-mode".as_slice(),
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
}
