use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};

const DATA_FILE: &str = "landing.data";
const MANIFEST_FILE: &str = "landing.manifest";
const MANIFEST_TMP_FILE: &str = "landing.manifest.tmp";
const MANIFEST_BACKUP_FILE: &str = "landing.manifest.bak";

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
    #[allow(dead_code)]
    data_path: PathBuf,
    manifest_path: PathBuf,
    manifest_tmp_path: PathBuf,
    manifest_backup_path: PathBuf,
    data_lock: Mutex<()>,
    manifest_lock: Mutex<()>,
    accepted_writes: AtomicU64,
    accepted_bytes: AtomicU64,
    recoverable_segments: AtomicUsize,
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
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&data_path)?;
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&manifest_path)?;
        let recoverable_segments = Self::count_manifest_segments(&manifest_path)?;
        Ok(Self {
            data_path,
            manifest_path,
            manifest_tmp_path,
            manifest_backup_path,
            data_lock: Mutex::new(()),
            manifest_lock: Mutex::new(()),
            accepted_writes: AtomicU64::new(0),
            accepted_bytes: AtomicU64::new(0),
            recoverable_segments: AtomicUsize::new(recoverable_segments),
        })
    }

    pub(super) fn accept(&self, write: LandingWrite<'_>) -> io::Result<RecoveredLandingSegment> {
        let _data_guard = self
            .data_lock
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let _manifest_guard = self
            .manifest_lock
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let mut data = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.data_path)?;
        let data_offset = data.seek(SeekFrom::End(0))?;
        data.write_all(write.data)?;
        data.sync_data()?;

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
        let mut manifest = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.manifest_path)?;
        manifest.write_all(segment.as_manifest_record().as_bytes())?;
        manifest.sync_data()?;

        self.accepted_writes.fetch_add(1, Ordering::Relaxed);
        self.accepted_bytes.fetch_add(len, Ordering::Relaxed);
        self.recoverable_segments.fetch_add(1, Ordering::Relaxed);
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
            .data_lock
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let _manifest_guard = self
            .manifest_lock
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let mut data = File::open(&self.data_path)?;
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
            .manifest_lock
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        self.recover_segments_unlocked()
            .map(|segments| segments.into_iter().map(Into::into).collect())
    }

    pub(super) fn recover_with_data(&self) -> io::Result<Vec<LandingRecoverySegment>> {
        let _data_guard = self
            .data_lock
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let _manifest_guard = self
            .manifest_lock
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let segments = self.recover_segments_unlocked()?;
        let mut data = File::open(&self.data_path)?;
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
        let _data_guard = self
            .data_lock
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let _manifest_guard = self
            .manifest_lock
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let retained: Vec<_> = self
            .recover_segments_unlocked()?
            .into_iter()
            .filter(|segment| segment.ino != ino)
            .collect();
        let retained_count = retained.len();
        self.rewrite_manifest_unlocked(&retained)?;
        if retained.is_empty() {
            OpenOptions::new()
                .write(true)
                .open(&self.data_path)?
                .set_len(0)?;
        }
        self.recoverable_segments
            .store(retained_count, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn clear_inodes(&self, inos: &std::collections::BTreeSet<u64>) -> io::Result<()> {
        if inos.is_empty() {
            return Ok(());
        }
        let _data_guard = self
            .data_lock
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let _manifest_guard = self
            .manifest_lock
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let retained: Vec<_> = self
            .recover_segments_unlocked()?
            .into_iter()
            .filter(|segment| !inos.contains(&segment.ino))
            .collect();
        let retained_count = retained.len();
        self.rewrite_manifest_unlocked(&retained)?;
        if retained.is_empty() {
            OpenOptions::new()
                .write(true)
                .open(&self.data_path)?
                .set_len(0)?;
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
        }
    }

    fn recover_segments_unlocked(&self) -> io::Result<Vec<LandingSegment>> {
        let manifest = File::open(&self.manifest_path)?;
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

    fn rewrite_manifest_unlocked(&self, segments: &[LandingSegment]) -> io::Result<()> {
        remove_file_if_exists(&self.manifest_tmp_path)?;
        let mut manifest = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.manifest_tmp_path)?;
        for segment in segments {
            manifest.write_all(segment.as_manifest_record().as_bytes())?;
        }
        manifest.flush()?;
        manifest.sync_data()?;
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
        remove_file_if_exists(&self.manifest_backup_path)
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
        let manifest = File::open(manifest_path)?;
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
}
