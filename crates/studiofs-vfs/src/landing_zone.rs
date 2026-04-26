use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

const DATA_FILE: &str = "landing.data";
const MANIFEST_FILE: &str = "landing.manifest";

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
    data_lock: Mutex<()>,
    manifest_lock: Mutex<()>,
    accepted_writes: AtomicU64,
    accepted_bytes: AtomicU64,
}

impl LandingZone {
    pub(super) fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let data_path = root.join(DATA_FILE);
        let manifest_path = root.join(MANIFEST_FILE);
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
        Ok(Self {
            data_path,
            manifest_path,
            data_lock: Mutex::new(()),
            manifest_lock: Mutex::new(()),
            accepted_writes: AtomicU64::new(0),
            accepted_bytes: AtomicU64::new(0),
        })
    }

    pub(super) fn accept(&self, write: LandingWrite<'_>) -> io::Result<RecoveredLandingSegment> {
        let data_guard = self
            .data_lock
            .lock()
            .map_err(|_| io::Error::other("landing data lock poisoned"))?;
        let mut data = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.data_path)?;
        let data_offset = data.seek(SeekFrom::End(0))?;
        data.write_all(write.data)?;
        data.flush()?;
        drop(data_guard);

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
        let _manifest_guard = self
            .manifest_lock
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let mut manifest = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.manifest_path)?;
        manifest.write_all(segment.as_manifest_record().as_bytes())?;
        manifest.flush()?;

        self.accepted_writes.fetch_add(1, Ordering::Relaxed);
        self.accepted_bytes.fetch_add(len, Ordering::Relaxed);
        Ok(segment.into())
    }

    #[allow(dead_code)]
    pub(super) fn read(&self, ino: u64, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let requested_end = offset.saturating_add(len as u64);
        let mut result = vec![0; len];
        let mut data = File::open(&self.data_path)?;
        for segment in self.recover_segments()? {
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

    pub(super) fn recover(&self) -> io::Result<Vec<RecoveredLandingSegment>> {
        self.recover_segments()
            .map(|segments| segments.into_iter().map(Into::into).collect())
    }

    pub(super) fn recover_with_data(&self) -> io::Result<Vec<LandingRecoverySegment>> {
        let segments = self.recover_segments()?;
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
        let retained: Vec<_> = self
            .recover_segments()?
            .into_iter()
            .filter(|segment| segment.ino != ino)
            .collect();
        self.rewrite_manifest(&retained)?;
        if retained.is_empty() {
            let _data_guard = self
                .data_lock
                .lock()
                .map_err(|_| io::Error::other("landing data lock poisoned"))?;
            OpenOptions::new()
                .write(true)
                .open(&self.data_path)?
                .set_len(0)?;
        }
        Ok(())
    }

    pub(super) fn stats(&self) -> LandingZoneStats {
        LandingZoneStats {
            accepted_writes: self.accepted_writes.load(Ordering::Relaxed),
            accepted_bytes: self.accepted_bytes.load(Ordering::Relaxed),
        }
    }

    fn recover_segments(&self) -> io::Result<Vec<LandingSegment>> {
        let manifest = std::fs::read_to_string(&self.manifest_path)?;
        manifest
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(LandingSegment::parse)
            .collect()
    }

    fn rewrite_manifest(&self, segments: &[LandingSegment]) -> io::Result<()> {
        let _manifest_guard = self
            .manifest_lock
            .lock()
            .map_err(|_| io::Error::other("landing manifest lock poisoned"))?;
        let mut manifest = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.manifest_path)?;
        for segment in segments {
            manifest.write_all(segment.as_manifest_record().as_bytes())?;
        }
        manifest.flush()
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
}
