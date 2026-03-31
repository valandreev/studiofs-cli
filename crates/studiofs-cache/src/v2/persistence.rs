use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use tracing::warn;

use crate::v2::format::{
    self, BLOCK_HEADER_SIZE, BlockHeader, PAGE_SIZE, SUPERBLOCK_SIZE, alloc_size_for,
};
use crate::v2::index::{BlockFlags, BlockIndex, PersistedIndexEntry};
use crate::v2::platform;

const SNAPSHOT_MAGIC: [u8; 8] = *b"STFSIDX\0";
const WAL_MAGIC: [u8; 8] = *b"STFSWAL\0";
const PERSISTENCE_VERSION: u32 = 1;

const SNAPSHOT_HEADER_SIZE: usize = 24;
const SNAPSHOT_ENTRY_SIZE: usize = 40;
pub(super) const WAL_HEADER_SIZE: u64 = 16;
pub(super) const WAL_RECORD_SIZE: u64 = 48;

const WAL_HEADER_SIZE_USIZE: usize = WAL_HEADER_SIZE as usize;
const WAL_RECORD_SIZE_USIZE: usize = WAL_RECORD_SIZE as usize;
const PERSISTED_FLAG_MASK: u8 = format::FLAG_DIRTY | format::FLAG_PINNED;
const CRC_CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy)]
enum WalOp {
    Put = 1,
    Remove = 2,
    UpdateFlags = 3,
}

impl WalOp {
    fn from_byte(byte: u8) -> io::Result<Self> {
        match byte {
            1 => Ok(Self::Put),
            2 => Ok(Self::Remove),
            3 => Ok(Self::UpdateFlags),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown WAL op {byte}"),
            )),
        }
    }
}

pub(super) struct WalAppender {
    file: File,
}

impl WalAppender {
    pub(super) fn open(cache_dir: &Path, reset: bool) -> io::Result<Self> {
        let path = wal_path(cache_dir);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        if reset {
            file.set_len(0)?;
        }
        if file.metadata()?.len() == 0 {
            write_wal_header(&mut file)?;
        }

        Ok(Self { file })
    }

    pub(super) fn append_put(&mut self, entry: PersistedIndexEntry) -> io::Result<()> {
        self.append_record(
            WalOp::Put,
            entry.object_id,
            entry.offset,
            entry.alloc_size,
            entry.data_size,
            entry.checksum,
            persisted_flag_bits(entry.flags),
        )
    }

    pub(super) fn append_remove(&mut self, object_id: u64) -> io::Result<()> {
        self.append_record(WalOp::Remove, object_id, 0, 0, 0, 0, 0)
    }

    pub(super) fn append_update_flags(
        &mut self,
        object_id: u64,
        flags: BlockFlags,
    ) -> io::Result<()> {
        self.append_record(
            WalOp::UpdateFlags,
            object_id,
            0,
            0,
            0,
            0,
            persisted_flag_bits(flags),
        )
    }

    pub(super) fn clear(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn append_record(
        &mut self,
        op: WalOp,
        object_id: u64,
        offset: u64,
        alloc_size: u64,
        data_size: u32,
        checksum: u32,
        flags: u8,
    ) -> io::Result<()> {
        let record = wal_record_bytes(
            op, object_id, offset, alloc_size, data_size, checksum, flags,
        );
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&record)?;
        Ok(())
    }
}

pub(super) fn load_entries(
    cache_dir: &Path,
    data_file: &File,
) -> io::Result<Option<Vec<PersistedIndexEntry>>> {
    let snapshot_path = snapshot_path(cache_dir);
    if !snapshot_path.exists() {
        return Ok(None);
    }

    let snapshot_entries = read_snapshot(&snapshot_path)?;
    let mut index = BlockIndex::with_capacity(snapshot_entries.len());
    for entry in snapshot_entries {
        index.insert(
            entry.object_id,
            entry.offset,
            entry.data_size,
            entry.alloc_size,
            entry.checksum,
            entry.flags,
        );
    }

    let wal_path = wal_path(cache_dir);
    if wal_path.exists() {
        replay_wal(&wal_path, &mut index)?;
    }

    Ok(Some(validate_recovered_entries(
        data_file,
        index.snapshot_for_persistence(),
    )?))
}

pub(super) fn write_snapshot(cache_dir: &Path, entries: &[PersistedIndexEntry]) -> io::Result<()> {
    let path = snapshot_path(cache_dir);
    let tmp_path = cache_dir.join("blocks.idx.tmp");
    let entry_count = u32::try_from(entries.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "too many cache entries to persist in a single snapshot",
        )
    })?;

    let mut body = Vec::with_capacity(entries.len() * SNAPSHOT_ENTRY_SIZE);
    for entry in entries {
        body.extend_from_slice(&snapshot_entry_bytes(*entry));
    }
    let checksum = crc32fast::hash(&body);

    let _ = std::fs::remove_file(&tmp_path);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)?;

    let mut header = [0u8; SNAPSHOT_HEADER_SIZE];
    header[0..8].copy_from_slice(&SNAPSHOT_MAGIC);
    header[8..12].copy_from_slice(&PERSISTENCE_VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&entry_count.to_le_bytes());
    header[16..20].copy_from_slice(&checksum.to_le_bytes());

    file.write_all(&header)?;
    file.write_all(&body)?;
    file.sync_all()?;

    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    std::fs::rename(tmp_path, path)?;
    Ok(())
}

pub(super) fn scan_data_file(data_file: &File) -> io::Result<Vec<PersistedIndexEntry>> {
    let file_len = data_file.metadata()?.len();
    let mut cursor = SUPERBLOCK_SIZE;
    let mut index = BlockIndex::new();
    let mut header_buf = [0u8; BLOCK_HEADER_SIZE as usize];

    while cursor + BLOCK_HEADER_SIZE as u64 <= file_len {
        match platform::pread(data_file, &mut header_buf, cursor) {
            Ok(()) => {}
            Err(e) => {
                warn!(offset = cursor, error = ?e, "block scan: header read failed");
                cursor += PAGE_SIZE;
                continue;
            }
        }

        let header = match BlockHeader::from_bytes(&header_buf) {
            Ok(header) => header,
            Err(_) => {
                cursor += PAGE_SIZE;
                continue;
            }
        };

        if header.flags & !PERSISTED_FLAG_MASK != 0 {
            warn!(
                offset = cursor,
                flags = header.flags,
                "block scan: skipping header with unknown persisted flags"
            );
            cursor += PAGE_SIZE;
            continue;
        }

        let alloc_size = alloc_size_for(header.data_size);
        if cursor + alloc_size > file_len {
            warn!(
                offset = cursor,
                alloc_size, file_len, "block scan: skipping truncated block",
            );
            cursor += PAGE_SIZE;
            continue;
        }

        if !payload_checksum_matches(data_file, cursor, header.data_size, header.checksum)? {
            warn!(
                offset = cursor,
                object_id = header.object_id,
                "block scan: checksum mismatch"
            );
            cursor += alloc_size;
            continue;
        }

        index.insert(
            header.object_id,
            cursor,
            header.data_size,
            alloc_size,
            header.checksum,
            decode_persisted_flags(header.flags)?,
        );
        cursor += alloc_size;
    }

    Ok(index.snapshot_for_persistence())
}

pub(super) fn persisted_flag_bits(flags: BlockFlags) -> u8 {
    flags.bits() & PERSISTED_FLAG_MASK
}

fn read_snapshot(path: &Path) -> io::Result<Vec<PersistedIndexEntry>> {
    let bytes = std::fs::read(path)?;
    if bytes.len() < SNAPSHOT_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot shorter than header",
        ));
    }

    let magic: [u8; 8] = bytes[0..8].try_into().unwrap();
    if magic != SNAPSHOT_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot magic mismatch",
        ));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if version != PERSISTENCE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot version mismatch: expected {PERSISTENCE_VERSION}, found {version}"),
        ));
    }

    let entry_count = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let expected_len = SNAPSHOT_HEADER_SIZE + entry_count * SNAPSHOT_ENTRY_SIZE;
    if bytes.len() != expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "snapshot length mismatch: expected {expected_len} bytes, found {}",
                bytes.len()
            ),
        ));
    }

    let expected_checksum = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    let actual_checksum = crc32fast::hash(&bytes[SNAPSHOT_HEADER_SIZE..]);
    if actual_checksum != expected_checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "snapshot checksum mismatch: expected {expected_checksum:#x}, found {actual_checksum:#x}"
            ),
        ));
    }

    let mut entries = Vec::with_capacity(entry_count);
    for chunk in bytes[SNAPSHOT_HEADER_SIZE..].chunks_exact(SNAPSHOT_ENTRY_SIZE) {
        let object_id = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
        let offset = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
        let data_size = u32::from_le_bytes(chunk[16..20].try_into().unwrap());
        let alloc_size = u64::from_le_bytes(chunk[20..28].try_into().unwrap());
        let checksum = u32::from_le_bytes(chunk[28..32].try_into().unwrap());
        let flags = decode_persisted_flags(chunk[32])?;

        entries.push(PersistedIndexEntry {
            object_id,
            offset,
            data_size,
            alloc_size,
            checksum,
            flags,
        });
    }

    Ok(entries)
}

fn replay_wal(path: &Path, index: &mut BlockIndex) -> io::Result<()> {
    let bytes = std::fs::read(path)?;
    if bytes.is_empty() {
        return Ok(());
    }
    if bytes.len() < WAL_HEADER_SIZE_USIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL shorter than header",
        ));
    }

    let magic: [u8; 8] = bytes[0..8].try_into().unwrap();
    if magic != WAL_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL magic mismatch",
        ));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if version != PERSISTENCE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("WAL version mismatch: expected {PERSISTENCE_VERSION}, found {version}"),
        ));
    }

    let records = &bytes[WAL_HEADER_SIZE_USIZE..];
    let complete_records = records.len() / WAL_RECORD_SIZE_USIZE;
    let tail_bytes = records.len() % WAL_RECORD_SIZE_USIZE;
    if tail_bytes != 0 {
        warn!(
            tail_bytes,
            "WAL has a partial tail; ignoring incomplete record suffix"
        );
    }

    for chunk in records
        .chunks_exact(WAL_RECORD_SIZE_USIZE)
        .take(complete_records)
    {
        let expected_crc = u32::from_le_bytes(chunk[40..44].try_into().unwrap());
        let actual_crc = crc32fast::hash(&chunk[0..40]);
        if actual_crc != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL record checksum mismatch: expected {expected_crc:#x}, found {actual_crc:#x}"
                ),
            ));
        }

        let op = WalOp::from_byte(chunk[0])?;
        let flags = chunk[1];
        let object_id = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
        let offset = u64::from_le_bytes(chunk[16..24].try_into().unwrap());
        let alloc_size = u64::from_le_bytes(chunk[24..32].try_into().unwrap());
        let data_size = u32::from_le_bytes(chunk[32..36].try_into().unwrap());
        let checksum = u32::from_le_bytes(chunk[36..40].try_into().unwrap());

        match op {
            WalOp::Put => {
                index.insert(
                    object_id,
                    offset,
                    data_size,
                    alloc_size,
                    checksum,
                    decode_persisted_flags(flags)?,
                );
            }
            WalOp::Remove => {
                index.remove(object_id);
            }
            WalOp::UpdateFlags => {
                index.update_flags(object_id, decode_persisted_flags(flags)?);
            }
        }
    }

    Ok(())
}

fn validate_recovered_entries(
    data_file: &File,
    entries: Vec<PersistedIndexEntry>,
) -> io::Result<Vec<PersistedIndexEntry>> {
    let file_len = data_file.metadata()?.len();
    let mut validated = Vec::with_capacity(entries.len());
    let mut header_buf = [0u8; BLOCK_HEADER_SIZE as usize];

    for entry in entries {
        if entry.offset < SUPERBLOCK_SIZE || !entry.offset.is_multiple_of(PAGE_SIZE) {
            continue;
        }
        if entry.offset + BLOCK_HEADER_SIZE as u64 > file_len {
            continue;
        }

        if platform::pread(data_file, &mut header_buf, entry.offset).is_err() {
            continue;
        }
        let header = match BlockHeader::from_bytes(&header_buf) {
            Ok(header) => header,
            Err(_) => continue,
        };
        if header.object_id != entry.object_id {
            continue;
        }
        if header.flags & !PERSISTED_FLAG_MASK != 0 {
            continue;
        }

        let alloc_size = alloc_size_for(header.data_size);
        if entry.offset + alloc_size > file_len {
            continue;
        }

        validated.push(PersistedIndexEntry {
            object_id: header.object_id,
            offset: entry.offset,
            data_size: header.data_size,
            alloc_size,
            checksum: header.checksum,
            flags: decode_persisted_flags(header.flags)?,
        });
    }

    Ok(validated)
}

fn payload_checksum_matches(
    data_file: &File,
    block_offset: u64,
    data_size: u32,
    expected_checksum: u32,
) -> io::Result<bool> {
    let mut hasher = crc32fast::Hasher::new();
    let mut remaining = data_size as usize;
    let mut offset = block_offset + BLOCK_HEADER_SIZE as u64;
    let mut buffer = vec![0u8; CRC_CHUNK_SIZE.min(remaining.max(1))];

    while remaining > 0 {
        let chunk_len = buffer.len().min(remaining);
        platform::pread(data_file, &mut buffer[..chunk_len], offset)?;
        hasher.update(&buffer[..chunk_len]);
        remaining -= chunk_len;
        offset += chunk_len as u64;
    }

    Ok(hasher.finalize() == expected_checksum)
}

fn decode_persisted_flags(bits: u8) -> io::Result<BlockFlags> {
    if bits & !PERSISTED_FLAG_MASK != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown persisted block flags {bits:#x}"),
        ));
    }
    Ok(BlockFlags::from_bits_truncate(bits))
}

fn snapshot_entry_bytes(entry: PersistedIndexEntry) -> [u8; SNAPSHOT_ENTRY_SIZE] {
    let mut buf = [0u8; SNAPSHOT_ENTRY_SIZE];
    buf[0..8].copy_from_slice(&entry.object_id.to_le_bytes());
    buf[8..16].copy_from_slice(&entry.offset.to_le_bytes());
    buf[16..20].copy_from_slice(&entry.data_size.to_le_bytes());
    buf[20..28].copy_from_slice(&entry.alloc_size.to_le_bytes());
    buf[28..32].copy_from_slice(&entry.checksum.to_le_bytes());
    buf[32] = persisted_flag_bits(entry.flags);
    buf
}

fn wal_record_bytes(
    op: WalOp,
    object_id: u64,
    offset: u64,
    alloc_size: u64,
    data_size: u32,
    checksum: u32,
    flags: u8,
) -> [u8; WAL_RECORD_SIZE_USIZE] {
    let mut buf = [0u8; WAL_RECORD_SIZE_USIZE];
    buf[0] = op as u8;
    buf[1] = flags;
    buf[8..16].copy_from_slice(&object_id.to_le_bytes());
    buf[16..24].copy_from_slice(&offset.to_le_bytes());
    buf[24..32].copy_from_slice(&alloc_size.to_le_bytes());
    buf[32..36].copy_from_slice(&data_size.to_le_bytes());
    buf[36..40].copy_from_slice(&checksum.to_le_bytes());
    let crc = crc32fast::hash(&buf[0..40]);
    buf[40..44].copy_from_slice(&crc.to_le_bytes());
    buf
}

fn write_wal_header(file: &mut File) -> io::Result<()> {
    let mut header = [0u8; WAL_HEADER_SIZE_USIZE];
    header[0..8].copy_from_slice(&WAL_MAGIC);
    header[8..12].copy_from_slice(&PERSISTENCE_VERSION.to_le_bytes());
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    Ok(())
}

fn snapshot_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("blocks.idx")
}

fn wal_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("blocks.wal")
}
