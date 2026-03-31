//! On-disk format definitions for the Cache v2 `blocks.data` file.
//!
//! Layout:
//! ```text
//! [Superblock: 4096 bytes] [Block₀: hdr+payload+pad] [Block₁: ...] ...
//! ```

use std::io;

/// Superblock occupies the first page of `blocks.data`.
pub const SUPERBLOCK_SIZE: u64 = 4096;

/// Alignment unit for all allocations.
pub const PAGE_SIZE: u64 = 4096;

/// File growth step when the data file needs to expand.
pub const GROWTH_CHUNK: u64 = 256 * 1024 * 1024; // 256 MiB

/// Superblock magic bytes: `STFSCA\x01\x00`.
pub const SUPERBLOCK_MAGIC: [u8; 8] = *b"STFSCA\x01\x00";

/// Current format version.
pub const FORMAT_VERSION: u32 = 1;

/// Block header magic bytes.
pub const BLOCK_MAGIC: [u8; 4] = *b"SBLK";

/// Size of a block header in bytes.
pub const BLOCK_HEADER_SIZE: u32 = 32;

/// Block flag: data not yet uploaded to S3.
pub const FLAG_DIRTY: u8 = 0x01;

/// Block flag: pinned by user (reserved for future use).
pub const FLAG_PINNED: u8 = 0x02;

/// Round `value` up to the next multiple of `align`.
#[inline]
pub const fn align_up(value: u64, align: u64) -> u64 {
    value.div_ceil(align) * align
}

/// Calculate the total on-disk allocation size for a block with `data_size` payload bytes.
///
/// Returns the smallest page-aligned size that fits the 32-byte header plus the payload.
#[inline]
pub const fn alloc_size_for(data_size: u32) -> u64 {
    align_up(BLOCK_HEADER_SIZE as u64 + data_size as u64, PAGE_SIZE)
}

// ---------------------------------------------------------------------------
// Superblock
// ---------------------------------------------------------------------------

/// Superblock (4096 bytes, stored at file offset 0).
///
/// | Offset | Size | Field          |
/// |--------|------|----------------|
/// | 0      | 8    | magic          |
/// | 8      | 4    | version        |
/// | 12     | 4    | page_size      |
/// | 16     | 8    | max_size       |
/// | 24     | 8    | created_at     |
/// | 32     | 16   | volume_uuid    |
/// | 48     | 4048 | _reserved (0)  |
#[derive(Debug, Clone)]
pub struct Superblock {
    pub magic: [u8; 8],
    pub version: u32,
    pub page_size: u32,
    pub max_size: u64,
    pub created_at: u64,
    pub volume_uuid: [u8; 16],
}

impl Superblock {
    pub fn new(max_size: u64, volume_uuid: [u8; 16]) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        Self {
            magic: SUPERBLOCK_MAGIC,
            version: FORMAT_VERSION,
            page_size: PAGE_SIZE as u32,
            max_size,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            volume_uuid,
        }
    }

    /// Serialize to a 4096-byte page.
    pub fn to_bytes(&self) -> [u8; 4096] {
        let mut buf = [0u8; 4096];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        buf[16..24].copy_from_slice(&self.max_size.to_le_bytes());
        buf[24..32].copy_from_slice(&self.created_at.to_le_bytes());
        buf[32..48].copy_from_slice(&self.volume_uuid);
        buf
    }

    /// Deserialize from a 4096-byte page.
    pub fn from_bytes(buf: &[u8; 4096]) -> io::Result<Self> {
        let magic: [u8; 8] = buf[0..8].try_into().unwrap();
        if magic != SUPERBLOCK_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad superblock magic: {magic:?}"),
            ));
        }
        let version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let page_size = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let max_size = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let created_at = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        let mut volume_uuid = [0u8; 16];
        volume_uuid.copy_from_slice(&buf[32..48]);
        Ok(Self {
            magic,
            version,
            page_size,
            max_size,
            created_at,
            volume_uuid,
        })
    }
}

// ---------------------------------------------------------------------------
// Block Header
// ---------------------------------------------------------------------------

/// Block header (32 bytes, stored at the start of each allocated region).
///
/// | Offset | Size | Field       |
/// |--------|------|-------------|
/// | 0      | 4    | magic       |
/// | 4      | 1    | flags       |
/// | 5      | 3    | _reserved   |
/// | 8      | 8    | object_id   |
/// | 16     | 4    | data_size   |
/// | 20     | 4    | checksum    |
/// | 24     | 8    | _padding    |
#[derive(Debug, Clone, Copy)]
pub struct BlockHeader {
    pub magic: [u8; 4],
    pub flags: u8,
    pub object_id: u64,
    pub data_size: u32,
    pub checksum: u32,
}

impl BlockHeader {
    pub fn new(object_id: u64, data_size: u32, checksum: u32, flags: u8) -> Self {
        Self {
            magic: BLOCK_MAGIC,
            flags,
            object_id,
            data_size,
            checksum,
        }
    }

    /// Serialize to a 32-byte on-disk representation.
    pub fn to_bytes(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4] = self.flags;
        // 5..8: reserved zeros
        buf[8..16].copy_from_slice(&self.object_id.to_le_bytes());
        buf[16..20].copy_from_slice(&self.data_size.to_le_bytes());
        buf[20..24].copy_from_slice(&self.checksum.to_le_bytes());
        // 24..32: padding zeros
        buf
    }

    /// Deserialize from a 32-byte buffer.
    pub fn from_bytes(buf: &[u8; 32]) -> io::Result<Self> {
        let magic: [u8; 4] = buf[0..4].try_into().unwrap();
        if magic != BLOCK_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad block magic: {magic:?}"),
            ));
        }
        let flags = buf[4];
        let object_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let data_size = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let checksum = u32::from_le_bytes(buf[20..24].try_into().unwrap());
        Ok(Self {
            magic,
            flags,
            object_id,
            data_size,
            checksum,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_up_basics() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
    }

    #[test]
    fn alloc_size_for_examples() {
        // 1 byte payload: hdr(32) + 1 = 33 → 4096
        assert_eq!(alloc_size_for(1), 4096);
        // 4064 bytes: hdr(32) + 4064 = 4096 → 4096
        assert_eq!(alloc_size_for(4064), 4096);
        // 4065 bytes: hdr(32) + 4065 = 4097 → 8192
        assert_eq!(alloc_size_for(4065), 8192);
        // 16 MiB block
        assert_eq!(alloc_size_for(16 * 1024 * 1024), 16 * 1024 * 1024 + 4096);
    }

    #[test]
    fn superblock_round_trip() {
        let uuid = [1u8; 16];
        let sb = Superblock::new(1024 * 1024, uuid);
        let bytes = sb.to_bytes();
        let sb2 = Superblock::from_bytes(&bytes).unwrap();
        assert_eq!(sb2.magic, SUPERBLOCK_MAGIC);
        assert_eq!(sb2.version, FORMAT_VERSION);
        assert_eq!(sb2.max_size, 1024 * 1024);
        assert_eq!(sb2.volume_uuid, uuid);
    }

    #[test]
    fn superblock_bad_magic() {
        let mut bytes = [0u8; 4096];
        bytes[0..8].copy_from_slice(b"BADBAD\x00\x00");
        assert!(Superblock::from_bytes(&bytes).is_err());
    }

    #[test]
    fn block_header_round_trip() {
        let hdr = BlockHeader::new(42, 1024, 0xDEAD_BEEF, FLAG_DIRTY);
        let bytes = hdr.to_bytes();
        let hdr2 = BlockHeader::from_bytes(&bytes).unwrap();
        assert_eq!(hdr2.object_id, 42);
        assert_eq!(hdr2.data_size, 1024);
        assert_eq!(hdr2.checksum, 0xDEAD_BEEF);
        assert_eq!(hdr2.flags, FLAG_DIRTY);
    }

    #[test]
    fn block_header_bad_magic() {
        let mut bytes = [0u8; 32];
        bytes[0..4].copy_from_slice(b"BAAD");
        assert!(BlockHeader::from_bytes(&bytes).is_err());
    }
}
