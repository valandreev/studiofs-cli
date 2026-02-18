//! StudioFS Metadata Types
//!
//! Core types for the virtual filesystem metadata.
//! 
//! Architecture v2 Key Changes:
//! - Files are split into fixed-size blocks (default 4MB)
//! - Each block has a sequential ID (not content hash)
//! - Blocks are immutable - never modified, only created/deleted
//! - No deduplication = no reference counting overhead
//! - All metadata in redb, S3 stores only raw data blocks

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

// =============================================================================
// Constants
// =============================================================================

/// Default block size: 4 MiB (configurable at volume init).
pub const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024 * 1024;

/// Root inode number.
pub const ROOT_INO: Ino = 1;

// =============================================================================
// Inode Types
// =============================================================================

/// Inode number type.
pub type Ino = u64;

/// File type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum FileType {
    RegularFile = 1,
    Directory = 2,
    Symlink = 3,
}

impl FileType {
    /// Convert from raw u8 value.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::RegularFile),
            2 => Some(Self::Directory),
            3 => Some(Self::Symlink),
            _ => None,
        }
    }
}

/// Inode attributes - stored in ATTRIBUTES table.
///
/// Key design: timestamps stored as nanoseconds since Unix epoch,
/// simplifying comparisons and range queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeAttr {
    /// Inode number.
    pub ino: Ino,
    /// File type.
    pub kind: FileType,
    /// Permission mode (Unix-style, e.g. 0o755).
    pub mode: u16,
    /// Owner user ID.
    pub uid: u32,
    /// Owner group ID.
    pub gid: u32,
    /// File size in bytes.
    pub size: u64,
    /// Number of hard links.
    pub nlink: u32,
    /// Modification time (nanoseconds since Unix epoch).
    pub mtime: i64,
    /// Change time (nanoseconds since Unix epoch).
    pub ctime: i64,
    /// Access time (nanoseconds since Unix epoch).
    pub atime: i64,
    /// Parent inode (0 for root).
    pub parent: Ino,
}

impl InodeAttr {
    /// Get current time as nanoseconds since Unix epoch.
    fn now_nanos() -> i64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    /// Create attributes for a new directory.
    pub fn new_dir(ino: Ino, parent: Ino, uid: u32, gid: u32, mode: u16) -> Self {
        let now = Self::now_nanos();
        Self {
            ino,
            kind: FileType::Directory,
            mode,
            uid,
            gid,
            size: 4096,
            nlink: 2, // '.' and '..'
            mtime: now,
            ctime: now,
            atime: now,
            parent,
        }
    }

    /// Create attributes for a new regular file.
    pub fn new_file(ino: Ino, parent: Ino, uid: u32, gid: u32, mode: u16) -> Self {
        let now = Self::now_nanos();
        Self {
            ino,
            kind: FileType::RegularFile,
            mode,
            uid,
            gid,
            size: 0,
            nlink: 1,
            mtime: now,
            ctime: now,
            atime: now,
            parent,
        }
    }

    /// Update modification and change times to now.
    pub fn touch_mtime(&mut self) {
        let now = Self::now_nanos();
        self.mtime = now;
        self.ctime = now;
    }

    /// Update access time to now.
    pub fn touch_atime(&mut self) {
        self.atime = Self::now_nanos();
    }
}

// =============================================================================
// Directory Entry Types
// =============================================================================

/// Directory entry value - stored in DIRECTORY_MAP.
///
/// Key: (parent_ino, name_hash) for O(1) lookup by path component.
/// Name is not stored here - it's part of the key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntryValue {
    /// Inode number of the child.
    pub ino: Ino,
    /// File type of the child.
    pub kind: FileType,
}

// =============================================================================
// Block Reference Types
// =============================================================================

bitflags::bitflags! {
    /// Block flags indicating compression and other properties.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BlockFlags: u8 {
        /// Block is compressed with LZ4.
        const COMPRESSED_LZ4 = 0x01;
        /// Block is partial (not a full block, typically the last block of a file).
        const PARTIAL_BLOCK = 0x02;
    }
}

impl Default for BlockFlags {
    fn default() -> Self {
        Self::COMPRESSED_LZ4
    }
}

/// Block reference - stored in FILE_BLOCKS table.
///
/// Points to an immutable S3 object containing compressed data.
/// Key: (inode, block_index) - each block has a single owner.
///
/// Design decisions:
/// - Sequential object_id (not content hash) - simpler and faster
/// - No reference counting - block belongs to exactly one file position
/// - Immutable objects - never modified, only created/deleted
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRef {
    /// Sequential block ID (unique across the volume).
    pub object_id: u64,
    /// Offset within the block (for partial blocks, typically 0).
    pub offset: u32,
    /// Length of valid data in this block.
    pub length: u32,
    /// Compression and other flags.
    pub flags: BlockFlags,
}

impl BlockRef {
    /// Create a new block reference for a full block.
    pub fn new_full(object_id: u64, length: u32) -> Self {
        Self {
            object_id,
            offset: 0,
            length,
            flags: BlockFlags::COMPRESSED_LZ4,
        }
    }

    /// Create a new block reference for a partial block (last block of file).
    pub fn new_partial(object_id: u64, length: u32) -> Self {
        Self {
            object_id,
            offset: 0,
            length,
            flags: BlockFlags::COMPRESSED_LZ4 | BlockFlags::PARTIAL_BLOCK,
        }
    }

    /// Check if this is a partial block.
    pub fn is_partial(&self) -> bool {
        self.flags.contains(BlockFlags::PARTIAL_BLOCK)
    }

    /// Check if this block is compressed.
    pub fn is_compressed(&self) -> bool {
        self.flags.contains(BlockFlags::COMPRESSED_LZ4)
    }
}

// =============================================================================
// Format Info
// =============================================================================

/// Volume format information - stored in FORMAT_INFO table.
///
/// Created at volume initialization, immutable thereafter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatInfo {
    /// Volume name (user-friendly identifier).
    pub name: String,
    /// Unique volume UUID.
    pub uuid: String,
    /// S3 endpoint URL.
    pub endpoint: String,
    /// S3 bucket name.
    pub bucket: String,
    /// S3 access key.
    pub access_key: String,
    /// S3 secret key.
    pub secret_key: String,
    /// S3 region.
    pub region: String,
    /// Block size in bytes (configurable at init, default 4MB).
    pub block_size: u32,
    /// Chunk size - deprecated, kept for compatibility.
    #[serde(default)]
    pub chunk_size: u64,
}

impl Default for FormatInfo {
    fn default() -> Self {
        Self {
            name: String::new(),
            uuid: String::new(),
            endpoint: String::new(),
            bucket: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            region: String::new(),
            block_size: DEFAULT_BLOCK_SIZE,
            chunk_size: 0, // Deprecated
        }
    }
}

impl FormatInfo {
    /// Create a new format info with the specified parameters.
    pub fn new(
        name: String,
        uuid: String,
        endpoint: String,
        bucket: String,
        access_key: String,
        secret_key: String,
        region: String,
        block_size_mb: u32,
    ) -> Self {
        Self {
            name,
            uuid,
            endpoint,
            bucket,
            access_key,
            secret_key,
            region,
            block_size: block_size_mb * 1024 * 1024, // Convert MB to bytes
            chunk_size: 0,
        }
    }

    /// Get block size in megabytes (for display purposes).
    pub fn block_size_mb(&self) -> u32 {
        self.block_size / (1024 * 1024)
    }
}

// =============================================================================
// Counter Names
// =============================================================================

/// Well-known counter names in the metadata store.
pub mod counters {
    /// Next available inode number.
    pub const NEXT_INODE: &str = "nextInode";
    /// Next available block ID (sequential).
    pub const NEXT_BLOCK_ID: &str = "nextBlockId";
    /// Total used space in bytes.
    pub const USED_SPACE: &str = "usedSpace";
    /// Total number of inodes in use.
    pub const TOTAL_INODES: &str = "totalInodes";
    
    /// Legacy: Next available slice ID.
    /// Kept for backward compatibility during migration.
    #[deprecated(since = "0.2.0", note = "Use NEXT_BLOCK_ID instead")]
    pub const NEXT_SLICE_ID: &str = "nextSliceId";
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Calculate the block index for a given file offset.
#[inline]
pub fn block_index_from_offset(offset: u64, block_size: u32) -> u32 {
    (offset / block_size as u64) as u32
}

/// Calculate the offset within a block for a given file offset.
#[inline]
pub fn offset_within_block(offset: u64, block_size: u32) -> u32 {
    (offset % block_size as u64) as u32
}

/// Calculate the number of blocks needed for a file of given size.
#[inline]
pub fn blocks_for_size(size: u64, block_size: u32) -> u32 {
    ((size + block_size as u64 - 1) / block_size as u64) as u32
}

// =============================================================================
// Backward Compatibility (Deprecated - will be removed in Phase 2)
// =============================================================================

/// Legacy constants for backward compatibility.
/// Use DEFAULT_BLOCK_SIZE instead.
#[deprecated(since = "0.2.0", note = "Use DEFAULT_BLOCK_SIZE instead")]
pub const BLOCK_SIZE: u32 = DEFAULT_BLOCK_SIZE;

/// Legacy chunk size constant.
/// The new architecture no longer uses chunks - files are split directly into blocks.
#[deprecated(since = "0.2.0", note = "Chunk-based storage is deprecated, use block-based instead")]
pub const CHUNK_SIZE: u64 = 64 * 1024 * 1024;

/// Legacy directory entry with name included.
/// Use DirEntryValue instead (name is now part of the key).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[deprecated(since = "0.2.0", note = "Use DirEntryValue instead")]
pub struct DirEntry {
    /// Entry name (filename).
    pub name: Vec<u8>,
    /// Inode number of the child.
    pub ino: Ino,
    /// File type of the child.
    pub kind: FileType,
}

/// Legacy slice structure for chunk-based storage.
/// The new architecture uses BlockRef instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[deprecated(since = "0.2.0", note = "Use BlockRef instead")]
pub struct Slice {
    /// Offset of the slice within the chunk.
    pub pos: u32,
    /// Globally unique slice ID. ID 0 means a hole.
    pub id: u64,
    /// Total size of data in this slice.
    pub size: u32,
    /// Offset of valid data within this slice.
    pub off: u32,
    /// Length of valid data within this slice.
    pub len: u32,
}

#[allow(deprecated)]
impl Slice {
    /// Size in bytes when binary-encoded.
    pub const ENCODED_SIZE: usize = 24;

    /// Create a "hole" slice (zero-filled region).
    pub fn hole(pos: u32, len: u32) -> Self {
        Self {
            pos,
            id: 0,
            size: len,
            off: 0,
            len,
        }
    }
}

/// Legacy chunk key for chunk-based storage.
/// The new architecture uses (inode, block_index) tuple directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[deprecated(since = "0.2.0", note = "Use (Ino, u32) tuple instead")]
pub struct ChunkKey {
    pub ino: Ino,
    pub index: u32,
}

#[allow(deprecated)]
impl ChunkKey {
    pub fn new(ino: Ino, index: u32) -> Self {
        Self { ino, index }
    }

    /// Calculate chunk key from a file offset.
    pub fn from_offset(ino: Ino, offset: u64) -> Self {
        Self {
            ino,
            index: (offset / CHUNK_SIZE) as u32,
        }
    }
}

/// Legacy counter name for slice IDs.
/// Use NEXT_BLOCK_ID instead.
#[deprecated(since = "0.2.0", note = "Use NEXT_BLOCK_ID instead")]
pub const NEXT_SLICE_ID: &str = "nextSliceId";

// Re-export in counters module for backward compatibility
#[allow(deprecated)]
pub use self::NEXT_SLICE_ID as legacy_NEXT_SLICE_ID;

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_index_calculation() {
        let block_size = 4 * 1024 * 1024; // 4MB
        
        assert_eq!(block_index_from_offset(0, block_size), 0);
        assert_eq!(block_index_from_offset(1, block_size), 0);
        assert_eq!(block_index_from_offset(4 * 1024 * 1024, block_size), 1);
        assert_eq!(block_index_from_offset(4 * 1024 * 1024 + 1, block_size), 1);
        assert_eq!(block_index_from_offset(8 * 1024 * 1024, block_size), 2);
    }

    #[test]
    fn test_offset_within_block() {
        let block_size = 4 * 1024 * 1024; // 4MB
        
        assert_eq!(offset_within_block(0, block_size), 0);
        assert_eq!(offset_within_block(100, block_size), 100);
        assert_eq!(offset_within_block(4 * 1024 * 1024, block_size), 0);
        assert_eq!(offset_within_block(4 * 1024 * 1024 + 100, block_size), 100);
    }

    #[test]
    fn test_blocks_for_size() {
        let block_size = 4 * 1024 * 1024; // 4MB
        
        assert_eq!(blocks_for_size(0, block_size), 0);
        assert_eq!(blocks_for_size(1, block_size), 1);
        assert_eq!(blocks_for_size(4 * 1024 * 1024, block_size), 1);
        assert_eq!(blocks_for_size(4 * 1024 * 1024 + 1, block_size), 2);
        assert_eq!(blocks_for_size(8 * 1024 * 1024, block_size), 2);
    }

    #[test]
    fn test_block_ref_flags() {
        let full_block = BlockRef::new_full(123, 4 * 1024 * 1024);
        assert!(!full_block.is_partial());
        assert!(full_block.is_compressed());

        let partial_block = BlockRef::new_partial(124, 1000);
        assert!(partial_block.is_partial());
        assert!(partial_block.is_compressed());
    }

    #[test]
    fn test_inode_attr_times() {
        let attr = InodeAttr::new_file(2, 1, 1000, 1000, 0o644);
        assert!(attr.mtime > 0);
        assert!(attr.ctime > 0);
        assert!(attr.atime > 0);
    }
}
