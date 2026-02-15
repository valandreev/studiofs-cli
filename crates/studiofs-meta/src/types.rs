//! StudioFS Metadata Types
//!
//! Core types for the virtual filesystem metadata, inspired by JuiceFS internals.
//! Files are split into 64MiB chunks, each containing slices (write units),
//! which are further divided into 4MiB blocks (S3 storage units).

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

// ─── Constants ───────────────────────────────────────────────────────────────

/// Size of a single chunk (64 MiB).
pub const CHUNK_SIZE: u64 = 64 * 1024 * 1024;

/// Size of a single block (4 MiB) — the unit stored in S3.
pub const BLOCK_SIZE: u32 = 4 * 1024 * 1024;

/// Root inode number.
pub const ROOT_INO: Ino = 1;

// ─── Inode ───────────────────────────────────────────────────────────────────

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

/// File attributes stored per inode.
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
    /// Last access time (seconds since epoch).
    pub atime_secs: i64,
    /// Last access time (nanoseconds part).
    pub atime_nanos: u32,
    /// Last modification time (seconds since epoch).
    pub mtime_secs: i64,
    /// Last modification time (nanoseconds part).
    pub mtime_nanos: u32,
    /// Last metadata change time (seconds since epoch).
    pub ctime_secs: i64,
    /// Last metadata change time (nanoseconds part).
    pub ctime_nanos: u32,
    /// Parent inode (0 for root).
    pub parent: Ino,
}

impl InodeAttr {
    /// Create attributes for a new directory.
    pub fn new_dir(ino: Ino, parent: Ino, uid: u32, gid: u32, mode: u16) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            ino,
            kind: FileType::Directory,
            mode,
            uid,
            gid,
            size: 4096,
            nlink: 2, // '.' and '..'
            atime_secs: now.as_secs() as i64,
            atime_nanos: now.subsec_nanos(),
            mtime_secs: now.as_secs() as i64,
            mtime_nanos: now.subsec_nanos(),
            ctime_secs: now.as_secs() as i64,
            ctime_nanos: now.subsec_nanos(),
            parent,
        }
    }

    /// Create attributes for a new regular file.
    pub fn new_file(ino: Ino, parent: Ino, uid: u32, gid: u32, mode: u16) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            ino,
            kind: FileType::RegularFile,
            mode,
            uid,
            gid,
            size: 0,
            nlink: 1,
            atime_secs: now.as_secs() as i64,
            atime_nanos: now.subsec_nanos(),
            mtime_secs: now.as_secs() as i64,
            mtime_nanos: now.subsec_nanos(),
            ctime_secs: now.as_secs() as i64,
            ctime_nanos: now.subsec_nanos(),
            parent,
        }
    }

    /// Update modification and change times to now.
    pub fn touch_mtime(&mut self) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        self.mtime_secs = now.as_secs() as i64;
        self.mtime_nanos = now.subsec_nanos();
        self.ctime_secs = now.as_secs() as i64;
        self.ctime_nanos = now.subsec_nanos();
    }
}

// ─── Directory Entry ─────────────────────────────────────────────────────────

/// A directory entry (edge in the file tree).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    /// Entry name (filename).
    pub name: Vec<u8>,
    /// Inode number of the child.
    pub ino: Ino,
    /// File type of the child.
    pub kind: FileType,
}

/// Compact directory entry value stored in KV (without name, since name is in the key).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntryValue {
    pub ino: Ino,
    pub kind: FileType,
}

// ─── Chunk / Slice ───────────────────────────────────────────────────────────

/// A slice within a chunk, representing one write operation.
///
/// Multiple slices can overlap within a chunk; later slices take precedence.
/// This is the exact same model as JuiceFS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Slice {
    /// Offset of the slice within the chunk (0..CHUNK_SIZE).
    pub pos: u32,
    /// Globally unique slice ID. ID 0 means a hole (zero-filled).
    pub id: u64,
    /// Total size of data in this slice.
    pub size: u32,
    /// Offset of valid data within this slice.
    pub off: u32,
    /// Length of valid data within this slice.
    pub len: u32,
}

impl Slice {
    /// Size in bytes when binary-encoded (matches JuiceFS: 24 bytes).
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

/// Key for looking up chunk data: (inode, chunk_index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkKey {
    pub ino: Ino,
    pub index: u32,
}

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

// ─── Format Info ─────────────────────────────────────────────────────────────

/// Filesystem format information, stored at init time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatInfo {
    /// Volume name.
    pub name: String,
    /// Unique volume ID.
    pub uuid: String,
    /// S3 endpoint URL.
    pub endpoint: String,
    /// S3 bucket name.
    pub bucket: String,
    /// S3 access key (stored encrypted in production).
    pub access_key: String,
    /// S3 secret key (stored encrypted in production).
    pub secret_key: String,
    /// S3 region (optional).
    pub region: String,
    /// Block size in bytes (default 4MiB, fixed).
    pub block_size: u32,
    /// Chunk size in bytes (default 64MiB, fixed).
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
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        }
    }
}

// ─── Counter Names ───────────────────────────────────────────────────────────

/// Well-known counter names in the metadata store.
pub mod counters {
    /// Next available inode number.
    pub const NEXT_INODE: &str = "nextInode";
    /// Next available slice ID.
    pub const NEXT_SLICE_ID: &str = "nextSliceId";
    /// Total used space in bytes.
    pub const USED_SPACE: &str = "usedSpace";
    /// Total number of inodes in use.
    pub const TOTAL_INODES: &str = "totalInodes";
}
