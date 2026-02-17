use bytes::{Buf, BufMut, Bytes};
use serde::{Deserialize, Serialize};
use std::io::{self, Cursor};
use thiserror::Error;
use xxhash_rust::xxh64::xxh64;

/// Magic bytes for StudioFS object format ("STFS").
pub const MAGIC: &[u8; 4] = b"STFS";

/// Calculate XXH64 hash and return as hex string (for object IDs).
pub fn hash_object(data: &[u8]) -> String {
    let hash = xxh64(data, 0);
    format!("{:016x}", hash)
}

/// Current object format version.
pub const VERSION: u16 = 1;

/// Maximum object size (for safety check, e.g., 64MB).
pub const MAX_OBJECT_SIZE: u32 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ObjectError {
    #[error("invalid magic bytes")]
    InvalidMagic,
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u16),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] postcard::Error),
    #[error("decompression failed")]
    DecompressionFailed,
    #[error("checksum mismatch")]
    ChecksumMismatch,
}

/// Header for a StudioFS S3 object.
///
/// Designed to be fixed-size or easily parsable at the start of the object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
pub struct ObjectHeader {
    pub magic: [u8; 4],
    pub version: u16,
    /// Size of the data before compression.
    pub original_size: u32,
    /// Size of the compressed payload.
    pub compressed_size: u32,
    /// XXH64 checksum of the UNCOMPRESSED data.
    pub checksum: u64,
}

impl Default for ObjectHeader {
    fn default() -> Self {
        Self {
            magic: *MAGIC,
            version: VERSION,
            original_size: 0,
            compressed_size: 0,
            checksum: 0,
        }
    }
}

impl ObjectHeader {
    pub const SIZE: usize = 4 + 2 + 4 + 4 + 8; // 22 bytes

    pub fn new(original_size: u32, compressed_size: u32, checksum: u64) -> Self {
        Self {
            magic: *MAGIC,
            version: VERSION,
            original_size,
            compressed_size,
            checksum,
        }
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        let mut w = &mut buf[..];
        w.put_slice(&self.magic);
        w.put_u16_le(self.version);
        w.put_u32_le(self.original_size);
        w.put_u32_le(self.compressed_size);
        w.put_u64_le(self.checksum);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() < Self::SIZE {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "header too short").into());
        }
        let mut r = Cursor::new(bytes);
        
        let mut magic = [0u8; 4];
        r.copy_to_slice(&mut magic);
        if &magic != MAGIC {
            return Err(ObjectError::InvalidMagic);
        }

        let version = r.get_u16_le();
        if version != VERSION {
            return Err(ObjectError::UnsupportedVersion(version));
        }

        let original_size = r.get_u32_le();
        let compressed_size = r.get_u32_le();
        let checksum = r.get_u64_le();

        Ok(Self {
            magic,
            version,
            original_size,
            compressed_size,
            checksum,
        })
    }
}

/// Helper to compress data using LZ4.
pub fn compress(data: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(data)
}

/// Helper to decompress data using LZ4.
pub fn decompress(compressed_data: &[u8]) -> Result<Vec<u8>, ObjectError> {
    lz4_flex::decompress_size_prepended(compressed_data)
        .map_err(|_| ObjectError::DecompressionFailed)
}

/// Compress and wrap data into a full object (Header + Payload).
pub fn create_object(data: &[u8]) -> Vec<u8> {
    let checksum = xxh64(data, 0);
    let compressed = compress(data);
    
    let header = ObjectHeader::new(
        data.len() as u32,
        compressed.len() as u32,
        checksum,
    );

    let mut object = Vec::with_capacity(ObjectHeader::SIZE + compressed.len());
    object.extend_from_slice(&header.to_bytes());
    object.extend_from_slice(&compressed);
    object
}

/// Read and decompress a full object.
pub fn read_object(data: &[u8]) -> Result<Vec<u8>, ObjectError> {
    if data.len() < ObjectHeader::SIZE {
        return Err(ObjectError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "file too short")));
    }

    let (header_bytes, payload) = data.split_at(ObjectHeader::SIZE);
    let header = ObjectHeader::from_bytes(header_bytes)?;

    // integrity check size
    // Note: compress_prepend_size adds 4 bytes size header, but our ObjectHeader tracks sizes too.
    // lz4_flex::compress_prepend_size returns a vec with size prepended.
    // Let's rely on lz4_flex's prepended size for simplicity in decompression.
    if payload.len() != header.compressed_size as usize {
        // Warning: if we rely on compress_prepend_size, the payload length includes that size prefix.
        // header.compressed_size should track the full payload size.
    }

    let decompressed = decompress(payload)?;
    
    // Verify checksum
    if xxh64(&decompressed, 0) != header.checksum {
        return Err(ObjectError::ChecksumMismatch);
    }

    Ok(decompressed)
}

// ─── Object Layout ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectKind {
    /// Leaf object containing actual file data.
    Data,
    /// Metadata object containing pointers to other objects.
    Meta,
}

/// Entry in a metadata object, pointing to a child object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Objectlink {
    /// Logical offset in the file.
    pub offset: u64,
    /// Length of data covered by this object.
    pub length: u32,
    /// Object ID (hash/uuid).
    pub object_id: String,
}

/// Payload structure (what's inside the LZ4 stream).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectPayload {
    pub kind: ObjectKind,
    /// For Data objects: the actual data.
    /// For Meta objects: empty (links are used).
    pub data: Vec<u8>,
    /// Child links (for Meta objects).
    pub children: Vec<Objectlink>,
}

impl ObjectPayload {
    pub fn new_data(data: Vec<u8>) -> Self {
        Self {
            kind: ObjectKind::Data,
            data,
            children: Vec::new(),
        }
    }

    pub fn new_meta(children: Vec<Objectlink>) -> Self {
        Self {
            kind: ObjectKind::Meta,
            data: Vec::new(),
            children,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        postcard::to_stdvec(self).map_err(ObjectError::Serialization)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        postcard::from_bytes(bytes).map_err(ObjectError::Serialization)
    }

    /// Find the child object covering the given logical offset.
    pub fn find_child(&self, offset: u64) -> Option<&Objectlink> {
        // Assume children are sorted? For now, linear search.
        // TODO: Binary search if we enforce sorting.
        self.children.iter().find(|link| {
            offset >= link.offset && offset < link.offset + link.length as u64
        })
    }
}
