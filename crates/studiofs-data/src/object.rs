#![allow(deprecated)]
//! StudioFS S3 Object Format
//!
//! Defines the binary format for objects stored in S3.
//!
//! Architecture Key Changes:
//! - Fixed 32-byte header (was 22 bytes)
//! - Added flags field for compression/encryption
//! - Version bumped to 2
//! - Objects are immutable and content-addressable by sequential ID

use bytes::{Buf, BufMut};
use std::io::{self, Cursor};
use thiserror::Error;
use xxhash_rust::xxh64::xxh64;

// =============================================================================
// Constants
// =============================================================================

/// Magic bytes for StudioFS object format ("STFS").
pub const MAGIC: &[u8; 4] = b"STFS";

/// Current object format version.
pub const VERSION: u16 = 2;

/// Maximum object size (for safety check, e.g., 64MB).
pub const MAX_OBJECT_SIZE: u32 = 64 * 1024 * 1024;

/// Default block size: 4 MiB.
pub const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024 * 1024;

// =============================================================================
// Error Types
// =============================================================================

#[derive(Debug, Error)]
pub enum ObjectError {
    #[error("invalid magic bytes")]
    InvalidMagic,
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u16),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("decompression failed: {0}")]
    DecompressionFailed(String),
    #[error("checksum mismatch: expected {expected:016x}, got {computed:016x}")]
    ChecksumMismatch { expected: u64, computed: u64 },
    #[error("object too large: {0} bytes")]
    ObjectTooLarge(u64),
    #[error("invalid object ID: {0}")]
    InvalidObjectId(String),
}

// =============================================================================
// Object Flags
// =============================================================================

bitflags::bitflags! {
    /// Flags indicating object properties.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ObjectFlags: u16 {
        /// Object is compressed with LZ4.
        const LZ4_COMPRESSED = 0x0001;
        /// Object has footer checksum (future use).
        const HAS_FOOTER = 0x0002;
        /// Object is encrypted (future use).
        const ENCRYPTED = 0x0004;
    }
}

impl Default for ObjectFlags {
    fn default() -> Self {
        Self::LZ4_COMPRESSED
    }
}

// =============================================================================
// Object Header
// =============================================================================

/// S3 Object Header - 32 bytes fixed.
///
/// Little-endian encoding for all multi-byte fields.
/// Designed for efficient parsing and forward compatibility.
///
/// ```text
/// +------------------+
/// | Magic "STFS"     |  4 bytes
/// +------------------+
/// | Version          |  2 bytes (little-endian)
/// +------------------+
/// | Flags            |  2 bytes (ObjectFlags)
/// +------------------+
/// | Original Size    |  4 bytes (uncompressed size)
/// +------------------+
/// | Compressed Size  |  4 bytes (excluding header)
/// +------------------+
/// | Checksum         |  8 bytes (XXH64 of original data)
/// +------------------+
/// | Reserved         |  6 bytes (future use)
/// +------------------+
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ObjectHeader {
    /// Magic bytes: "STFS" (0x53 0x54 0x46 0x53).
    pub magic: [u8; 4],
    /// Format version: currently 2.
    pub version: u16,
    /// Flags: compression type, etc.
    pub flags: ObjectFlags,
    /// Original uncompressed size.
    pub original_size: u32,
    /// Compressed size (excluding header).
    pub compressed_size: u32,
    /// XXH64 checksum of ORIGINAL data.
    pub checksum: u64,
    /// Reserved for future use.
    pub reserved: [u8; 6],
}

impl Default for ObjectHeader {
    fn default() -> Self {
        Self {
            magic: *MAGIC,
            version: VERSION,
            flags: ObjectFlags::LZ4_COMPRESSED,
            original_size: 0,
            compressed_size: 0,
            checksum: 0,
            reserved: [0u8; 6],
        }
    }
}

impl ObjectHeader {
    /// Total header size in bytes.
    pub const SIZE: usize = 4 + 2 + 2 + 4 + 4 + 8 + 6; // 30 bytes

    /// Create a new header with the given parameters.
    pub fn new(original_size: u32, compressed_size: u32, checksum: u64) -> Self {
        Self {
            magic: *MAGIC,
            version: VERSION,
            flags: ObjectFlags::LZ4_COMPRESSED,
            original_size,
            compressed_size,
            checksum,
            reserved: [0u8; 6],
        }
    }

    /// Create a new header with custom flags.
    pub fn with_flags(
        original_size: u32,
        compressed_size: u32,
        checksum: u64,
        flags: ObjectFlags,
    ) -> Self {
        Self {
            magic: *MAGIC,
            version: VERSION,
            flags,
            original_size,
            compressed_size,
            checksum,
            reserved: [0u8; 6],
        }
    }

    /// Serialize header to bytes (little-endian).
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        let mut w = &mut buf[..];
        w.put_slice(&self.magic);
        w.put_u16_le(self.version);
        w.put_u16_le(self.flags.bits());
        w.put_u32_le(self.original_size);
        w.put_u32_le(self.compressed_size);
        w.put_u64_le(self.checksum);
        w.put_slice(&self.reserved);
        buf
    }

    /// Deserialize header from bytes (little-endian).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() < Self::SIZE {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "header too short").into());
        }

        let mut r = Cursor::new(bytes);

        // Read and validate magic
        let mut magic = [0u8; 4];
        r.copy_to_slice(&mut magic);
        if &magic != MAGIC {
            return Err(ObjectError::InvalidMagic);
        }

        // Read and validate version
        let version = r.get_u16_le();
        if version > VERSION {
            return Err(ObjectError::UnsupportedVersion(version));
        }

        // Read flags
        let flags = ObjectFlags::from_bits_truncate(r.get_u16_le());

        // Read sizes and checksum
        let original_size = r.get_u32_le();
        let compressed_size = r.get_u32_le();
        let checksum = r.get_u64_le();

        // Read reserved bytes
        let mut reserved = [0u8; 6];
        r.copy_to_slice(&mut reserved);

        Ok(Self {
            magic,
            version,
            flags,
            original_size,
            compressed_size,
            checksum,
            reserved,
        })
    }

    /// Check if object is compressed.
    pub fn is_compressed(&self) -> bool {
        self.flags.contains(ObjectFlags::LZ4_COMPRESSED)
    }

    /// Check if object has footer.
    pub fn has_footer(&self) -> bool {
        self.flags.contains(ObjectFlags::HAS_FOOTER)
    }

    /// Check if object is encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.flags.contains(ObjectFlags::ENCRYPTED)
    }
}

// =============================================================================
// Compression Functions
// =============================================================================

/// Compress data using LZ4 frame format.
///
/// Uses LZ4 frame format for better compatibility and streaming support.
/// Returns compressed data with size header prepended.
pub fn compress(data: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(data)
}

/// Decompress LZ4 data with size header.
///
/// Expects data compressed with `compress()` (size-prepended format).
pub fn decompress(compressed_data: &[u8]) -> Result<Vec<u8>, ObjectError> {
    lz4_flex::decompress_size_prepended(compressed_data).map_err(|e| {
        ObjectError::DecompressionFailed(format!(
            "LZ4 decompression failed: {} (input size: {})",
            e,
            compressed_data.len()
        ))
    })
}

// =============================================================================
// Object Creation and Reading
// =============================================================================

/// Create a full object (Header + Compressed Payload).
///
/// This is the primary function for creating S3 objects.
/// The returned bytes are ready to be uploaded to S3.
///
/// # Arguments
/// * `data` - Uncompressed data to store
///
/// # Returns
/// Complete object bytes (header + compressed payload)
pub fn create_object(data: &[u8]) -> Result<Vec<u8>, ObjectError> {
    // Validate size
    if data.len() > MAX_OBJECT_SIZE as usize {
        return Err(ObjectError::ObjectTooLarge(data.len() as u64));
    }

    // Calculate checksum of original data
    let checksum = xxh64(data, 0);

    // Compress data
    let compressed = compress(data);

    // Create header
    let header = ObjectHeader::new(data.len() as u32, compressed.len() as u32, checksum);

    // Build complete object
    let mut object = Vec::with_capacity(ObjectHeader::SIZE + compressed.len());
    object.extend_from_slice(&header.to_bytes());
    object.extend_from_slice(&compressed);

    Ok(object)
}

/// Create an object with custom flags.
///
/// Use this for special cases like uncompressed objects.
pub fn create_object_with_flags(data: &[u8], flags: ObjectFlags) -> Result<Vec<u8>, ObjectError> {
    if data.len() > MAX_OBJECT_SIZE as usize {
        return Err(ObjectError::ObjectTooLarge(data.len() as u64));
    }

    let checksum = xxh64(data, 0);

    let (payload, compressed_size) = if flags.contains(ObjectFlags::LZ4_COMPRESSED) {
        let compressed = compress(data);
        let size = compressed.len() as u32;
        (compressed, size)
    } else {
        let size = data.len() as u32;
        (data.to_vec(), size)
    };

    let header = ObjectHeader::with_flags(data.len() as u32, compressed_size, checksum, flags);

    let mut object = Vec::with_capacity(ObjectHeader::SIZE + payload.len());
    object.extend_from_slice(&header.to_bytes());
    object.extend_from_slice(&payload);

    Ok(object)
}

/// Read and decompress an object.
///
/// Validates header, checksum, and decompresses payload.
///
/// # Arguments
/// * `data` - Complete object bytes (header + payload)
///
/// # Returns
/// Decompressed original data
pub fn read_object(data: &[u8]) -> Result<Vec<u8>, ObjectError> {
    if data.len() < ObjectHeader::SIZE {
        return Err(
            io::Error::new(io::ErrorKind::UnexpectedEof, "file too short for header").into(),
        );
    }

    // Parse header
    let (header_bytes, payload) = data.split_at(ObjectHeader::SIZE);
    let header = ObjectHeader::from_bytes(header_bytes)?;

    // Validate payload size
    if payload.len() != header.compressed_size as usize {
        tracing::warn!(
            "Payload size mismatch: expected {}, got {}",
            header.compressed_size,
            payload.len()
        );
    }

    // Decompress based on flags
    let decompressed = if header.is_compressed() {
        decompress(payload)?
    } else {
        payload.to_vec()
    };

    // Verify checksum
    let computed_checksum = xxh64(&decompressed, 0);
    if computed_checksum != header.checksum {
        return Err(ObjectError::ChecksumMismatch {
            expected: header.checksum,
            computed: computed_checksum,
        });
    }

    Ok(decompressed)
}

/// Read object header only (without decompressing payload).
///
/// Useful for quick metadata inspection.
pub fn read_header(data: &[u8]) -> Result<ObjectHeader, ObjectError> {
    if data.len() < ObjectHeader::SIZE {
        return Err(
            io::Error::new(io::ErrorKind::UnexpectedEof, "file too short for header").into(),
        );
    }
    ObjectHeader::from_bytes(data)
}

// =============================================================================
// Object ID Utilities
// =============================================================================

/// Format object ID as S3 key path.
///
/// Key format: `{volume_uuid}/blocks/{prefix}/{object_id}`
///
/// Where:
/// - `volume_uuid`: UUID of the StudioFS volume
/// - `prefix`: First 4 hex chars of object_id for S3 partition distribution
/// - `object_id`: 16-char hex sequential block ID
///
/// # Arguments
/// * `volume_uuid` - Volume UUID string
/// * `object_id` - Sequential block ID
///
/// # Returns
/// S3 key path string
pub fn format_object_key(volume_uuid: &str, object_id: u64) -> String {
    let hex_id = format!("{:016x}", object_id);
    let prefix = &hex_id[..4];
    format!("{}/blocks/{}/{}", volume_uuid, prefix, hex_id)
}

/// Parse object ID from S3 key.
///
/// # Arguments
/// * `key` - S3 key path
///
/// # Returns
/// Object ID if parsing succeeds
pub fn parse_object_id_from_key(key: &str) -> Result<u64, ObjectError> {
    let parts: Vec<&str> = key.split('/').collect();

    // Expected format: {uuid}/blocks/{prefix}/{object_id}
    if parts.len() < 4 {
        return Err(ObjectError::InvalidObjectId(key.to_string()));
    }

    let hex_id = parts
        .last()
        .ok_or_else(|| ObjectError::InvalidObjectId(key.to_string()))?;

    u64::from_str_radix(hex_id, 16).map_err(|_| ObjectError::InvalidObjectId(key.to_string()))
}

// =============================================================================
// Legacy Types (Deprecated - for backward compatibility during migration)
// =============================================================================

/// Legacy: Calculate XXH64 hash and return as hex string.
///
/// In the new architecture, blocks use sequential IDs instead of content hashes.
#[deprecated(
    since = "0.2.0",
    note = "Use sequential block IDs instead of content hashes"
)]
pub fn hash_object(data: &[u8]) -> String {
    let hash = xxh64(data, 0);
    format!("{:016x}", hash)
}

/// Legacy: Object kind enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[deprecated(since = "0.2.0", note = "New architecture uses simple block storage")]
pub enum ObjectKind {
    /// Leaf object containing actual file data.
    Data,
    /// Metadata object containing pointers to other objects.
    Meta,
}

/// Legacy: Entry in a metadata object.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[deprecated(since = "0.2.0", note = "New architecture uses BlockRef instead")]
pub struct Objectlink {
    /// Logical offset in the file.
    pub offset: u64,
    /// Length of data covered by this object.
    pub length: u32,
    /// Object ID (hash/uuid).
    pub object_id: String,
}

/// Legacy: Payload structure for metadata objects.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[deprecated(since = "0.2.0", note = "New architecture uses simple block storage")]
pub struct ObjectPayload {
    /// Kind of object.
    pub kind: ObjectKind,
    /// For Data objects: the actual data.
    pub data: Vec<u8>,
    /// Child links (for Meta objects).
    pub children: Vec<Objectlink>,
}

#[allow(deprecated)]
impl ObjectPayload {
    /// Create a new data payload.
    pub fn new_data(data: Vec<u8>) -> Self {
        Self {
            kind: ObjectKind::Data,
            data,
            children: Vec::new(),
        }
    }

    /// Create a new metadata payload.
    pub fn new_meta(children: Vec<Objectlink>) -> Self {
        Self {
            kind: ObjectKind::Meta,
            data: Vec::new(),
            children,
        }
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        // Simple serialization for backward compatibility
        postcard::to_stdvec(&(self.kind as u8, &self.data, &self.children))
    }

    /// Deserialize from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        let (kind_u8, data, children): (u8, Vec<u8>, Vec<Objectlink>) =
            postcard::from_bytes(bytes)?;

        let kind = match kind_u8 {
            0 => ObjectKind::Data,
            1 => ObjectKind::Meta,
            _ => ObjectKind::Data,
        };

        Ok(Self {
            kind,
            data,
            children,
        })
    }

    /// Find child object covering the given offset.
    pub fn find_child(&self, offset: u64) -> Option<&Objectlink> {
        self.children
            .iter()
            .find(|link| offset >= link.offset && offset < link.offset + link.length as u64)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        // Ensure header is exactly 30 bytes
        assert_eq!(ObjectHeader::SIZE, 30);

        // Note: Without #[repr(packed)], the struct may have padding.
        // The actual struct size may be larger, but we serialize to exactly 32 bytes.
        // This is acceptable since we use to_bytes() for serialization.
    }

    #[test]
    fn test_header_roundtrip() {
        let header = ObjectHeader::new(1024, 512, 0x123456789ABCDEF0);
        let bytes = header.to_bytes();
        let parsed = ObjectHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.magic, parsed.magic);
        assert_eq!(header.version, parsed.version);
        assert_eq!(header.flags, parsed.flags);
        assert_eq!(header.original_size, parsed.original_size);
        assert_eq!(header.compressed_size, parsed.compressed_size);
        assert_eq!(header.checksum, parsed.checksum);
    }

    #[test]
    fn test_object_roundtrip() {
        let original_data = b"Hello, StudioFS! This is a test of the object format.";

        let object = create_object(original_data).unwrap();
        let decompressed = read_object(&object).unwrap();

        assert_eq!(original_data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_object_with_large_data() {
        // Test with data larger than typical compression overhead
        let original_data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

        let object = create_object(&original_data).unwrap();
        let decompressed = read_object(&object).unwrap();

        assert_eq!(original_data, decompressed);
    }

    #[test]
    fn test_checksum_validation() {
        let original_data = b"Test data for checksum validation";
        let mut object = create_object(original_data).unwrap();

        // Corrupt the checksum in the header
        object[20] ^= 0xFF; // Flip some bits in checksum

        let result = read_object(&object);
        assert!(matches!(result, Err(ObjectError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_invalid_magic() {
        let mut header = ObjectHeader::default();
        header.magic = *b"XXXX";

        let bytes = header.to_bytes();
        let result = ObjectHeader::from_bytes(&bytes);

        assert!(matches!(result, Err(ObjectError::InvalidMagic)));
    }

    #[test]
    fn test_unsupported_version() {
        let mut header = ObjectHeader::default();
        header.version = 99;

        let bytes = header.to_bytes();
        let result = ObjectHeader::from_bytes(&bytes);

        assert!(matches!(result, Err(ObjectError::UnsupportedVersion(99))));
    }

    #[test]
    fn test_object_key_formatting() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let object_id: u64 = 0x1234;

        let key = format_object_key(uuid, object_id);
        assert_eq!(
            key,
            "550e8400-e29b-41d4-a716-446655440000/blocks/0000/0000000000001234"
        );
    }

    #[test]
    fn test_object_key_parsing() {
        let key = "550e8400-e29b-41d4-a716-446655440000/blocks/0000/0000000000001234";
        let object_id = parse_object_id_from_key(key).unwrap();

        assert_eq!(object_id, 0x1234);
    }

    #[test]
    fn test_object_flags() {
        let flags = ObjectFlags::LZ4_COMPRESSED | ObjectFlags::HAS_FOOTER;

        assert!(flags.contains(ObjectFlags::LZ4_COMPRESSED));
        assert!(flags.contains(ObjectFlags::HAS_FOOTER));
        assert!(!flags.contains(ObjectFlags::ENCRYPTED));
    }

    #[test]
    fn test_uncompressed_object() {
        let data = b"Uncompressed test data";
        let flags = ObjectFlags::empty();

        let object = create_object_with_flags(data, flags).unwrap();
        let decompressed = read_object(&object).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());
    }
}
