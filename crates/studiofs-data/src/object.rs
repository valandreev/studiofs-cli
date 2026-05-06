//! StudioFS S3 Object Format
//!
//! Defines the binary format for objects stored in S3.
//!
//! Architecture Key Changes:
//! - Fixed 32-byte header (was 22 bytes)
//! - Added flags field for future encryption/footer features
//! - Version bumped to 2
//! - Objects are immutable and content-addressable by sequential ID

use bytes::{Buf, BufMut, Bytes};
use opendal::Buffer;
use std::io::{self, Cursor};
use thiserror::Error;
use xxhash_rust::xxh64::{Xxh64, xxh64};

// =============================================================================
// Constants
// =============================================================================

/// Magic bytes for StudioFS object format ("STFS").
pub const MAGIC: &[u8; 4] = b"STFS";

/// Current object format version.
pub const VERSION: u16 = 2;

/// Maximum object size (for safety check, e.g., 64MB).
pub const MAX_OBJECT_SIZE: u32 = 64 * 1024 * 1024;

/// Fixed logical block size: 4 MiB.
pub const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024 * 1024;
/// Cloud fragment size per logical block: 4 MiB.
pub const CLOUD_FRAGMENT_SIZE: usize = 4 * 1024 * 1024;
/// Number of cloud fragments per logical block.
pub const CLOUD_FRAGMENT_COUNT: usize = 1;
/// Compile-time invariant: one logical block must map exactly to fixed cloud fragments.
const _: [(); DEFAULT_BLOCK_SIZE as usize] = [(); CLOUD_FRAGMENT_SIZE * CLOUD_FRAGMENT_COUNT];

// =============================================================================
// Error Types
// =============================================================================

#[derive(Debug, Error)]
/// Enum $name.
pub enum ObjectError {
    #[error("invalid magic bytes")]
    /// Variant $name.
    InvalidMagic,
    #[error("unsupported version: {0}")]
    /// Variant $name.
    UnsupportedVersion(u16),
    #[error("IO error: {0}")]
    /// Variant $name.
    Io(#[from] io::Error),
    #[error("legacy compressed object is not supported in this build")]
    /// Variant $name.
    LegacyCompressionUnsupported,
    #[error("checksum mismatch: expected {expected:016x}, got {computed:016x}")]
    /// Field $name.
    ChecksumMismatch {
        /// The expected value.
        expected: u64,
        /// The computed value.
        computed: u64,
    },
    #[error("object too large: {0} bytes")]
    /// Variant $name.
    ObjectTooLarge(u64),
    #[error("invalid object ID: {0}")]
    /// Variant $name.
    InvalidObjectId(String),
    #[error("invalid cloud fragment layout: {0}")]
    /// Variant $name.
    InvalidFragmentLayout(String),
}

// =============================================================================
// Object Flags
// =============================================================================

bitflags::bitflags! {
    /// Flags indicating object properties.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ObjectFlags: u16 {
        /// Legacy flag from older builds that used LZ4 compression.
        /// This build rejects such objects.
        const LEGACY_LZ4_COMPRESSED = 0x0001;
        /// Object has footer checksum (future use).
        const HAS_FOOTER = 0x0002;
        /// Object is encrypted (future use).
        const ENCRYPTED = 0x0004;
    }
}

impl Default for ObjectFlags {
    fn default() -> Self {
        Self::empty()
    }
}

// =============================================================================
// Object Header
// =============================================================================

/// S3 Object Header - 30 bytes fixed on wire.
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
    /// Payload size (excluding header).
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
            flags: ObjectFlags::empty(),
            original_size: 0,
            compressed_size: 0,
            checksum: 0,
            reserved: [0u8; 6],
        }
    }
}

impl ObjectHeader {
    /// Total header size in bytes.
    pub const SIZE: usize = 4 + 2 + 2 + 4 + 4 + 8 + 6; // 30 bytes wire format

    /// Create a new header with the given parameters.
    pub fn new(original_size: u32, compressed_size: u32, checksum: u64) -> Self {
        Self {
            magic: *MAGIC,
            version: VERSION,
            flags: ObjectFlags::empty(),
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
        self.flags.contains(ObjectFlags::LEGACY_LZ4_COMPRESSED)
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
// Object Creation and Reading
// =============================================================================

/// Create a full object buffer (Header + Payload) without copying payload bytes.
///
/// This is intended for upload paths where the payload is already owned as `Bytes`.
/// The returned `Buffer` keeps header and payload as separate chunks.
pub fn create_object_buffer(data: Bytes) -> Result<Buffer, ObjectError> {
    create_object_buffer_with(data, ObjectFlags::empty())
}

/// Create an object buffer with custom flags without copying payload bytes.
///
/// Compression flags are not supported in this build.
pub fn create_object_buffer_with(data: Bytes, flags: ObjectFlags) -> Result<Buffer, ObjectError> {
    let data_len = u64::try_from(data.len()).map_err(|_| ObjectError::ObjectTooLarge(u64::MAX))?;
    if data_len > u64::from(MAX_OBJECT_SIZE) {
        return Err(ObjectError::ObjectTooLarge(data_len));
    }
    if flags.contains(ObjectFlags::LEGACY_LZ4_COMPRESSED) {
        return Err(ObjectError::LegacyCompressionUnsupported);
    }

    let checksum = xxh64(data.as_ref(), 0);
    let size = u32::try_from(data_len).map_err(|_| ObjectError::ObjectTooLarge(data_len))?;
    let header = ObjectHeader::with_flags(size, size, checksum, flags).to_bytes();

    // Chunk the data into 1MB pieces for better network transmission behavior.
    // When sending 16MB contiguous buffers over a local network -> router -> external IP
    // loopback, hyper/reqwest tries to push large chunks into the socket. This can
    // cause micro-bursts that exceed router queue depths (bufferbloat) and drop packets,
    // severely limiting upload throughput. By slicing the `Bytes` (zero-copy), we force
    // hyper to write to the socket in smaller 1MB chunks.
    let chunk_size = 1024 * 1024;
    let num_data_chunks = data.len().div_ceil(chunk_size);
    let mut chunks = Vec::with_capacity(1 + num_data_chunks);
    chunks.push(Bytes::copy_from_slice(&header));

    for i in 0..num_data_chunks {
        let start = i * chunk_size;
        let end = (start + chunk_size).min(data.len());
        chunks.push(data.slice(start..end));
    }

    Ok(Buffer::from(chunks))
}

pub use create_object_buffer_with as create_object_buffer_with_flags;

/// Read and validate an object.
///
/// Validates header and checksum, then returns raw payload bytes.
///
/// # Arguments
/// * `data` - Complete object bytes (header + payload)
///
/// # Returns
/// Original payload data
pub fn read_object(data: impl Into<Buffer>) -> Result<Bytes, ObjectError> {
    let data: Buffer = data.into();
    if data.len() < ObjectHeader::SIZE {
        return Err(
            io::Error::new(io::ErrorKind::UnexpectedEof, "file too short for header").into(),
        );
    }

    // Parse header
    let mut header_buf = data.slice(..ObjectHeader::SIZE);
    let mut header_bytes = [0u8; ObjectHeader::SIZE];
    header_buf.copy_to_slice(&mut header_bytes);
    let header = ObjectHeader::from_bytes(&header_bytes)?;
    let payload = data.slice(ObjectHeader::SIZE..);

    // Validate payload size
    let compressed_size = usize::try_from(header.compressed_size)
        .map_err(|_| ObjectError::InvalidFragmentLayout("compressed size overflow".into()))?;
    if payload.len() != compressed_size {
        tracing::warn!(
            "Payload size mismatch: expected {}, got {}",
            header.compressed_size,
            payload.len()
        );
    }

    if header.is_compressed() {
        return Err(ObjectError::LegacyCompressionUnsupported);
    }

    // Verify checksum
    let computed_checksum = xxh64_buffer(&payload);
    if computed_checksum != header.checksum {
        return Err(ObjectError::ChecksumMismatch {
            expected: header.checksum,
            computed: computed_checksum,
        });
    }

    // Preserve zero-copy when payload is a single remaining chunk.
    if Buffer::count(&payload) == 1 {
        Ok(payload.current())
    } else {
        Ok(payload.to_bytes())
    }
}

fn xxh64_buffer(data: &Buffer) -> u64 {
    let mut hasher = Xxh64::new(0);
    let mut remaining = data.clone();
    while !remaining.is_empty() {
        let chunk = remaining.current();
        hasher.update(&chunk);
        remaining.advance(chunk.len());
    }
    hasher.digest()
}

/// Read object header only (without loading payload bytes into a second buffer).
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

/// Format object ID as S3 key base path.
///
/// Key format: `{data_prefix}{prefix}/{object_id}`
///
/// Where:
/// - `data_prefix`: volume data root, e.g. `mydisk/objects/`
/// - `prefix`: First 4 hex chars of object_id for S3 partition distribution
/// - `object_id`: 16-char hex sequential object ID
///
/// # Arguments
/// * `data_prefix` - Volume data prefix
/// * `object_id` - Sequential object ID
///
/// # Returns
/// S3 key path string
pub fn format_object_key(data_prefix: &str, object_id: u64) -> String {
    let normalized_prefix = if data_prefix.ends_with('/') {
        data_prefix.to_string()
    } else {
        format!("{}/", data_prefix)
    };
    let hex_id = format!("{:016x}", object_id);
    let prefix = &hex_id[..4];
    format!("{}{}/{}", normalized_prefix, prefix, hex_id)
}

/// Format one cloud fragment key for an object.
///
/// Key format: `{data_prefix}{prefix}/{object_id}/{fragment_index}`
pub fn format_object_fragment_key(data_prefix: &str, object_id: u64, fragment_index: u8) -> String {
    assert!(
        usize::from(fragment_index) < CLOUD_FRAGMENT_COUNT,
        "fragment index out of range"
    );
    format!(
        "{}/{}",
        format_object_key(data_prefix, object_id),
        fragment_index
    )
}

/// Format all cloud fragment keys for an object in stable order [0..N).
pub fn format_fragment_keys(data_prefix: &str, object_id: u64) -> [String; CLOUD_FRAGMENT_COUNT] {
    std::array::from_fn(|idx| {
        let idx = idx as u8;
        format_object_fragment_key(data_prefix, object_id, idx)
    })
}

pub use format_fragment_keys as format_object_fragment_keys;

/// Validate cloud fragment shape and return reconstructed payload size.
///
/// Rules:
/// - exactly `CLOUD_FRAGMENT_COUNT` fragments are expected;
/// - no fragment may exceed `CLOUD_FRAGMENT_SIZE`;
/// - after the first short fragment (`len < CLOUD_FRAGMENT_SIZE`), all following
///   fragments must be empty.
pub fn validate_cloud_fragment_layout(lengths: &[usize]) -> Result<usize, ObjectError> {
    if lengths.len() != CLOUD_FRAGMENT_COUNT {
        return Err(ObjectError::InvalidFragmentLayout(format!(
            "expected {} fragments, got {}",
            CLOUD_FRAGMENT_COUNT,
            lengths.len()
        )));
    }

    let mut seen_short_fragment = false;
    let mut total_len = 0usize;

    for (idx, &len) in lengths.iter().enumerate() {
        if len > CLOUD_FRAGMENT_SIZE {
            return Err(ObjectError::InvalidFragmentLayout(format!(
                "fragment {idx} length {len} exceeds max {CLOUD_FRAGMENT_SIZE}"
            )));
        }
        if seen_short_fragment && len != 0 {
            return Err(ObjectError::InvalidFragmentLayout(format!(
                "fragment {idx} has len {len} after prior short fragment"
            )));
        }
        if len < CLOUD_FRAGMENT_SIZE {
            seen_short_fragment = true;
        }
        total_len = total_len
            .checked_add(len)
            .ok_or_else(|| ObjectError::InvalidFragmentLayout("fragment length overflow".into()))?;
    }

    Ok(total_len)
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

    // Legacy format: {data_prefix}/{prefix}/{object_id}
    // Fragmented format: {data_prefix}/{prefix}/{object_id}/{fragment_index}
    if parts.len() < 2 {
        return Err(ObjectError::InvalidObjectId(key.to_string()));
    }

    let tail = parts
        .last()
        .ok_or_else(|| ObjectError::InvalidObjectId(key.to_string()))?;

    // Try fragmented key first: .../{object_id}/{fragment_index}
    if let Ok(fragment_index) = tail.parse::<u64>()
        && fragment_index < CLOUD_FRAGMENT_COUNT as u64
    {
        let hex_id = parts
            .get(parts.len().saturating_sub(2))
            .ok_or_else(|| ObjectError::InvalidObjectId(key.to_string()))?;
        if hex_id.len() == 16 && hex_id.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return u64::from_str_radix(hex_id, 16)
                .map_err(|_| ObjectError::InvalidObjectId(key.to_string()));
        }
    }

    // Legacy key: .../{object_id}
    u64::from_str_radix(tail, 16).map_err(|_| ObjectError::InvalidObjectId(key.to_string()))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_object_bytes(data: &[u8], flags: ObjectFlags) -> Vec<u8> {
        create_object_buffer_with(Bytes::copy_from_slice(data), flags)
            .expect("create test object buffer")
            .to_bytes()
            .to_vec()
    }

    #[test]
    fn test_header_size() {
        // Ensure header is exactly 30 bytes
        assert_eq!(ObjectHeader::SIZE, 30);

        // Note: Without #[repr(packed)], the struct may have padding.
        // The actual struct size may be larger due to alignment,
        // but wire serialization is exactly 30 bytes.
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

        let object = create_test_object_bytes(original_data, ObjectFlags::empty());
        let decoded = read_object(Bytes::from(object)).unwrap();

        assert_eq!(original_data.as_slice(), decoded.as_ref());
    }

    #[test]
    fn test_object_with_large_data() {
        // Test with data larger than typical compression overhead
        let original_data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

        let object = create_test_object_bytes(&original_data, ObjectFlags::empty());
        let decoded = read_object(Bytes::from(object)).unwrap();

        assert_eq!(original_data.as_slice(), decoded.as_ref());
    }

    #[test]
    fn test_checksum_validation() {
        let original_data = b"Test data for checksum validation";
        let mut object = create_test_object_bytes(original_data, ObjectFlags::empty());

        // Corrupt the checksum in the header
        object[20] ^= 0xFF; // Flip some bits in checksum

        let result = read_object(Bytes::from(object));
        assert!(matches!(result, Err(ObjectError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_invalid_magic() {
        let header = ObjectHeader {
            magic: *b"XXXX",
            ..Default::default()
        };

        let bytes = header.to_bytes();
        let result = ObjectHeader::from_bytes(&bytes);

        assert!(matches!(result, Err(ObjectError::InvalidMagic)));
    }

    #[test]
    fn test_unsupported_version() {
        let header = ObjectHeader {
            version: 99,
            ..Default::default()
        };

        let bytes = header.to_bytes();
        let result = ObjectHeader::from_bytes(&bytes);

        assert!(matches!(result, Err(ObjectError::UnsupportedVersion(99))));
    }

    #[test]
    fn test_object_key_formatting() {
        let data_prefix = "disk-a/objects/";
        let object_id: u64 = 0x1234;

        let key = format_object_key(data_prefix, object_id);
        assert_eq!(key, "disk-a/objects/0000/0000000000001234");
    }

    #[test]
    fn test_object_key_parsing() {
        let key = "disk-a/objects/0000/0000000000001234";
        let object_id = parse_object_id_from_key(key).unwrap();

        assert_eq!(object_id, 0x1234);
    }

    #[test]
    fn test_fragment_key_formatting() {
        let data_prefix = "disk-a/objects/";
        let object_id: u64 = 0x1234;

        let key = format_object_fragment_key(data_prefix, object_id, 0);
        assert_eq!(key, "disk-a/objects/0000/0000000000001234/0");
    }

    #[test]
    fn test_fragment_key_parsing() {
        let key = "disk-a/objects/0000/0000000000001234/0";
        let object_id = parse_object_id_from_key(key).unwrap();

        assert_eq!(object_id, 0x1234);
    }

    #[test]
    fn test_validate_cloud_fragment_layout_ok() {
        let full = CLOUD_FRAGMENT_SIZE;
        let mut lengths = vec![0usize; CLOUD_FRAGMENT_COUNT];
        lengths[0] = full.saturating_sub(123);
        let total = validate_cloud_fragment_layout(&lengths).unwrap();
        assert_eq!(total, full.saturating_sub(123));
    }

    #[test]
    fn test_validate_cloud_fragment_layout_rejects_non_empty_after_short() {
        if CLOUD_FRAGMENT_COUNT < 2 {
            return;
        }
        let full = CLOUD_FRAGMENT_SIZE;
        let mut lengths = vec![0usize; CLOUD_FRAGMENT_COUNT];
        lengths[0] = full.saturating_sub(1);
        lengths[1] = 1;
        let err = validate_cloud_fragment_layout(&lengths).unwrap_err();
        assert!(matches!(err, ObjectError::InvalidFragmentLayout(_)));
    }

    #[test]
    fn test_validate_cloud_fragment_layout_rejects_wrong_count() {
        let err = validate_cloud_fragment_layout(&[]).unwrap_err();
        assert!(matches!(err, ObjectError::InvalidFragmentLayout(_)));
    }

    #[test]
    fn test_object_flags() {
        let flags = ObjectFlags::HAS_FOOTER;

        assert!(!flags.contains(ObjectFlags::LEGACY_LZ4_COMPRESSED));
        assert!(flags.contains(ObjectFlags::HAS_FOOTER));
        assert!(!flags.contains(ObjectFlags::ENCRYPTED));
    }

    #[test]
    fn test_uncompressed_object() {
        let data = b"Uncompressed test data";
        let flags = ObjectFlags::empty();

        let object = create_test_object_bytes(data, flags);
        let decoded = read_object(Bytes::from(object)).unwrap();

        assert_eq!(data.as_slice(), decoded.as_ref());
    }

    #[test]
    fn test_object_buffer_roundtrip() {
        let original_data = Bytes::from_static(b"buffer roundtrip payload");

        let object = create_object_buffer(original_data.clone()).unwrap();
        let decoded = read_object(object.to_bytes()).unwrap();

        assert_eq!(original_data.as_ref(), decoded.as_ref());
    }

    #[test]
    fn test_object_buffer_reuses_payload_bytes() {
        let payload = Bytes::from(vec![0xAB; 4096]);
        let payload_ptr = payload.as_ptr();

        let mut object = create_object_buffer(payload.clone()).unwrap();
        assert_eq!(
            Buffer::count(&object),
            2,
            "expected header + payload chunks"
        );

        let header = object.current();
        assert_eq!(header.len(), ObjectHeader::SIZE);

        object.advance(ObjectHeader::SIZE);
        let payload_chunk = object.current();
        assert_eq!(payload_chunk.len(), payload.len());
        assert_eq!(
            payload_chunk.as_ptr(),
            payload_ptr,
            "payload chunk should reuse original Bytes allocation"
        );
    }

    #[test]
    fn test_legacy_compressed_object_rejected() {
        let data = b"legacy-compressed-data";
        let checksum = xxh64(data, 0);
        let header = ObjectHeader::with_flags(
            data.len() as u32,
            data.len() as u32,
            checksum,
            ObjectFlags::LEGACY_LZ4_COMPRESSED,
        );
        let mut object = Vec::with_capacity(ObjectHeader::SIZE + data.len());
        object.extend_from_slice(&header.to_bytes());
        object.extend_from_slice(data);

        let result = read_object(Bytes::from(object));
        assert!(matches!(
            result,
            Err(ObjectError::LegacyCompressionUnsupported)
        ));
    }
}
