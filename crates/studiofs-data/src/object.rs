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
use std::ops::Range;
use thiserror::Error;
use xxhash_rust::xxh64::{Xxh64, xxh64};

// =============================================================================
// Constants
// =============================================================================

/// Magic bytes for StudioFS object format ("STFS").
pub const MAGIC: &[u8; 4] = b"STFS";

/// Default uncompressed object format version (v2).
pub const VERSION: u16 = 2;

/// Compressed object format version with chunk seek table (v3, ADR 0015).
pub const VERSION_V3: u16 = 3;

/// Default STFS v3 chunk size exponent (`1 << 17` = 128 KiB).
pub const DEFAULT_V3_CHUNK_LOG2: u8 = 17;
/// Default STFS v3 chunk size in bytes (128 KiB).
pub const DEFAULT_V3_CHUNK_SIZE: usize = 1 << DEFAULT_V3_CHUNK_LOG2;
/// Minimum supported STFS v3 chunk size exponent (`1 << 12` = 4 KiB).
pub const MIN_V3_CHUNK_LOG2: u8 = 12;
/// Maximum supported STFS v3 chunk size exponent (`1 << 22` = 4 MiB).
pub const MAX_V3_CHUNK_LOG2: u8 = 22;

/// Bit 31 of an STFS v3 seek-table entry: set when the chunk is stored raw.
pub const V3_SEEK_ENTRY_RAW_FLAG: u32 = 1 << 31;
/// Bits 0..=30 of an STFS v3 seek-table entry: stored chunk byte length.
pub const V3_SEEK_ENTRY_LEN_MASK: u32 = !V3_SEEK_ENTRY_RAW_FLAG;

/// Maximum payload size accepted by the object format: 64 MiB.
/// Decoding checks this limit after buffering; it does not bound download or
/// decryption allocations.
pub const MAX_OBJECT_SIZE: u32 = 64 * 1024 * 1024;

/// Fixed logical block size: 4 MiB.
pub const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024 * 1024;
/// Cloud fragment size per logical block: 4 MiB.
pub const CLOUD_FRAGMENT_SIZE: usize = 4 * 1024 * 1024;
/// Number of cloud fragments per logical block.
pub const CLOUD_FRAGMENT_COUNT: usize = 1;
/// Maximum seek table byte length for a valid STFS v3 fragment
/// (`(CLOUD_FRAGMENT_SIZE >> MIN_V3_CHUNK_LOG2) * 4`).
pub const MAX_V3_SEEK_TABLE_LEN: usize =
    (CLOUD_FRAGMENT_SIZE >> MIN_V3_CHUNK_LOG2) * std::mem::size_of::<u32>();
/// Maximum STFS plaintext byte length for a regular fragment object (v2 or v3).
///
/// Accounts for the 30-byte STFS header, up to `CLOUD_FRAGMENT_SIZE` payload bytes,
/// and the largest valid v3 seek table.
pub const MAX_REGULAR_OBJECT_PLAINTEXT_LEN: usize =
    ObjectHeader::SIZE + MAX_V3_SEEK_TABLE_LEN + CLOUD_FRAGMENT_SIZE;
/// Compile-time invariant: one logical block must map exactly to fixed cloud fragments.
const _: [(); DEFAULT_BLOCK_SIZE as usize] = [(); CLOUD_FRAGMENT_SIZE * CLOUD_FRAGMENT_COUNT];
/// Compile-time invariant: the largest v3 chunk size equals one cloud fragment.
const _: [(); CLOUD_FRAGMENT_SIZE] = [(); 1usize << MAX_V3_CHUNK_LOG2];

// =============================================================================
// Error Types
// =============================================================================

#[derive(Debug, Error)]
/// Errors that can occur while encoding, decoding, or validating object payloads.
pub enum ObjectError {
    /// Header magic bytes do not match the expected STFS marker.
    #[error("invalid magic bytes")]
    InvalidMagic,

    /// The serialized object uses a version newer than supported.
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u16),

    /// Underlying I/O failure while reading or writing object bytes.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// The object declares legacy LZ4 compression that is no longer supported.
    #[error("legacy compressed object is not supported in this build")]
    LegacyCompressionUnsupported,

    /// An uncompressed payload does not match both declared header sizes.
    #[error(
        "payload size mismatch: original {original_size} bytes, compressed {compressed_size} bytes, actual {actual_size} bytes"
    )]
    SizeMismatch {
        /// Original payload size declared in the header.
        original_size: u32,
        /// Stored payload size declared in the header.
        compressed_size: u32,
        /// Actual payload length excluding the header.
        actual_size: usize,
    },

    /// Header checksum check failed against the payload contents.
    #[error("checksum mismatch: expected {expected:016x}, got {computed:016x}")]
    ChecksumMismatch {
        /// Expected checksum from header.
        expected: u64,
        /// Computed checksum of the payload bytes.
        computed: u64,
    },

    /// Encoded object payload exceeds the supported maximum size.
    #[error("object too large: {0} bytes")]
    ObjectTooLarge(u64),

    /// The parsed object identifier is malformed or cannot be interpreted.
    #[error("invalid object ID: {0}")]
    InvalidObjectId(String),

    /// Cloud fragment list does not satisfy layout constraints.
    #[error("invalid cloud fragment layout: {0}")]
    InvalidFragmentLayout(String),

    /// The STFS v3 header declares a compression codec not supported by this build.
    #[error("unsupported compression codec: {0}")]
    UnsupportedCodec(u8),

    /// The STFS v3 header or seek table violates format geometry invariants.
    #[error("invalid STFS v3 format: {0}")]
    InvalidV3Format(String),

    /// Decompressing a chunk failed or produced an unexpected byte length.
    #[error("chunk {chunk_index} decompression failed: {message}")]
    ChunkDecompression {
        /// Zero-based index of the chunk that failed to decompress.
        chunk_index: usize,
        /// Description of the decompression failure.
        message: String,
    },
}

// =============================================================================
// Compression Codec (STFS v3)
// =============================================================================

/// Compression codec identifier stored in an STFS v3 header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionCodec {
    /// Zstandard chunk compression (ADR 0015).
    Zstd = 1,
}

impl CompressionCodec {
    /// Decode a codec identifier byte from the STFS v3 header.
    pub fn from_byte(byte: u8) -> Result<Self, ObjectError> {
        match byte {
            1 => Ok(Self::Zstd),
            other => Err(ObjectError::UnsupportedCodec(other)),
        }
    }

    /// Encode the codec identifier byte for the STFS v3 header.
    pub const fn to_byte(self) -> u8 {
        self as u8
    }
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
    ///
    /// Checks header length, magic, and version. Use [`read_object`] to validate
    /// payload sizes, compression support, and checksum.
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
        if version > VERSION_V3 {
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

    /// Check if object is compressed (either legacy LZ4 or STFS v3).
    pub fn is_compressed(&self) -> bool {
        self.flags.contains(ObjectFlags::LEGACY_LZ4_COMPRESSED) || self.version == VERSION_V3
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

/// Alias for creating an object buffer with explicit flags.
pub use create_object_buffer_with as create_object_buffer_with_flags;

/// Read and validate an object.
///
/// Validates header, payload sizes, and checksum, then returns raw payload bytes.
///
/// # Arguments
/// * `data` - Complete object bytes (header + payload)
///
/// # Returns
/// Original payload data
///
/// # Errors
/// Returns [`ObjectError::Io`] for a truncated header, [`ObjectError::InvalidMagic`]
/// or [`ObjectError::UnsupportedVersion`] for an invalid header (including STFS v3,
/// which must be decoded with [`read_v3_object`]), and
/// [`ObjectError::LegacyCompressionUnsupported`] for legacy compression.
/// Uncompressed payloads return [`ObjectError::SizeMismatch`] for inconsistent
/// sizes, [`ObjectError::ObjectTooLarge`] for consistent sizes above the limit,
/// or [`ObjectError::ChecksumMismatch`] for a checksum failure.
pub fn read_object(data: impl Into<Buffer>) -> Result<Bytes, ObjectError> {
    read_object_with_checksum(data).map(|(payload, _)| payload)
}

/// Performs the same validation as [`read_object`] and returns the verified XXH64.
pub fn read_object_with_checksum(data: impl Into<Buffer>) -> Result<(Bytes, u64), ObjectError> {
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

    let payload_len = payload.len();
    validate_object_payload_size(&header, payload_len)?;

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
        Ok((payload.current(), computed_checksum))
    } else {
        Ok((payload.to_bytes(), computed_checksum))
    }
}

/// Validate an authenticated STFS v2 header against the complete declared payload length.
/// Partial readers use frame authentication rather than the whole-payload checksum.
pub(crate) fn validate_object_payload_size(
    header: &ObjectHeader,
    payload_len: usize,
) -> Result<(), ObjectError> {
    if header.flags.contains(ObjectFlags::LEGACY_LZ4_COMPRESSED) {
        return Err(ObjectError::LegacyCompressionUnsupported);
    }
    if header.version == VERSION_V3 {
        return Err(ObjectError::UnsupportedVersion(header.version));
    }
    if header.original_size != header.compressed_size
        || !usize::try_from(header.compressed_size).is_ok_and(|size| size == payload_len)
    {
        return Err(ObjectError::SizeMismatch {
            original_size: header.original_size,
            compressed_size: header.compressed_size,
            actual_size: payload_len,
        });
    }

    if header.compressed_size > MAX_OBJECT_SIZE {
        return Err(ObjectError::ObjectTooLarge(u64::from(
            header.compressed_size,
        )));
    }

    Ok(())
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
/// Validates header length, magic, and supported version only. Payload sizes,
/// compression support, and checksum are validated by [`read_object`].
pub fn read_header(data: &[u8]) -> Result<ObjectHeader, ObjectError> {
    if data.len() < ObjectHeader::SIZE {
        return Err(
            io::Error::new(io::ErrorKind::UnexpectedEof, "file too short for header").into(),
        );
    }
    ObjectHeader::from_bytes(data)
}

// =============================================================================
// STFS v3 Compressed Object Format (ADR 0015)
// =============================================================================

/// Parsed STFS v3 metadata carried in the 6 reserved bytes of [`ObjectHeader`].
///
/// Layout inside `ObjectHeader::reserved`:
/// - byte 0: `codec: u8` (`1 = zstd`)
/// - byte 1: `chunk_log2: u8` (`17 = 128 KiB`)
/// - bytes 2..4: `chunk_count: u16` (little-endian)
/// - bytes 4..6: reserved zero bytes (`[0, 0]`)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V3HeaderMeta {
    /// Compression codec used for non-raw chunks.
    pub codec: CompressionCodec,
    /// Log2 of the nominal chunk size (e.g. `17` for 128 KiB).
    pub chunk_log2: u8,
    /// Number of entries in the chunk seek table.
    pub chunk_count: u16,
}

impl V3HeaderMeta {
    /// Parse and validate STFS v3 metadata from an [`ObjectHeader`].
    ///
    /// Checks version, legacy flag rejection, codec support, zero reserved bytes,
    /// supported `chunk_log2` range, `original_size <= CLOUD_FRAGMENT_SIZE`,
    /// derived `chunk_count`, and that `compressed_size` can hold the seek table.
    pub fn parse(header: &ObjectHeader) -> Result<Self, ObjectError> {
        if header.version != VERSION_V3 {
            return Err(ObjectError::UnsupportedVersion(header.version));
        }
        if header.flags.contains(ObjectFlags::LEGACY_LZ4_COMPRESSED) {
            return Err(ObjectError::LegacyCompressionUnsupported);
        }

        let codec = CompressionCodec::from_byte(header.reserved[0])?;

        if header.reserved[4] != 0 || header.reserved[5] != 0 {
            return Err(ObjectError::InvalidV3Format(format!(
                "nonzero reserved header bytes: [{:#04x}, {:#04x}]",
                header.reserved[4], header.reserved[5]
            )));
        }

        let chunk_log2 = header.reserved[1];
        validate_v3_chunk_log2(chunk_log2)?;

        let original_size = usize::try_from(header.original_size)
            .map_err(|_| ObjectError::ObjectTooLarge(u64::from(header.original_size)))?;
        if original_size > CLOUD_FRAGMENT_SIZE {
            return Err(ObjectError::ObjectTooLarge(u64::from(header.original_size)));
        }

        let chunk_count = u16::from_le_bytes([header.reserved[2], header.reserved[3]]);
        let expected_chunks = expected_v3_chunk_count(header.original_size, chunk_log2)?;
        if chunk_count != expected_chunks {
            return Err(ObjectError::InvalidV3Format(format!(
                "chunk_count mismatch: header declares {chunk_count}, expected {expected_chunks} \
                 for original_size={original_size} and chunk_log2={chunk_log2}"
            )));
        }

        let meta = Self {
            codec,
            chunk_log2,
            chunk_count,
        };
        let seek_table_len = meta.seek_table_len();
        if (header.compressed_size as usize) < seek_table_len {
            return Err(ObjectError::InvalidV3Format(format!(
                "compressed_size ({}) is smaller than seek table length ({seek_table_len})",
                header.compressed_size
            )));
        }

        Ok(meta)
    }

    /// Encode this v3 metadata into the 6-byte `ObjectHeader::reserved` field.
    pub fn to_reserved_bytes(self) -> [u8; 6] {
        let count_bytes = self.chunk_count.to_le_bytes();
        [
            self.codec.to_byte(),
            self.chunk_log2,
            count_bytes[0],
            count_bytes[1],
            0,
            0,
        ]
    }

    /// Nominal uncompressed chunk size in bytes (`1 << chunk_log2`).
    pub const fn chunk_size(self) -> usize {
        1usize << self.chunk_log2
    }

    /// Byte length of the seek table (`chunk_count * 4`).
    pub const fn seek_table_len(self) -> usize {
        (self.chunk_count as usize) * std::mem::size_of::<u32>()
    }
}

fn validate_v3_chunk_log2(chunk_log2: u8) -> Result<(), ObjectError> {
    if !(MIN_V3_CHUNK_LOG2..=MAX_V3_CHUNK_LOG2).contains(&chunk_log2) {
        return Err(ObjectError::InvalidV3Format(format!(
            "chunk_log2 {chunk_log2} outside supported range {MIN_V3_CHUNK_LOG2}..={MAX_V3_CHUNK_LOG2}"
        )));
    }
    Ok(())
}

/// Derive the expected STFS v3 chunk count for `original_size` and `chunk_log2`.
pub fn expected_v3_chunk_count(original_size: u32, chunk_log2: u8) -> Result<u16, ObjectError> {
    validate_v3_chunk_log2(chunk_log2)?;
    let original_size_usize = usize::try_from(original_size)
        .map_err(|_| ObjectError::ObjectTooLarge(u64::from(original_size)))?;
    if original_size_usize > CLOUD_FRAGMENT_SIZE {
        return Err(ObjectError::ObjectTooLarge(u64::from(original_size)));
    }
    let chunk_size = 1usize << chunk_log2;
    let count = original_size_usize.div_ceil(chunk_size);
    u16::try_from(count).map_err(|_| {
        ObjectError::InvalidV3Format(format!("derived chunk count {count} exceeds u16::MAX"))
    })
}

/// Shared encoder/decoder rule: a raw chunk stores exactly its derived length, and a
/// compressed chunk must shrink (ADR 0015: a chunk that does not shrink is stored raw).
fn validate_v3_stored_len(
    idx: usize,
    is_raw: bool,
    stored_len: usize,
    original_len: usize,
) -> Result<(), ObjectError> {
    if is_raw {
        if stored_len != original_len {
            return Err(ObjectError::InvalidV3Format(format!(
                "raw chunk {idx} stored length {stored_len} != derived original length {original_len}"
            )));
        }
    } else if stored_len == 0 || stored_len >= original_len {
        return Err(ObjectError::InvalidV3Format(format!(
            "compressed chunk {idx} stored length {stored_len} outside 1..{original_len}"
        )));
    }
    Ok(())
}

/// Derive the uncompressed byte length of chunk `index` within a v3 fragment.
fn derived_v3_chunk_original_len(original_size: usize, chunk_size: usize, index: usize) -> usize {
    let logical_offset = index * chunk_size;
    (original_size - logical_offset).min(chunk_size)
}

/// One validated entry in an STFS v3 seek table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V3ChunkEntry {
    /// Zero-based chunk index.
    pub index: usize,
    /// `true` when the chunk payload is stored raw (uncompressed).
    pub is_raw: bool,
    /// Byte offset of the stored chunk payload within the STFS plaintext
    /// (relative to byte 0 of the 30-byte STFS header).
    pub plaintext_offset: usize,
    /// Stored byte length of the chunk payload.
    pub stored_len: usize,
    /// Derived logical (uncompressed) byte length of this chunk.
    pub original_len: usize,
    /// Logical byte offset of this chunk within the original payload.
    pub logical_offset: usize,
}

impl V3ChunkEntry {
    /// Stored byte range inside the STFS plaintext.
    pub const fn plaintext_range(self) -> Range<usize> {
        self.plaintext_offset..(self.plaintext_offset + self.stored_len)
    }

    /// Logical byte range inside the original uncompressed payload.
    pub const fn logical_range(self) -> Range<usize> {
        self.logical_offset..(self.logical_offset + self.original_len)
    }
}

/// One chunk slice required to satisfy a logical payload byte range read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V3ChunkSlice {
    /// Validated seek-table entry for this chunk.
    pub entry: V3ChunkEntry,
    /// Byte range within this chunk's decompressed bytes belonging to the request.
    pub slice_in_chunk: Range<usize>,
}

/// Mapping from a logical payload byte range to STFS v3 chunks and plaintext bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V3RangeMapping {
    /// Requested logical payload byte range after clamping to `0..original_size`.
    pub clamped_logical_range: Range<usize>,
    /// Contiguous chunk indices (`start..end`) covering the requested bytes.
    pub chunk_indices: Range<usize>,
    /// Contiguous byte range inside the STFS plaintext covering every needed stored chunk.
    /// `None` when the clamped logical range is empty.
    pub plaintext_range: Option<Range<u64>>,
    /// Per-chunk entries and intra-chunk slice bounds in ascending chunk order.
    pub chunks: Vec<V3ChunkSlice>,
}

/// Validated STFS v3 header and chunk seek table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V3SeekTable {
    header: ObjectHeader,
    meta: V3HeaderMeta,
    entries: Vec<V3ChunkEntry>,
}

impl V3SeekTable {
    /// Parse and validate an STFS v3 header and seek table from the beginning of
    /// an STFS plaintext buffer (for example, SFEC frame 0 during a range read).
    ///
    /// Does not require chunk payload bytes to be present in `data`.
    pub fn parse_from_plaintext(data: &[u8]) -> Result<Self, ObjectError> {
        let header = read_header(data)?;
        Self::parse_with_header(header, &data[ObjectHeader::SIZE..])
    }

    /// Parse and validate an STFS v3 seek table given an already-parsed header
    /// and the bytes immediately following the 30-byte header.
    pub fn parse_with_header(
        header: ObjectHeader,
        after_header: &[u8],
    ) -> Result<Self, ObjectError> {
        let meta = V3HeaderMeta::parse(&header)?;
        let seek_table_len = meta.seek_table_len();
        if after_header.len() < seek_table_len {
            return Err(ObjectError::InvalidV3Format(format!(
                "truncated seek table: need {seek_table_len} bytes, got {}",
                after_header.len()
            )));
        }

        let original_size = header.original_size as usize;
        let chunk_size = meta.chunk_size();
        let chunk_count = usize::from(meta.chunk_count);

        let mut entries = Vec::with_capacity(chunk_count);
        let mut plaintext_offset: usize = ObjectHeader::SIZE + seek_table_len;

        for idx in 0..chunk_count {
            let entry_offset = idx * std::mem::size_of::<u32>();
            let raw_entry = u32::from_le_bytes([
                after_header[entry_offset],
                after_header[entry_offset + 1],
                after_header[entry_offset + 2],
                after_header[entry_offset + 3],
            ]);
            let is_raw = (raw_entry & V3_SEEK_ENTRY_RAW_FLAG) != 0;
            let stored_len = (raw_entry & V3_SEEK_ENTRY_LEN_MASK) as usize;
            let original_len = derived_v3_chunk_original_len(original_size, chunk_size, idx);
            let logical_offset = idx * chunk_size;

            validate_v3_stored_len(idx, is_raw, stored_len, original_len)?;

            entries.push(V3ChunkEntry {
                index: idx,
                is_raw,
                plaintext_offset,
                stored_len,
                original_len,
                logical_offset,
            });

            plaintext_offset = plaintext_offset.checked_add(stored_len).ok_or_else(|| {
                ObjectError::InvalidV3Format("chunk plaintext offset overflow".into())
            })?;
        }

        let stored_total = plaintext_offset - ObjectHeader::SIZE;
        if stored_total != header.compressed_size as usize {
            return Err(ObjectError::InvalidV3Format(format!(
                "seek table total ({stored_total}) != header compressed_size ({})",
                header.compressed_size
            )));
        }

        Ok(Self {
            header,
            meta,
            entries,
        })
    }

    /// Return the parsed 30-byte STFS header.
    pub const fn header(&self) -> &ObjectHeader {
        &self.header
    }

    /// Return the parsed v3 header metadata.
    pub const fn meta(&self) -> V3HeaderMeta {
        self.meta
    }

    /// Return the compression codec.
    pub const fn codec(&self) -> CompressionCodec {
        self.meta.codec
    }

    /// Return the nominal uncompressed chunk size in bytes.
    pub const fn chunk_size(&self) -> usize {
        self.meta.chunk_size()
    }

    /// Return the logical (uncompressed) payload size in bytes.
    pub const fn original_size(&self) -> usize {
        self.header.original_size as usize
    }

    /// Return the declared bytes after the header (seek table + chunk payloads).
    pub const fn compressed_size(&self) -> usize {
        self.header.compressed_size as usize
    }

    /// Return the byte length of the seek table.
    pub const fn seek_table_len(&self) -> usize {
        self.meta.seek_table_len()
    }

    /// Return the validated chunk entries in ascending order.
    pub fn entries(&self) -> &[V3ChunkEntry] {
        &self.entries
    }

    /// Map a logical payload byte range to the covering chunk indices and their
    /// stored byte ranges inside the STFS plaintext.
    ///
    /// Out-of-bounds endpoints are clamped to `0..original_size`. Empty or
    /// inverted ranges produce an empty mapping with `plaintext_range: None`.
    pub fn map_range(&self, range: Range<u64>) -> V3RangeMapping {
        let orig_u64 = u64::from(self.header.original_size);
        let start = range.start.min(orig_u64) as usize;
        let end = range.end.min(orig_u64) as usize;

        if start >= end {
            return V3RangeMapping {
                clamped_logical_range: start..start,
                chunk_indices: 0..0,
                plaintext_range: None,
                chunks: Vec::new(),
            };
        }

        let chunk_size = self.chunk_size();
        let first_chunk = start / chunk_size;
        let last_chunk = (end - 1) / chunk_size;
        let chunk_indices = first_chunk..(last_chunk + 1);

        let first_entry = self.entries[first_chunk];
        let last_entry = self.entries[last_chunk];
        let plaintext_start = first_entry.plaintext_offset as u64;
        let plaintext_end = (last_entry.plaintext_offset + last_entry.stored_len) as u64;

        let mut chunks = Vec::with_capacity(chunk_indices.len());
        for entry in &self.entries[chunk_indices.clone()] {
            let slice_start = start.saturating_sub(entry.logical_offset);
            let slice_end = (end - entry.logical_offset).min(entry.original_len);
            chunks.push(V3ChunkSlice {
                entry: *entry,
                slice_in_chunk: slice_start..slice_end,
            });
        }

        V3RangeMapping {
            clamped_logical_range: start..end,
            chunk_indices,
            plaintext_range: Some(plaintext_start..plaintext_end),
            chunks,
        }
    }
}

/// One encoded chunk supplied to [`create_v3_object_buffer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum V3EncodedChunk {
    /// Chunk stored uncompressed (raw); bit 31 is set in the seek table.
    Raw(Bytes),
    /// Chunk stored compressed using the object's codec; bit 31 is clear.
    Compressed(Bytes),
}

/// Create a complete STFS v3 object buffer (header + seek table + chunk payloads)
/// without copying chunk payload bytes.
///
/// Pure format encoder: performs no IO and invokes no compression codec.
pub fn create_v3_object_buffer(
    original_size: u32,
    checksum: u64,
    codec: CompressionCodec,
    chunk_log2: u8,
    flags: ObjectFlags,
    chunks: Vec<V3EncodedChunk>,
) -> Result<Buffer, ObjectError> {
    if flags.contains(ObjectFlags::LEGACY_LZ4_COMPRESSED) {
        return Err(ObjectError::LegacyCompressionUnsupported);
    }

    let expected_chunks = expected_v3_chunk_count(original_size, chunk_log2)?;
    if chunks.len() != usize::from(expected_chunks) {
        return Err(ObjectError::InvalidV3Format(format!(
            "chunk count mismatch: got {}, expected {expected_chunks} \
             for original_size={original_size} and chunk_log2={chunk_log2}",
            chunks.len()
        )));
    }

    let meta = V3HeaderMeta {
        codec,
        chunk_log2,
        chunk_count: expected_chunks,
    };
    let chunk_size = meta.chunk_size();
    let original_size_usize = original_size as usize;
    let seek_table_len = meta.seek_table_len();

    let mut compressed_size = seek_table_len;
    let mut header_and_table = Vec::with_capacity(ObjectHeader::SIZE + seek_table_len);
    header_and_table.resize(ObjectHeader::SIZE, 0);

    let mut buffers = Vec::with_capacity(1 + chunks.len());
    // Placeholder at index 0; populated once header + seek table are finalized.
    buffers.push(Bytes::new());

    for (idx, chunk) in chunks.into_iter().enumerate() {
        let original_len = derived_v3_chunk_original_len(original_size_usize, chunk_size, idx);
        let (is_raw, payload) = match chunk {
            V3EncodedChunk::Raw(bytes) => (true, bytes),
            V3EncodedChunk::Compressed(bytes) => (false, bytes),
        };
        validate_v3_stored_len(idx, is_raw, payload.len(), original_len)?;

        let stored_len_u32 = u32::try_from(payload.len()).map_err(|_| {
            ObjectError::InvalidV3Format(format!("chunk {idx} length exceeds u32::MAX"))
        })?;
        if (stored_len_u32 & V3_SEEK_ENTRY_RAW_FLAG) != 0 {
            return Err(ObjectError::InvalidV3Format(format!(
                "chunk {idx} length {stored_len_u32} exceeds 31-bit seek entry limit"
            )));
        }

        let entry = if is_raw {
            stored_len_u32 | V3_SEEK_ENTRY_RAW_FLAG
        } else {
            stored_len_u32
        };
        header_and_table.extend_from_slice(&entry.to_le_bytes());

        compressed_size = compressed_size
            .checked_add(payload.len())
            .ok_or_else(|| ObjectError::InvalidV3Format("v3 compressed_size overflow".into()))?;

        buffers.push(payload);
    }

    let compressed_size_u32 = u32::try_from(compressed_size)
        .map_err(|_| ObjectError::ObjectTooLarge(compressed_size as u64))?;

    let header = ObjectHeader {
        magic: *MAGIC,
        version: VERSION_V3,
        flags,
        original_size,
        compressed_size: compressed_size_u32,
        checksum,
        reserved: meta.to_reserved_bytes(),
    };
    header_and_table[..ObjectHeader::SIZE].copy_from_slice(&header.to_bytes());
    buffers[0] = Bytes::from(header_and_table);

    Ok(Buffer::from(buffers))
}

/// Read, validate, and decode a complete STFS v3 object using the caller's chunk
/// decompressor, returning the original payload bytes.
pub fn read_v3_object<F, E>(
    data: impl Into<Buffer>,
    decompress_chunk: F,
) -> Result<Bytes, ObjectError>
where
    F: FnMut(CompressionCodec, &[u8], &mut [u8]) -> Result<usize, E>,
    E: std::fmt::Display,
{
    read_v3_object_with_checksum(data, decompress_chunk).map(|(payload, _)| payload)
}

/// Read, validate, and decode a complete STFS v3 object, returning the original
/// payload bytes and verified XXH64 checksum.
///
/// Validates the header, codec, reserved bytes, chunk geometry, seek table, and
/// total stored length before allocating the output buffer or invoking
/// `decompress_chunk`.
pub fn read_v3_object_with_checksum<F, E>(
    data: impl Into<Buffer>,
    mut decompress_chunk: F,
) -> Result<(Bytes, u64), ObjectError>
where
    F: FnMut(CompressionCodec, &[u8], &mut [u8]) -> Result<usize, E>,
    E: std::fmt::Display,
{
    let data: Buffer = data.into();
    let seek_table = parse_complete_v3_seek_table(&data)?;
    let header = *seek_table.header();
    let codec = seek_table.codec();

    let mut out = vec![0u8; seek_table.original_size()];
    for entry in seek_table.entries() {
        let mut chunk_buf = data.slice(entry.plaintext_range());
        let dst = &mut out[entry.logical_range()];
        if entry.is_raw {
            chunk_buf.copy_to_slice(dst);
        } else {
            let stored = if Buffer::count(&chunk_buf) == 1 {
                chunk_buf.current()
            } else {
                chunk_buf.to_bytes()
            };
            let written = decompress_chunk(codec, stored.as_ref(), dst).map_err(|err| {
                ObjectError::ChunkDecompression {
                    chunk_index: entry.index,
                    message: err.to_string(),
                }
            })?;
            if written != entry.original_len {
                return Err(ObjectError::ChunkDecompression {
                    chunk_index: entry.index,
                    message: format!(
                        "decompressed length {written} != derived chunk length {}",
                        entry.original_len
                    ),
                });
            }
        }
    }

    let computed_checksum = xxh64(&out, 0);
    if computed_checksum != header.checksum {
        return Err(ObjectError::ChecksumMismatch {
            expected: header.checksum,
            computed: computed_checksum,
        });
    }

    Ok((Bytes::from(out), computed_checksum))
}

/// Parse and validate the header and seek table of a complete STFS v3 object.
///
/// Checks header, codec, chunk geometry, seek table, and that the stored payload
/// length equals `compressed_size`, so every entry's ranges are in bounds of `data`.
pub(crate) fn parse_complete_v3_seek_table(data: &Buffer) -> Result<V3SeekTable, ObjectError> {
    if data.len() < ObjectHeader::SIZE {
        return Err(
            io::Error::new(io::ErrorKind::UnexpectedEof, "file too short for header").into(),
        );
    }

    let mut header_buf = data.slice(..ObjectHeader::SIZE);
    let mut header_bytes = [0u8; ObjectHeader::SIZE];
    header_buf.copy_to_slice(&mut header_bytes);
    let header = ObjectHeader::from_bytes(&header_bytes)?;
    let meta = V3HeaderMeta::parse(&header)?;

    let payload = data.slice(ObjectHeader::SIZE..);
    let seek_table_len = meta.seek_table_len();
    if payload.len() < seek_table_len {
        return Err(ObjectError::InvalidV3Format(format!(
            "truncated seek table: need {seek_table_len} bytes, got {}",
            payload.len()
        )));
    }

    let mut seek_table_bytes = vec![0u8; seek_table_len];
    let mut seek_table_buf = payload.slice(..seek_table_len);
    seek_table_buf.copy_to_slice(&mut seek_table_bytes);
    let seek_table = V3SeekTable::parse_with_header(header, &seek_table_bytes)?;

    if payload.len() != header.compressed_size as usize {
        return Err(ObjectError::SizeMismatch {
            original_size: header.original_size,
            compressed_size: header.compressed_size,
            actual_size: payload.len(),
        });
    }

    Ok(seek_table)
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
/// - `prefix`: Low 16 bits of object_id for S3 partition distribution
/// - `object_id`: 16-char hex sequential object ID
///
/// # Arguments
/// * `data_prefix` - Volume data prefix
/// * `object_id` - Sequential object ID
///
/// # Returns
/// S3 key path string
pub fn format_object_key(data_prefix: &str, object_id: u64) -> String {
    let separator = if data_prefix.ends_with('/') { "" } else { "/" };
    format!(
        "{data_prefix}{separator}{:04x}/{object_id:016x}",
        object_id & 0xffff
    )
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

/// Alias for generating all fragment keys for an object.
pub use format_fragment_keys as format_object_fragment_keys;

/// Returns the object ID and every canonical key (legacy plus each fragment) for
/// `listed_key`, or `None` when it is not a canonical object key under `data_prefix`.
pub fn canonical_object_delete_keys(
    data_prefix: &str,
    listed_key: &str,
) -> Option<(u64, Vec<String>)> {
    let object_id = parse_object_id_from_key(listed_key).ok()?;
    let legacy_key = format_object_key(data_prefix, object_id);
    let fragment_keys = format_fragment_keys(data_prefix, object_id);
    if listed_key != legacy_key && !fragment_keys.iter().any(|key| key == listed_key) {
        return None;
    }
    Some((
        object_id,
        std::iter::once(legacy_key).chain(fragment_keys).collect(),
    ))
}

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
    fn canonical_delete_keys_cover_legacy_and_fragments_and_reject_foreign_keys() {
        let expected: Vec<String> = std::iter::once(format_object_key("vol/objects/", 0x1234))
            .chain(format_fragment_keys("vol/objects/", 0x1234))
            .collect();
        for listed in &expected {
            assert_eq!(
                canonical_object_delete_keys("vol/objects/", listed),
                Some((0x1234, expected.clone()))
            );
        }
        for foreign in [
            "vol/objects/unrelated/0000000000001234",
            "other/objects/1234/0000000000001234",
            "vol/objects/1234/0000000000001234/extra",
            "vol/objects/1234/1234",
        ] {
            assert_eq!(canonical_object_delete_keys("vol/objects/", foreign), None);
        }
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
    fn test_object_rejects_mismatched_header_sizes_with_valid_checksum() {
        let payload = b"checksummed payload";
        let size = payload.len() as u32;
        for (original_size, compressed_size) in [
            (size - 1, size),
            (size + 1, size),
            (size, size - 1),
            (size, size + 1),
            (size - 1, size - 1),
            (size + 1, size + 1),
            (0, 0),
            (0, size),
            (size, 0),
        ] {
            let header = ObjectHeader::new(original_size, compressed_size, xxh64(payload, 0));
            let object = Buffer::from(vec![
                Bytes::copy_from_slice(&header.to_bytes()),
                Bytes::copy_from_slice(payload),
            ]);
            for object in [object.clone(), Buffer::from(object.to_bytes())] {
                let result = read_object(object);
                assert!(
                    matches!(
                        result,
                        Err(ObjectError::SizeMismatch {
                            original_size: original,
                            compressed_size: compressed,
                            actual_size,
                        }) if original == original_size
                            && compressed == compressed_size
                            && actual_size == payload.len()
                    ),
                    "failed rejection for original_size={original_size}, compressed_size={compressed_size}: {result:?}"
                );
            }
        }
    }

    #[test]
    fn test_empty_object_roundtrip() {
        let object = create_object_buffer(Bytes::new()).unwrap();
        assert!(read_object(object).unwrap().is_empty());
    }

    #[test]
    fn test_object_accepts_maximum_payload_size() {
        let chunk = Bytes::from(vec![0xAB; 1024 * 1024]);
        let mut chunks = vec![chunk; MAX_OBJECT_SIZE as usize / (1024 * 1024)];
        let payload = Buffer::from(chunks.clone());
        let header = ObjectHeader::new(MAX_OBJECT_SIZE, MAX_OBJECT_SIZE, xxh64_buffer(&payload));
        chunks.insert(0, Bytes::copy_from_slice(&header.to_bytes()));

        let decoded = read_object(Buffer::from(chunks)).unwrap();
        assert_eq!(decoded.len(), MAX_OBJECT_SIZE as usize);
        assert!(decoded.iter().all(|byte| *byte == 0xAB));
        // The writer accepts the same inclusive boundary as the reader.
        let encoded = create_object_buffer(decoded).unwrap();
        assert_eq!(encoded.len(), ObjectHeader::SIZE + MAX_OBJECT_SIZE as usize);
    }

    #[test]
    fn test_object_rejects_oversized_payload_before_checksum() {
        // Share the allocation across chunks to keep this boundary fixture small.
        let chunk = Bytes::from(vec![0xAB; 1024 * 1024]);
        let mut chunks = vec![chunk; MAX_OBJECT_SIZE as usize / (1024 * 1024)];
        chunks.push(Bytes::from_static(b"x"));
        let size = MAX_OBJECT_SIZE + 1;
        // Size rejection must happen before checksum verification.
        let header = ObjectHeader::new(size, size, 0);
        chunks.insert(0, Bytes::copy_from_slice(&header.to_bytes()));
        assert!(matches!(
            read_object(Buffer::from(chunks)),
            Err(ObjectError::ObjectTooLarge(actual)) if actual == u64::from(size)
        ));
    }

    #[test]
    fn test_empty_payload_rejects_nonzero_header_sizes() {
        for (original_size, compressed_size) in [(1, 0), (0, 1), (1, 1)] {
            let header = ObjectHeader::new(original_size, compressed_size, xxh64(b"", 0));
            assert!(
                matches!(
                    read_object(Bytes::copy_from_slice(&header.to_bytes())),
                    Err(ObjectError::SizeMismatch {
                        original_size: original,
                        compressed_size: compressed,
                        actual_size: 0,
                    }) if original == original_size && compressed == compressed_size
                ),
                "empty payload accepted sizes ({original_size}, {compressed_size})"
            );
        }
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
        assert_eq!(key, "disk-a/objects/1234/0000000000001234");
    }

    #[test]
    fn composite_object_keys_use_low_bits_for_shards_and_preserve_ids() {
        let first = (0x12_3456_u64 << 39) | 0xffff;
        let second = first + 1;

        let first_key = format_object_key("disk-a/objects", first);
        let second_key = format_object_key("disk-a/objects", second);

        assert_eq!(first_key, "disk-a/objects/ffff/091a2b000000ffff");
        assert_eq!(second_key, "disk-a/objects/0000/091a2b0000010000");
        assert_eq!(parse_object_id_from_key(&first_key).unwrap(), first);
        assert_eq!(parse_object_id_from_key(&second_key).unwrap(), second);
    }

    #[test]
    fn test_object_key_parsing() {
        let key = "disk-a/objects/1234/0000000000001234";
        let object_id = parse_object_id_from_key(key).unwrap();

        assert_eq!(object_id, 0x1234);
    }

    #[test]
    fn test_fragment_key_formatting() {
        let data_prefix = "disk-a/objects/";
        let object_id: u64 = 0x1234;

        let key = format_object_fragment_key(data_prefix, object_id, 0);
        assert_eq!(key, "disk-a/objects/1234/0000000000001234/0");
    }

    #[test]
    fn test_fragment_key_parsing() {
        let key = "disk-a/objects/1234/0000000000001234/0";
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

    // =========================================================================
    // STFS v3 Tests
    // =========================================================================

    /// Reversible mock compressor for pure-format tests (no real zstd codec).
    /// Encodes a chunk of repeated `byte` as `[0xAA, byte]` (2 bytes), or if the
    /// chunk is not uniform, stores `[0x55, len_lo, len_hi, ...]` only when smaller.
    /// For testing, `mock_compress_uniform` always produces a 2-byte compressed
    /// representation of any uniform slice of length >= 2.
    fn mock_compress_uniform(chunk: &[u8]) -> Option<Bytes> {
        let first = *chunk.first()?;
        // The 2-byte encoding must shrink the chunk, otherwise it is stored raw.
        if chunk.len() > 2 && chunk.iter().all(|&b| b == first) {
            Some(Bytes::copy_from_slice(&[0xAA, first]))
        } else {
            None
        }
    }

    fn mock_decompress_uniform(
        codec: CompressionCodec,
        compressed: &[u8],
        out: &mut [u8],
    ) -> Result<usize, String> {
        if codec != CompressionCodec::Zstd {
            return Err(format!("unexpected codec: {codec:?}"));
        }
        if compressed.len() == 2 && compressed[0] == 0xAA {
            out.fill(compressed[1]);
            Ok(out.len())
        } else {
            Err(format!(
                "invalid mock compressed payload of len {}",
                compressed.len()
            ))
        }
    }

    fn encode_v3_with_mock(original: &[u8], chunk_log2: u8, force_raw: bool) -> (Buffer, u64) {
        let chunk_size = 1usize << chunk_log2;
        let checksum = xxh64(original, 0);
        let chunks: Vec<V3EncodedChunk> = original
            .chunks(chunk_size)
            .map(|chunk| {
                if !force_raw && let Some(compressed) = mock_compress_uniform(chunk) {
                    V3EncodedChunk::Compressed(compressed)
                } else {
                    V3EncodedChunk::Raw(Bytes::copy_from_slice(chunk))
                }
            })
            .collect();
        let buffer = create_v3_object_buffer(
            original.len() as u32,
            checksum,
            CompressionCodec::Zstd,
            chunk_log2,
            ObjectFlags::empty(),
            chunks,
        )
        .expect("encode v3 object");
        (buffer, checksum)
    }

    #[test]
    fn test_v3_roundtrip_all_compressed() {
        let chunk_log2 = MIN_V3_CHUNK_LOG2; // 4 KiB
        let chunk_size = 1usize << chunk_log2;
        let mut original = Vec::with_capacity(2 * chunk_size + 500);
        original.extend(std::iter::repeat_n(0x11u8, chunk_size));
        original.extend(std::iter::repeat_n(0x22u8, chunk_size));
        original.extend(std::iter::repeat_n(0x33u8, 500));

        let (encoded, expected_checksum) = encode_v3_with_mock(&original, chunk_log2, false);
        let raw_bytes = encoded.to_bytes();

        let seek = V3SeekTable::parse_from_plaintext(&raw_bytes).unwrap();
        assert_eq!(seek.header().version, VERSION_V3);
        assert_eq!(seek.header().version, VERSION_V3);
        assert!(seek.header().is_compressed());
        assert_eq!(seek.codec(), CompressionCodec::Zstd);
        assert_eq!(seek.chunk_size(), chunk_size);
        assert_eq!(seek.original_size(), original.len());
        assert_eq!(seek.entries().len(), 3);
        assert!(
            seek.entries()
                .iter()
                .all(|e| !e.is_raw && e.stored_len == 2)
        );
        assert_eq!(seek.compressed_size(), 3 * 4 + 3 * 2);

        let (decoded, checksum) =
            read_v3_object_with_checksum(encoded, mock_decompress_uniform).unwrap();
        assert_eq!(checksum, expected_checksum);
        assert_eq!(decoded.as_ref(), original.as_slice());
    }

    #[test]
    fn test_v3_roundtrip_all_raw_including_full_fragment_boundary() {
        let chunk_size = DEFAULT_V3_CHUNK_SIZE;
        let original: Vec<u8> = (0..CLOUD_FRAGMENT_SIZE)
            .map(|i| ((i.wrapping_mul(31) ^ (i >> 7)) & 0xFF) as u8)
            .collect();

        let (encoded, expected_checksum) =
            encode_v3_with_mock(&original, DEFAULT_V3_CHUNK_LOG2, true);
        let raw_bytes = encoded.to_bytes();

        let seek = V3SeekTable::parse_from_plaintext(&raw_bytes[..ObjectHeader::SIZE + 32 * 4])
            .expect("parse seek table from prefix alone");
        assert_eq!(seek.entries().len(), CLOUD_FRAGMENT_SIZE / chunk_size);
        assert!(
            seek.entries()
                .iter()
                .all(|e| e.is_raw && e.stored_len == chunk_size)
        );
        assert_eq!(seek.compressed_size(), 32 * 4 + CLOUD_FRAGMENT_SIZE);

        let (decoded, checksum) =
            read_v3_object_with_checksum(encoded, |_, _, _| -> Result<usize, &str> {
                panic!("decompressor must not be called for all-raw v3 object");
            })
            .unwrap();
        assert_eq!(checksum, expected_checksum);
        assert_eq!(decoded.as_ref(), original.as_slice());
    }

    #[test]
    fn test_v3_roundtrip_mixed_chunks() {
        let chunk_log2 = MIN_V3_CHUNK_LOG2; // 4 KiB
        let chunk_size = 1usize << chunk_log2;
        let mut original = Vec::with_capacity(3 * chunk_size + 73);
        // Chunk 0: compressed (uniform 0x41)
        original.extend(std::iter::repeat_n(0x41u8, chunk_size));
        // Chunk 1: raw (non-uniform)
        original.extend((0..chunk_size).map(|i| (i % 251) as u8));
        // Chunk 2: compressed (uniform 0x7E)
        original.extend(std::iter::repeat_n(0x7Eu8, chunk_size));
        // Chunk 3: short raw tail
        original.extend((0..73).map(|i| (i * 7) as u8));

        let (encoded, expected_checksum) = encode_v3_with_mock(&original, chunk_log2, false);
        let seek = V3SeekTable::parse_from_plaintext(&encoded.to_bytes()).unwrap();
        assert_eq!(seek.entries().len(), 4);
        assert!(!seek.entries()[0].is_raw);
        assert!(seek.entries()[1].is_raw);
        assert!(!seek.entries()[2].is_raw);
        assert!(seek.entries()[3].is_raw);
        assert_eq!(seek.entries()[3].original_len, 73);
        assert_eq!(seek.entries()[3].stored_len, 73);

        let (decoded, checksum) =
            read_v3_object_with_checksum(encoded, mock_decompress_uniform).unwrap();
        assert_eq!(checksum, expected_checksum);
        assert_eq!(decoded.as_ref(), original.as_slice());
    }

    #[test]
    fn test_v3_roundtrip_single_short_chunk() {
        // Single short raw chunk (1 byte)
        let one_byte = b"x";
        let (encoded_raw, _) = encode_v3_with_mock(one_byte, DEFAULT_V3_CHUNK_LOG2, true);
        let decoded_raw = read_v3_object(encoded_raw, mock_decompress_uniform).unwrap();
        assert_eq!(decoded_raw.as_ref(), one_byte);

        // Single short compressed chunk (37 uniform bytes)
        let short_uniform = vec![0x5Au8; 37];
        let (encoded_comp, _) = encode_v3_with_mock(&short_uniform, DEFAULT_V3_CHUNK_LOG2, false);
        let seek = V3SeekTable::parse_from_plaintext(&encoded_comp.to_bytes()).unwrap();
        assert_eq!(seek.entries().len(), 1);
        assert!(!seek.entries()[0].is_raw);
        assert_eq!(seek.entries()[0].original_len, 37);
        assert_eq!(seek.entries()[0].stored_len, 2);
        let decoded_comp = read_v3_object(encoded_comp, mock_decompress_uniform).unwrap();
        assert_eq!(decoded_comp.as_ref(), short_uniform.as_slice());
    }

    #[test]
    fn test_v3_roundtrip_empty_payload() {
        let checksum = xxh64(b"", 0);
        let encoded = create_v3_object_buffer(
            0,
            checksum,
            CompressionCodec::Zstd,
            DEFAULT_V3_CHUNK_LOG2,
            ObjectFlags::empty(),
            Vec::new(),
        )
        .unwrap();
        let bytes = encoded.to_bytes();
        assert_eq!(bytes.len(), ObjectHeader::SIZE);

        let seek = V3SeekTable::parse_from_plaintext(&bytes).unwrap();
        assert_eq!(seek.original_size(), 0);
        assert_eq!(seek.compressed_size(), 0);
        assert_eq!(seek.seek_table_len(), 0);
        assert!(seek.entries().is_empty());
        assert_eq!(seek.map_range(0..10).plaintext_range, None);

        let (decoded, got_checksum) =
            read_v3_object_with_checksum(encoded, |_, _, _| -> Result<usize, &str> {
                panic!("decompressor must not be called for empty payload");
            })
            .unwrap();
        assert!(decoded.is_empty());
        assert_eq!(got_checksum, checksum);
    }

    #[test]
    fn test_v3_rejects_truncated_seek_table() {
        let original = vec![0x42u8; (1 << MIN_V3_CHUNK_LOG2) * 2];
        let (encoded, _) = encode_v3_with_mock(&original, MIN_V3_CHUNK_LOG2, false);
        let full = encoded.to_bytes();

        // Truncate inside the 8-byte seek table (leave only 5 bytes after header).
        let truncated = full.slice(..ObjectHeader::SIZE + 5);
        assert!(matches!(
            V3SeekTable::parse_from_plaintext(&truncated),
            Err(ObjectError::InvalidV3Format(_))
        ));
        let err = read_v3_object(truncated, |_, _, _| -> Result<usize, &str> {
            panic!("geometry validation must reject before calling decompressor");
        })
        .unwrap_err();
        assert!(matches!(err, ObjectError::InvalidV3Format(_)));
    }

    #[test]
    fn test_v3_rejects_inconsistent_sums_and_truncated_payload() {
        let original = vec![0x42u8; (1 << MIN_V3_CHUNK_LOG2) + 100];
        let (encoded, _) = encode_v3_with_mock(&original, MIN_V3_CHUNK_LOG2, false);
        let full = encoded.to_bytes().to_vec();

        // 1. Header compressed_size smaller than seek table length.
        let mut too_small_cs = full.clone();
        too_small_cs[12..16].copy_from_slice(&4u32.to_le_bytes()); // seek table is 8 bytes
        assert!(matches!(
            V3SeekTable::parse_from_plaintext(&too_small_cs),
            Err(ObjectError::InvalidV3Format(_))
        ));

        // 2. Header compressed_size disagrees with seek table sum (+1 and -1).
        let orig_cs = u32::from_le_bytes(full[12..16].try_into().unwrap());
        for bad_cs in [orig_cs - 1, orig_cs + 1] {
            let mut bad = full.clone();
            bad[12..16].copy_from_slice(&bad_cs.to_le_bytes());
            assert!(matches!(
                V3SeekTable::parse_from_plaintext(&bad),
                Err(ObjectError::InvalidV3Format(_))
            ));
            let err = read_v3_object(Bytes::from(bad), |_, _, _| -> Result<usize, &str> {
                panic!("geometry validation must reject before calling decompressor");
            })
            .unwrap_err();
            assert!(matches!(err, ObjectError::InvalidV3Format(_)));
        }

        // 3. Valid header + seek table, but actual chunk bytes truncated or trailing.
        let truncated_body = Bytes::copy_from_slice(&full[..full.len() - 1]);
        let err = read_v3_object(truncated_body, |_, _, _| -> Result<usize, &str> {
            panic!("size validation must reject before calling decompressor");
        })
        .unwrap_err();
        assert!(matches!(err, ObjectError::SizeMismatch { .. }));

        let mut trailing_body = full;
        trailing_body.push(0x99);
        let err = read_v3_object(
            Bytes::from(trailing_body),
            |_, _, _| -> Result<usize, &str> {
                panic!("size validation must reject before calling decompressor");
            },
        )
        .unwrap_err();
        assert!(matches!(err, ObjectError::SizeMismatch { .. }));
    }

    #[test]
    fn test_v3_rejects_oversized_and_invalid_chunks() {
        let chunk_log2 = MIN_V3_CHUNK_LOG2; // 4 KiB
        let chunk_size = 1usize << chunk_log2;

        // Encoder rejects original_size > CLOUD_FRAGMENT_SIZE.
        let err = create_v3_object_buffer(
            (CLOUD_FRAGMENT_SIZE + 1) as u32,
            0,
            CompressionCodec::Zstd,
            DEFAULT_V3_CHUNK_LOG2,
            ObjectFlags::empty(),
            Vec::new(),
        )
        .unwrap_err();
        assert!(matches!(err, ObjectError::ObjectTooLarge(_)));

        // Encoder rejects a compressed chunk that does not shrink (stored_len == original_len).
        let err = create_v3_object_buffer(
            10,
            0,
            CompressionCodec::Zstd,
            chunk_log2,
            ObjectFlags::empty(),
            vec![V3EncodedChunk::Compressed(Bytes::from(vec![1u8; 10]))],
        )
        .unwrap_err();
        assert!(matches!(err, ObjectError::InvalidV3Format(_)));

        // Encoder rejects oversized compressed chunk (stored_len > original_len).
        let err = create_v3_object_buffer(
            10,
            xxh64(&[0u8; 10], 0),
            CompressionCodec::Zstd,
            chunk_log2,
            ObjectFlags::empty(),
            vec![V3EncodedChunk::Compressed(Bytes::from(vec![1u8; 11]))],
        )
        .unwrap_err();
        assert!(matches!(err, ObjectError::InvalidV3Format(_)));

        // Encoder rejects zero-length compressed chunk.
        let err = create_v3_object_buffer(
            10,
            xxh64(&[0u8; 10], 0),
            CompressionCodec::Zstd,
            chunk_log2,
            ObjectFlags::empty(),
            vec![V3EncodedChunk::Compressed(Bytes::new())],
        )
        .unwrap_err();
        assert!(matches!(err, ObjectError::InvalidV3Format(_)));

        // Encoder rejects raw chunk with wrong length.
        let err = create_v3_object_buffer(
            10,
            xxh64(&[0u8; 10], 0),
            CompressionCodec::Zstd,
            chunk_log2,
            ObjectFlags::empty(),
            vec![V3EncodedChunk::Raw(Bytes::from(vec![0u8; 9]))],
        )
        .unwrap_err();
        assert!(matches!(err, ObjectError::InvalidV3Format(_)));

        // Decoder rejects oversized compressed chunk in seek table (stored_len = chunk_size + 1).
        let meta = V3HeaderMeta {
            codec: CompressionCodec::Zstd,
            chunk_log2,
            chunk_count: 1,
        };
        let oversized_len = (chunk_size + 1) as u32;
        let header = ObjectHeader {
            magic: *MAGIC,
            version: VERSION_V3,
            flags: ObjectFlags::empty(),
            original_size: chunk_size as u32,
            compressed_size: 4 + oversized_len,
            checksum: 0,
            reserved: meta.to_reserved_bytes(),
        };
        let mut buf = Vec::new();
        buf.extend_from_slice(&header.to_bytes());
        buf.extend_from_slice(&oversized_len.to_le_bytes());
        assert!(matches!(
            V3SeekTable::parse_from_plaintext(&buf),
            Err(ObjectError::InvalidV3Format(_))
        ));

        // Decoder rejects zero-length compressed chunk in seek table.
        let header_zero = ObjectHeader {
            compressed_size: 4,
            ..header
        };
        let mut buf_zero = Vec::new();
        buf_zero.extend_from_slice(&header_zero.to_bytes());
        buf_zero.extend_from_slice(&0u32.to_le_bytes());
        assert!(matches!(
            V3SeekTable::parse_from_plaintext(&buf_zero),
            Err(ObjectError::InvalidV3Format(_))
        ));

        // Decoder rejects raw chunk whose stored_len != original_len.
        for bad_raw_len in [(chunk_size - 1) as u32, (chunk_size + 1) as u32] {
            let header_raw = ObjectHeader {
                compressed_size: 4 + bad_raw_len,
                ..header
            };
            let mut buf_raw = Vec::new();
            buf_raw.extend_from_slice(&header_raw.to_bytes());
            buf_raw.extend_from_slice(&(bad_raw_len | V3_SEEK_ENTRY_RAW_FLAG).to_le_bytes());
            assert!(matches!(
                V3SeekTable::parse_from_plaintext(&buf_raw),
                Err(ObjectError::InvalidV3Format(_))
            ));
        }

        // Decoder rejects original_size > CLOUD_FRAGMENT_SIZE.
        let header_huge = ObjectHeader {
            original_size: (CLOUD_FRAGMENT_SIZE + 1) as u32,
            ..header
        };
        assert!(matches!(
            V3HeaderMeta::parse(&header_huge),
            Err(ObjectError::ObjectTooLarge(sz)) if sz == (CLOUD_FRAGMENT_SIZE + 1) as u64
        ));
    }

    #[test]
    fn test_v3_rejects_unknown_codec_as_typed_error() {
        let original = vec![0x42u8; 64];
        let (encoded, _) = encode_v3_with_mock(&original, DEFAULT_V3_CHUNK_LOG2, false);
        let full = encoded.to_bytes().to_vec();

        for unknown_codec in [0u8, 2u8, 42u8, 255u8] {
            let mut bad = full.clone();
            bad[24] = unknown_codec; // reserved[0] is at byte 24 of the 30-byte header
            assert!(matches!(
                V3SeekTable::parse_from_plaintext(&bad),
                Err(ObjectError::UnsupportedCodec(c)) if c == unknown_codec
            ));
            let err = read_v3_object(Bytes::from(bad), |_, _, _| -> Result<usize, &str> {
                panic!("must not call decompressor for unknown codec");
            })
            .unwrap_err();
            assert!(matches!(err, ObjectError::UnsupportedCodec(c) if c == unknown_codec));
        }
    }

    #[test]
    fn test_v3_rejects_nonzero_reserved_bytes() {
        let original = vec![0x42u8; 64];
        let (encoded, _) = encode_v3_with_mock(&original, DEFAULT_V3_CHUNK_LOG2, false);
        let full = encoded.to_bytes().to_vec();

        for (offset, value) in [(28, 0x01), (29, 0x80)] {
            let mut bad = full.clone();
            bad[offset] = value;
            assert!(matches!(
                V3SeekTable::parse_from_plaintext(&bad),
                Err(ObjectError::InvalidV3Format(_))
            ));
            assert!(matches!(
                read_v3_object(Bytes::from(bad), mock_decompress_uniform),
                Err(ObjectError::InvalidV3Format(_))
            ));
        }
    }

    #[test]
    fn test_v3_rejects_chunk_log2_outside_supported_range() {
        let original = vec![0x42u8; 64];
        let (encoded, _) = encode_v3_with_mock(&original, DEFAULT_V3_CHUNK_LOG2, false);
        let full = encoded.to_bytes().to_vec();

        for bad_log2 in [
            0u8,
            1u8,
            MIN_V3_CHUNK_LOG2 - 1,
            MAX_V3_CHUNK_LOG2 + 1,
            31u8,
            255u8,
        ] {
            // Encoder rejection
            assert!(matches!(
                create_v3_object_buffer(
                    0,
                    xxh64(b"", 0),
                    CompressionCodec::Zstd,
                    bad_log2,
                    ObjectFlags::empty(),
                    Vec::new(),
                ),
                Err(ObjectError::InvalidV3Format(_))
            ));

            // Decoder rejection
            let mut bad = full.clone();
            bad[25] = bad_log2; // reserved[1] is chunk_log2
            assert!(matches!(
                V3SeekTable::parse_from_plaintext(&bad),
                Err(ObjectError::InvalidV3Format(_))
            ));
        }
    }

    #[test]
    fn test_v3_rejects_wrong_chunk_count() {
        let original = vec![0x42u8; 64];
        let (encoded, _) = encode_v3_with_mock(&original, DEFAULT_V3_CHUNK_LOG2, false);
        let full = encoded.to_bytes().to_vec();

        for bad_count in [0u16, 2u16, 100u16] {
            let mut bad = full.clone();
            bad[26..28].copy_from_slice(&bad_count.to_le_bytes());
            assert!(matches!(
                V3SeekTable::parse_from_plaintext(&bad),
                Err(ObjectError::InvalidV3Format(_))
            ));
        }
    }

    #[test]
    fn test_v3_rejects_legacy_lz4_flag() {
        let err = create_v3_object_buffer(
            0,
            xxh64(b"", 0),
            CompressionCodec::Zstd,
            DEFAULT_V3_CHUNK_LOG2,
            ObjectFlags::LEGACY_LZ4_COMPRESSED,
            Vec::new(),
        )
        .unwrap_err();
        assert!(matches!(err, ObjectError::LegacyCompressionUnsupported));

        let original = vec![0x42u8; 64];
        let (encoded, _) = encode_v3_with_mock(&original, DEFAULT_V3_CHUNK_LOG2, false);
        let mut bad = encoded.to_bytes().to_vec();
        bad[6..8].copy_from_slice(&ObjectFlags::LEGACY_LZ4_COMPRESSED.bits().to_le_bytes());
        assert!(matches!(
            V3SeekTable::parse_from_plaintext(&bad),
            Err(ObjectError::LegacyCompressionUnsupported)
        ));
        assert!(matches!(
            read_v3_object(Bytes::from(bad), mock_decompress_uniform),
            Err(ObjectError::LegacyCompressionUnsupported)
        ));
    }

    #[test]
    fn test_v3_rejects_decompressor_errors_wrong_length_and_bad_checksum() {
        let original = vec![0x42u8; 64];
        let (encoded, _) = encode_v3_with_mock(&original, DEFAULT_V3_CHUNK_LOG2, false);

        // Wrong decompressed length
        let err = read_v3_object(encoded.clone(), |_, _, out| -> Result<usize, &str> {
            out.fill(0x42);
            Ok(out.len() - 1)
        })
        .unwrap_err();
        assert!(matches!(
            err,
            ObjectError::ChunkDecompression { chunk_index: 0, .. }
        ));

        // Decompressor returns Err
        let err = read_v3_object(encoded.clone(), |_, _, _| -> Result<usize, &str> {
            Err("corrupt zstd frame")
        })
        .unwrap_err();
        assert!(matches!(
            err,
            ObjectError::ChunkDecompression { chunk_index: 0, .. }
        ));

        // Corrupted header checksum
        let mut bad_cksum = encoded.to_bytes().to_vec();
        bad_cksum[16] ^= 0xFF;
        let err = read_v3_object(Bytes::from(bad_cksum), mock_decompress_uniform).unwrap_err();
        assert!(matches!(err, ObjectError::ChecksumMismatch { .. }));
    }

    #[test]
    fn test_v2_read_object_rejects_v3_without_misreporting_corruption() {
        let original = vec![0x42u8; 64];
        let (encoded, _) = encode_v3_with_mock(&original, DEFAULT_V3_CHUNK_LOG2, false);
        let err = read_object(encoded).unwrap_err();
        assert!(matches!(err, ObjectError::UnsupportedVersion(VERSION_V3)));
    }

    #[test]
    fn test_v3_range_mapping_property() {
        // Deterministic xorshift64* PRNG for repeatable property coverage.
        struct Rng(u64);
        impl Rng {
            fn next_u64(&mut self) -> u64 {
                let mut x = self.0;
                x ^= x >> 12;
                x ^= x << 25;
                x ^= x >> 27;
                self.0 = x;
                x.wrapping_mul(0x2545_F491_4F6C_DD1D)
            }
            fn gen_range(&mut self, bound: usize) -> usize {
                (self.next_u64() as usize) % bound
            }
        }

        let mut rng = Rng(0x57F5_0003_C0DE_BEEF);

        for chunk_log2 in [
            MIN_V3_CHUNK_LOG2,
            14,
            DEFAULT_V3_CHUNK_LOG2,
            MAX_V3_CHUNK_LOG2,
        ] {
            let chunk_size = 1usize << chunk_log2;
            let mut sizes = vec![0usize, 1, 2, chunk_size - 1, chunk_size];
            if chunk_size < CLOUD_FRAGMENT_SIZE {
                sizes.push(chunk_size + 1);
                sizes.push((3 * chunk_size + 137).min(CLOUD_FRAGMENT_SIZE));
            }
            sizes.push(CLOUD_FRAGMENT_SIZE);

            for orig_len in sizes {
                let expected_chunks = orig_len.div_ceil(chunk_size);
                // Synthesize a valid seek table with mixed raw and variable-length
                // compressed chunks without allocating 4 MiB per iteration.
                let mut seek_entries = Vec::with_capacity(expected_chunks * 4);
                let mut compressed_size = expected_chunks * 4;
                for idx in 0..expected_chunks {
                    let c_orig = derived_v3_chunk_original_len(orig_len, chunk_size, idx);
                    let is_raw = (rng.next_u64() & 1) == 0 || c_orig == 1;
                    let entry = if is_raw {
                        compressed_size += c_orig;
                        (c_orig as u32) | V3_SEEK_ENTRY_RAW_FLAG
                    } else {
                        // Compressed chunks must shrink: stored in 1..c_orig.
                        let stored = 1 + rng.gen_range(c_orig - 1);
                        compressed_size += stored;
                        stored as u32
                    };
                    seek_entries.extend_from_slice(&entry.to_le_bytes());
                }

                let meta = V3HeaderMeta {
                    codec: CompressionCodec::Zstd,
                    chunk_log2,
                    chunk_count: expected_chunks as u16,
                };
                let header = ObjectHeader {
                    magic: *MAGIC,
                    version: VERSION_V3,
                    flags: ObjectFlags::empty(),
                    original_size: orig_len as u32,
                    compressed_size: compressed_size as u32,
                    checksum: 0,
                    reserved: meta.to_reserved_bytes(),
                };
                let seek = V3SeekTable::parse_with_header(header, &seek_entries).unwrap();

                // Boundary + randomized ranges
                let mut ranges: Vec<Range<u64>> = vec![
                    0..0,
                    0..1,
                    0..(orig_len as u64),
                    0..u64::MAX,
                    (orig_len as u64)..(orig_len as u64 + 10),
                    (orig_len.saturating_sub(1) as u64)..(orig_len as u64),
                    Range { start: 10, end: 5 }, // inverted
                ];
                if orig_len > chunk_size {
                    let b = chunk_size as u64;
                    ranges.push((b - 1)..(b + 1));
                    ranges.push((b - 1)..b);
                    ranges.push(b..(b + 1));
                }
                let upper = (orig_len + 17) as u64;
                for _ in 0..48 {
                    let a = rng.next_u64() % (upper + 1);
                    let b = rng.next_u64() % (upper + 1);
                    ranges.push(a..b);
                }

                for range in ranges {
                    let mapping = seek.map_range(range.clone());
                    let expected_start = range.start.min(orig_len as u64) as usize;
                    let expected_end = range.end.min(orig_len as u64) as usize;

                    if expected_start >= expected_end {
                        assert!(mapping.clamped_logical_range.is_empty());
                        assert!(mapping.chunk_indices.is_empty());
                        assert!(mapping.plaintext_range.is_none());
                        assert!(mapping.chunks.is_empty());
                        continue;
                    }

                    assert_eq!(mapping.clamped_logical_range, expected_start..expected_end);
                    assert_eq!(mapping.chunk_indices.len(), mapping.chunks.len());
                    assert!(!mapping.chunks.is_empty());

                    // Verify exact byte coverage and minimality.
                    let mut cursor = expected_start;
                    let mut expected_pt_start: Option<u64> = None;
                    let mut expected_pt_end: u64 = 0;

                    for (pos, slice) in mapping.chunks.iter().enumerate() {
                        assert_eq!(slice.entry.index, mapping.chunk_indices.start + pos);
                        assert!(!slice.slice_in_chunk.is_empty());
                        assert!(slice.slice_in_chunk.end <= slice.entry.original_len);

                        let abs_start = slice.entry.logical_offset + slice.slice_in_chunk.start;
                        let abs_end = slice.entry.logical_offset + slice.slice_in_chunk.end;
                        assert_eq!(abs_start, cursor, "gap or overlap in logical coverage");
                        cursor = abs_end;

                        let pt = slice.entry.plaintext_range();
                        let pt = pt.start as u64..pt.end as u64;
                        if expected_pt_start.is_some() {
                            assert_eq!(
                                pt.start, expected_pt_end,
                                "non-contiguous plaintext chunks"
                            );
                        } else {
                            expected_pt_start = Some(pt.start);
                        }
                        expected_pt_end = pt.end;
                    }

                    assert_eq!(cursor, expected_end);
                    assert_eq!(
                        mapping.plaintext_range,
                        Some(expected_pt_start.unwrap()..expected_pt_end)
                    );
                }
            }
        }
    }

    const STFS_V3_BASELINE_FIXTURE: &[u8] = include_bytes!("../tests/fixtures/stfs-v3-mixed.bin");

    #[test]
    fn test_v3_baseline_fixture() {
        // Chunk 0: 128 KiB of 0x42 compressed as an RFC 8878 zstd RLE frame (no content checksum).
        const CHUNK0_ZSTD_RLE_FRAME: [u8; 13] = [
            0x28, 0xB5, 0x2F, 0xFD, // Zstd magic
            0xA0, // FHD: 4-byte FCS, single segment, no content checksum, no dict
            0x00, 0x00, 0x02, 0x00, // FCS = 131072 (128 KiB)
            0x03, 0x00, 0x10, // Last_Block=1, Block_Type=RLE(1), Block_Size=131072
            0x42, // Repeated byte 0x42 ('B')
        ];
        const CHUNK1_RAW: &[u8] = b"v3 raw tail chunk";

        let mut expected_original = vec![0x42u8; DEFAULT_V3_CHUNK_SIZE];
        expected_original.extend_from_slice(CHUNK1_RAW);
        let expected_checksum = xxh64(&expected_original, 0);

        let seek = V3SeekTable::parse_from_plaintext(STFS_V3_BASELINE_FIXTURE).unwrap();
        assert_eq!(seek.header().version, VERSION_V3);
        assert_eq!(seek.codec(), CompressionCodec::Zstd);
        assert_eq!(seek.meta().chunk_log2, DEFAULT_V3_CHUNK_LOG2);
        assert_eq!(
            seek.original_size(),
            DEFAULT_V3_CHUNK_SIZE + CHUNK1_RAW.len()
        );
        assert_eq!(seek.entries().len(), 2);
        assert!(!seek.entries()[0].is_raw);
        assert_eq!(seek.entries()[0].stored_len, CHUNK0_ZSTD_RLE_FRAME.len());
        assert_eq!(seek.entries()[0].original_len, DEFAULT_V3_CHUNK_SIZE);
        assert!(seek.entries()[1].is_raw);
        assert_eq!(seek.entries()[1].stored_len, CHUNK1_RAW.len());
        assert_eq!(seek.entries()[1].original_len, CHUNK1_RAW.len());

        // Range spanning the chunk 0 / chunk 1 boundary covers both entries.
        let boundary = DEFAULT_V3_CHUNK_SIZE as u64;
        let mapping = seek.map_range((boundary - 4)..(boundary + 5));
        assert_eq!(mapping.chunk_indices, 0..2);
        assert_eq!(
            mapping.plaintext_range,
            Some(
                (ObjectHeader::SIZE + 8) as u64
                    ..(ObjectHeader::SIZE + 8 + CHUNK0_ZSTD_RLE_FRAME.len() + CHUNK1_RAW.len())
                        as u64
            )
        );
        assert_eq!(
            mapping.chunks[0].slice_in_chunk,
            (DEFAULT_V3_CHUNK_SIZE - 4)..DEFAULT_V3_CHUNK_SIZE
        );
        assert_eq!(mapping.chunks[1].slice_in_chunk, 0..5);

        // Decode with verified XXH64 checksum.
        let (decoded, checksum) = read_v3_object_with_checksum(
            Bytes::from_static(STFS_V3_BASELINE_FIXTURE),
            |codec, compressed, out| -> Result<usize, &'static str> {
                assert_eq!(codec, CompressionCodec::Zstd);
                assert_eq!(compressed, &CHUNK0_ZSTD_RLE_FRAME);
                out.fill(0x42);
                Ok(out.len())
            },
        )
        .unwrap();
        assert_eq!(checksum, expected_checksum);
        assert_eq!(decoded.as_ref(), expected_original.as_slice());

        // Re-encoding must reproduce the pinned fixture byte-for-byte.
        let reencoded = create_v3_object_buffer(
            expected_original.len() as u32,
            expected_checksum,
            CompressionCodec::Zstd,
            DEFAULT_V3_CHUNK_LOG2,
            ObjectFlags::empty(),
            vec![
                V3EncodedChunk::Compressed(Bytes::from_static(&CHUNK0_ZSTD_RLE_FRAME)),
                V3EncodedChunk::Raw(Bytes::from_static(CHUNK1_RAW)),
            ],
        )
        .unwrap()
        .to_bytes();
        assert_eq!(reencoded.as_ref(), STFS_V3_BASELINE_FIXTURE);
    }
}
