//! SFEC v1 encrypted remote object envelope.
//!
//! This module owns the outer encrypted-object wire format. It intentionally
//! does not implement the XChaCha20-Poly1305 transform; crypto wiring belongs
//! to the follow-up encryption/decryption layer.

use bytes::{Buf, BufMut, Bytes};
use std::io::Cursor;
use studiofs_meta::CURRENT_ENCRYPTION_KEY_ID;
use thiserror::Error;

/// Magic bytes for the encrypted StudioFS object envelope ("SFEC").
pub const SFEC_MAGIC: &[u8; 4] = b"SFEC";

/// Current SFEC envelope format version.
pub const SFEC_VERSION: u16 = 1;

/// Default plaintext frame size for encrypted remote objects.
pub const DEFAULT_SFEC_FRAME_SIZE: usize = 1024 * 1024;

/// XChaCha20-Poly1305 authentication tag length appended to each frame.
pub const SFEC_FRAME_TAG_LEN: usize = 16;

const SFEC_HEADER_FIXED_LEN: usize = 40;
const XCHACHA20_POLY1305_V1_ID: u16 = 1;

/// Supported SFEC encryption algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SfecAlgorithm {
    /// XChaCha20-Poly1305 envelope format v1.
    XChaCha20Poly1305V1,
}

impl SfecAlgorithm {
    fn wire_id(self) -> u16 {
        match self {
            Self::XChaCha20Poly1305V1 => XCHACHA20_POLY1305_V1_ID,
        }
    }

    fn from_wire_id(id: u16) -> Result<Self, SfecError> {
        match id {
            XCHACHA20_POLY1305_V1_ID => Ok(Self::XChaCha20Poly1305V1),
            other => Err(SfecError::UnsupportedAlgorithm(other)),
        }
    }
}

/// Errors returned by strict SFEC envelope parsing and serialization.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SfecError {
    /// Header magic bytes do not match the expected SFEC marker.
    #[error("invalid SFEC magic bytes")]
    InvalidMagic,
    /// The serialized envelope uses an unsupported version.
    #[error("unsupported SFEC version: {0}")]
    UnsupportedVersion(u16),
    /// The serialized envelope uses an unsupported algorithm identifier.
    #[error("unsupported SFEC algorithm: {0}")]
    UnsupportedAlgorithm(u16),
    /// The declared header length is invalid.
    #[error("invalid SFEC header length: {0}")]
    InvalidHeaderLength(u32),
    /// The active key identifier is invalid.
    #[error("invalid SFEC key id: {0}")]
    InvalidKeyId(u32),
    /// A variable-length header field is too large for the SFEC v1 wire format.
    #[error("SFEC field too large: {0}")]
    FieldTooLarge(&'static str),
    /// The declared plaintext frame size is invalid.
    #[error("invalid SFEC frame size: {0}")]
    InvalidFrameSize(u32),
    /// The declared frame count is inconsistent or cannot be represented safely.
    #[error("SFEC frame count overflow")]
    FrameCountOverflow,
    /// The declared plaintext or ciphertext object length overflows supported sizes.
    #[error("SFEC object length overflow")]
    ObjectLengthOverflow,
    /// The envelope ends before the full declared header is available.
    #[error("truncated SFEC header")]
    TruncatedHeader,
    /// The envelope ends before all encrypted frames are available.
    #[error("truncated SFEC ciphertext")]
    TruncatedCiphertext,
    /// The envelope contains bytes past the expected encrypted frames.
    #[error("trailing malformed SFEC data")]
    TrailingMalformedData,
}

/// Parsed SFEC v1 header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SfecHeader {
    algorithm: SfecAlgorithm,
    flags: u16,
    key_id: u32,
    plaintext_len: u64,
    frame_size: u32,
    frame_count: u32,
    wrap_nonce: Bytes,
    data_nonce_prefix: Bytes,
    wrapped_dek: Bytes,
}

impl SfecHeader {
    /// Build a current SFEC header and derive frame count from plaintext length.
    pub fn new(
        algorithm: SfecAlgorithm,
        key_id: u32,
        plaintext_len: u64,
        frame_size: usize,
        wrap_nonce: Bytes,
        data_nonce_prefix: Bytes,
        wrapped_dek: Bytes,
    ) -> Result<Self, SfecError> {
        if key_id != CURRENT_ENCRYPTION_KEY_ID {
            return Err(SfecError::InvalidKeyId(key_id));
        }
        let frame_size = u32::try_from(frame_size).map_err(|_| SfecError::ObjectLengthOverflow)?;
        if frame_size == 0 {
            return Err(SfecError::InvalidFrameSize(frame_size));
        }
        validate_variable_len("wrap_nonce", wrap_nonce.len())?;
        validate_variable_len("data_nonce_prefix", data_nonce_prefix.len())?;
        validate_variable_len("wrapped_dek", wrapped_dek.len())?;

        let frame_count = expected_frame_count(plaintext_len, frame_size)?;
        Ok(Self {
            algorithm,
            flags: 0,
            key_id,
            plaintext_len,
            frame_size,
            frame_count,
            wrap_nonce,
            data_nonce_prefix,
            wrapped_dek,
        })
    }

    /// Encryption algorithm declared by the envelope.
    pub fn algorithm(&self) -> SfecAlgorithm {
        self.algorithm
    }

    /// Active key identifier needed to unwrap/decrypt this object.
    pub fn key_id(&self) -> u32 {
        self.key_id
    }

    /// Plaintext STFS object length before encryption.
    pub fn plaintext_len(&self) -> u64 {
        self.plaintext_len
    }

    /// Plaintext frame size in bytes.
    pub fn frame_size(&self) -> u32 {
        self.frame_size
    }

    /// Number of encrypted frames stored after the header.
    pub fn frame_count(&self) -> u32 {
        self.frame_count
    }

    /// Wrapped data-encryption-key nonce bytes.
    pub fn wrap_nonce(&self) -> &Bytes {
        &self.wrap_nonce
    }

    /// Data frame nonce prefix bytes.
    pub fn data_nonce_prefix(&self) -> &Bytes {
        &self.data_nonce_prefix
    }

    /// Wrapped data-encryption key bytes.
    pub fn wrapped_dek(&self) -> &Bytes {
        &self.wrapped_dek
    }

    /// Encrypted frame length including AEAD tag for a frame index.
    pub fn encrypted_frame_len(&self, frame_index: u32) -> Option<usize> {
        if frame_index >= self.frame_count {
            return None;
        }
        let full_frame_size = u64::from(self.frame_size);
        let start = u64::from(frame_index).checked_mul(full_frame_size)?;
        let remaining = self.plaintext_len.checked_sub(start)?;
        let plaintext_frame_len = remaining.min(full_frame_size);
        usize::try_from(plaintext_frame_len)
            .ok()?
            .checked_add(SFEC_FRAME_TAG_LEN)
    }

    fn header_len(&self) -> Result<usize, SfecError> {
        SFEC_HEADER_FIXED_LEN
            .checked_add(self.wrap_nonce.len())
            .and_then(|len| len.checked_add(self.data_nonce_prefix.len()))
            .and_then(|len| len.checked_add(self.wrapped_dek.len()))
            .ok_or(SfecError::ObjectLengthOverflow)
    }

    fn expected_ciphertext_len(&self) -> Result<usize, SfecError> {
        if self.frame_count == 0 {
            return Ok(0);
        }

        let full_frames = self.frame_count - 1;
        let full_frame_cipher_len = usize::try_from(self.frame_size)
            .ok()
            .and_then(|frame_size| frame_size.checked_add(SFEC_FRAME_TAG_LEN))
            .ok_or(SfecError::ObjectLengthOverflow)?;
        let full_frames_len = usize::try_from(full_frames)
            .map_err(|_| SfecError::ObjectLengthOverflow)?
            .checked_mul(full_frame_cipher_len)
            .ok_or(SfecError::ObjectLengthOverflow)?;
        let final_frame_len = self
            .encrypted_frame_len(full_frames)
            .ok_or(SfecError::ObjectLengthOverflow)?;

        full_frames_len
            .checked_add(final_frame_len)
            .ok_or(SfecError::ObjectLengthOverflow)
    }

    fn validate_shape(&self) -> Result<(), SfecError> {
        if self.key_id == 0 {
            return Err(SfecError::InvalidKeyId(self.key_id));
        }
        if self.frame_size == 0 {
            return Err(SfecError::InvalidFrameSize(self.frame_size));
        }
        let expected = expected_frame_count(self.plaintext_len, self.frame_size)?;
        if expected != self.frame_count {
            return Err(SfecError::FrameCountOverflow);
        }
        self.header_len()?;
        self.expected_ciphertext_len()?;
        Ok(())
    }
}

/// Parsed SFEC envelope: strict header plus encrypted frame bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SfecEnvelope {
    header: SfecHeader,
    ciphertext: Bytes,
}

impl SfecEnvelope {
    /// Serialize a validated SFEC header followed by encrypted frames.
    pub fn serialize(header: SfecHeader, ciphertext: Bytes) -> Result<Bytes, SfecError> {
        header.validate_shape()?;
        validate_ciphertext_len(&header, ciphertext.len())?;

        let header_len = header.header_len()?;
        let header_len_u32 =
            u32::try_from(header_len).map_err(|_| SfecError::ObjectLengthOverflow)?;
        let mut bytes = Vec::with_capacity(header_len + ciphertext.len());
        bytes.put_slice(SFEC_MAGIC);
        bytes.put_u16_le(SFEC_VERSION);
        bytes.put_u32_le(header_len_u32);
        bytes.put_u16_le(header.algorithm.wire_id());
        bytes.put_u16_le(header.flags);
        bytes.put_u32_le(header.key_id);
        bytes.put_u64_le(header.plaintext_len);
        bytes.put_u32_le(header.frame_size);
        bytes.put_u32_le(header.frame_count);
        bytes.put_u16_le(header.wrap_nonce.len() as u16);
        bytes.put_u16_le(header.data_nonce_prefix.len() as u16);
        bytes.put_u16_le(header.wrapped_dek.len() as u16);
        bytes.put_slice(&header.wrap_nonce);
        bytes.put_slice(&header.data_nonce_prefix);
        bytes.put_slice(&header.wrapped_dek);
        bytes.put_slice(&ciphertext);
        Ok(Bytes::from(bytes))
    }

    /// Parse and strictly validate a complete SFEC envelope.
    pub fn parse(data: Bytes) -> Result<Self, SfecError> {
        if data.len() < SFEC_HEADER_FIXED_LEN {
            return Err(SfecError::TruncatedHeader);
        }

        let mut fixed = Cursor::new(&data[..SFEC_HEADER_FIXED_LEN]);
        let mut magic = [0u8; 4];
        fixed.copy_to_slice(&mut magic);
        if &magic != SFEC_MAGIC {
            return Err(SfecError::InvalidMagic);
        }

        let version = fixed.get_u16_le();
        if version != SFEC_VERSION {
            return Err(SfecError::UnsupportedVersion(version));
        }

        let header_len = fixed.get_u32_le();
        let header_len_usize =
            usize::try_from(header_len).map_err(|_| SfecError::ObjectLengthOverflow)?;
        if header_len_usize < SFEC_HEADER_FIXED_LEN {
            return Err(SfecError::InvalidHeaderLength(header_len));
        }
        if data.len() < header_len_usize {
            return Err(SfecError::TruncatedHeader);
        }

        let algorithm = SfecAlgorithm::from_wire_id(fixed.get_u16_le())?;
        let flags = fixed.get_u16_le();
        let key_id = fixed.get_u32_le();
        let plaintext_len = fixed.get_u64_le();
        let frame_size = fixed.get_u32_le();
        let frame_count = fixed.get_u32_le();
        let wrap_nonce_len = usize::from(fixed.get_u16_le());
        let data_nonce_prefix_len = usize::from(fixed.get_u16_le());
        let wrapped_dek_len = usize::from(fixed.get_u16_le());

        let expected_header_len = SFEC_HEADER_FIXED_LEN
            .checked_add(wrap_nonce_len)
            .and_then(|len| len.checked_add(data_nonce_prefix_len))
            .and_then(|len| len.checked_add(wrapped_dek_len))
            .ok_or(SfecError::ObjectLengthOverflow)?;
        if header_len_usize != expected_header_len {
            return Err(SfecError::InvalidHeaderLength(header_len));
        }

        let wrap_nonce_start = SFEC_HEADER_FIXED_LEN;
        let data_nonce_prefix_start = wrap_nonce_start + wrap_nonce_len;
        let wrapped_dek_start = data_nonce_prefix_start + data_nonce_prefix_len;
        let ciphertext_start = wrapped_dek_start + wrapped_dek_len;

        let header = SfecHeader {
            algorithm,
            flags,
            key_id,
            plaintext_len,
            frame_size,
            frame_count,
            wrap_nonce: data.slice(wrap_nonce_start..data_nonce_prefix_start),
            data_nonce_prefix: data.slice(data_nonce_prefix_start..wrapped_dek_start),
            wrapped_dek: data.slice(wrapped_dek_start..ciphertext_start),
        };
        header.validate_shape()?;

        let ciphertext = data.slice(ciphertext_start..);
        validate_ciphertext_len(&header, ciphertext.len())?;

        Ok(Self { header, ciphertext })
    }

    /// Parsed SFEC header.
    pub fn header(&self) -> &SfecHeader {
        &self.header
    }

    /// Encrypted frame bytes following the header.
    pub fn ciphertext(&self) -> &Bytes {
        &self.ciphertext
    }
}

fn validate_variable_len(field: &'static str, len: usize) -> Result<(), SfecError> {
    u16::try_from(len)
        .map(|_| ())
        .map_err(|_| SfecError::FieldTooLarge(field))
}

fn expected_frame_count(plaintext_len: u64, frame_size: u32) -> Result<u32, SfecError> {
    if frame_size == 0 {
        return Err(SfecError::InvalidFrameSize(frame_size));
    }
    if plaintext_len == 0 {
        return Ok(0);
    }
    let frame_size = u64::from(frame_size);
    let frames = (plaintext_len - 1) / frame_size + 1;
    u32::try_from(frames).map_err(|_| SfecError::ObjectLengthOverflow)
}

fn validate_ciphertext_len(header: &SfecHeader, actual_len: usize) -> Result<(), SfecError> {
    let expected_len = header.expected_ciphertext_len()?;
    match actual_len.cmp(&expected_len) {
        std::cmp::Ordering::Less => Err(SfecError::TruncatedCiphertext),
        std::cmp::Ordering::Greater => Err(SfecError::TrailingMalformedData),
        std::cmp::Ordering::Equal => Ok(()),
    }
}
