//! XChaCha20-Poly1305 encryption for SFEC remote objects.

use crate::envelope::{
    DEFAULT_SFEC_FRAME_SIZE, SFEC_VERSION, SfecAlgorithm, SfecEnvelope, SfecError, SfecHeader,
};
use bytes::{BufMut, Bytes};
use chacha20poly1305::{
    Key, Tag, XChaCha20Poly1305, XNonce,
    aead::{Aead, AeadInPlace, KeyInit, OsRng, Payload, rand_core::RngCore},
};
use hkdf::Hkdf;
use opendal::Buffer;
use sha2::Sha256;
use thiserror::Error;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// Default plaintext frame size for encrypted remote objects.
pub const DEFAULT_ENCRYPTION_FRAME_SIZE: usize = DEFAULT_SFEC_FRAME_SIZE;

const KEY_LEN: usize = 32;
const XNONCE_LEN: usize = 24;
const DATA_NONCE_PREFIX_LEN: usize = 16;

/// Volume master key material used to derive object wrapping keys.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct VolumeMasterKey {
    bytes: [u8; KEY_LEN],
}

impl VolumeMasterKey {
    /// Construct a master key from raw 32-byte key material.
    pub const fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self { bytes }
    }

    fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.bytes
    }
}

/// Context authenticated into every encrypted object.
#[derive(Debug, Clone, Copy)]
pub struct ObjectEncryptionContext<'a> {
    /// Immutable volume UUID from `FormatInfo.uuid`.
    pub volume_uuid: &'a str,
    /// Exact remote object storage key.
    pub storage_key: &'a str,
    /// Active wrapping key identifier.
    pub key_id: u32,
}

/// Errors returned by object encryption/decryption.
#[derive(Debug, Error)]
pub enum CryptoError {
    /// The SFEC envelope is malformed.
    #[error("malformed SFEC envelope: {0}")]
    MalformedEnvelope(SfecError),
    /// The SFEC envelope declares an unsupported format version.
    #[error("unsupported SFEC version: {0}")]
    UnsupportedVersion(u16),
    /// The SFEC envelope declares an unsupported encryption algorithm.
    #[error("unsupported SFEC algorithm: {0}")]
    UnsupportedAlgorithm(u16),
    /// The SFEC envelope references a key id this runtime cannot use.
    #[error("unknown SFEC key id: {0}")]
    UnknownKeyId(u32),
    /// The key derivation operation failed.
    #[error("key derivation failed")]
    KeyDerivation,
    /// Authentication failed while unwrapping or decrypting object data.
    #[error("object authentication failed")]
    AuthenticationFailed,
    /// The decrypted data encryption key has an invalid length.
    #[error("invalid data encryption key length: {0}")]
    InvalidDataKeyLength(usize),
    /// Nonce material in the envelope has an invalid length.
    #[error("invalid nonce length for {field}: {actual}")]
    InvalidNonceLength {
        /// Nonce field name.
        field: &'static str,
        /// Observed nonce length.
        actual: usize,
    },
    /// The frame layout cannot be represented safely.
    #[error("encrypted frame layout overflow")]
    FrameLayoutOverflow,
    /// The declared encrypted object length cannot be represented safely.
    #[error("encrypted object length overflow")]
    ObjectLengthOverflow,
    /// The authenticated context cannot be encoded safely.
    #[error("authenticated context length overflow")]
    AadLengthOverflow,
}

impl From<SfecError> for CryptoError {
    fn from(value: SfecError) -> Self {
        match value {
            SfecError::UnsupportedVersion(version) => Self::UnsupportedVersion(version),
            SfecError::UnsupportedAlgorithm(algorithm) => Self::UnsupportedAlgorithm(algorithm),
            SfecError::InvalidKeyId(key_id) => Self::UnknownKeyId(key_id),
            SfecError::FrameCountOverflow | SfecError::ObjectLengthOverflow => {
                Self::ObjectLengthOverflow
            }
            SfecError::InvalidMagic
            | SfecError::InvalidHeaderLength(_)
            | SfecError::UnsupportedFlags(_)
            | SfecError::FieldTooLarge(_)
            | SfecError::InvalidFrameSize(_)
            | SfecError::TruncatedHeader
            | SfecError::TruncatedCiphertext
            | SfecError::TrailingMalformedData => Self::MalformedEnvelope(value),
        }
    }
}

/// Encrypt one complete STFS object into an SFEC envelope.
pub fn encrypt_object(
    plaintext: &[u8],
    master_key: &VolumeMasterKey,
    context: ObjectEncryptionContext<'_>,
) -> Result<Buffer, CryptoError> {
    let dek = random_array::<KEY_LEN>();
    let wrap_nonce = random_array::<XNONCE_LEN>();
    let data_nonce_prefix = random_array::<DATA_NONCE_PREFIX_LEN>();
    let wrapping_key = derive_wrapping_key(master_key, context)?;
    let plaintext_len =
        u64::try_from(plaintext.len()).map_err(|_| CryptoError::FrameLayoutOverflow)?;
    let wrap_aad = wrap_aad(context.key_id, context, plaintext_len, &data_nonce_prefix)?;
    let wrapped_dek = encrypt_with_key(&wrapping_key, &wrap_nonce, &wrap_aad, dek.as_ref())?;

    let header = SfecHeader::new(
        SfecAlgorithm::XChaCha20Poly1305V1,
        context.key_id,
        plaintext_len,
        DEFAULT_ENCRYPTION_FRAME_SIZE,
        Bytes::copy_from_slice(wrap_nonce.as_ref()),
        Bytes::copy_from_slice(data_nonce_prefix.as_ref()),
        Bytes::from(wrapped_dek),
    )?;
    let header_bytes = header.serialized_header()?;
    let ciphertext = encrypt_frames(
        plaintext,
        &dek,
        &data_nonce_prefix,
        &header_bytes,
        context.key_id,
        context,
    )?;

    SfecEnvelope::serialize_buffer(&header, Bytes::from(ciphertext)).map_err(CryptoError::from)
}

/// Decrypt one complete SFEC envelope back into its STFS object bytes.
pub fn decrypt_object(
    envelope: Bytes,
    master_key: &VolumeMasterKey,
    context: ObjectEncryptionContext<'_>,
) -> Result<Bytes, CryptoError> {
    let envelope = SfecEnvelope::parse(envelope)?;
    let header = envelope.header();
    let wrap_nonce = fixed_slice::<XNONCE_LEN>("wrap_nonce", header.wrap_nonce())?;
    let data_nonce_prefix =
        fixed_slice::<DATA_NONCE_PREFIX_LEN>("data_nonce_prefix", header.data_nonce_prefix())?;
    let wrapping_key = derive_wrapping_key(master_key, context)?;
    let wrap_aad = wrap_aad(
        header.key_id(),
        context,
        header.plaintext_len(),
        &data_nonce_prefix,
    )?;
    let dek = decrypt_with_key(&wrapping_key, &wrap_nonce, &wrap_aad, header.wrapped_dek())?;
    let dek_bytes: &[u8; KEY_LEN] = dek
        .as_slice()
        .try_into()
        .map_err(|_| CryptoError::InvalidDataKeyLength(dek.len()))?;

    decrypt_frames(
        envelope.ciphertext(),
        dek_bytes,
        &data_nonce_prefix,
        envelope.header_bytes(),
        header.key_id(),
        context,
        header,
    )
}

fn random_array<const N: usize>() -> Zeroizing<[u8; N]> {
    let mut bytes = Zeroizing::new([0u8; N]);
    OsRng.fill_bytes(bytes.as_mut());
    bytes
}

fn derive_wrapping_key(
    master_key: &VolumeMasterKey,
    context: ObjectEncryptionContext<'_>,
) -> Result<Zeroizing<[u8; KEY_LEN]>, CryptoError> {
    let hkdf = Hkdf::<Sha256>::new(Some(context.volume_uuid.as_bytes()), master_key.as_bytes());
    let mut output = Zeroizing::new([0u8; KEY_LEN]);
    hkdf.expand(
        b"studiofs sfec v1 xchacha20poly1305 object wrapping key",
        output.as_mut(),
    )
    .map_err(|_| CryptoError::KeyDerivation)?;
    Ok(output)
}

fn encrypt_frames(
    plaintext: &[u8],
    dek: &[u8; KEY_LEN],
    data_nonce_prefix: &[u8; DATA_NONCE_PREFIX_LEN],
    header_bytes: &[u8],
    key_id: u32,
    context: ObjectEncryptionContext<'_>,
) -> Result<Vec<u8>, CryptoError> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(dek));
    let frame_count = plaintext.len().div_ceil(DEFAULT_ENCRYPTION_FRAME_SIZE);
    let output_len = plaintext
        .len()
        .checked_add(
            frame_count
                .checked_mul(SfecAlgorithm::XChaCha20Poly1305V1.tag_len())
                .ok_or(CryptoError::FrameLayoutOverflow)?,
        )
        .ok_or(CryptoError::FrameLayoutOverflow)?;
    let mut output = Vec::with_capacity(output_len);
    let mut aad = frame_aad_base(key_id, context, header_bytes)?;
    let aad_base_len = aad.len();

    for (frame_index, frame) in plaintext.chunks(DEFAULT_ENCRYPTION_FRAME_SIZE).enumerate() {
        let frame_index =
            u32::try_from(frame_index).map_err(|_| CryptoError::FrameLayoutOverflow)?;
        let nonce = data_nonce(data_nonce_prefix, frame_index);
        set_frame_aad(&mut aad, aad_base_len, frame_index);

        let start = output.len();
        output.extend_from_slice(frame);
        let tag = cipher
            .encrypt_in_place_detached(XNonce::from_slice(&nonce), &aad, &mut output[start..])
            .map_err(|_| CryptoError::AuthenticationFailed)?;
        output.extend_from_slice(&tag);
    }

    Ok(output)
}

fn decrypt_frames(
    ciphertext: &[u8],
    dek: &[u8; KEY_LEN],
    data_nonce_prefix: &[u8; DATA_NONCE_PREFIX_LEN],
    header_bytes: &[u8],
    key_id: u32,
    context: ObjectEncryptionContext<'_>,
    header: &SfecHeader,
) -> Result<Bytes, CryptoError> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(dek));
    let plaintext_len =
        usize::try_from(header.plaintext_len()).map_err(|_| CryptoError::FrameLayoutOverflow)?;
    let mut plaintext = Vec::with_capacity(plaintext_len);
    let mut offset = 0usize;
    let mut aad = frame_aad_base(key_id, context, header_bytes)?;
    let aad_base_len = aad.len();

    for frame_index in 0..header.frame_count() {
        let frame_len = header
            .encrypted_frame_len(frame_index)
            .ok_or(CryptoError::FrameLayoutOverflow)?;
        let end = offset
            .checked_add(frame_len)
            .ok_or(CryptoError::FrameLayoutOverflow)?;
        let frame = ciphertext
            .get(offset..end)
            .ok_or(CryptoError::FrameLayoutOverflow)?;
        let tag_offset = frame
            .len()
            .checked_sub(SfecAlgorithm::XChaCha20Poly1305V1.tag_len())
            .ok_or(CryptoError::FrameLayoutOverflow)?;
        let (frame_ciphertext, tag) = frame.split_at(tag_offset);
        let nonce = data_nonce(data_nonce_prefix, frame_index);
        set_frame_aad(&mut aad, aad_base_len, frame_index);

        let start = plaintext.len();
        plaintext.extend_from_slice(frame_ciphertext);
        cipher
            .decrypt_in_place_detached(
                XNonce::from_slice(&nonce),
                &aad,
                &mut plaintext[start..],
                Tag::from_slice(tag),
            )
            .map_err(|_| CryptoError::AuthenticationFailed)?;
        offset = end;
    }

    if offset != ciphertext.len() {
        return Err(CryptoError::FrameLayoutOverflow);
    }

    Ok(Bytes::from(plaintext))
}

fn encrypt_with_key(
    key: &[u8; KEY_LEN],
    nonce: &[u8; XNONCE_LEN],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(key));
    cipher
        .encrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| CryptoError::AuthenticationFailed)
}

fn decrypt_with_key(
    key: &[u8; KEY_LEN],
    nonce: &[u8; XNONCE_LEN],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(key));
    cipher
        .decrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| CryptoError::AuthenticationFailed)
}

fn wrap_aad(
    key_id: u32,
    context: ObjectEncryptionContext<'_>,
    plaintext_len: u64,
    data_nonce_prefix: &[u8; DATA_NONCE_PREFIX_LEN],
) -> Result<Vec<u8>, CryptoError> {
    let mut aad = Vec::with_capacity(256);
    put_common_aad(&mut aad, key_id, context)?;
    aad.put_u64_le(plaintext_len);
    aad.put_u32_le(DEFAULT_ENCRYPTION_FRAME_SIZE as u32);
    aad.put_slice(data_nonce_prefix);
    Ok(aad)
}

fn frame_aad_base(
    key_id: u32,
    context: ObjectEncryptionContext<'_>,
    header_bytes: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let mut aad = Vec::with_capacity(header_bytes.len() + 256);
    put_common_aad(&mut aad, key_id, context)?;
    aad.put_slice(header_bytes);
    Ok(aad)
}

fn set_frame_aad(aad: &mut Vec<u8>, base_len: usize, frame_index: u32) {
    aad.truncate(base_len);
    aad.put_u32_le(frame_index);
}

fn put_common_aad(
    aad: &mut Vec<u8>,
    key_id: u32,
    context: ObjectEncryptionContext<'_>,
) -> Result<(), CryptoError> {
    aad.put_slice(b"studiofs-sfec-aad-v1");
    aad.put_u16_le(SFEC_VERSION);
    aad.put_slice(b"xchacha20poly1305-v1");
    aad.put_u16_le(SfecAlgorithm::XChaCha20Poly1305V1.tag_len() as u16);
    aad.put_u32_le(key_id);
    aad.put_u32_le(
        u32::try_from(context.volume_uuid.len()).map_err(|_| CryptoError::AadLengthOverflow)?,
    );
    aad.put_slice(context.volume_uuid.as_bytes());
    aad.put_u32_le(
        u32::try_from(context.storage_key.len()).map_err(|_| CryptoError::AadLengthOverflow)?,
    );
    aad.put_slice(context.storage_key.as_bytes());
    Ok(())
}

fn data_nonce(prefix: &[u8; DATA_NONCE_PREFIX_LEN], frame_index: u32) -> [u8; XNONCE_LEN] {
    let mut nonce = [0u8; XNONCE_LEN];
    nonce[..DATA_NONCE_PREFIX_LEN].copy_from_slice(prefix);
    nonce[DATA_NONCE_PREFIX_LEN..].copy_from_slice(&u64::from(frame_index).to_le_bytes());
    nonce
}

fn fixed_slice<const N: usize>(field: &'static str, bytes: &[u8]) -> Result<[u8; N], CryptoError> {
    bytes
        .try_into()
        .map_err(|_| CryptoError::InvalidNonceLength {
            field,
            actual: bytes.len(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{SFEC_MAGIC, SfecEnvelope};
    use bytes::Bytes;

    const MASTER_KEY: VolumeMasterKey = VolumeMasterKey::from_bytes([0x42; 32]);

    fn context(storage_key: &'static str) -> ObjectEncryptionContext<'static> {
        ObjectEncryptionContext {
            volume_uuid: "1a4c0fe2-8550-4892-952c-5ec26c0e0d8d",
            storage_key,
            key_id: 7,
        }
    }

    fn encrypt_object_bytes(
        plaintext: &[u8],
        master_key: &VolumeMasterKey,
        context: ObjectEncryptionContext<'_>,
    ) -> Bytes {
        encrypt_object(plaintext, master_key, context)
            .unwrap()
            .to_bytes()
    }

    fn roundtrip_payload(size: usize) {
        let plaintext = Bytes::from_iter((0..size).map(|idx| (idx % 251) as u8));
        let ctx = context("vol/objects/0000/0000000000000001/0");

        let ciphertext = encrypt_object_bytes(&plaintext, &MASTER_KEY, ctx);
        assert!(ciphertext.starts_with(SFEC_MAGIC));
        assert_ne!(ciphertext.as_ref(), plaintext.as_ref());

        let decrypted = decrypt_object(ciphertext, &MASTER_KEY, ctx).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn encrypt_decrypt_roundtrips_common_payload_sizes() {
        for size in [
            0,
            1,
            DEFAULT_ENCRYPTION_FRAME_SIZE - 1,
            DEFAULT_ENCRYPTION_FRAME_SIZE,
            DEFAULT_ENCRYPTION_FRAME_SIZE + 1,
            4 * 1024 * 1024,
        ] {
            roundtrip_payload(size);
        }
    }

    #[test]
    fn encrypting_same_plaintext_twice_produces_different_ciphertext() {
        let plaintext = Bytes::from_static(b"same plaintext");
        let ctx = context("vol/objects/0000/0000000000000002/0");

        let first = encrypt_object_bytes(&plaintext, &MASTER_KEY, ctx);
        let second = encrypt_object_bytes(&plaintext, &MASTER_KEY, ctx);

        assert_ne!(first, second);
    }

    #[test]
    fn decrypt_rejects_changed_storage_key_or_volume_uuid() {
        let ctx = context("vol/objects/0000/0000000000000003/0");
        let ciphertext = encrypt_object_bytes(b"bound aad", &MASTER_KEY, ctx);

        let wrong_key = context("vol/objects/0000/0000000000000004/0");
        assert!(matches!(
            decrypt_object(ciphertext.clone(), &MASTER_KEY, wrong_key),
            Err(CryptoError::AuthenticationFailed)
        ));

        let wrong_volume = ObjectEncryptionContext {
            volume_uuid: "8160fc5f-6a18-43c9-a34c-1c4f6e82dce1",
            ..ctx
        };
        assert!(matches!(
            decrypt_object(ciphertext, &MASTER_KEY, wrong_volume),
            Err(CryptoError::AuthenticationFailed)
        ));
    }

    #[test]
    fn decrypt_accepts_historical_key_id_from_envelope() {
        let old_key_context = context("vol/objects/0000/0000000000000004/0");
        let current_key_context = ObjectEncryptionContext {
            key_id: 8,
            ..old_key_context
        };
        let ciphertext = encrypt_object_bytes(b"historical key id", &MASTER_KEY, old_key_context);

        let decrypted = decrypt_object(ciphertext, &MASTER_KEY, current_key_context).unwrap();

        assert_eq!(decrypted.as_ref(), b"historical key id");
    }

    #[test]
    fn decrypt_rejects_wrong_master_key() {
        let ctx = context("vol/objects/0000/0000000000000005/0");
        let ciphertext = encrypt_object_bytes(b"secret", &MASTER_KEY, ctx);
        let wrong_master = VolumeMasterKey::from_bytes([0x24; 32]);

        assert!(matches!(
            decrypt_object(ciphertext, &wrong_master, ctx),
            Err(CryptoError::AuthenticationFailed)
        ));
    }

    #[test]
    fn decrypt_rejects_mutated_envelope_parts() {
        let ctx = context("vol/objects/0000/0000000000000006/0");
        let ciphertext = encrypt_object_bytes(
            &vec![0xAB; DEFAULT_ENCRYPTION_FRAME_SIZE + 17],
            &MASTER_KEY,
            ctx,
        );
        let envelope = SfecEnvelope::parse(ciphertext.clone()).unwrap();
        let header_len = envelope.header_bytes().len();

        for offset in [4, 40, 64, header_len, ciphertext.len() - 1] {
            let mut mutated = ciphertext.to_vec();
            mutated[offset] ^= 0x55;
            assert!(
                decrypt_object(Bytes::from(mutated), &MASTER_KEY, ctx).is_err(),
                "mutation at offset {offset} should be rejected"
            );
        }
    }

    #[test]
    fn decrypt_rejects_header_truncation_to_empty_object() {
        let ctx = context("vol/objects/0000/0000000000000007/0");
        let ciphertext = encrypt_object_bytes(b"must not become empty", &MASTER_KEY, ctx);
        let envelope = SfecEnvelope::parse(ciphertext).unwrap();
        let mut truncated = envelope.header_bytes().to_vec();

        let plaintext_len_offset = 18;
        truncated[plaintext_len_offset..plaintext_len_offset + 8]
            .copy_from_slice(&0u64.to_le_bytes());
        let frame_count_offset = 30;
        truncated[frame_count_offset..frame_count_offset + 4].copy_from_slice(&0u32.to_le_bytes());

        assert!(matches!(
            decrypt_object(Bytes::from(truncated), &MASTER_KEY, ctx),
            Err(CryptoError::AuthenticationFailed)
        ));
    }

    #[test]
    fn sfec_parse_failures_map_to_specific_safe_crypto_errors() {
        assert!(matches!(
            CryptoError::from(SfecError::UnsupportedVersion(99)),
            CryptoError::UnsupportedVersion(99)
        ));
        assert!(matches!(
            CryptoError::from(SfecError::UnsupportedAlgorithm(24)),
            CryptoError::UnsupportedAlgorithm(24)
        ));
        assert!(matches!(
            CryptoError::from(SfecError::InvalidKeyId(0)),
            CryptoError::UnknownKeyId(0)
        ));
        assert!(matches!(
            CryptoError::from(SfecError::ObjectLengthOverflow),
            CryptoError::ObjectLengthOverflow
        ));
        assert!(matches!(
            CryptoError::from(SfecError::TruncatedHeader),
            CryptoError::MalformedEnvelope(SfecError::TruncatedHeader)
        ));
    }

    #[test]
    fn crypto_error_display_does_not_include_sensitive_material() {
        let sensitive = [
            "plaintext-secret",
            "wrapped-dek-secret",
            "4242424242424242",
            "vol/objects/aa/bb/secret-object",
        ];
        let errors = [
            CryptoError::AuthenticationFailed,
            CryptoError::InvalidDataKeyLength(17),
            CryptoError::InvalidNonceLength {
                field: "wrap_nonce",
                actual: 7,
            },
            CryptoError::UnsupportedAlgorithm(99),
            CryptoError::UnknownKeyId(42),
        ];

        for err in errors {
            let rendered = err.to_string();
            for needle in sensitive {
                assert!(
                    !rendered.contains(needle),
                    "{rendered:?} leaked sensitive marker {needle:?}"
                );
            }
        }
    }
}
