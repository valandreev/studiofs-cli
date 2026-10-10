//! XChaCha20-Poly1305 encryption for SFEC remote objects.

use crate::envelope::{
    DEFAULT_SFEC_FRAME_SIZE, SFEC_VERSION, SfecAlgorithm, SfecEnvelope, SfecError, SfecHeader,
};
use bytes::{Buf, BufMut, Bytes};
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

/// Per-volume object cipher holding the HKDF-derived wrapping key.
///
/// The wrapping key depends only on the master key and volume UUID, so it is
/// derived once here and zeroized on drop. Build a new cipher if either changes.
pub struct ObjectCipher {
    volume_uuid: Box<str>,
    wrapping_key: Zeroizing<[u8; KEY_LEN]>,
}

/// Context authenticated into every encrypted object.
#[derive(Debug, Clone, Copy)]
struct ObjectEncryptionContext<'a> {
    volume_uuid: &'a str,
    storage_key: &'a str,
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

impl ObjectCipher {
    /// Derive the wrapping key for one volume.
    pub fn new(master_key: &VolumeMasterKey, volume_uuid: &str) -> Result<Self, CryptoError> {
        Ok(Self {
            volume_uuid: volume_uuid.into(),
            wrapping_key: derive_wrapping_key(master_key, volume_uuid)?,
        })
    }

    /// Encrypt one complete STFS object into an SFEC envelope.
    ///
    /// `plaintext` may be chunked (e.g. an OpenDAL `Buffer`); frames are
    /// copied straight from its chunks into the ciphertext allocation.
    pub fn encrypt_object(
        &self,
        plaintext: impl Buf,
        storage_key: &str,
        key_id: u32,
    ) -> Result<Buffer, CryptoError> {
        self.encrypt_object_with_workers(plaintext, storage_key, key_id, 1)
    }

    /// Like [`Self::encrypt_object`], sealing independent frames on up to
    /// `workers` threads. The output format is identical for any count.
    pub fn encrypt_object_with_workers(
        &self,
        plaintext: impl Buf,
        storage_key: &str,
        key_id: u32,
        workers: usize,
    ) -> Result<Buffer, CryptoError> {
        let context = ObjectEncryptionContext {
            volume_uuid: &self.volume_uuid,
            storage_key,
        };
        // One OS RNG call yields the DEK, wrap nonce and data nonce prefix.
        let mut random = Zeroizing::new([0u8; KEY_LEN + XNONCE_LEN + DATA_NONCE_PREFIX_LEN]);
        OsRng.fill_bytes(random.as_mut());
        // Infallible: the buffer length is exactly the sum of the three chunks.
        let (dek, rest) = random.split_first_chunk::<KEY_LEN>().expect("fixed buffer");
        let (wrap_nonce, rest) = rest
            .split_first_chunk::<XNONCE_LEN>()
            .expect("fixed buffer");
        let data_nonce_prefix = rest
            .first_chunk::<DATA_NONCE_PREFIX_LEN>()
            .expect("fixed buffer");
        let plaintext_len =
            u64::try_from(plaintext.remaining()).map_err(|_| CryptoError::FrameLayoutOverflow)?;
        let wrap_aad = wrap_aad(key_id, context, plaintext_len, data_nonce_prefix)?;
        let wrapped_dek = encrypt_with_key(&self.wrapping_key, wrap_nonce, &wrap_aad, dek)?;

        let header = SfecHeader::new(
            SfecAlgorithm::XChaCha20Poly1305V1,
            key_id,
            plaintext_len,
            DEFAULT_ENCRYPTION_FRAME_SIZE,
            Bytes::copy_from_slice(wrap_nonce),
            Bytes::copy_from_slice(data_nonce_prefix),
            Bytes::from(wrapped_dek),
        )?;
        let header_bytes = header.serialized_header()?;
        let ciphertext = encrypt_frames(
            plaintext,
            dek,
            data_nonce_prefix,
            &header_bytes,
            key_id,
            context,
            workers,
        )?;

        SfecEnvelope::serialize_buffer(&header, Bytes::from(ciphertext)).map_err(CryptoError::from)
    }

    /// Decrypt one complete SFEC envelope back into its STFS object bytes.
    ///
    /// A chunked envelope is decrypted frame by frame across chunk boundaries
    /// without first concatenating the whole object.
    pub fn decrypt_object(
        &self,
        envelope: impl Into<Buffer>,
        storage_key: &str,
    ) -> Result<Bytes, CryptoError> {
        self.decrypt_object_with_workers(envelope, storage_key, 1)
    }

    /// Like [`Self::decrypt_object`], opening independent frames on up to
    /// `workers` threads.
    pub fn decrypt_object_with_workers(
        &self,
        envelope: impl Into<Buffer>,
        storage_key: &str,
        workers: usize,
    ) -> Result<Bytes, CryptoError> {
        let context = ObjectEncryptionContext {
            volume_uuid: &self.volume_uuid,
            storage_key,
        };
        let envelope = SfecEnvelope::parse(envelope)?;
        let header = envelope.header();
        let wrap_nonce = fixed_slice::<XNONCE_LEN>("wrap_nonce", header.wrap_nonce())?;
        let data_nonce_prefix =
            fixed_slice::<DATA_NONCE_PREFIX_LEN>("data_nonce_prefix", header.data_nonce_prefix())?;
        let wrap_aad = wrap_aad(
            header.key_id(),
            context,
            header.plaintext_len(),
            &data_nonce_prefix,
        )?;
        let dek = decrypt_with_key(
            &self.wrapping_key,
            &wrap_nonce,
            &wrap_aad,
            header.wrapped_dek(),
        )?;
        let dek_bytes: &[u8; KEY_LEN] = dek
            .as_slice()
            .try_into()
            .map_err(|_| CryptoError::InvalidDataKeyLength(dek.len()))?;

        decrypt_frames(
            envelope.ciphertext().clone(),
            dek_bytes,
            &data_nonce_prefix,
            envelope.header_bytes(),
            context,
            header,
            workers,
        )
    }
}

fn derive_wrapping_key(
    master_key: &VolumeMasterKey,
    volume_uuid: &str,
) -> Result<Zeroizing<[u8; KEY_LEN]>, CryptoError> {
    let hkdf = Hkdf::<Sha256>::new(Some(volume_uuid.as_bytes()), master_key.as_bytes());
    let mut output = Zeroizing::new([0u8; KEY_LEN]);
    hkdf.expand(
        b"studiofs sfec v1 xchacha20poly1305 object wrapping key",
        output.as_mut(),
    )
    .map_err(|_| CryptoError::KeyDerivation)?;
    Ok(output)
}

fn encrypt_frames(
    mut plaintext: impl Buf,
    dek: &[u8; KEY_LEN],
    data_nonce_prefix: &[u8; DATA_NONCE_PREFIX_LEN],
    header_bytes: &[u8],
    key_id: u32,
    context: ObjectEncryptionContext<'_>,
    workers: usize,
) -> Result<Vec<u8>, CryptoError> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(dek));
    let tag_len = SfecAlgorithm::XChaCha20Poly1305V1.tag_len();
    let plaintext_len = plaintext.remaining();
    let frame_count = plaintext_len.div_ceil(DEFAULT_ENCRYPTION_FRAME_SIZE);
    let output_len = plaintext_len
        .checked_add(
            frame_count
                .checked_mul(tag_len)
                .ok_or(CryptoError::FrameLayoutOverflow)?,
        )
        .ok_or(CryptoError::FrameLayoutOverflow)?;
    let mut output = Vec::with_capacity(output_len);
    // Lay out `body || tag slot` per frame, then seal the frames in place.
    while plaintext.has_remaining() {
        let frame_len = plaintext.remaining().min(DEFAULT_ENCRYPTION_FRAME_SIZE);
        output.put((&mut plaintext).take(frame_len));
        output.put_bytes(0, tag_len);
    }
    let aad_base = frame_aad_base(key_id, context, header_bytes)?;

    for_each_frame(
        &mut output,
        DEFAULT_ENCRYPTION_FRAME_SIZE + tag_len,
        workers,
        &aad_base,
        |frame_index, aad, frame| {
            let nonce = data_nonce(data_nonce_prefix, frame_index);
            let (body, tag_slot) = frame.split_at_mut(frame.len() - tag_len);
            let tag = cipher
                .encrypt_in_place_detached(XNonce::from_slice(&nonce), aad, body)
                .map_err(|_| CryptoError::AuthenticationFailed)?;
            tag_slot.copy_from_slice(&tag);
            Ok(())
        },
    )?;

    Ok(output)
}

fn decrypt_frames(
    mut ciphertext: Buffer,
    dek: &[u8; KEY_LEN],
    data_nonce_prefix: &[u8; DATA_NONCE_PREFIX_LEN],
    header_bytes: &[u8],
    context: ObjectEncryptionContext<'_>,
    header: &SfecHeader,
    workers: usize,
) -> Result<Bytes, CryptoError> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(dek));
    let plaintext_len =
        usize::try_from(header.plaintext_len()).map_err(|_| CryptoError::FrameLayoutOverflow)?;
    let mut plaintext = Vec::with_capacity(plaintext_len);
    let mut tags = Vec::with_capacity(header.frame_count() as usize);

    // Validate the layout and gather bodies and tags sequentially; both may
    // straddle Buffer chunks.
    for frame_index in 0..header.frame_count() {
        let frame_len = header
            .encrypted_frame_len(frame_index)
            .ok_or(CryptoError::FrameLayoutOverflow)?;
        if frame_len > ciphertext.remaining() {
            return Err(CryptoError::FrameLayoutOverflow);
        }
        let body_len = frame_len
            .checked_sub(SfecAlgorithm::XChaCha20Poly1305V1.tag_len())
            .ok_or(CryptoError::FrameLayoutOverflow)?;
        plaintext.put(Buf::take(&mut ciphertext, body_len));
        let mut tag = Tag::default();
        ciphertext.copy_to_slice(&mut tag);
        tags.push(tag);
    }

    if ciphertext.has_remaining() {
        return Err(CryptoError::FrameLayoutOverflow);
    }

    let aad_base = frame_aad_base(header.key_id(), context, header_bytes)?;
    let tags = tags.as_slice();
    for_each_frame(
        &mut plaintext,
        DEFAULT_ENCRYPTION_FRAME_SIZE,
        workers,
        &aad_base,
        |frame_index, aad, frame| {
            let nonce = data_nonce(data_nonce_prefix, frame_index);
            let tag = tags
                .get(frame_index as usize)
                .ok_or(CryptoError::FrameLayoutOverflow)?;
            cipher
                .decrypt_in_place_detached(XNonce::from_slice(&nonce), aad, frame, tag)
                .map_err(|_| CryptoError::AuthenticationFailed)
        },
    )?;

    Ok(Bytes::from(plaintext))
}

/// Run `seal` over each `stride`-sized frame of `buffer` on up to `workers`
/// scoped threads, each owning a contiguous run of frames and its own AAD.
///
/// The caller must already hold admission for `workers` CPU slots.
fn for_each_frame<F>(
    buffer: &mut [u8],
    stride: usize,
    workers: usize,
    aad_base: &[u8],
    seal: F,
) -> Result<(), CryptoError>
where
    F: Fn(u32, &[u8], &mut [u8]) -> Result<(), CryptoError> + Sync,
{
    let run = |first_frame: usize, frames: &mut [u8]| {
        let mut aad = aad_base.to_vec();
        for (offset, frame) in frames.chunks_mut(stride).enumerate() {
            let frame_index = u32::try_from(first_frame + offset)
                .map_err(|_| CryptoError::FrameLayoutOverflow)?;
            set_frame_aad(&mut aad, aad_base.len(), frame_index);
            seal(frame_index, &aad, frame)?;
        }
        Ok(())
    };

    let frame_count = buffer.len().div_ceil(stride);
    let workers = workers.clamp(1, frame_count.max(1));
    if workers == 1 {
        return run(0, buffer);
    }
    let frames_per_worker = frame_count.div_ceil(workers);
    let (first, rest) = buffer.split_at_mut(stride * frames_per_worker);
    std::thread::scope(|scope| {
        let handles: Vec<_> = rest
            .chunks_mut(stride * frames_per_worker)
            .enumerate()
            .map(|(group, frames)| {
                scope.spawn(move || run((group + 1) * frames_per_worker, frames))
            })
            .collect();
        // The calling thread seals the first group itself.
        let mut result = run(0, first);
        for handle in handles {
            let joined = handle
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            result = result.and(joined);
        }
        result
    })
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
    const UUID: &str = "1a4c0fe2-8550-4892-952c-5ec26c0e0d8d";
    const OBJECT_KEY: &str = "vol/objects/0000/0000000000000001/0";
    const KEY_ID: u32 = 7;

    fn cipher() -> ObjectCipher {
        ObjectCipher::new(&MASTER_KEY, UUID).unwrap()
    }

    fn encrypt(plaintext: &[u8], storage_key: &str) -> Bytes {
        cipher()
            .encrypt_object(plaintext, storage_key, KEY_ID)
            .unwrap()
            .to_bytes()
    }

    #[test]
    fn decrypts_envelope_produced_before_wrapping_key_caching() {
        // Generated by the per-object HKDF implementation (pre SFS-786).
        const HISTORICAL: &str = "534645430100800000000100000007000000170000000000000000001000010000001800100030000858a4106e6436ea7fde8ec4ef8eb14a76bb3f5bd407e68d38fbb26c32400a31898510efb228dc4fd52b108b812935a8e34d4f974c2b96c6d7671d1f3725261090c0ad8853bbcf7ef900897366fb79aa5e23ee2610c603a6e6a6490191e4357efd25836cd3c810b403c24b69d4cdd4f8fd2029ff4c48e7001b8f96ce887bb6";
        let envelope: Vec<u8> = (0..HISTORICAL.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&HISTORICAL[i..i + 2], 16).unwrap())
            .collect();
        let envelope = Bytes::from(envelope);
        for input in [Buffer::from(envelope.clone()), fragment(&envelope)] {
            let plaintext = cipher().decrypt_object(input, OBJECT_KEY).unwrap();
            assert_eq!(plaintext.as_ref(), b"historical sfec payload");
        }
    }

    #[test]
    fn worker_count_does_not_change_frame_format_or_compatibility() {
        for size in [
            0,
            1,
            3 * DEFAULT_ENCRYPTION_FRAME_SIZE + 17,
            4 * 1024 * 1024,
        ] {
            let plaintext = Bytes::from_iter((0..size).map(|idx| (idx % 251) as u8));
            let sequential = encrypt(&plaintext, OBJECT_KEY);
            for workers in [2, 3, 4, 8] {
                let parallel = cipher()
                    .encrypt_object_with_workers(fragment(&plaintext), OBJECT_KEY, KEY_ID, workers)
                    .unwrap()
                    .to_bytes();
                assert_eq!(parallel.len(), sequential.len(), "size {size}");
                // Parallel output decrypts sequentially and vice versa.
                assert_eq!(
                    cipher().decrypt_object(parallel, OBJECT_KEY).unwrap(),
                    plaintext
                );
                assert_eq!(
                    cipher()
                        .decrypt_object_with_workers(fragment(&sequential), OBJECT_KEY, workers)
                        .unwrap(),
                    plaintext,
                    "size {size} workers {workers}"
                );
            }
        }
    }

    #[test]
    fn parallel_decrypt_rejects_tampering_in_any_frame() {
        let ciphertext = encrypt(&vec![0xC3; 4 * DEFAULT_ENCRYPTION_FRAME_SIZE], OBJECT_KEY);
        let header_len = SfecEnvelope::parse(ciphertext.clone())
            .unwrap()
            .header_bytes()
            .len();
        let stride = DEFAULT_ENCRYPTION_FRAME_SIZE + SfecAlgorithm::XChaCha20Poly1305V1.tag_len();
        for frame in 0..4 {
            let mut mutated = ciphertext.to_vec();
            mutated[header_len + frame * stride + 5] ^= 0x01;
            assert!(matches!(
                cipher().decrypt_object_with_workers(Bytes::from(mutated), OBJECT_KEY, 4),
                Err(CryptoError::AuthenticationFailed)
            ));
        }
    }

    #[test]
    fn one_cipher_gives_each_object_fresh_random_material() {
        let cipher = cipher();
        let parse = |bytes: Bytes| SfecEnvelope::parse(bytes).unwrap();
        let first = parse(
            cipher
                .encrypt_object(&b"x"[..], "k", KEY_ID)
                .unwrap()
                .to_bytes(),
        );
        let second = parse(
            cipher
                .encrypt_object(&b"x"[..], "k", KEY_ID)
                .unwrap()
                .to_bytes(),
        );

        assert_ne!(first.header().wrap_nonce(), second.header().wrap_nonce());
        assert_ne!(
            first.header().data_nonce_prefix(),
            second.header().data_nonce_prefix()
        );
        assert_ne!(first.header().wrapped_dek(), second.header().wrapped_dek());
    }

    fn roundtrip_payload(size: usize) {
        let plaintext = Bytes::from_iter((0..size).map(|idx| (idx % 251) as u8));
        let ciphertext = encrypt(&plaintext, OBJECT_KEY);
        assert!(ciphertext.starts_with(SFEC_MAGIC));
        assert_ne!(ciphertext.as_ref(), plaintext.as_ref());

        let decrypted = cipher().decrypt_object(ciphertext, OBJECT_KEY).unwrap();
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

    /// Split into odd-sized chunks so header fields, frame bodies and tags
    /// all straddle chunk boundaries.
    fn fragment(bytes: &Bytes) -> Buffer {
        let mut chunks = Vec::new();
        let mut offset = 0;
        for len in [1, 6, 33].into_iter().chain(std::iter::repeat(4093)) {
            if offset == bytes.len() {
                break;
            }
            let end = (offset + len).min(bytes.len());
            chunks.push(bytes.slice(offset..end));
            offset = end;
        }
        Buffer::from(chunks)
    }

    #[test]
    fn fragmented_buffers_roundtrip_across_chunk_boundaries() {
        for size in [
            0,
            1,
            DEFAULT_ENCRYPTION_FRAME_SIZE,
            2 * DEFAULT_ENCRYPTION_FRAME_SIZE + 17,
        ] {
            let plaintext = Bytes::from_iter((0..size).map(|idx| (idx % 251) as u8));
            let ciphertext = cipher()
                .encrypt_object(fragment(&plaintext), OBJECT_KEY, KEY_ID)
                .unwrap()
                .to_bytes();
            assert_eq!(
                cipher()
                    .decrypt_object(ciphertext.clone(), OBJECT_KEY)
                    .unwrap(),
                plaintext
            );
            let fragmented = fragment(&ciphertext);
            assert!(Buffer::count(&fragmented) > 3);
            assert_eq!(
                cipher().decrypt_object(fragmented, OBJECT_KEY).unwrap(),
                plaintext,
                "size {size}"
            );
        }
    }

    #[test]
    fn fragmented_ciphertext_rejects_truncation_and_trailing_bytes() {
        let ciphertext = encrypt(&[0x5A; 3 * 1024], OBJECT_KEY);
        let truncated = ciphertext.slice(..ciphertext.len() - 1);
        let mut trailing = ciphertext.to_vec();
        trailing.push(0);

        for envelope in [truncated, Bytes::from(trailing)] {
            assert!(
                cipher()
                    .decrypt_object(fragment(&envelope), OBJECT_KEY)
                    .is_err()
            );
        }
    }

    #[test]
    fn encrypting_same_plaintext_twice_produces_different_ciphertext() {
        let plaintext = Bytes::from_static(b"same plaintext");
        let first = encrypt(&plaintext, OBJECT_KEY);
        let second = encrypt(&plaintext, OBJECT_KEY);

        assert_ne!(first, second);
    }

    #[test]
    fn decrypt_rejects_changed_storage_key_or_volume_uuid() {
        let ciphertext = encrypt(b"bound aad", OBJECT_KEY);

        assert!(matches!(
            cipher().decrypt_object(ciphertext.clone(), "vol/objects/0000/0000000000000004/0"),
            Err(CryptoError::AuthenticationFailed)
        ));

        let wrong_volume =
            ObjectCipher::new(&MASTER_KEY, "8160fc5f-6a18-43c9-a34c-1c4f6e82dce1").unwrap();
        assert!(matches!(
            wrong_volume.decrypt_object(ciphertext, OBJECT_KEY),
            Err(CryptoError::AuthenticationFailed)
        ));
    }

    #[test]
    fn decrypt_accepts_historical_key_id_from_envelope() {
        let ciphertext = cipher()
            .encrypt_object(&b"historical key id"[..], OBJECT_KEY, KEY_ID + 1)
            .unwrap()
            .to_bytes();

        let decrypted = cipher().decrypt_object(ciphertext, OBJECT_KEY).unwrap();

        assert_eq!(decrypted.as_ref(), b"historical key id");
    }

    #[test]
    fn decrypt_rejects_wrong_master_key() {
        let ciphertext = encrypt(b"secret", OBJECT_KEY);
        let wrong_master =
            ObjectCipher::new(&VolumeMasterKey::from_bytes([0x24; 32]), UUID).unwrap();

        assert!(matches!(
            wrong_master.decrypt_object(ciphertext, OBJECT_KEY),
            Err(CryptoError::AuthenticationFailed)
        ));
    }

    #[test]
    fn decrypt_rejects_mutated_envelope_parts() {
        let ciphertext = encrypt(&vec![0xAB; DEFAULT_ENCRYPTION_FRAME_SIZE + 17], OBJECT_KEY);
        let envelope = SfecEnvelope::parse(ciphertext.clone()).unwrap();
        let header_len = envelope.header_bytes().len();

        for offset in [4, 40, 64, header_len, ciphertext.len() - 1] {
            let mut mutated = ciphertext.to_vec();
            mutated[offset] ^= 0x55;
            assert!(
                cipher()
                    .decrypt_object(Bytes::from(mutated), OBJECT_KEY)
                    .is_err(),
                "mutation at offset {offset} should be rejected"
            );
        }
    }

    #[test]
    fn decrypt_rejects_header_truncation_to_empty_object() {
        let ciphertext = encrypt(b"must not become empty", OBJECT_KEY);
        let envelope = SfecEnvelope::parse(ciphertext).unwrap();
        let mut truncated = envelope.header_bytes().to_vec();

        let plaintext_len_offset = 18;
        truncated[plaintext_len_offset..plaintext_len_offset + 8]
            .copy_from_slice(&0u64.to_le_bytes());
        let frame_count_offset = 30;
        truncated[frame_count_offset..frame_count_offset + 4].copy_from_slice(&0u32.to_le_bytes());

        assert!(matches!(
            cipher().decrypt_object(Bytes::from(truncated), OBJECT_KEY),
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
