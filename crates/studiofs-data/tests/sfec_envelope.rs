//! SFEC envelope parser and framing contract tests.

use bytes::Bytes;
use std::sync::mpsc;
use std::time::Duration;
use studiofs_data::envelope::{
    DEFAULT_SFEC_FRAME_SIZE, SFEC_FRAME_TAG_LEN, SfecAlgorithm, SfecEnvelope, SfecError, SfecHeader,
};

const KEY_ID: u32 = 1;
const WRAP_NONCE: &[u8] = b"wrap nonce bytes";
const DATA_NONCE_PREFIX: &[u8] = b"data nonce prefix bytes";
const WRAPPED_DEK: &[u8] = b"wrapped data encryption key";

fn header_for_len(plaintext_len: u64) -> SfecHeader {
    SfecHeader::new(
        SfecAlgorithm::XChaCha20Poly1305V1,
        KEY_ID,
        plaintext_len,
        DEFAULT_SFEC_FRAME_SIZE,
        Bytes::from_static(WRAP_NONCE),
        Bytes::from_static(DATA_NONCE_PREFIX),
        Bytes::from_static(WRAPPED_DEK),
    )
    .expect("valid SFEC header")
}

fn ciphertext_for(header: &SfecHeader) -> Bytes {
    let mut len = 0usize;
    for frame_index in 0..header.frame_count() {
        len += header
            .encrypted_frame_len(frame_index)
            .expect("frame length fits usize");
    }
    Bytes::from(vec![0xA5; len])
}

fn envelope_for_len(plaintext_len: u64) -> Vec<u8> {
    let header = header_for_len(plaintext_len);
    SfecEnvelope::serialize(header.clone(), ciphertext_for(&header))
        .expect("serialize SFEC envelope")
        .to_vec()
}

#[test]
fn sfec_header_roundtrip_preserves_encryption_metadata() {
    let header = header_for_len((DEFAULT_SFEC_FRAME_SIZE as u64) + 17);
    let bytes = SfecEnvelope::serialize(header.clone(), ciphertext_for(&header))
        .expect("serialize SFEC envelope");

    let parsed = SfecEnvelope::parse(bytes).expect("parse SFEC envelope");

    assert_eq!(parsed.header(), &header);
    assert_eq!(
        parsed.ciphertext().len(),
        DEFAULT_SFEC_FRAME_SIZE + SFEC_FRAME_TAG_LEN + 17 + SFEC_FRAME_TAG_LEN
    );
}

#[test]
fn sfec_parser_rejects_malformed_headers() {
    let valid = envelope_for_len(17);

    let cases: Vec<(&str, usize, u8, SfecError)> = vec![
        ("wrong magic", 0, b'X', SfecError::InvalidMagic),
        (
            "unsupported version",
            4,
            99,
            SfecError::UnsupportedVersion(99),
        ),
        (
            "unsupported algorithm",
            10,
            99,
            SfecError::UnsupportedAlgorithm(99),
        ),
        ("invalid key id", 14, 0, SfecError::InvalidKeyId(0)),
    ];

    for (name, offset, value, expected) in cases {
        let mut bytes = valid.clone();
        bytes[offset] = value;
        let err = SfecEnvelope::parse(Bytes::from(bytes)).unwrap_err();
        assert_eq!(err, expected, "{name}");
    }

    let mut invalid_frame_size = valid.clone();
    invalid_frame_size[26..30].copy_from_slice(&0u32.to_le_bytes());
    assert_eq!(
        SfecEnvelope::parse(Bytes::from(invalid_frame_size)).unwrap_err(),
        SfecError::InvalidFrameSize(0)
    );

    let mut rotated_key_id = valid.clone();
    rotated_key_id[14..18].copy_from_slice(&2u32.to_le_bytes());
    let parsed = SfecEnvelope::parse(Bytes::from(rotated_key_id)).expect("parser accepts key id");
    assert_eq!(parsed.header().key_id(), 2);

    let mut invalid_key_id = valid.clone();
    invalid_key_id[14..18].copy_from_slice(&0u32.to_le_bytes());
    assert_eq!(
        SfecEnvelope::parse(Bytes::from(invalid_key_id)).unwrap_err(),
        SfecError::InvalidKeyId(0)
    );

    let mut invalid_header_len = valid.clone();
    invalid_header_len[6..10].copy_from_slice(&39u32.to_le_bytes());
    assert_eq!(
        SfecEnvelope::parse(Bytes::from(invalid_header_len)).unwrap_err(),
        SfecError::InvalidHeaderLength(39)
    );

    let truncated_header = Bytes::from(valid[..20].to_vec());
    assert_eq!(
        SfecEnvelope::parse(truncated_header).unwrap_err(),
        SfecError::TruncatedHeader
    );
}

#[test]
fn sfec_parser_rejects_frame_count_and_object_length_overflow() {
    let valid = envelope_for_len(17);

    let mut frame_count_overflow = valid.clone();
    frame_count_overflow[30..34].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        SfecEnvelope::parse(Bytes::from(frame_count_overflow)).unwrap_err(),
        SfecError::FrameCountOverflow
    );

    let mut object_length_overflow = valid;
    object_length_overflow[18..26].copy_from_slice(&u64::MAX.to_le_bytes());
    assert_eq!(
        SfecEnvelope::parse(Bytes::from(object_length_overflow)).unwrap_err(),
        SfecError::ObjectLengthOverflow
    );
}

#[test]
fn sfec_parser_rejects_large_valid_frame_count_without_linear_scan() {
    let mut header = Vec::new();
    header.extend_from_slice(b"SFEC");
    header.extend_from_slice(&1u16.to_le_bytes());
    header.extend_from_slice(&40u32.to_le_bytes());
    header.extend_from_slice(&1u16.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes());
    header.extend_from_slice(&1u32.to_le_bytes());
    header.extend_from_slice(&(u32::MAX as u64).to_le_bytes());
    header.extend_from_slice(&1u32.to_le_bytes());
    header.extend_from_slice(&u32::MAX.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes());

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let err = SfecEnvelope::parse(Bytes::from(header)).unwrap_err();
        tx.send(err).expect("send parse result");
    });

    let err = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("parser should not scan every declared frame");
    assert_eq!(err, SfecError::TruncatedCiphertext);
}

#[test]
fn sfec_header_builder_reports_oversized_variable_fields_directly() {
    let err = SfecHeader::new(
        SfecAlgorithm::XChaCha20Poly1305V1,
        KEY_ID,
        0,
        DEFAULT_SFEC_FRAME_SIZE,
        Bytes::from(vec![0u8; (u16::MAX as usize) + 1]),
        Bytes::new(),
        Bytes::new(),
    )
    .unwrap_err();

    assert_eq!(err, SfecError::FieldTooLarge("wrap_nonce"));
}

#[test]
fn sfec_parser_rejects_truncated_ciphertext_and_trailing_malformed_data() {
    let valid = envelope_for_len(17);

    let truncated_ciphertext = Bytes::from(valid[..valid.len() - 1].to_vec());
    assert_eq!(
        SfecEnvelope::parse(truncated_ciphertext).unwrap_err(),
        SfecError::TruncatedCiphertext
    );

    let mut trailing = valid;
    trailing.push(0xFF);
    assert_eq!(
        SfecEnvelope::parse(Bytes::from(trailing)).unwrap_err(),
        SfecError::TrailingMalformedData
    );
}

#[test]
fn sfec_envelope_validates_empty_small_exact_and_partial_final_frames() {
    let cases = [
        (0, 0, 0),
        (17, 1, 17 + SFEC_FRAME_TAG_LEN),
        (
            DEFAULT_SFEC_FRAME_SIZE as u64,
            1,
            DEFAULT_SFEC_FRAME_SIZE + SFEC_FRAME_TAG_LEN,
        ),
        (
            (DEFAULT_SFEC_FRAME_SIZE as u64) + 17,
            2,
            DEFAULT_SFEC_FRAME_SIZE + SFEC_FRAME_TAG_LEN + 17 + SFEC_FRAME_TAG_LEN,
        ),
    ];

    for (plaintext_len, expected_frames, expected_ciphertext_len) in cases {
        let parsed = SfecEnvelope::parse(Bytes::from(envelope_for_len(plaintext_len)))
            .expect("parse SFEC envelope");

        assert_eq!(parsed.header().plaintext_len(), plaintext_len);
        assert_eq!(parsed.header().frame_count(), expected_frames);
        assert_eq!(parsed.ciphertext().len(), expected_ciphertext_len);
    }
}
