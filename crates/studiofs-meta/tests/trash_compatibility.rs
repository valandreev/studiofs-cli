//! Executes the version-first decoder from the actual pre-trash client source.
#[path = "fixtures/v0_2_2_checkpoint_decoder.rs"]
mod old_checkpoint;
#[path = "fixtures/v0_2_2_format_decoder.rs"]
mod old_client;

#[test]
fn actual_old_checkpoint_decoder_rejects_new_checkpoint_suffix() {
    let legacy = old_checkpoint::ServingCheckpoint::default();
    let mut bytes = postcard::to_stdvec(&legacy).unwrap();
    assert!(old_checkpoint::ServingCheckpoint::decode(&bytes).is_ok());
    // Format 3 appends VolumeSettings after the complete format-2 prefix.
    bytes.extend(postcard::to_stdvec(&studiofs_meta::trash::VolumeSettings::default()).unwrap());
    assert!(old_checkpoint::ServingCheckpoint::decode(&bytes).is_err());
    assert!(!old_checkpoint::ServingCheckpoint::supports_format_version(
        3
    ));
    assert_eq!(old_checkpoint::ServingCheckpoint::SCHEMA_VERSION, 1);
}

#[test]
fn v0_2_2_decoder_rejects_next_format_before_decoding_new_fields() {
    let error = old_client::decode_format_info(br#"{"format_version":6}"#).unwrap_err();
    assert!(matches!(
        error,
        studiofs_meta::MetaError::UnsupportedFormatVersion {
            found: 6,
            expected: 5
        }
    ));
}

#[test]
fn v0_2_2_decoder_does_not_treat_unknown_capability_as_format_boundary() {
    let error = old_client::decode_format_info(br#"{"format_version":5,"recoverable_trash":true}"#)
        .unwrap_err();
    assert!(matches!(error, studiofs_meta::MetaError::Serialization(_)));
}
