#[test]
fn test_format_info_roundtrip() {
    let engine = temp_engine();
    let info = FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: "test-vol".into(),
        uuid: "abc-123".into(),
        endpoint: "http://localhost:9000".into(),
        bucket: "testbucket".into(),
        access_key: "studiofs".into(),
        secret_key: "studiofs123".into(),
        region: "".into(),
        data_prefix: "test-volume/objects/".into(),
        metadata: MetadataConfig::default(),
        encryption: EncryptionConfig::new([0xA5; ENCRYPTION_KEY_LENGTH]),
        block_size: DEFAULT_BLOCK_SIZE,
    };
    engine.set_format_info(&info).unwrap();
    let loaded = engine.get_format_info().unwrap();
    assert_eq!(loaded.name, "test-vol");
    assert_eq!(loaded.endpoint, "http://localhost:9000");
}

#[test]
fn format_info_roundtrip_preserves_required_encryption_config() {
    let engine = temp_engine();
    let info = FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: "encrypted-vol".into(),
        uuid: "encrypted-uuid".into(),
        endpoint: "http://localhost:9000".into(),
        bucket: "encrypted-bucket".into(),
        access_key: "studiofs".into(),
        secret_key: "studiofs123".into(),
        region: "".into(),
        data_prefix: "encrypted-volume/objects/".into(),
        metadata: MetadataConfig::default(),
        encryption: EncryptionConfig::new([0xA5; ENCRYPTION_KEY_LENGTH]),
        block_size: DEFAULT_BLOCK_SIZE,
    };

    engine.set_format_info(&info).unwrap();
    let loaded = engine.get_format_info().unwrap();

    assert_eq!(loaded.encryption.algorithm, EncryptionAlgorithm::XChaCha20Poly1305V1);
    assert_eq!(loaded.encryption.active_key_id, 1);
    assert_eq!(loaded.encryption.format_version, 1);
    assert_eq!(loaded.encryption.volume_master_key, [0xA5; ENCRYPTION_KEY_LENGTH]);
    assert!(loaded.encryption.enabled);
}

#[test]
fn format_info_deserialization_rejects_missing_encryption_config() {
    let json = serde_json::json!({
        "format_version": CURRENT_FORMAT_VERSION,
        "name": "legacy-plaintext",
        "uuid": "legacy-uuid",
        "endpoint": "http://localhost:9000",
        "bucket": "legacy-bucket",
        "access_key": "studiofs",
        "secret_key": "studiofs123",
        "region": "",
        "data_prefix": "legacy-volume/objects/",
        "metadata": {},
        "block_size": DEFAULT_BLOCK_SIZE
    });

    let err = serde_json::from_value::<FormatInfo>(json).unwrap_err();

    assert!(
        err.to_string().contains("encryption"),
        "missing encryption config must not deserialize as legacy plaintext: {err}"
    );
}

#[test]
fn format_info_decode_reports_unsupported_version_before_missing_encryption() {
    let legacy = serde_json::json!({
        "format_version": CURRENT_FORMAT_VERSION - 1,
        "name": "legacy-plaintext",
        "uuid": "legacy-uuid",
        "endpoint": "http://localhost:9000",
        "bucket": "legacy-bucket",
        "access_key": "studiofs",
        "secret_key": "studiofs123",
        "region": "",
        "data_prefix": "legacy-volume/objects/",
        "metadata": {},
        "block_size": DEFAULT_BLOCK_SIZE
    });

    let err =
        super::format::decode_format_info(serde_json::to_vec(&legacy).unwrap().as_slice())
            .unwrap_err();

    assert!(matches!(
        err,
        MetaError::UnsupportedFormatVersion {
            found,
            expected
        } if found == CURRENT_FORMAT_VERSION - 1 && expected == CURRENT_FORMAT_VERSION
    ));
}

#[test]
fn encryption_config_validation_rejects_disabled_or_unsupported_values() {
    let mut disabled = EncryptionConfig::new([0x11; ENCRYPTION_KEY_LENGTH]);
    disabled.enabled = false;
    assert!(matches!(
        disabled.validate(),
        Err(EncryptionContractError::Disabled)
    ));

    let mut unsupported_key = EncryptionConfig::new([0x22; ENCRYPTION_KEY_LENGTH]);
    unsupported_key.active_key_id = 2;
    assert!(matches!(
        unsupported_key.validate(),
        Err(EncryptionContractError::UnsupportedKeyId { found, expected })
            if found == 2 && expected == CURRENT_ENCRYPTION_KEY_ID
    ));

    let mut unsupported_format = EncryptionConfig::new([0x33; ENCRYPTION_KEY_LENGTH]);
    unsupported_format.format_version = 2;
    assert!(matches!(
        unsupported_format.validate(),
        Err(EncryptionContractError::UnsupportedFormatVersion { found, expected })
            if found == 2 && expected == CURRENT_ENCRYPTION_FORMAT_VERSION
    ));
}

#[test]
fn generated_encryption_config_and_format_info_use_fallible_contract() {
    let encryption = EncryptionConfig::generate().expect("generate encryption config");
    encryption.validate().expect("generated config is valid");

    let info = FormatInfo::new(
        "generated-vol".into(),
        "generated-uuid".into(),
        FormatInfoDataStore {
            endpoint: "http://localhost:9000".into(),
            bucket: "generated-bucket".into(),
            access_key: "studiofs".into(),
            secret_key: "studiofs123".into(),
            region: String::new(),
        },
        16,
    )
    .expect("generate format info");

    info.validate_format_contract()
        .expect("generated format info is valid");
    assert_ne!(info.encryption.volume_master_key, [0; ENCRYPTION_KEY_LENGTH]);
}

#[test]
fn encryption_config_deserialization_rejects_malformed_or_unsupported_json() {
    let short_key = serde_json::json!({
        "algorithm": "xchacha20poly1305-v1",
        "active_key_id": 1,
        "format_version": 1,
        "volume_master_key": [1, 2, 3],
        "enabled": true
    });
    assert!(serde_json::from_value::<EncryptionConfig>(short_key).is_err());

    let unsupported_algorithm = serde_json::json!({
        "algorithm": "aes-256-gcm",
        "active_key_id": 1,
        "format_version": 1,
        "volume_master_key": vec![0u8; ENCRYPTION_KEY_LENGTH],
        "enabled": true
    });
    assert!(serde_json::from_value::<EncryptionConfig>(unsupported_algorithm).is_err());
}

#[test]
fn test_read_format_info_readonly_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("probe.db");

    let engine = RedbMetaEngine::open(&path).unwrap();
    engine.init_root(1000, 1000).unwrap();
    let info = FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: "probe-vol".into(),
        uuid: "probe-uuid".into(),
        endpoint: "http://localhost:9000".into(),
        bucket: "probe-bucket".into(),
        access_key: "studiofs".into(),
        secret_key: "studiofs123".into(),
        region: "".into(),
        data_prefix: "probe-volume/objects/".into(),
        metadata: MetadataConfig::default(),
        encryption: EncryptionConfig::new([0xA5; ENCRYPTION_KEY_LENGTH]),
        block_size: DEFAULT_BLOCK_SIZE,
    };
    engine.set_format_info(&info).unwrap();
    drop(engine);

    let loaded = RedbMetaEngine::read_format_info_readonly(&path).unwrap();
    assert_eq!(loaded.name, "probe-vol");
    assert_eq!(loaded.uuid, "probe-uuid");
}

#[test]
fn test_read_format_info_readonly_returns_not_formatted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("probe-not-formatted.db");

    let engine = RedbMetaEngine::open(&path).unwrap();
    engine.init_root(1000, 1000).unwrap();
    drop(engine);

    let result = RedbMetaEngine::read_format_info_readonly(&path);
    assert!(matches!(result, Err(MetaError::NotFormatted)));
}

#[test]
fn test_read_format_info_readonly_reports_serialization_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("probe-invalid-format.db");

    let engine = RedbMetaEngine::open(&path).unwrap();
    let txn = engine.db.begin_write().unwrap();
    {
        let mut table = txn.open_table(SETTINGS_TABLE).unwrap();
        table.insert("format", b"{not-json".as_slice()).unwrap();
    }
    txn.commit().unwrap();
    drop(engine);

    let result = RedbMetaEngine::read_format_info_readonly(&path);
    assert!(matches!(result, Err(MetaError::Serialization(_))));
}
