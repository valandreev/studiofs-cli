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
        block_size: DEFAULT_BLOCK_SIZE,
    };
    engine.set_format_info(&info).unwrap();
    engine.validate_migration_completed().unwrap();
    let loaded = engine.get_format_info().unwrap();
    assert_eq!(loaded.name, "test-vol");
    assert_eq!(loaded.endpoint, "http://localhost:9000");
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
