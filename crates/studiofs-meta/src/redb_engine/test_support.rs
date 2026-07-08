use super::*;
use std::ops::Deref;

pub(super) struct TestEngine {
    engine: RedbMetaEngine,
    _dir: tempfile::TempDir,
}

impl Deref for TestEngine {
    type Target = RedbMetaEngine;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

pub(super) fn test_format_info(name: &str) -> FormatInfo {
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: name.to_string(),
        uuid: format!("{name}-uuid"),
        endpoint: "http://localhost:9000".into(),
        bucket: "testbucket".into(),
        access_key: "studiofs".into(),
        secret_key: "studiofs123".into(),
        region: "".into(),
        data_prefix: format!("{name}/objects/"),
        metadata: MetadataConfig::default(),
        encryption: EncryptionConfig::new([0xA5; ENCRYPTION_KEY_LENGTH]),
        block_size: DEFAULT_BLOCK_SIZE,
    }
}

pub(super) fn temp_engine() -> TestEngine {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("test.db");
    let engine = RedbMetaEngine::open(&path).unwrap();
    engine.init_root(1000, 1000).unwrap();
    engine
        .set_format_info(&test_format_info("test-vol"))
        .unwrap();
    TestEngine { engine, _dir: dir }
}

pub(super) fn set_regular_file_chunk_rows(
    engine: &RedbMetaEngine,
    ino: Ino,
    overlay_rows: &[(ChunkId, RegularFileChunkState)],
    base_rows: &[(ChunkId, RegularFileChunkState)],
) {
    let txn = engine.begin_write_txn().unwrap();
    {
        RedbMetaEngine::clear_regular_file_chunk_rows_in_txn(&txn, REGULAR_FILE_OVERLAY_TABLE, ino)
            .unwrap();
        RedbMetaEngine::clear_regular_file_chunk_rows_in_txn(&txn, REGULAR_FILE_BASE_TABLE, ino)
            .unwrap();

        let mut overlay = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        for (chunk_id, chunk_state) in overlay_rows {
            let key = encode_regular_file_chunk_key(ino, *chunk_id);
            let bytes = encode(chunk_state).unwrap();
            overlay.insert(key.as_slice(), bytes.as_slice()).unwrap();
        }
        drop(overlay);

        let mut base = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
        for (chunk_id, chunk_state) in base_rows {
            let key = encode_regular_file_chunk_key(ino, *chunk_id);
            let bytes = encode(chunk_state).unwrap();
            base.insert(key.as_slice(), bytes.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
}

pub(super) fn set_regular_file_pending_compaction_rows(
    engine: &RedbMetaEngine,
    ino: Ino,
    chunks: &[ChunkId],
) {
    let txn = engine.begin_write_txn().unwrap();
    {
        let mut pending = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .unwrap();
        for chunk_id in chunks {
            let key = encode_regular_file_chunk_key(ino, *chunk_id);
            pending.insert(key.as_slice(), 1).unwrap();
        }
    }
    txn.commit().unwrap();
}
