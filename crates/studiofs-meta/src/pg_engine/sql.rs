pub(super) const SELECT_METADATA_VERSION: &str = "SELECT version FROM metadata_state WHERE vol=$1";

pub(super) const SELECT_METADATA_SNAPSHOT: &str = "SELECT version, checkpoint_version, snapshot, metadata_format_version, checkpoint_sha256, checkpoint_publication_state, checkpoint_generation FROM metadata_state WHERE vol=$1";

pub(super) const BUMP_METADATA_VERSION: &str = r#"
    UPDATE metadata_state
    SET version = version + 1,
        updated_at = now()
    WHERE vol = $1
    RETURNING version
    "#;

pub(super) const INSERT_EVENT_LOG_BATCH: &str = r#"
    INSERT INTO event_log (
        vol,
        batch_version,
        event_type,
        event_payload,
        client_id,
        client_timestamp
    )
    SELECT
        $1,
        $2,
        batch.event_type,
        batch.event_payload,
        $3,
        $4
    FROM UNNEST($5::text[], $6::bytea[]) AS batch(event_type, event_payload)
    "#;

pub(super) const UPSERT_METADATA_CHECKPOINT: &str = r#"
    INSERT INTO metadata_state (
        vol,
        metadata_format_version,
        version,
        checkpoint_version,
        snapshot,
        checkpoint_sha256,
        format_info,
        checkpoint_publication_state,
        checkpoint_generation
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, 'published', 1)
    ON CONFLICT (vol) DO UPDATE
    SET metadata_format_version = EXCLUDED.metadata_format_version,
        version = GREATEST(metadata_state.version, EXCLUDED.version),
        checkpoint_version = EXCLUDED.checkpoint_version,
        snapshot = EXCLUDED.snapshot,
        checkpoint_sha256 = EXCLUDED.checkpoint_sha256,
        format_info = EXCLUDED.format_info,
        checkpoint_publication_state = EXCLUDED.checkpoint_publication_state,
        checkpoint_generation = metadata_state.checkpoint_generation + 1,
        updated_at = now()
    WHERE EXCLUDED.checkpoint_version >= metadata_state.checkpoint_version
    "#;

pub(super) const SELECT_FOREIGN_EVENT_ROWS: &str = r#"
    SELECT batch_version, seq_id, event_type, event_payload, client_id
    FROM event_log
    WHERE vol=$1 AND batch_version > $2
    ORDER BY batch_version ASC, seq_id ASC
    "#;

pub(super) const SELECT_SNAPSHOT_SOURCE: &str = r#"
    SELECT
        c.cut_version,
        c.captured_at,
        s.checkpoint_version,
        s.checkpoint_format_version,
        s.checkpoint_schema_version,
        s.checkpoint_bytes,
        s.checkpoint_sha256,
        s.event_start_seq,
        s.event_end_seq,
        s.event_start_version,
        s.event_end_version,
        s.event_row_count,
        m.format_info
    FROM snapshot_catalog AS c
    INNER JOIN snapshot_source_checkpoints AS s
        ON s.vol = c.vol AND s.snapshot_id = c.id
    INNER JOIN metadata_state AS m
        ON m.vol = c.vol
    WHERE c.vol = $1 AND c.id = $2 AND c.lifecycle <> 'deleted'
    "#;

pub(super) const SELECT_SNAPSHOT_EVENT_ROWS: &str = r#"
    SELECT batch_version, seq_id, event_type, event_payload, client_id
    FROM event_log
    WHERE vol = $1
      AND batch_version > $2
      AND batch_version <= $3
      AND ($4::BIGINT IS NULL OR seq_id >= $4)
      AND ($5::BIGINT IS NULL OR seq_id <= $5)
    ORDER BY batch_version ASC, seq_id ASC
    "#;

pub(super) const SELECT_REMOTE_METADATA_SIZE: &str = r#"
    SELECT
        (SELECT COALESCE(SUM(pg_column_size(snapshot)), 0) FROM metadata_state WHERE vol = $1) +
        (SELECT COALESCE(SUM(pg_column_size(event_payload)), 0) FROM event_log WHERE vol = $1)
"#;

pub(super) const SELECT_FILE_LOCK_LEASE: &str = r#"
    SELECT ino, kind, exclusive_owner, shared_owners, lease_expires_at_unix_ms, generation
    FROM file_lock_leases
    WHERE vol = $1 AND ino = $2
"#;
