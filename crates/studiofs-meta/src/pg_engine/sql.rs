pub(super) const SELECT_METADATA_VERSION: &str = "SELECT version FROM metadata_state WHERE vol=$1";

pub(super) const SELECT_METADATA_SNAPSHOT: &str =
    "SELECT version, snapshot FROM metadata_state WHERE vol=$1";

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
    INSERT INTO metadata_state (vol, version, snapshot)
    VALUES ($1, $2, $3)
    ON CONFLICT (vol) DO UPDATE
    SET version = EXCLUDED.version,
        snapshot = EXCLUDED.snapshot,
        updated_at = now()
    WHERE EXCLUDED.version >= metadata_state.version
    "#;

pub(super) const SELECT_FOREIGN_EVENT_ROWS: &str = r#"
    SELECT batch_version, seq_id, event_type, event_payload, client_id
    FROM event_log
    WHERE vol=$1 AND batch_version > $2
    ORDER BY batch_version ASC, seq_id ASC
    "#;

pub(super) const SELECT_REMOTE_METADATA_SIZE: &str = r#"
    SELECT
        (SELECT COALESCE(SUM(pg_column_size(snapshot)), 0) FROM metadata_state WHERE vol = $1) +
        (SELECT COALESCE(SUM(pg_column_size(event_payload)), 0) FROM event_log WHERE vol = $1) +
        (SELECT COALESCE(SUM(pg_column_size(snapshot_payload)), 0) FROM snapshots WHERE vol = $1)
"#;

pub(super) const SELECT_FILE_LOCK_LEASE: &str = r#"
    SELECT ino, kind, exclusive_owner, shared_owners, lease_expires_at_unix_ms, generation
    FROM file_lock_leases
    WHERE vol = $1 AND ino = $2
"#;
