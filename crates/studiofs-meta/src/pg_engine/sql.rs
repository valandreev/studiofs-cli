pub(super) const SELECT_METADATA_VERSION: &str = "SELECT version FROM metadata_state WHERE vol=$1";

pub(super) const SELECT_BOOTSTRAP_CHECKPOINT_DESCRIPTOR: &str = r#"
    SELECT
        v.vol,
        v.uuid,
        ms.metadata_format_version,
        ms.version,
        ms.checkpoint_version,
        ms.snapshot,
        ms.checkpoint_sha256,
        ms.format_info,
        ms.checkpoint_publication_state,
        ms.checkpoint_generation
    FROM volumes AS v
    INNER JOIN metadata_state AS ms ON ms.vol = v.vol
    WHERE v.vol = $1
    "#;

pub(super) const SELECT_METADATA_FORMAT_VERSION: &str =
    "SELECT metadata_format_version FROM metadata_state WHERE vol = $1";

pub(super) const BUMP_METADATA_VERSION: &str = r#"
    UPDATE metadata_state
    SET version = version + 1,
        updated_at = now()
    WHERE vol = $1 AND metadata_format_version = $2
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
    SET version = GREATEST(metadata_state.version, EXCLUDED.version),
        checkpoint_version = EXCLUDED.checkpoint_version,
        snapshot = EXCLUDED.snapshot,
        checkpoint_sha256 = EXCLUDED.checkpoint_sha256,
        format_info = EXCLUDED.format_info,
        checkpoint_publication_state = EXCLUDED.checkpoint_publication_state,
        checkpoint_generation = metadata_state.checkpoint_generation + 1,
        updated_at = now()
    WHERE metadata_state.metadata_format_version = EXCLUDED.metadata_format_version
      AND EXCLUDED.checkpoint_version >= metadata_state.checkpoint_version
    RETURNING metadata_format_version
    "#;

pub(super) const SELECT_FOREIGN_EVENT_ROWS: &str = r#"
    WITH candidate_batches AS (
        SELECT batch_version
        FROM event_log
        WHERE vol = $1
          AND batch_version > $2
          AND batch_version <= $3
        GROUP BY batch_version
        ORDER BY batch_version ASC
        LIMIT $4::BIGINT
    ),
    candidate_batch_sizes AS (
        SELECT
            e.batch_version,
            COUNT(*)::BIGINT AS event_count,
            SUM(
                COALESCE(pg_column_size(e.batch_version), 0)::BIGINT
                + COALESCE(pg_column_size(e.seq_id), 0)::BIGINT
                + COALESCE(pg_column_size(e.event_type), 0)::BIGINT
                + COALESCE(octet_length(e.event_payload), 0)::BIGINT
                + COALESCE(pg_column_size(e.client_id), 0)::BIGINT
            )::BIGINT AS response_bytes
        FROM event_log AS e
        INNER JOIN candidate_batches AS c
            ON c.batch_version = e.batch_version
        WHERE e.vol = $1
        GROUP BY e.batch_version
    ),
    ranked_batches AS (
        SELECT
            batch_version,
            event_count,
            response_bytes,
            ROW_NUMBER() OVER (ORDER BY batch_version ASC) AS batch_ordinal,
            SUM(response_bytes) OVER (
                ORDER BY batch_version ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_response_bytes
        FROM candidate_batch_sizes
    ),
    selected_batches AS (
        SELECT batch_version, event_count, response_bytes
        FROM ranked_batches
        WHERE batch_ordinal = 1
           OR cumulative_response_bytes <= $5::BIGINT
    ),
    page_metadata AS (
        SELECT
            MIN(batch_version) AS first_batch_version,
            MAX(batch_version) AS last_batch_version,
            COUNT(*)::BIGINT AS batch_count,
            SUM(event_count)::BIGINT AS event_count,
            SUM(response_bytes)::BIGINT AS response_bytes,
            COALESCE(BOOL_OR(response_bytes > $5::BIGINT), false) AS oversized
        FROM selected_batches
    ),
    page_bounds AS (
        SELECT
            p.*,
            EXISTS (
                SELECT 1
                FROM event_log AS e
                WHERE e.vol = $1
                  AND e.batch_version > p.last_batch_version
                  AND e.batch_version <= $3
            ) AS has_more
        FROM page_metadata AS p
    )
    SELECT
        e.batch_version,
        e.seq_id,
        e.event_type,
        e.event_payload,
        e.client_id,
        p.first_batch_version,
        p.last_batch_version,
        p.batch_count,
        p.event_count,
        p.response_bytes,
        p.has_more,
        p.oversized
    FROM event_log AS e
    INNER JOIN selected_batches AS s
        ON s.batch_version = e.batch_version
    CROSS JOIN page_bounds AS p
    WHERE e.vol = $1
    ORDER BY e.batch_version ASC, e.seq_id ASC
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
