pub(super) const SELECT_METADATA_VERSION: &str = "SELECT version FROM metadata_state WHERE vol=$1";

pub(super) const SELECT_METADATA_HEAD_AND_CHECKPOINT_VERSION: &str =
    "SELECT version, checkpoint_version, event_log_prune_floor FROM metadata_state WHERE vol=$1";

pub(super) const SELECT_METADATA_HEAD_AND_COMPATIBLE_CHECKPOINT_VERSION: &str = r#"
    SELECT
        ms.version,
        GREATEST(
            ms.checkpoint_version,
            COALESCE((
                SELECT MAX(enc.checkpoint_version)
                FROM serving_checkpoint_encodings AS enc
                WHERE enc.vol = ms.vol
                  AND enc.checkpoint_version BETWEEN ms.event_log_prune_floor AND ms.version
                  AND enc.checkpoint_format BETWEEN $2 AND $3
                  AND enc.checkpoint_schema BETWEEN $4 AND $5
            ), -1)
        ) AS checkpoint_version,
        ms.event_log_prune_floor
    FROM metadata_state AS ms
    WHERE ms.vol = $1
    "#;

macro_rules! checkpoint_freshness_columns {
    () => {
        r#"
        version,
        checkpoint_version,
        total_event_payload_bytes,
        checkpoint_total_event_bytes,
        GREATEST(
            (EXTRACT(EPOCH FROM (clock_timestamp() - checkpoint_updated_at)) * 1000)::BIGINT,
            0::BIGINT
        ) AS checkpoint_age_ms,
        octet_length(format_info) > 0 AS format_info_present,
        metadata_format_version
        "#
    };
}

pub(super) const SELECT_CHECKPOINT_FRESHNESS: &str = concat!(
    "SELECT ",
    checkpoint_freshness_columns!(),
    " FROM metadata_state WHERE vol = $1",
);

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
        ms.checkpoint_generation,
        ms.event_log_prune_floor,
        NULL::SMALLINT AS checkpoint_format,
        NULL::SMALLINT AS checkpoint_schema,
        false AS incompatible_alternative_present
    FROM volumes AS v
    INNER JOIN metadata_state AS ms ON ms.vol = v.vol
    WHERE v.vol = $1
    "#;

pub(super) const SELECT_COMPATIBLE_CHECKPOINT_DESCRIPTOR: &str = r#"
    SELECT v.vol, v.uuid, ms.metadata_format_version, ms.version, ms.format_info,
        ms.event_log_prune_floor,
        COALESCE(candidate.checkpoint_version, ms.checkpoint_version) AS checkpoint_version,
        COALESCE(candidate.snapshot, ms.snapshot) AS snapshot,
        COALESCE(candidate.checkpoint_sha256, ms.checkpoint_sha256) AS checkpoint_sha256,
        COALESCE(candidate.publication_state, ms.checkpoint_publication_state)
            AS checkpoint_publication_state,
        COALESCE(candidate.publication_generation, ms.checkpoint_generation) AS checkpoint_generation,
        candidate.checkpoint_format, candidate.checkpoint_schema,
        CASE WHEN candidate.checkpoint_version IS NULL
             AND ms.checkpoint_publication_state <> 'published'
             THEN EXISTS (
                 SELECT 1 FROM serving_checkpoint_encodings
                 WHERE vol = ms.vol
                   AND checkpoint_version BETWEEN ms.event_log_prune_floor AND ms.version
             )
             ELSE false
        END AS incompatible_alternative_present
    FROM volumes AS v
    INNER JOIN metadata_state AS ms ON ms.vol = v.vol
    LEFT JOIN LATERAL (
        SELECT enc.checkpoint_version, enc.snapshot, enc.checkpoint_sha256,
            'published'::TEXT AS publication_state, enc.publication_generation,
            enc.checkpoint_format, enc.checkpoint_schema
        FROM serving_checkpoint_encodings AS enc
        WHERE enc.vol = ms.vol
          AND enc.checkpoint_version BETWEEN ms.event_log_prune_floor AND ms.version
          AND enc.checkpoint_format BETWEEN $2 AND $3
          AND enc.checkpoint_schema BETWEEN $4 AND $5
          AND NOT EXISTS (
              SELECT 1
              FROM unnest($6::SMALLINT[], $7::SMALLINT[], $8::BIGINT[])
                  AS excluded(checkpoint_format, checkpoint_schema, checkpoint_version)
              WHERE enc.checkpoint_format = excluded.checkpoint_format
                AND enc.checkpoint_schema = excluded.checkpoint_schema
                AND enc.checkpoint_version = excluded.checkpoint_version
          )
          AND (ms.checkpoint_publication_state <> 'published'
               OR enc.checkpoint_version >= ms.checkpoint_version)
        ORDER BY enc.checkpoint_version DESC, enc.checkpoint_format DESC, enc.checkpoint_schema DESC
        LIMIT 1
    ) AS candidate ON true
    WHERE v.vol = $1
    "#;

pub(super) const CHECKPOINT_ENCODINGS_AVAILABLE: &str = r#"
    SELECT to_regclass(quote_ident(current_schema()) || '.serving_checkpoint_encodings') IS NOT NULL
    "#;

pub(super) const SELECT_COMPATIBLE_CHECKPOINT_MAX_VERSION: &str = r#"
    SELECT MAX(enc.checkpoint_version)
    FROM serving_checkpoint_encodings AS enc
    INNER JOIN metadata_state AS ms ON ms.vol = enc.vol
    WHERE enc.vol = $1
      AND enc.checkpoint_version BETWEEN $2 AND ms.version
      AND enc.checkpoint_format BETWEEN $3 AND $4
      AND enc.checkpoint_schema BETWEEN $5 AND $6
    "#;

#[cfg(feature = "test-successor")]
pub(super) const INSERT_CHECKPOINT_ENCODING: &str = r#"
    INSERT INTO serving_checkpoint_encodings (
        vol, checkpoint_version, checkpoint_format, checkpoint_schema,
        publication_generation, snapshot, checkpoint_sha256
    )
    SELECT $1, $2, $3, $4, checkpoint_generation, $5, $6
    FROM metadata_state WHERE vol = $1
    ON CONFLICT (vol, checkpoint_format, checkpoint_schema, checkpoint_version) DO NOTHING
    "#;

#[cfg(feature = "test-successor")]
pub(super) const CHECK_EXISTING_CHECKPOINT_ENCODING: &str = r#"
    SELECT EXISTS (
        SELECT 1 FROM serving_checkpoint_encodings
        WHERE vol = $1 AND checkpoint_version = $2
          AND checkpoint_format = $3 AND checkpoint_schema = $4
          AND snapshot = $5 AND checkpoint_sha256 = $6
    )
    "#;

#[cfg(feature = "test-successor")]
pub(super) const DELETE_SUPERSEDED_CHECKPOINT_ENCODINGS: &str = r#"
    DELETE FROM serving_checkpoint_encodings
    WHERE vol = $1 AND checkpoint_format = $3 AND checkpoint_schema = $4
      AND checkpoint_version < $2
    "#;

#[cfg(test)]
pub(super) const SELECT_METADATA_FORMAT_VERSION: &str =
    "SELECT metadata_format_version FROM metadata_state WHERE vol = $1";

/// Publication gates, reread to explain a refused `BUMP_METADATA_VERSION`.
pub(super) const SELECT_PUBLICATION_GATES: &str = "
    SELECT metadata_format_version, write_requirement, publication_epoch,
           NULLIF(current_setting('studiofs.policy_epoch', true), '')::BIGINT
    FROM metadata_state WHERE vol = $1";

pub(super) const SELECT_METADATA_REPLAY_STATE: &str = r#"
    SELECT m.metadata_format_version, m.checkpoint_version, m.event_log_prune_floor,
           m.checkpoint_publication_state, p.policy_epoch, m.write_requirement,
           p.requirements::TEXT AS policy_requirements
    FROM metadata_state m LEFT JOIN volume_protocol p ON p.vol = m.vol
    WHERE m.vol = $1
    "#;

pub(super) const ACQUIRE_METADATA_REPLAY_ADVISORY_LOCK: &str =
    "SELECT pg_advisory_xact_lock_shared($1::BIGINT)";

macro_rules! bump_metadata_version {
    ($format:literal, $bytes:literal, $requirement:literal, $epoch:literal) => {
        concat!(
            "UPDATE metadata_state
             SET version = version + 1,
                 total_event_payload_bytes = total_event_payload_bytes + ",
            $bytes,
            ",
                 updated_at = now()
             WHERE vol = $1 AND metadata_format_version = ",
            $format,
            " AND write_requirement <= ",
            $requirement,
            " AND publication_epoch",
            $epoch,
            " RETURNING ",
            checkpoint_freshness_columns!(),
        )
    };
}

pub(super) const BUMP_METADATA_VERSION: &str = bump_metadata_version!(
    "$2",
    "$3",
    "$4",
    "::TEXT = current_setting('studiofs.policy_epoch', true)"
);

// Materialize once so the volatile GUC writes run before the dependent bump
// or receipt lock. FOR SHARE pins the exact lease generation until tx ends.
macro_rules! with_publication_stamp {
    ($($tail:expr),+ $(,)?) => {
        concat!(
            r#"
            WITH locked AS MATERIALIZED (
                SELECT policy_epoch, lease_expires_at FROM client_sessions
                WHERE vol = $1 AND session_id = $2 AND owner = $3 AND fencing_token = $4
                FOR SHARE
            ),
            current_policy AS MATERIALIZED (
                SELECT COALESCE((SELECT policy_epoch FROM volume_protocol WHERE vol = $1), 0) AS epoch
            ),
            lease AS MATERIALIZED (
                -- Read the clock after FOR SHARE: a wait on a concurrent renewal
                -- must not judge the lease against a pre-lock timestamp.
                SELECT locked.lease_expires_at > clock_timestamp() AS lease_active,
                       locked.policy_epoch = current_policy.epoch
                           AND locked.policy_epoch = $5 AS policy_current,
                       current_policy.epoch AS current_epoch
                FROM locked CROSS JOIN current_policy
            ),
            stamp AS MATERIALIZED (
                SELECT lease.*,
                       CASE WHEN lease_active AND policy_current
                            THEN set_config('studiofs.session_id', $2, true) END AS stamped_session,
                       CASE WHEN lease_active AND policy_current
                            THEN set_config('studiofs.session_fencing_token', $4::BIGINT::TEXT, true)
                       END AS stamped_token,
                       CASE WHEN lease_active AND policy_current
                            THEN set_config('studiofs.policy_epoch', current_epoch::TEXT, true)::BIGINT
                       END AS stamped_epoch
                FROM lease
            )
            "#,
            $($tail),+,
        )
    };
}

pub(super) const STAMP_PUBLICATION_SESSION: &str = with_publication_stamp!("SELECT * FROM stamp");

// The epoch predicate is checked again against the latest metadata_state row
// if the bump waits for an activation to commit under READ COMMITTED.
pub(super) const STAMP_AND_BUMP_METADATA_VERSION: &str = with_publication_stamp!(
    ", bumped AS (",
    bump_metadata_version!("$6", "$7", "$8", " = (SELECT stamped_epoch FROM stamp)"),
    ") SELECT bumped.*, stamp.* FROM stamp LEFT JOIN bumped ON TRUE",
);

// Receipt lookup needs a fresh snapshot after acquiring this lock.
pub(super) const STAMP_AND_LOCK_RETAINED_PUBLICATION: &str = with_publication_stamp!(
    ", locked_state AS MATERIALIZED (SELECT ",
    checkpoint_freshness_columns!(),
    " FROM metadata_state WHERE vol = $1
      AND (SELECT stamped_epoch FROM stamp) IS NOT NULL FOR UPDATE)
      SELECT locked_state.*, stamp.* FROM stamp LEFT JOIN locked_state ON TRUE",
);

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

macro_rules! checkpoint_upsert {
    ($prefix:expr, $values:expr, $conflict_where:expr) => {
        concat!(
            $prefix,
            r#"
    INSERT INTO metadata_state (
        vol,
        metadata_format_version,
        version,
        checkpoint_version,
        total_event_payload_bytes,
        checkpoint_total_event_bytes,
        snapshot,
        checkpoint_sha256,
        format_info,
        checkpoint_publication_state,
        checkpoint_generation,
        checkpoint_updated_at
    )
    "#,
            $values,
            r#"
    ON CONFLICT (vol) DO UPDATE
    SET
        version = GREATEST(metadata_state.version, EXCLUDED.version),
        checkpoint_version = EXCLUDED.checkpoint_version,
        checkpoint_total_event_bytes = EXCLUDED.checkpoint_total_event_bytes,
        snapshot = EXCLUDED.snapshot,
        checkpoint_sha256 = EXCLUDED.checkpoint_sha256,
        format_info = CASE
            WHEN COALESCE(octet_length(metadata_state.format_info), 0) = 0
            THEN EXCLUDED.format_info
            ELSE metadata_state.format_info
        END,
        checkpoint_publication_state = EXCLUDED.checkpoint_publication_state,
        checkpoint_generation = metadata_state.checkpoint_generation + 1,
        checkpoint_updated_at = now(),
        updated_at = now()
    "#,
            $conflict_where,
            r#"
    RETURNING
        version,
        checkpoint_version,
        total_event_payload_bytes,
        checkpoint_total_event_bytes,
        GREATEST(
            (EXTRACT(EPOCH FROM (clock_timestamp() - checkpoint_updated_at)) * 1000)::BIGINT,
            0::BIGINT
        ) AS checkpoint_age_ms,
        octet_length(format_info) > 0 AS format_info_present,
        metadata_format_version
    "#
        )
    };
}

#[cfg(test)]
pub(super) const UPSERT_METADATA_CHECKPOINT: &str = checkpoint_upsert!(
    "",
    r#"
    VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8, 'published', 1, now())
    "#,
    r#"
    WHERE metadata_state.metadata_format_version = EXCLUDED.metadata_format_version
      AND EXCLUDED.checkpoint_version >= metadata_state.checkpoint_version
    "#
);

pub(super) const CLAIM_BOOTSTRAP_CHECKPOINT_LEASE: &str = r#"
    INSERT INTO bootstrap_checkpoint_leases (
        vol,
        owner,
        lease_expires_at,
        fencing_token
    )
    VALUES (
        $1,
        $2,
        clock_timestamp() + ($3::bigint * interval '1 millisecond'),
        1
    )
    ON CONFLICT (vol) DO UPDATE
    SET owner = EXCLUDED.owner,
        lease_expires_at = EXCLUDED.lease_expires_at,
        fencing_token = CASE
            WHEN bootstrap_checkpoint_leases.owner = EXCLUDED.owner
                AND bootstrap_checkpoint_leases.lease_expires_at > clock_timestamp()
            THEN bootstrap_checkpoint_leases.fencing_token
            ELSE bootstrap_checkpoint_leases.fencing_token + 1
        END,
        acquired_at = CASE
            WHEN bootstrap_checkpoint_leases.owner = EXCLUDED.owner
                AND bootstrap_checkpoint_leases.lease_expires_at > clock_timestamp()
            THEN bootstrap_checkpoint_leases.acquired_at
            ELSE clock_timestamp()
        END,
        renewed_at = clock_timestamp()
    WHERE bootstrap_checkpoint_leases.owner = EXCLUDED.owner
       OR bootstrap_checkpoint_leases.lease_expires_at <= clock_timestamp()
    RETURNING owner, lease_expires_at, fencing_token
    "#;

pub(super) const RELEASE_BOOTSTRAP_CHECKPOINT_LEASE: &str = r#"
    UPDATE bootstrap_checkpoint_leases
    SET lease_expires_at = clock_timestamp(),
        renewed_at = clock_timestamp()
    WHERE vol = $1
      AND owner = $2
      AND fencing_token = $3
    "#;

pub(super) const UPSERT_FENCED_METADATA_CHECKPOINT: &str = checkpoint_upsert!(
    r#"
    WITH active_lease AS MATERIALIZED (
        SELECT vol
        FROM bootstrap_checkpoint_leases
        WHERE vol = $1
          AND owner = $9
          AND fencing_token = $10
          AND lease_expires_at > clock_timestamp()
        FOR SHARE
    )
    "#,
    r#"
    SELECT
        $1,
        $2,
        $3,
        $4,
        $5,
        $5,
        $6,
        $7,
        $8,
        'published',
        1,
        now()
    FROM active_lease
    "#,
    r#"
    WHERE EXISTS (SELECT 1 FROM active_lease)
      AND metadata_state.metadata_format_version = EXCLUDED.metadata_format_version
      AND (
          EXCLUDED.checkpoint_version > metadata_state.checkpoint_version
          OR COALESCE(octet_length(metadata_state.format_info), 0) = 0
      )
    "#
);

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

pub(super) const SELECT_EVENT_LOG_PRUNE_STATE: &str = r#"
    SELECT checkpoint_version, event_log_prune_floor
    FROM metadata_state
    WHERE vol = $1
      AND checkpoint_publication_state = 'published'
    FOR UPDATE
    "#;

pub(super) const TRY_EVENT_LOG_PRUNE_ADVISORY_LOCK: &str = "SELECT pg_try_advisory_xact_lock($1)";

pub(super) const SELECT_EVENT_LOG_PRUNE_PROGRESS: &str = r#"
    SELECT checkpoint_version, event_log_prune_floor
    FROM metadata_state
    WHERE vol = $1
      AND checkpoint_publication_state = 'published'
    "#;

pub(super) const SELECT_EVENT_LOG_SOURCE_PRUNE_FLOOR: &str = r#"
    SELECT MIN(
        CASE
            WHEN source.event_row_count > 0
            THEN GREATEST(COALESCE(source.event_start_version, 0) - 1, 0)
        END
    ) AS source_prune_floor
    FROM snapshot_source_checkpoints AS source
    INNER JOIN snapshot_catalog AS catalog
        ON catalog.vol = source.vol
       AND catalog.id = source.snapshot_id
    WHERE source.vol = $1
      AND catalog.lifecycle <> 'deleted'
    "#;

pub(super) const DELETE_EVENT_LOG_PRUNE_BATCH: &str = r#"
    WITH candidate_batches AS MATERIALIZED (
        SELECT batch_version
        FROM event_log
        WHERE vol = $1
          AND batch_version <= $2
        GROUP BY batch_version
        ORDER BY batch_version ASC
        LIMIT $3::BIGINT
    ),
    deleted_batches AS (
        DELETE FROM event_log AS event
        USING candidate_batches AS candidate
        WHERE event.vol = $1
          AND event.batch_version = candidate.batch_version
        RETURNING event.batch_version
    )
    SELECT COUNT(*)::BIGINT AS pruned_rows, MAX(batch_version) AS max_deleted_batch
    FROM deleted_batches
    "#;

pub(super) const SELECT_EVENT_LOG_REMAINING_BELOW_FLOOR: &str = r#"
    SELECT 1
    FROM event_log
    WHERE vol = $1
      AND batch_version <= $2
    LIMIT 1
    "#;

pub(super) const UPDATE_EVENT_LOG_PRUNE_STATE: &str = r#"
    UPDATE metadata_state
    SET event_log_prune_floor = GREATEST(event_log_prune_floor, $2),
        event_log_pruned_rows = event_log_pruned_rows + $3,
        updated_at = clock_timestamp()
    WHERE vol = $1
    RETURNING event_log_prune_floor, event_log_pruned_rows
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
