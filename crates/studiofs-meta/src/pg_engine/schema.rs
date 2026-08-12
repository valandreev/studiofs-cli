use crate::engine::{MetaError, MetaResult};

/// The PostgreSQL schema generation owned by the Snapshot v2 control plane.
pub const STUDIOFS_PG_SCHEMA_VERSION: i32 = 2;

/// Returns the canonical fresh-install PostgreSQL DDL for StudioFS.
///
/// This is the single schema definition used by initialization, dump/load, and
/// PostgreSQL integration fixtures. Snapshot v2 is PostgreSQL-authoritative:
/// the catalog, durable creation queue, leases, policy slots, source
/// checkpoints, artifacts, and reachability intervals are all represented by
/// scalar rows. The legacy thick snapshot table is intentionally not created.
///
/// The returned SQL creates objects if absent and does not copy or migrate v1
/// snapshot rows.
pub fn studiofs_pg_schema_sql() -> &'static str {
    r#"
        CREATE TABLE IF NOT EXISTS volumes (
            vol BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            uuid TEXT NOT NULL UNIQUE,
            block_size INTEGER NOT NULL,
            encryption_enabled BOOLEAN NOT NULL DEFAULT TRUE,
            encryption_format_version SMALLINT NOT NULL DEFAULT 1,
            active_crypto_key_id INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        ALTER TABLE volumes
            ADD COLUMN IF NOT EXISTS encryption_enabled BOOLEAN NOT NULL DEFAULT TRUE;
        ALTER TABLE volumes
            ADD COLUMN IF NOT EXISTS encryption_format_version SMALLINT NOT NULL DEFAULT 1;
        ALTER TABLE volumes
            ADD COLUMN IF NOT EXISTS active_crypto_key_id INTEGER NOT NULL DEFAULT 1;

        CREATE TABLE IF NOT EXISTS volume_crypto_keys (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            key_id INTEGER NOT NULL,
            algorithm TEXT NOT NULL,
            key_material BYTEA NOT NULL,
            state TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, key_id)
        );

        CREATE TABLE IF NOT EXISTS counters (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            name TEXT NOT NULL,
            value BIGINT NOT NULL,
            PRIMARY KEY (vol, name)
        );

        CREATE TABLE IF NOT EXISTS event_log (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            seq_id BIGSERIAL PRIMARY KEY,
            batch_version BIGINT NOT NULL,
            event_type TEXT NOT NULL,
            event_payload BYTEA NOT NULL,
            client_id TEXT,
            client_timestamp BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS metadata_state (
            vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
            version BIGINT NOT NULL,
            snapshot BYTEA NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS acl_rules (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            ino BIGINT NOT NULL,
            principal TEXT NOT NULL,
            access_level SMALLINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, ino, principal)
        );

        CREATE TABLE IF NOT EXISTS file_lock_leases (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            ino BIGINT NOT NULL,
            kind SMALLINT NOT NULL,
            exclusive_owner TEXT,
            shared_owners TEXT[] NOT NULL DEFAULT '{}',
            lease_expires_at_unix_ms BIGINT NOT NULL,
            generation BIGINT NOT NULL DEFAULT 1,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, ino)
        );

        CREATE TABLE IF NOT EXISTS snapshot_catalog (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            id TEXT NOT NULL,
            origin TEXT NOT NULL CHECK (origin IN ('manual', 'automatic')),
            cut_version BIGINT NOT NULL CHECK (cut_version >= 0),
            captured_at TIMESTAMPTZ NOT NULL,
            lifecycle TEXT NOT NULL CHECK (
                lifecycle IN ('creating', 'ready', 'failed', 'deleting', 'deleted')
            ),
            retention_seconds BIGINT CHECK (
                retention_seconds IS NULL OR retention_seconds > 0
            ),
            ready_at TIMESTAMPTZ,
            expires_at TIMESTAMPTZ,
            failure_code TEXT,
            failure_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, id)
        );

        CREATE TABLE IF NOT EXISTS snapshot_creation_jobs (
            vol BIGINT NOT NULL,
            snapshot_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (
                state IN ('queued', 'running', 'retry_wait', 'completed', 'failed')
            ),
            retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
            next_retry_at TIMESTAMPTZ,
            failure_code TEXT,
            failure_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, snapshot_id),
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_catalog(vol, id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS snapshot_creation_leases (
            vol BIGINT NOT NULL,
            snapshot_id TEXT NOT NULL,
            owner TEXT NOT NULL,
            lease_expires_at TIMESTAMPTZ NOT NULL,
            fencing_token BIGINT NOT NULL DEFAULT 1,
            acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            renewed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, snapshot_id),
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_creation_jobs(vol, snapshot_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS snapshot_source_checkpoints (
            vol BIGINT NOT NULL,
            snapshot_id TEXT NOT NULL,
            cut_version BIGINT NOT NULL CHECK (cut_version >= 0),
            checkpoint_version BIGINT NOT NULL CHECK (checkpoint_version >= 0),
            checkpoint_format_version SMALLINT NOT NULL,
            checkpoint_schema_version SMALLINT NOT NULL,
            checkpoint_bytes BYTEA NOT NULL,
            checkpoint_sha256 BYTEA NOT NULL CHECK (octet_length(checkpoint_sha256) = 32),
            event_start_seq BIGINT,
            event_end_seq BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, snapshot_id),
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_catalog(vol, id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS snapshot_artifacts (
            vol BIGINT NOT NULL,
            snapshot_id TEXT NOT NULL,
            object_key TEXT NOT NULL,
            format_version SMALLINT NOT NULL,
            schema_version SMALLINT NOT NULL,
            sha256 BYTEA NOT NULL CHECK (octet_length(sha256) = 32),
            byte_length BIGINT NOT NULL CHECK (byte_length >= 0),
            verified_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, snapshot_id),
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_catalog(vol, id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS snapshot_artifact_payload_proofs (
            vol BIGINT NOT NULL,
            snapshot_id TEXT NOT NULL,
            object_id BIGINT NOT NULL,
            byte_offset BIGINT NOT NULL CHECK (byte_offset >= 0),
            byte_length BIGINT NOT NULL CHECK (byte_length >= 0),
            sha256 BYTEA NOT NULL CHECK (octet_length(sha256) = 32),
            verified_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (vol, snapshot_id, object_id, byte_offset),
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_catalog(vol, id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS snapshot_artifact_attempts (
            vol BIGINT NOT NULL,
            snapshot_id TEXT NOT NULL,
            attempt BIGINT NOT NULL CHECK (attempt > 0),
            object_key TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ,
            PRIMARY KEY (vol, snapshot_id, attempt),
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_catalog(vol, id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS snapshot_mount_leases (
            lease_id BIGSERIAL PRIMARY KEY,
            vol BIGINT NOT NULL,
            snapshot_id TEXT NOT NULL,
            owner TEXT NOT NULL,
            cache_volume_uuid TEXT NOT NULL,
            cache_sha256 BYTEA NOT NULL,
            lease_expires_at TIMESTAMPTZ NOT NULL,
            heartbeat_at TIMESTAMPTZ NOT NULL,
            fencing_token BIGINT NOT NULL DEFAULT 1,
            released_at TIMESTAMPTZ,
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_catalog(vol, id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS snapshot_policies (
            vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
            generation BIGINT NOT NULL CHECK (generation > 0),
            daily_at TIME NOT NULL,
            timezone TEXT NOT NULL DEFAULT 'UTC',
            retention_seconds BIGINT NOT NULL CHECK (retention_seconds > 0),
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS snapshot_policy_slots (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            generation BIGINT NOT NULL,
            slot_date DATE NOT NULL,
            slot_time TIME NOT NULL,
            state TEXT NOT NULL,
            snapshot_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, generation, slot_date, slot_time),
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_catalog(vol, id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS snapshot_policy_leases (
            vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
            owner TEXT NOT NULL,
            lease_expires_at TIMESTAMPTZ NOT NULL,
            fencing_token BIGINT NOT NULL DEFAULT 1,
            renewed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_event_log_vol_seq
            ON event_log(vol, seq_id);
        CREATE INDEX IF NOT EXISTS idx_event_log_vol_batch_seq
            ON event_log(vol, batch_version, seq_id);
        CREATE INDEX IF NOT EXISTS idx_snapshot_catalog_vol_lifecycle
            ON snapshot_catalog(vol, lifecycle, captured_at);
        CREATE INDEX IF NOT EXISTS idx_snapshot_creation_jobs_due
            ON snapshot_creation_jobs(state, next_retry_at);
        CREATE INDEX IF NOT EXISTS idx_snapshot_mount_leases_active
            ON snapshot_mount_leases(vol, snapshot_id, lease_expires_at)
            WHERE released_at IS NULL;
    "#
}

/// Ensures the canonical Snapshot v2 schema exists for a client or transaction.
///
/// The caller owns transaction boundaries. Passing a transaction makes schema
/// creation part of that transaction; passing a client executes it directly.
/// This helper performs no v1 row migration or compatibility write.
///
/// # Errors
///
/// Returns a redacted metadata error when PostgreSQL rejects the DDL. The
/// backend error text is intentionally not included because it may contain
/// connection or credential data.
pub async fn ensure_studiofs_pg_schema<C>(client: &C) -> MetaResult<()>
where
    C: tokio_postgres::GenericClient + Sync,
{
    client
        .batch_execute(studiofs_pg_schema_sql())
        .await
        .map_err(|_| MetaError::RemoteUnavailable("postgres schema ensure failed".to_owned()))
}
