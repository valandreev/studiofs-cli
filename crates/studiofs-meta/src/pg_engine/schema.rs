use crate::engine::{MetaError, MetaResult};

/// The PostgreSQL schema generation owned by the Snapshot v2 control plane.
pub const STUDIOFS_PG_SCHEMA_VERSION: i32 = 7;

/// Returns the canonical fresh-install PostgreSQL DDL for StudioFS.
///
/// This is the single schema definition used by initialization, dump/load, and
/// PostgreSQL integration fixtures. Snapshot v2 is PostgreSQL-authoritative:
/// the catalog, durable creation queue, creation leases, daily policy, source
/// checkpoints, and artifacts are all represented by scalar rows. The legacy
/// thick snapshot table is intentionally not created.
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

        ALTER TABLE event_log
            ADD COLUMN IF NOT EXISTS batch_version BIGINT;
        UPDATE event_log
            SET batch_version = 0
            WHERE batch_version IS NULL;
        ALTER TABLE event_log
            ALTER COLUMN batch_version SET NOT NULL;

        CREATE TABLE IF NOT EXISTS metadata_state (
            vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
            metadata_format_version SMALLINT NOT NULL,
            version BIGINT NOT NULL,
            checkpoint_version BIGINT NOT NULL DEFAULT 0,
            snapshot BYTEA NOT NULL,
            checkpoint_sha256 BYTEA NOT NULL
                DEFAULT decode('0000000000000000000000000000000000000000000000000000000000000000', 'hex')
                CHECK (octet_length(checkpoint_sha256) = 32),
            format_info BYTEA NOT NULL DEFAULT decode('', 'hex'),
            checkpoint_publication_state TEXT NOT NULL DEFAULT 'published'
                CHECK (checkpoint_publication_state IN ('pending', 'published')),
            checkpoint_generation BIGINT NOT NULL DEFAULT 0
                CHECK (checkpoint_generation >= 0),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        ALTER TABLE metadata_state
            ADD COLUMN IF NOT EXISTS metadata_format_version SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE metadata_state
            ADD COLUMN IF NOT EXISTS checkpoint_version BIGINT NOT NULL DEFAULT 0;
        ALTER TABLE metadata_state
            ADD COLUMN IF NOT EXISTS checkpoint_sha256 BYTEA NOT NULL
                DEFAULT decode('0000000000000000000000000000000000000000000000000000000000000000', 'hex')
                CHECK (octet_length(checkpoint_sha256) = 32);
        ALTER TABLE metadata_state
            ADD COLUMN IF NOT EXISTS format_info BYTEA NOT NULL DEFAULT decode('', 'hex');
        ALTER TABLE metadata_state
            ADD COLUMN IF NOT EXISTS checkpoint_publication_state TEXT NOT NULL DEFAULT 'published'
                CHECK (checkpoint_publication_state IN ('pending', 'published'));
        ALTER TABLE metadata_state
            ADD COLUMN IF NOT EXISTS checkpoint_generation BIGINT NOT NULL DEFAULT 0
                CHECK (checkpoint_generation >= 0);

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
            retention_seconds BIGINT,
            ready_at TIMESTAMPTZ,
            expires_at TIMESTAMPTZ,
            failure_code TEXT,
            failure_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT snapshot_catalog_retention_bounds_check
                CHECK (retention_seconds IS NULL OR retention_seconds BETWEEN 1 AND 4294967295),
            PRIMARY KEY (vol, id)
        );

        DO $$
        DECLARE
            legacy_constraint_name TEXT;
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'snapshot_catalog'::regclass
                  AND conname = 'snapshot_catalog_automatic_retention_check'
            ) THEN
                ALTER TABLE snapshot_catalog
                    ADD CONSTRAINT snapshot_catalog_automatic_retention_check
                    CHECK (origin = 'manual' OR retention_seconds IS NOT NULL);
            END IF;
            FOR legacy_constraint_name IN
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'snapshot_catalog'::regclass
                  AND contype = 'c'
                  AND conname NOT IN (
                      'snapshot_catalog_automatic_retention_check',
                      'snapshot_catalog_retention_bounds_check'
                  )
                  AND pg_get_constraintdef(oid) LIKE '%retention_seconds%'
            LOOP
                IF EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conrelid = 'snapshot_catalog'::regclass
                      AND conname = 'snapshot_catalog_retention_bounds_check'
                ) THEN
                    EXECUTE format(
                        'ALTER TABLE snapshot_catalog DROP CONSTRAINT %I',
                        legacy_constraint_name
                    );
                ELSE
                    EXECUTE format(
                        'ALTER TABLE snapshot_catalog RENAME CONSTRAINT %I TO snapshot_catalog_retention_bounds_check',
                        legacy_constraint_name
                    );
                END IF;
            END LOOP;

            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'snapshot_catalog'::regclass
                  AND conname = 'snapshot_catalog_retention_bounds_check'
            ) THEN
                ALTER TABLE snapshot_catalog
                    ADD CONSTRAINT snapshot_catalog_retention_bounds_check
                    CHECK (retention_seconds IS NULL OR retention_seconds BETWEEN 1 AND 4294967295);
            END IF;
        END
        $$;

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

        CREATE TABLE IF NOT EXISTS snapshot_cleanup_leases (
            vol BIGINT NOT NULL,
            snapshot_id TEXT NOT NULL,
            owner TEXT NOT NULL,
            lease_expires_at TIMESTAMPTZ NOT NULL,
            fencing_token BIGINT NOT NULL DEFAULT 1,
            acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            renewed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, snapshot_id),
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_catalog(vol, id) ON DELETE CASCADE
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
            event_start_version BIGINT,
            event_end_version BIGINT,
            event_row_count BIGINT NOT NULL DEFAULT 0 CHECK (event_row_count >= 0),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, snapshot_id),
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_catalog(vol, id)
        );

        ALTER TABLE snapshot_source_checkpoints
            ADD COLUMN IF NOT EXISTS event_start_version BIGINT;
        ALTER TABLE snapshot_source_checkpoints
            ADD COLUMN IF NOT EXISTS event_end_version BIGINT;
        ALTER TABLE snapshot_source_checkpoints
            ADD COLUMN IF NOT EXISTS event_row_count BIGINT NOT NULL DEFAULT 0;

        DO $$
        DECLARE
            constraint_name TEXT;
        BEGIN
            FOR constraint_name IN
                SELECT pg_constraint.conname
                FROM pg_constraint
                WHERE pg_constraint.conrelid = 'snapshot_source_checkpoints'::regclass
                  AND pg_constraint.confrelid = 'snapshot_catalog'::regclass
                  AND pg_constraint.contype = 'f'
                  AND pg_constraint.conname <> 'snapshot_source_checkpoints_catalog_fk'
            LOOP
                EXECUTE format(
                    'ALTER TABLE snapshot_source_checkpoints DROP CONSTRAINT %I',
                    constraint_name
                );
            END LOOP;
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'snapshot_source_checkpoints'::regclass
                  AND conname = 'snapshot_source_checkpoints_catalog_fk'
            ) THEN
                ALTER TABLE snapshot_source_checkpoints
                    ADD CONSTRAINT snapshot_source_checkpoints_catalog_fk
                    FOREIGN KEY (vol, snapshot_id)
                    REFERENCES snapshot_catalog(vol, id);
            END IF;
        END
        $$;

        CREATE OR REPLACE FUNCTION studiofs_reject_snapshot_source_event_prune()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'TRUNCATE' THEN
                IF EXISTS (
                    SELECT 1
                    FROM snapshot_source_checkpoints AS s
                    INNER JOIN snapshot_catalog AS c
                        ON c.vol = s.vol AND c.id = s.snapshot_id
                    WHERE c.lifecycle = 'creating'
                      AND s.event_row_count > 0
                ) THEN
                    RAISE EXCEPTION 'event rows are retained by a creating snapshot'
                        USING ERRCODE = 'restrict_violation';
                END IF;
                RETURN NULL;
            END IF;
            IF EXISTS (
                SELECT 1
                FROM snapshot_source_checkpoints AS s
                INNER JOIN snapshot_catalog AS c
                    ON c.vol = s.vol AND c.id = s.snapshot_id
                WHERE c.lifecycle = 'creating'
                  AND s.vol = OLD.vol
                  AND s.event_row_count > 0
                  AND s.event_start_seq IS NOT NULL
                  AND s.event_end_seq IS NOT NULL
                  AND OLD.seq_id BETWEEN s.event_start_seq AND s.event_end_seq
                  AND OLD.batch_version > s.checkpoint_version
                  AND OLD.batch_version <= s.cut_version
            ) THEN
                RAISE EXCEPTION 'event row is retained by a creating snapshot'
                    USING ERRCODE = 'restrict_violation';
            END IF;
            RETURN OLD;
        END
        $$;

        DROP TRIGGER IF EXISTS studiofs_snapshot_source_event_prune_guard ON event_log;
        CREATE TRIGGER studiofs_snapshot_source_event_prune_guard
            BEFORE DELETE ON event_log
            FOR EACH ROW
            EXECUTE FUNCTION studiofs_reject_snapshot_source_event_prune();
        DROP TRIGGER IF EXISTS studiofs_snapshot_source_event_truncate_guard ON event_log;
        CREATE TRIGGER studiofs_snapshot_source_event_truncate_guard
            BEFORE TRUNCATE ON event_log
            FOR EACH STATEMENT
            EXECUTE FUNCTION studiofs_reject_snapshot_source_event_prune();

        CREATE OR REPLACE FUNCTION studiofs_reject_snapshot_source_prune()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'TRUNCATE' THEN
                IF EXISTS (
                    SELECT 1
                    FROM snapshot_source_checkpoints AS s
                    INNER JOIN snapshot_catalog AS c
                        ON c.vol = s.vol AND c.id = s.snapshot_id
                    WHERE c.lifecycle = 'creating'
                       OR EXISTS (
                           SELECT 1
                           FROM snapshot_creation_leases AS l
                           WHERE l.vol = s.vol
                             AND l.snapshot_id = s.snapshot_id
                             AND l.owner <> ''
                             AND l.lease_expires_at > clock_timestamp()
                       )
                       OR EXISTS (
                           SELECT 1
                           FROM snapshot_mount_leases AS l
                           WHERE l.vol = s.vol
                             AND l.snapshot_id = s.snapshot_id
                             AND l.released_at IS NULL
                             AND l.lease_expires_at > clock_timestamp()
                       )
                ) THEN
                    RAISE EXCEPTION 'snapshot sources are still referenced'
                        USING ERRCODE = 'restrict_violation';
                END IF;
                RETURN NULL;
            END IF;
            IF EXISTS (
                SELECT 1
                FROM snapshot_catalog AS c
                WHERE c.vol = OLD.vol
                  AND c.id = OLD.snapshot_id
                  AND (
                      c.lifecycle = 'creating'
                      OR EXISTS (
                          SELECT 1
                          FROM snapshot_creation_leases AS l
                          WHERE l.vol = OLD.vol
                            AND l.snapshot_id = OLD.snapshot_id
                            AND l.owner <> ''
                            AND l.lease_expires_at > clock_timestamp()
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM snapshot_mount_leases AS l
                          WHERE l.vol = OLD.vol
                            AND l.snapshot_id = OLD.snapshot_id
                            AND l.released_at IS NULL
                            AND l.lease_expires_at > clock_timestamp()
                      )
                  )
            ) THEN
                RAISE EXCEPTION 'snapshot source is still referenced'
                    USING ERRCODE = 'restrict_violation';
            END IF;
            RETURN OLD;
        END
        $$;

        DROP TRIGGER IF EXISTS studiofs_snapshot_source_prune_guard
            ON snapshot_source_checkpoints;
        CREATE TRIGGER studiofs_snapshot_source_prune_guard
            BEFORE DELETE ON snapshot_source_checkpoints
            FOR EACH ROW
            EXECUTE FUNCTION studiofs_reject_snapshot_source_prune();
        DROP TRIGGER IF EXISTS studiofs_snapshot_source_truncate_guard
            ON snapshot_source_checkpoints;
        CREATE TRIGGER studiofs_snapshot_source_truncate_guard
            BEFORE TRUNCATE ON snapshot_source_checkpoints
            FOR EACH STATEMENT
            EXECUTE FUNCTION studiofs_reject_snapshot_source_prune();

        CREATE TABLE IF NOT EXISTS snapshot_current_references (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            ino BIGINT NOT NULL,
            object_id BIGINT NOT NULL CHECK (object_id >= 0),
            PRIMARY KEY (vol, ino, object_id)
        );

        CREATE TABLE IF NOT EXISTS snapshot_object_reachability (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            object_id BIGINT NOT NULL CHECK (object_id >= 0),
            start_version BIGINT NOT NULL CHECK (start_version >= 0),
            end_version BIGINT,
            CHECK (end_version IS NULL OR end_version > start_version),
            PRIMARY KEY (vol, object_id, start_version)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_object_reachability_open
            ON snapshot_object_reachability(vol, object_id)
            WHERE end_version IS NULL;

        CREATE TABLE IF NOT EXISTS snapshot_reachability_batches (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            batch_version BIGINT NOT NULL CHECK (batch_version >= 0),
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, batch_version)
        );

        CREATE TABLE IF NOT EXISTS snapshot_reachability_bootstrap (
            vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
            initialized_version BIGINT NOT NULL CHECK (initialized_version >= 0),
            initialized_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS snapshot_payload_upload_objects (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            object_id BIGINT NOT NULL CHECK (object_id >= 0),
            state TEXT NOT NULL CHECK (
                state IN ('uploading', 'uploaded', 'published', 'delete_pending', 'retry_wait', 'deleted')
            ),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            reconcile_owner TEXT,
            reconcile_lease_expires_at TIMESTAMPTZ,
            reconcile_fence BIGINT NOT NULL DEFAULT 0 CHECK (reconcile_fence >= 0),
            next_retry_at TIMESTAMPTZ,
            publish_batch_version BIGINT,
            closed_at TIMESTAMPTZ,
            last_error TEXT,
            PRIMARY KEY (vol, object_id)
        );

        CREATE TABLE IF NOT EXISTS snapshot_payload_upload_fragments (
            vol BIGINT NOT NULL,
            object_id BIGINT NOT NULL,
            fragment_index INTEGER NOT NULL CHECK (fragment_index >= 0),
            object_key TEXT NOT NULL,
            expected_byte_length BIGINT NOT NULL CHECK (expected_byte_length >= 0),
            expected_sha256 BYTEA NOT NULL CHECK (octet_length(expected_sha256) = 32),
            uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            confirmed_at TIMESTAMPTZ,
            confirmation_state TEXT NOT NULL CHECK (confirmation_state IN ('pending', 'confirmed')),
            last_error TEXT,
            PRIMARY KEY (vol, object_id, fragment_index),
            FOREIGN KEY (vol, object_id)
                REFERENCES snapshot_payload_upload_objects(vol, object_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS snapshot_payload_reconcile_cursors (
            vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
            object_id BIGINT NOT NULL DEFAULT 0 CHECK (object_id >= 0),
            fragment_index INTEGER NOT NULL DEFAULT 0 CHECK (fragment_index >= 0),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
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

        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'snapshot_artifact_payload_proofs'
                  AND column_name = 'offset'
            ) AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'snapshot_artifact_payload_proofs'
                  AND column_name = 'byte_offset'
            ) THEN
                ALTER TABLE snapshot_artifact_payload_proofs
                    RENAME COLUMN "offset" TO byte_offset;
            END IF;
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'snapshot_artifact_payload_proofs'
                  AND column_name = 'length'
            ) AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'snapshot_artifact_payload_proofs'
                  AND column_name = 'byte_length'
            ) THEN
                ALTER TABLE snapshot_artifact_payload_proofs
                    RENAME COLUMN "length" TO byte_length;
            END IF;
        END
        $$;

        ALTER TABLE snapshot_artifact_payload_proofs
            ADD COLUMN IF NOT EXISTS byte_offset BIGINT;
        UPDATE snapshot_artifact_payload_proofs
            SET byte_offset = 0
            WHERE byte_offset IS NULL;
        ALTER TABLE snapshot_artifact_payload_proofs
            ALTER COLUMN byte_offset SET NOT NULL;

        DO $$
        DECLARE
            primary_key_name TEXT;
        BEGIN
            SELECT tc.constraint_name
            INTO primary_key_name
            FROM information_schema.table_constraints AS tc
            WHERE tc.table_schema = current_schema()
              AND tc.table_name = 'snapshot_artifact_payload_proofs'
              AND tc.constraint_type = 'PRIMARY KEY'
              AND NOT EXISTS (
                  SELECT 1
                  FROM information_schema.key_column_usage
                  WHERE table_schema = current_schema()
                    AND table_name = 'snapshot_artifact_payload_proofs'
                    AND constraint_name = tc.constraint_name
                    AND column_name = 'byte_offset'
              );
            IF primary_key_name IS NOT NULL THEN
                EXECUTE format(
                    'ALTER TABLE snapshot_artifact_payload_proofs DROP CONSTRAINT %I',
                    primary_key_name
                );
                ALTER TABLE snapshot_artifact_payload_proofs
                    ADD CONSTRAINT snapshot_artifact_payload_proofs_pkey
                    PRIMARY KEY (vol, snapshot_id, object_id, byte_offset);
            END IF;
        END
        $$;

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
            latest_completed_slot TIMESTAMPTZ,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        ALTER TABLE snapshot_policies
            ADD COLUMN IF NOT EXISTS latest_completed_slot TIMESTAMPTZ;

        CREATE TABLE IF NOT EXISTS snapshot_policy_slots (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            generation BIGINT NOT NULL CHECK (generation > 0),
            local_date DATE NOT NULL,
            daily_at TIME NOT NULL,
            slot_kind TEXT NOT NULL CHECK (slot_kind IN ('due', 'skipped_dst_gap')),
            due_at TIMESTAMPTZ,
            snapshot_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, generation, local_date, daily_at),
            FOREIGN KEY (vol, snapshot_id)
                REFERENCES snapshot_catalog(vol, id) ON DELETE CASCADE,
            CHECK (
                (slot_kind = 'due' AND due_at IS NOT NULL)
                OR (slot_kind = 'skipped_dst_gap' AND due_at IS NULL AND snapshot_id IS NULL)
            )
        );

        CREATE TABLE IF NOT EXISTS snapshot_policy_evaluator_leases (
            vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
            owner TEXT NOT NULL,
            lease_expires_at TIMESTAMPTZ NOT NULL,
            fencing_token BIGINT NOT NULL DEFAULT 1,
            acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            renewed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_event_log_vol_seq
            ON event_log(vol, seq_id);
        CREATE INDEX IF NOT EXISTS idx_event_log_vol_batch_seq
            ON event_log(vol, batch_version, seq_id);
        CREATE INDEX IF NOT EXISTS idx_snapshot_catalog_vol_lifecycle
            ON snapshot_catalog(vol, lifecycle, captured_at);
        CREATE INDEX IF NOT EXISTS idx_snapshot_catalog_expiry_queue
            ON snapshot_catalog(vol, updated_at, id)
            WHERE lifecycle = 'ready' AND ready_at IS NOT NULL AND expires_at IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_snapshot_creation_jobs_due
            ON snapshot_creation_jobs(state, next_retry_at);
        CREATE INDEX IF NOT EXISTS idx_snapshot_creation_jobs_queue
            ON snapshot_creation_jobs(vol, created_at, snapshot_id)
            WHERE state IN ('queued', 'retry_wait', 'running');
        CREATE INDEX IF NOT EXISTS idx_snapshot_policy_slots_snapshot_id
            ON snapshot_policy_slots(vol, snapshot_id)
            WHERE snapshot_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_snapshot_policy_evaluator_leases_active
            ON snapshot_policy_evaluator_leases(vol, lease_expires_at)
            WHERE owner <> '';
        CREATE INDEX IF NOT EXISTS idx_snapshot_cleanup_leases_active
            ON snapshot_cleanup_leases(vol, snapshot_id, lease_expires_at)
            WHERE owner <> '';
        CREATE INDEX IF NOT EXISTS idx_snapshot_mount_leases_active
            ON snapshot_mount_leases(vol, snapshot_id, lease_expires_at)
            WHERE released_at IS NULL;
        CREATE INDEX IF NOT EXISTS idx_snapshot_current_references_object
            ON snapshot_current_references(vol, object_id);
        CREATE INDEX IF NOT EXISTS idx_snapshot_object_reachability_cut
            ON snapshot_object_reachability(vol, start_version, end_version, object_id);
        CREATE INDEX IF NOT EXISTS idx_snapshot_payload_upload_fragments_due
            ON snapshot_payload_upload_fragments(vol, uploaded_at, object_id, fragment_index);
        CREATE INDEX IF NOT EXISTS idx_snapshot_payload_upload_objects_reconcile
            ON snapshot_payload_upload_objects(vol, next_retry_at, created_at, object_id)
            WHERE state IN ('uploading', 'uploaded', 'retry_wait');
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_declares_incompatible_sync_boundary_and_checkpoint_descriptor() {
        let sql = studiofs_pg_schema_sql();
        for fragment in [
            "metadata_format_version SMALLINT NOT NULL",
            "checkpoint_sha256 BYTEA NOT NULL",
            "checkpoint_publication_state TEXT NOT NULL",
            "checkpoint_generation BIGINT NOT NULL",
        ] {
            assert!(sql.contains(fragment), "schema is missing {fragment}");
        }
        assert!(sql.contains("octet_length(checkpoint_sha256) = 32"));
        assert!(sql.contains("checkpoint_publication_state IN ('pending', 'published')"));
        assert!(sql.contains("checkpoint_generation >= 0"));
        assert!(
            sql.matches("CHECK (octet_length(checkpoint_sha256) = 32)")
                .count()
                >= 2
        );
        assert!(
            sql.matches("CHECK (checkpoint_publication_state IN ('pending', 'published'))")
                .count()
                >= 2
        );
        assert!(sql.matches("CHECK (checkpoint_generation >= 0)").count() >= 2);
    }
}
