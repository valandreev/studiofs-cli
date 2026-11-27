//! SFS-1005: the PostgreSQL migration registry against a live database.

use super::*;
use crate::pg_engine::migrations::{
    CHECKPOINT_ENCODINGS_COMPONENT, CLIENT_SESSIONS_COMPONENT, CORE_COMPONENT,
    EVENT_PUBLICATION_FENCE_COMPONENT, FORMAT_FENCES_COMPONENT, MIGRATIONS, Migration,
    MigrationPhase, ensure_with, migrate_with,
};

const fn step(version: i32, phase: MigrationPhase, sql: &'static str) -> Migration {
    Migration {
        component: "sfs1005",
        version,
        name: "test",
        phase,
        sql,
    }
}

const ADDITIVE: Migration = step(
    1,
    MigrationPhase::Additive,
    "CREATE TABLE sfs1005_t (a INT, b INT); CREATE TABLE sfs1005_log (version INT); INSERT INTO sfs1005_t (a) VALUES (7);",
);
const BACKFILL: Migration = step(
    2,
    MigrationPhase::Backfill,
    "UPDATE sfs1005_t SET b = a; INSERT INTO sfs1005_log VALUES (2);",
);
const CONTRACT: Migration = step(
    3,
    MigrationPhase::Contract,
    "ALTER TABLE sfs1005_t ALTER b SET NOT NULL",
);

fn registry(extra: &[Migration]) -> Vec<Migration> {
    // Exercise baseline reconciliation even in the synthetic successor build.
    MIGRATIONS
        .iter()
        .filter(|step| step.component != CORE_COMPONENT || step.version == 1)
        .chain(extra)
        .copied()
        .collect()
}

/// A client whose connection task is aborted when the guard drops.
struct Connection {
    client: tokio_postgres::Client,
    task: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn connect(dsn: &str) -> MetaResult<Connection> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| MetaError::Storage(pg_err(error)))?;
    let task = tokio::spawn(connection);
    Ok(Connection { client, task })
}

/// `ensure` the way `init` and `load` call it: in a transaction they commit.
async fn ensure_in_tx(
    client: &mut tokio_postgres::Client,
    registry: &[Migration],
) -> MetaResult<()> {
    let tx = client
        .transaction()
        .await
        .map_err(|e| MetaError::Storage(pg_err(e)))?;
    ensure_with(&tx, registry).await?;
    tx.commit().await.map_err(|e| MetaError::Storage(pg_err(e)))
}

async fn scalar(client: &tokio_postgres::Client, sql: &str) -> i64 {
    client
        .query_one(sql, &[])
        .await
        .map_err(pg_err)
        .expect(sql)
        .get(0)
}

async fn applied(
    client: &mut tokio_postgres::Client,
    registry: &[Migration],
) -> MetaResult<Vec<i32>> {
    Ok(migrate_with(client, registry, false)
        .await?
        .into_iter()
        .filter(|step| step.component == "sfs1005")
        .map(|step| step.version)
        .collect())
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn migration_registry_applies_resumes_and_refuses_drift_on_live_postgres() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this explicit test");
    let schema = IsolatedPgSchema::new(&dsn);
    let dsn = schema.dsn.clone();
    run_async(async move {
        let mut connection = connect(&dsn).await?;
        let client = &mut connection.client;

        // An existing baseline gains the independent additive components.
        let migrations = registry(&[]);
        let baseline = migrations[0];
        let migration = |component| *migrations.iter().find(|step| step.component == component).unwrap();
        let sessions = migration(CLIENT_SESSIONS_COMPONENT);
        let encodings = migration(CHECKPOINT_ENCODINGS_COMPONENT);
        let format_fences = migration(FORMAT_FENCES_COMPONENT);
        let event_fence = migration(EVENT_PUBLICATION_FENCE_COMPONENT);
        ensure_in_tx(client, &[baseline]).await?;
        client.batch_execute(
            "INSERT INTO volumes (vol, name, uuid, block_size) VALUES
                (1, 'baseline', 'sfs1014-baseline', 4096), (2, 'legacy', 'sfs1014-legacy', 4096);
             INSERT INTO metadata_state
                 (vol, metadata_format_version, version, snapshot, checkpoint_sha256, checkpoint_publication_state, checkpoint_generation)
             VALUES
                 (1, 7, 0, ''::bytea, decode(repeat('00', 32), 'hex'), 'pending', 0),
                 (2, 6, 0, ''::bytea, decode(repeat('00', 32), 'hex'), 'pending', 0);"
        ).await.map_err(|e| MetaError::Storage(pg_err(e)))?;
        crate::pg_engine::insert_initial_volume_protocol(client, 1, "existing baseline").await?;
        assert_eq!(migrate_with(client, &migrations, false).await?, [sessions, encodings, format_fences, event_fence]);
        assert_eq!(scalar(client, "SELECT publication_epoch FROM metadata_state WHERE vol = 1").await, 1);
        assert_eq!(scalar(client, "SELECT publication_epoch FROM metadata_state WHERE vol = 2").await, 0);
        ensure_in_tx(client, &migrations).await?;
        assert_eq!(
            scalar(client, "SELECT count(*) FROM studiofs_schema_components WHERE (component, version) IN (('core', 1), ('payload_gc', 2)) AND (component = 'payload_gc' OR octet_length(checksum) = 32)").await,
            2
        );

        // A verified schema gets no DDL from either path.
        let lock_table = "SELECT count(*) FROM pg_class WHERE relname = 'file_lock_leases' AND relnamespace = current_schema()::regnamespace";
        let sessions_table = "SELECT count(*) FROM pg_class WHERE relname = 'client_sessions' AND relnamespace = current_schema()::regnamespace";
        let encodings_table = "SELECT count(*) FROM pg_class WHERE relname = 'serving_checkpoint_encodings' AND relnamespace = current_schema()::regnamespace";
        client
            .batch_execute("DROP TABLE file_lock_leases; DROP TABLE client_sessions; DROP TABLE serving_checkpoint_encodings")
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        ensure_in_tx(client, &migrations).await?;
        assert!(migrate_with(client, &migrations, false).await?.is_empty());
        assert_eq!(scalar(client, lock_table).await, 0);
        assert_eq!(scalar(client, sessions_table).await, 0);
        assert_eq!(scalar(client, encodings_table).await, 0);
        // `--repair-schema` re-applies each idempotent component once.
        assert_eq!(
            migrate_with(client, &migrations, true).await?,
            [baseline, sessions, encodings, event_fence]
        );
        assert_eq!(scalar(client, lock_table).await, 1);
        assert_eq!(scalar(client, sessions_table).await, 1);
        assert_eq!(scalar(client, encodings_table).await, 1);
        // Baseline DDL (also what a 0.2.x `init` re-applies) must not touch these.
        let current_fence = "SELECT count(*) FROM pg_trigger WHERE tgrelid IN ('snapshot_payload_upload_objects'::regclass, 'metadata_state'::regclass, 'event_log'::regclass) AND tgname IN ('studiofs_deletion_claim_volume_format_fence', 'studiofs_adopted_format_downgrade_fence', 'studiofs_event_publication_fence') AND tgenabled IN ('O', 'A')";
        assert_eq!(scalar(client, current_fence).await, 3);
        client
            .batch_execute("DROP TABLE file_lock_leases")
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;

        // Upgrade reconciles a differing baseline by re-applying it, which
        // also restores the dropped table; implicit provisioning refuses it.
        client
            .batch_execute("UPDATE studiofs_schema_components SET checksum = NULL WHERE component = 'core'")
            .await
            .map_err(|e| MetaError::Storage(pg_err(e)))?;
        assert!(matches!(ensure_in_tx(client, &migrations).await, Err(MetaError::RepairRequired(_))));
        assert_eq!(migrate_with(client, &migrations, false).await?, [baseline]);
        assert_eq!(scalar(client, lock_table).await, 1);
        assert_eq!(scalar(client, current_fence).await, 3);

        // A newer core is refused before any DDL runs.
        client.batch_execute("DROP TABLE file_lock_leases; UPDATE studiofs_schema_components SET version = 99 WHERE component = 'core'").await.map_err(|e| MetaError::Storage(pg_err(e)))?;
        assert!(matches!(
            ensure_in_tx(client, &migrations).await,
            Err(MetaError::UpgradeRequired { .. })
        ));
        assert_eq!(scalar(client, lock_table).await, 0);
        client.batch_execute("UPDATE studiofs_schema_components SET version = 1 WHERE component = 'core'").await.map_err(|e| MetaError::Storage(pg_err(e)))?;

        // Implicit provisioning refuses a pending backfill and applies nothing.
        let full = registry(&[ADDITIVE, BACKFILL, CONTRACT]);
        assert!(matches!(
            ensure_in_tx(client, &full).await,
            Err(MetaError::SchemaMigrationPending { version: 2, phase: "backfill", .. })
        ));
        assert_eq!(scalar(client, "SELECT count(*) FROM pg_class WHERE relname = 'sfs1005_t' AND relnamespace = current_schema()::regnamespace").await, 0);
        ensure_in_tx(client, &registry(&[ADDITIVE])).await?;

        // An interrupted backfill rolls back and leaves the recorded version.
        let broken = registry(&[ADDITIVE, step(2, MigrationPhase::Backfill, "INSERT INTO sfs1005_log VALUES (2); UPDATE sfs1005_missing SET b = 1")]);
        let Err(MetaError::Storage(message)) = migrate_with(client, &broken, false).await else {
            panic!("expected the broken backfill to fail");
        };
        assert!(message.contains("sfs1005 2") && message.contains("42P01"), "{message}");
        assert_eq!(scalar(client, "SELECT version::BIGINT FROM studiofs_schema_components WHERE component = 'sfs1005'").await, 1);
        assert_eq!(scalar(client, "SELECT count(*) FROM sfs1005_log").await, 0);

        // Retrying with the fixed step resumes; contract runs only here.
        assert_eq!(applied(client, &full).await?, [2, 3]);
        assert_eq!(applied(client, &full).await?, Vec::<i32>::new());
        assert_eq!(scalar(client, "SELECT count(*) FROM sfs1005_t WHERE b = 7").await, 1);
        assert_eq!(scalar(client, "SELECT count(*) FROM pg_attribute WHERE attrelid = 'sfs1005_t'::regclass AND attname = 'b' AND attnotnull").await, 1);

        // Recorded history that differs from the registry is refused.
        let drifted = registry(&[ADDITIVE, step(2, MigrationPhase::Backfill, "UPDATE sfs1005_t SET b = a + 1"), CONTRACT]);
        assert!(matches!(migrate_with(client, &drifted, false).await, Err(MetaError::RepairRequired(_))));
        assert!(matches!(ensure_in_tx(client, &drifted).await, Err(MetaError::RepairRequired(_))));

        // Concurrent migrators serialize: the step runs once.
        let next = registry(&[ADDITIVE, BACKFILL, CONTRACT, step(4, MigrationPhase::Backfill, "INSERT INTO sfs1005_log VALUES (4)")]);
        let mut other = connect(&dsn).await?;
        let (left, right) = tokio::join!(applied(client, &next), applied(&mut other.client, &next));
        // The lock is database-wide; a parallel test may hold it past the 2s
        // lock_timeout, which fails one side without changes (SQLSTATE 55P03).
        let tolerate = |result: MetaResult<Vec<i32>>| match result {
            Err(MetaError::Storage(message)) if message.contains("55P03") => Ok(Vec::new()),
            other => other,
        };
        let mut runs: Vec<i32> = tolerate(left)?.into_iter().chain(tolerate(right)?).collect();
        runs.sort_unstable();
        assert_eq!(runs, [4]);
        assert_eq!(scalar(client, "SELECT count(*) FROM sfs1005_log WHERE version = 4").await, 1);
        assert_eq!(scalar(client, &format!("SELECT version::BIGINT FROM studiofs_schema_components WHERE component = '{CORE_COMPONENT}'")).await, 1);
        Ok(())
    })
    .expect("migration registry scenario");
}
