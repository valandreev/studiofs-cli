//! SFS-1005: the PostgreSQL migration registry against a live database.

use super::*;
use crate::pg_engine::migrations::{
    CORE_COMPONENT, MIGRATIONS, Migration, MigrationPhase, ensure_registered_schema, ensure_with,
    migrate_with,
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
    MIGRATIONS.iter().chain(extra).copied().collect()
}

async fn connect(dsn: &str) -> MetaResult<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| MetaError::Storage(pg_err(error)))?;
    tokio::spawn(connection);
    Ok(client)
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
    Ok(migrate_with(client, registry)
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
        let mut client = connect(&dsn).await?;

        // Fresh database: the baseline records core 1 and payload_gc 2.
        ensure_registered_schema(&client).await?;
        ensure_registered_schema(&client).await?;
        assert_eq!(
            scalar(&client, "SELECT count(*) FROM studiofs_schema_components WHERE (component, version) IN (('core', 1), ('payload_gc', 2)) AND (component = 'payload_gc' OR octet_length(checksum) = 32)").await,
            2
        );

        // A verified schema gets no implicit DDL; the explicit upgrade path
        // repairs a dropped baseline object.
        let lock_table = "SELECT count(*) FROM pg_class WHERE relname = 'file_lock_leases' AND relnamespace = current_schema()::regnamespace";
        client.batch_execute("DROP TABLE file_lock_leases").await.map_err(|e| MetaError::Storage(pg_err(e)))?;
        ensure_registered_schema(&client).await?;
        assert_eq!(scalar(&client, lock_table).await, 0);
        assert_eq!(migrate_with(&mut client, MIGRATIONS).await?.len(), MIGRATIONS.len());
        assert_eq!(scalar(&client, lock_table).await, 1);

        // A newer core is refused before any DDL runs.
        client.batch_execute("DROP TABLE file_lock_leases; UPDATE studiofs_schema_components SET version = 99 WHERE component = 'core'").await.map_err(|e| MetaError::Storage(pg_err(e)))?;
        assert!(matches!(
            ensure_registered_schema(&client).await,
            Err(MetaError::UpgradeRequired { .. })
        ));
        assert_eq!(scalar(&client, lock_table).await, 0);
        client.batch_execute("UPDATE studiofs_schema_components SET version = 1 WHERE component = 'core'").await.map_err(|e| MetaError::Storage(pg_err(e)))?;

        // Implicit provisioning refuses a pending backfill and applies nothing.
        let full = registry(&[ADDITIVE, BACKFILL, CONTRACT]);
        assert!(matches!(
            ensure_with(&client, &full).await,
            Err(MetaError::SchemaMigrationPending { version: 2, phase: "backfill", .. })
        ));
        assert_eq!(scalar(&client, "SELECT count(*) FROM pg_class WHERE relname = 'sfs1005_t' AND relnamespace = current_schema()::regnamespace").await, 0);
        ensure_with(&client, &registry(&[ADDITIVE])).await?;

        // An interrupted backfill rolls back and leaves the recorded version.
        let broken = registry(&[ADDITIVE, step(2, MigrationPhase::Backfill, "INSERT INTO sfs1005_log VALUES (2); UPDATE sfs1005_missing SET b = 1")]);
        let Err(MetaError::RemoteUnavailable(message)) = migrate_with(&mut client, &broken).await else {
            panic!("expected the broken backfill to fail");
        };
        assert!(message.contains("sfs1005 2") && message.contains("42P01"), "{message}");
        assert_eq!(scalar(&client, "SELECT version::BIGINT FROM studiofs_schema_components WHERE component = 'sfs1005'").await, 1);
        assert_eq!(scalar(&client, "SELECT count(*) FROM sfs1005_log").await, 0);

        // Retrying with the fixed step resumes; contract runs only here.
        assert_eq!(applied(&mut client, &full).await?, [2, 3]);
        assert_eq!(applied(&mut client, &full).await?, Vec::<i32>::new());
        assert_eq!(scalar(&client, "SELECT count(*) FROM sfs1005_t WHERE b = 7").await, 1);
        assert_eq!(scalar(&client, "SELECT count(*) FROM pg_attribute WHERE attrelid = 'sfs1005_t'::regclass AND attname = 'b' AND attnotnull").await, 1);

        // Recorded history that differs from the registry is refused.
        let drifted = registry(&[ADDITIVE, step(2, MigrationPhase::Backfill, "UPDATE sfs1005_t SET b = a + 1"), CONTRACT]);
        assert!(matches!(migrate_with(&mut client, &drifted).await, Err(MetaError::RepairRequired(_))));
        assert!(matches!(ensure_with(&client, &drifted).await, Err(MetaError::RepairRequired(_))));

        // Concurrent migrators serialize: the step runs once.
        let next = registry(&[ADDITIVE, BACKFILL, CONTRACT, step(4, MigrationPhase::Backfill, "INSERT INTO sfs1005_log VALUES (4)")]);
        let mut other = connect(&dsn).await?;
        let (left, right) = tokio::join!(applied(&mut client, &next), applied(&mut other, &next));
        let mut runs: Vec<i32> = left?.into_iter().chain(right?).collect();
        runs.sort_unstable();
        assert_eq!(runs, [4]);
        assert_eq!(scalar(&client, "SELECT count(*) FROM sfs1005_log WHERE version = 4").await, 1);
        assert_eq!(scalar(&client, &format!("SELECT version::BIGINT FROM studiofs_schema_components WHERE component = '{CORE_COMPONENT}'")).await, 1);
        Ok(())
    })
    .expect("migration registry scenario");
}
