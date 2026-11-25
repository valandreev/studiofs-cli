//! Ordered, checksummed PostgreSQL schema migrations (SFS-1005).
//!
//! The registry extends `studiofs_schema_components` (SFS-980) instead of
//! adding a second version table. Each row records a component's highest
//! applied version and a SHA-256 chained over every migration of that
//! component up to it, so a database whose history differs from this binary's
//! registry is refused as drift. A migration commits together with its marker,
//! so an interrupted run leaves the last committed version and resumes from
//! the next step. Every run holds [`SCHEMA_ADVISORY_LOCK`](crate::pg_engine::migrations::SCHEMA_ADVISORY_LOCK), so concurrent
//! migrators serialize.
//!
//! `core` version 1 is the canonical 0.2.5 baseline ([`studiofs_pg_schema_sql`](crate::pg_engine::schema::studiofs_pg_schema_sql)).
//! `init` and `load` apply only pending additive steps
//! ([`ensure_registered_schema`](crate::pg_engine::migrations::ensure_registered_schema)) and run no DDL against a verified schema.
//! `studiofs upgrade` ([`migrate_registered_schema`](crate::pg_engine::migrations::migrate_registered_schema)) also runs backfill and
//! contract steps and, while nothing is recorded above the baseline,
//! reconciles a baseline whose checksum differs by re-applying it (it is
//! idempotent). Every migration applies exactly once otherwise. Mounts run no
//! DDL.

use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;

use super::schema::studiofs_pg_schema_sql;
use crate::engine::{MetaError, MetaResult, UpgradeRequiredReason};
use crate::version::POSTGRES_PAYLOAD_GC_SCHEMA;

/// Transaction-scoped advisory lock key that serializes every schema change
/// and volume provisioning (`init`, `load`, `upgrade`).
pub const SCHEMA_ADVISORY_LOCK: i64 = 902_202_603;

/// Database-wide component that the baseline and its successors version.
pub const CORE_COMPONENT: &str = "core";

/// Recorded by the baseline itself and checked exactly by the CLI Janitor.
const PAYLOAD_GC_COMPONENT: &str = "payload_gc";

/// Kind of change a migration makes; only additive steps run implicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    /// New tables, nullable or defaulted columns, indexes; safe for running clients.
    Additive,
    /// Rewrites existing rows.
    Backfill,
    /// Tightens or removes something older clients may still need.
    Contract,
}

impl MigrationPhase {
    /// Stable lowercase name for diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Additive => "additive",
            Self::Backfill => "backfill",
            Self::Contract => "contract",
        }
    }
}

/// One numbered schema step of a component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Migration {
    /// Component row in `studiofs_schema_components`.
    pub component: &'static str,
    /// Version recorded once the step commits; versions start at 1 and increase by 1.
    pub version: i32,
    /// Short human-readable name.
    pub name: &'static str,
    /// Kind of change.
    pub phase: MigrationPhase,
    /// SQL run in the step's transaction.
    pub sql: &'static str,
}

impl std::fmt::Display for Migration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} {} {} ({})",
            self.component,
            self.version,
            self.name,
            self.phase.as_str()
        )
    }
}

// Tagged additive: its UPDATE and SET NOT NULL statements only converge
// pre-0.2.5 layouts and are no-ops on its own layout. `ensure` applies it only
// to a database that has no `core` row yet.
const BASELINE: Migration = Migration {
    component: CORE_COMPONENT,
    version: 1,
    name: "0.2.5 baseline",
    phase: MigrationPhase::Additive,
    sql: studiofs_pg_schema_sql(),
};

/// Test-successor change (SFS-1131): one additive table that 0.2.5 ignores.
#[cfg(feature = "test-successor")]
const SUCCESSOR_PROBE: Migration = Migration {
    component: CORE_COMPONENT,
    version: 2,
    name: "test successor probe table",
    phase: MigrationPhase::Additive,
    sql: "CREATE TABLE IF NOT EXISTS studiofs_successor_probe (
              vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
              note TEXT
          );",
};

/// Every migration this binary knows, grouped by component in version order.
#[cfg(not(feature = "test-successor"))]
pub const MIGRATIONS: &[Migration] = &[BASELINE];
/// Every migration this binary knows, grouped by component in version order.
#[cfg(feature = "test-successor")]
pub const MIGRATIONS: &[Migration] = &[BASELINE, SUCCESSOR_PROBE];

/// A `studiofs_schema_components` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedComponent {
    /// Component name.
    pub component: String,
    /// Highest applied version.
    pub version: i32,
    /// Chained checksum; `None` for rows written before SFS-1005.
    pub checksum: Option<Vec<u8>>,
}

const BOOTSTRAP_SQL: &str = "
    CREATE TABLE IF NOT EXISTS studiofs_schema_components (
        component TEXT PRIMARY KEY,
        version INTEGER NOT NULL CHECK (version > 0),
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    ALTER TABLE studiofs_schema_components ADD COLUMN IF NOT EXISTS checksum BYTEA;
";

// `<=` lets the baseline re-application refresh its own row; never lowers a version.
const RECORD_SQL: &str = "
    INSERT INTO studiofs_schema_components (component, version, checksum)
    VALUES ($1, $2, $3)
    ON CONFLICT (component) DO UPDATE
        SET version = EXCLUDED.version, checksum = EXCLUDED.checksum, applied_at = now()
        WHERE studiofs_schema_components.version <= EXCLUDED.version
";

/// Chained checksum of `component` migrations 1..=`version` in `registry`.
fn chain_checksum(registry: &[Migration], component: &str, version: i32) -> Vec<u8> {
    registry
        .iter()
        .filter(|step| step.component == component && step.version <= version)
        .fold(vec![0_u8; 32], |previous, step| {
            let mut hasher = Sha256::new();
            hasher.update(&previous);
            hasher.update(step.version.to_be_bytes());
            hasher.update(step.sql.as_bytes());
            hasher.finalize().to_vec()
        })
}

fn latest_version(registry: &[Migration], component: &str) -> i32 {
    registry
        .iter()
        .filter(|step| step.component == component)
        .map(|step| step.version)
        .max()
        .unwrap_or(0)
}

/// Steps still to run for `recorded`, in order. With `reconcile`, a recorded
/// `core` baseline whose checksum differs is listed again instead of refused.
/// Refuses a newer or unknown component and any other recorded history that
/// differs from `registry`.
fn plan(
    recorded: &[RecordedComponent],
    registry: &[Migration],
    reconcile: bool,
) -> MetaResult<Vec<Migration>> {
    let recorded_version = |component: &str| {
        recorded
            .iter()
            .find(|row| row.component == component)
            .map_or(0, |row| row.version)
    };
    let mut pending = Vec::new();
    for row in recorded {
        let known = if row.component == PAYLOAD_GC_COMPONENT {
            i32::from(POSTGRES_PAYLOAD_GC_SCHEMA.current)
        } else {
            latest_version(registry, &row.component)
        };
        if row.version > known {
            return Err(MetaError::UpgradeRequired {
                reason: UpgradeRequiredReason::SchemaComponentNewer {
                    component: row.component.clone(),
                    found: row.version,
                    known,
                },
            });
        }
        if row.component == PAYLOAD_GC_COMPONENT {
            continue;
        }
        let matches = row.checksum.as_deref()
            == Some(chain_checksum(registry, &row.component, row.version).as_slice());
        let baseline = row.component == CORE_COMPONENT && row.version == BASELINE.version;
        if matches {
            continue;
        }
        if reconcile && baseline {
            // Nothing is built on the baseline yet, so re-applying it is safe.
            pending.push(BASELINE);
        } else {
            let remedy = if baseline {
                "stop mounts and janitors and run `studiofs upgrade NAME` to reconcile the baseline"
            } else {
                "use the StudioFS release that applied it, or restore the database from backup"
            };
            return Err(MetaError::RepairRequired(format!(
                "PostgreSQL schema component `{}` version {} was applied by a migration history this binary does not match; nothing was changed. To continue, {remedy}",
                row.component, row.version
            )));
        }
    }
    pending.extend(
        registry
            .iter()
            .filter(|step| step.version > recorded_version(step.component)),
    );
    Ok(pending)
}

fn pg_failure(context: &str, error: &tokio_postgres::Error) -> MetaError {
    // Backend text may echo connection data, so only the SQLSTATE is kept.
    // A statement PostgreSQL rejected is a storage error, not lost connectivity.
    match error.code() {
        Some(code) => MetaError::Storage(format!("{context} failed (SQLSTATE {})", code.code())),
        None => MetaError::RemoteUnavailable(format!("{context} failed")),
    }
}

/// Reads the registry rows without creating anything; an absent table or a
/// pre-SFS-1005 table without `checksum` reads as empty or `None` checksums.
/// The flag is false when the registry table needs [`BOOTSTRAP_SQL`].
async fn read_recorded<C>(client: &C) -> MetaResult<(Vec<RecordedComponent>, bool)>
where
    C: GenericClient + Sync,
{
    let shape = client
        .query_one(
            "SELECT to_regclass('studiofs_schema_components') IS NOT NULL,
                    EXISTS (SELECT 1 FROM pg_attribute
                            WHERE attrelid = to_regclass('studiofs_schema_components')
                              AND attname = 'checksum' AND NOT attisdropped)",
            &[],
        )
        .await
        .map_err(|error| pg_failure("read schema components", &error))?;
    let has_checksum: bool = shape.get(1);
    if !shape.get::<_, bool>(0) {
        return Ok((Vec::new(), false));
    }
    let query = if has_checksum {
        "SELECT component, version, checksum FROM studiofs_schema_components ORDER BY component"
    } else {
        "SELECT component, version, NULL::BYTEA FROM studiofs_schema_components ORDER BY component"
    };
    let rows = client
        .query(query, &[])
        .await
        .map_err(|error| pg_failure("read schema components", &error))?
        .iter()
        .map(|row| {
            Ok(RecordedComponent {
                component: row.try_get(0)?,
                version: row.try_get(1)?,
                checksum: row.try_get(2)?,
            })
        })
        .collect::<Result<_, tokio_postgres::Error>>()
        .map_err(|error| pg_failure("decode schema components", &error))?;
    Ok((rows, has_checksum))
}

async fn lock_and_read<C>(client: &C) -> MetaResult<Vec<RecordedComponent>>
where
    C: GenericClient + Sync,
{
    client
        .execute("SELECT pg_advisory_xact_lock($1)", &[&SCHEMA_ADVISORY_LOCK])
        .await
        .map_err(|error| pg_failure("acquire schema migration lock", &error))?;
    let (recorded, complete) = read_recorded(client).await?;
    if complete {
        return Ok(recorded);
    }
    client
        .batch_execute(BOOTSTRAP_SQL)
        .await
        .map_err(|error| pg_failure("create schema component registry", &error))?;
    Ok(read_recorded(client).await?.0)
}

async fn apply<C>(client: &C, step: &Migration, registry: &[Migration]) -> MetaResult<()>
where
    C: GenericClient + Sync,
{
    let context = format!("PostgreSQL schema migration {step}");
    client
        .batch_execute(step.sql)
        .await
        .map_err(|error| pg_failure(&context, &error))?;
    let checksum = chain_checksum(registry, step.component, step.version);
    client
        .execute(RECORD_SQL, &[&step.component, &step.version, &checksum])
        .await
        .map_err(|error| pg_failure(&context, &error))?;
    Ok(())
}

/// Applies this binary's pending additive migrations: the implicit DDL of
/// `init` and `load`.
///
/// It takes a transaction because the schema lock is transaction-scoped: the
/// steps commit together with their markers and the caller's work, and the
/// lock is held until the transaction ends.
///
/// # Errors
///
/// `UpgradeRequired` when the database records a newer or unknown component,
/// `RepairRequired` on history drift, `SchemaMigrationPending` when a backfill
/// or contract step is pending (nothing is applied then), and a redacted
/// `Storage` or `RemoteUnavailable` error when PostgreSQL rejects a statement
/// or cannot be reached.
pub async fn ensure_registered_schema(tx: &tokio_postgres::Transaction<'_>) -> MetaResult<()> {
    ensure_with(tx, MIGRATIONS).await
}

pub(crate) async fn ensure_with(
    client: &tokio_postgres::Transaction<'_>,
    registry: &[Migration],
) -> MetaResult<()> {
    // A verified baseline is not re-applied: no DDL, so no table locks behind live writers.
    let pending = plan(&lock_and_read(client).await?, registry, false)?;
    if let Some(step) = pending
        .iter()
        .find(|step| step.phase != MigrationPhase::Additive)
    {
        return Err(MetaError::SchemaMigrationPending {
            component: step.component,
            version: step.version,
            name: step.name,
            phase: step.phase.as_str(),
        });
    }
    for step in &pending {
        apply(client, step, registry).await?;
    }
    Ok(())
}

/// Applies every pending migration, each in its own transaction with its
/// marker, and returns the applied steps. Used by `studiofs upgrade`.
///
/// A failure leaves earlier steps committed and the failed step rolled back;
/// rerunning resumes from it. A concurrent migrator waits for the lock up to
/// two seconds and then fails without changes.
///
/// # Errors
///
/// As [`ensure_registered_schema`], except that backfill and contract steps
/// are applied instead of refused.
///
/// `repair_baseline` re-applies the idempotent baseline once to restore
/// dropped objects, only while nothing is recorded above it.
pub async fn migrate_registered_schema(
    client: &mut tokio_postgres::Client,
    repair_baseline: bool,
) -> MetaResult<Vec<Migration>> {
    migrate_with(client, MIGRATIONS, repair_baseline).await
}

/// Puts the baseline first when a repair was asked for and nothing is built on it.
fn with_repair(
    recorded: &[RecordedComponent],
    mut pending: Vec<Migration>,
    repair: bool,
) -> Vec<Migration> {
    let at_baseline = recorded
        .iter()
        .any(|row| row.component == CORE_COMPONENT && row.version == BASELINE.version);
    if repair && at_baseline && !pending.contains(&BASELINE) {
        pending.insert(0, BASELINE);
    }
    pending
}

pub(crate) async fn migrate_with(
    client: &mut tokio_postgres::Client,
    registry: &[Migration],
    mut repair_baseline: bool,
) -> MetaResult<Vec<Migration>> {
    let mut applied: Vec<Migration> = Vec::new();
    loop {
        let tx = client
            .transaction()
            .await
            .map_err(|error| pg_failure("start schema migration", &error))?;
        tx.batch_execute("SET LOCAL lock_timeout = '2s'")
            .await
            .map_err(|error| pg_failure("start schema migration", &error))?;
        let recorded = lock_and_read(&tx).await?;
        let pending = plan(&recorded, registry, true)?;
        let next = with_repair(&recorded, pending, std::mem::take(&mut repair_baseline))
            .into_iter()
            .next();
        let Some(step) = next else {
            tx.commit()
                .await
                .map_err(|error| pg_failure("finish schema migration", &error))?;
            return Ok(applied);
        };
        apply(&tx, &step, registry).await?;
        tx.commit()
            .await
            .map_err(|error| pg_failure("commit schema migration", &error))?;
        applied.push(step);
    }
}

/// Read-only preflight for `studiofs upgrade --dry-run`: the recorded
/// components and the steps a migration would run. Creates nothing.
///
/// # Errors
///
/// Same refusals as [`ensure_registered_schema`] for newer or drifted history.
pub async fn registered_schema_status<C>(
    client: &C,
    repair_baseline: bool,
) -> MetaResult<(Vec<RecordedComponent>, Vec<Migration>)>
where
    C: GenericClient + Sync,
{
    let (recorded, _) = read_recorded(client).await?;
    let pending = plan(&recorded, MIGRATIONS, true)?;
    let pending = with_repair(&recorded, pending, repair_baseline);
    Ok((recorded, pending))
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn step(version: i32, phase: MigrationPhase, sql: &'static str) -> Migration {
        Migration {
            component: "sfs1005",
            version,
            name: "test",
            phase,
            sql,
        }
    }

    const TEST: &[Migration] = &[
        step(1, MigrationPhase::Additive, "CREATE TABLE t (a INT, b INT)"),
        step(2, MigrationPhase::Backfill, "UPDATE t SET b = a"),
        step(
            3,
            MigrationPhase::Contract,
            "ALTER TABLE t ALTER b SET NOT NULL",
        ),
    ];

    fn row(component: &str, version: i32, registry: &[Migration]) -> RecordedComponent {
        RecordedComponent {
            component: component.to_owned(),
            version,
            checksum: Some(chain_checksum(registry, component, version)),
        }
    }

    #[test]
    fn registry_is_ordered_contiguous_and_matches_the_core_identity() {
        let versions: Vec<i32> = MIGRATIONS
            .iter()
            .filter(|step| step.component == CORE_COMPONENT)
            .map(|step| step.version)
            .collect();
        assert_eq!(versions.len(), MIGRATIONS.len(), "only core is registered");
        assert!(
            versions
                .iter()
                .zip(1..)
                .all(|(version, expected)| *version == expected)
        );
        assert_eq!(
            latest_version(MIGRATIONS, CORE_COMPONENT),
            i32::from(crate::version::POSTGRES_CORE_SCHEMA.current)
        );
        assert_eq!(MIGRATIONS[0], BASELINE);
    }

    /// The baseline is frozen: a schema change is a new migration, not an edit.
    #[test]
    fn baseline_checksum_is_pinned() {
        let digest = chain_checksum(&[BASELINE], CORE_COMPONENT, 1);
        let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            hex,
            "b4892a10cc5baaa989eaa7db891a85778c340e70ad49e2fef8cdcdc31809f392"
        );
    }

    #[test]
    fn empty_database_plans_every_step_in_order() {
        assert_eq!(plan(&[], TEST, true).unwrap(), TEST);
    }

    #[test]
    fn recorded_history_resumes_after_the_last_committed_step() {
        assert_eq!(
            plan(&[row("sfs1005", 1, TEST)], TEST, false).unwrap(),
            &TEST[1..]
        );
        assert!(
            plan(&[row("sfs1005", 3, TEST)], TEST, true)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn newer_or_unknown_components_require_an_upgrade() {
        for (recorded, known) in [
            (row("sfs1005", 4, TEST), 3),
            (row("future", 1, TEST), 0),
            (row(PAYLOAD_GC_COMPONENT, 3, TEST), 2),
        ] {
            let Err(MetaError::UpgradeRequired {
                reason: UpgradeRequiredReason::SchemaComponentNewer { known: found, .. },
            }) = plan(&[recorded], TEST, true)
            else {
                panic!("expected a newer-component refusal");
            };
            assert_eq!(found, known);
        }
    }

    #[test]
    fn drifted_or_unchecksummed_history_is_refused() {
        let mut edited = TEST.to_vec();
        edited[1].sql = "UPDATE t SET b = a + 1";
        assert!(matches!(
            plan(&[row("sfs1005", 2, TEST)], &edited, false),
            Err(MetaError::RepairRequired(_))
        ));
        // History below the drifted step still verifies.
        assert_eq!(
            plan(&[row("sfs1005", 1, TEST)], &edited, false).unwrap(),
            &edited[1..]
        );
        let legacy = RecordedComponent {
            checksum: None,
            ..row("sfs1005", 1, TEST)
        };
        assert!(matches!(
            plan(&[legacy], TEST, true),
            Err(MetaError::RepairRequired(_))
        ));
    }

    #[test]
    fn only_a_differing_baseline_is_reconciled_and_payload_gc_is_not_versioned_here() {
        let payload_gc = RecordedComponent {
            component: PAYLOAD_GC_COMPONENT.to_owned(),
            version: 2,
            checksum: None,
        };
        let verified = [row(CORE_COMPONENT, 1, MIGRATIONS), payload_gc.clone()];
        let later = MIGRATIONS.len() - 1;
        for reconcile in [false, true] {
            assert_eq!(plan(&verified, MIGRATIONS, reconcile).unwrap().len(), later);
        }
        // A differing baseline is drift for implicit provisioning; only the
        // explicit upgrade, with nothing built on it, reconciles it.
        let stale = [
            RecordedComponent {
                checksum: Some(vec![0; 32]),
                ..row(CORE_COMPONENT, 1, MIGRATIONS)
            },
            payload_gc,
        ];
        assert!(matches!(
            plan(&stale, MIGRATIONS, false),
            Err(MetaError::RepairRequired(message)) if message.contains("studiofs upgrade")
        ));
        assert_eq!(plan(&stale, MIGRATIONS, true).unwrap()[0], BASELINE);
        // An explicit repair re-lists a verified baseline exactly once.
        let repaired = with_repair(&verified, plan(&verified, MIGRATIONS, true).unwrap(), true);
        assert_eq!((repaired[0], repaired.len()), (BASELINE, later + 1));
        let reconciled = with_repair(&stale, plan(&stale, MIGRATIONS, true).unwrap(), true);
        assert_eq!(
            reconciled.iter().filter(|step| **step == BASELINE).count(),
            1
        );
    }
}
