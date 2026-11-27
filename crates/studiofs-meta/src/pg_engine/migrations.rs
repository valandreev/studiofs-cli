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

/// Database-wide additive component for per-volume client session leases (SFS-1008).
pub const CLIENT_SESSIONS_COMPONENT: &str = "client_sessions";
pub(super) const CHECKPOINT_ENCODINGS_COMPONENT: &str = "checkpoint_encodings";
pub(super) const FORMAT_FENCES_COMPONENT: &str = "format_fences";
pub(super) const EVENT_PUBLICATION_FENCE_COMPONENT: &str = "event_publication_fence";
pub(super) const MAINTENANCE_FENCE_COMPONENT: &str = "maintenance_fence";

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

const CLIENT_SESSIONS: Migration = Migration {
    component: CLIENT_SESSIONS_COMPONENT,
    version: 1,
    name: "per-volume client session leases",
    phase: MigrationPhase::Additive,
    sql: super::client_sessions::SCHEMA,
};

const CHECKPOINT_ENCODINGS: Migration = Migration {
    component: CHECKPOINT_ENCODINGS_COMPONENT,
    version: 1,
    name: "independent serving checkpoint encodings",
    phase: MigrationPhase::Additive,
    sql: "CREATE TABLE IF NOT EXISTS serving_checkpoint_encodings (
              vol BIGINT NOT NULL REFERENCES metadata_state(vol) ON DELETE CASCADE,
              checkpoint_version BIGINT NOT NULL CHECK (checkpoint_version >= 0),
              checkpoint_format SMALLINT NOT NULL CHECK (checkpoint_format > 0),
              checkpoint_schema SMALLINT NOT NULL CHECK (checkpoint_schema > 0),
              publication_generation BIGINT NOT NULL CHECK (publication_generation > 0),
              snapshot BYTEA NOT NULL,
              checkpoint_sha256 BYTEA NOT NULL CHECK (octet_length(checkpoint_sha256) = 32),
              PRIMARY KEY (vol, checkpoint_format, checkpoint_schema, checkpoint_version)
          );",
};

/// Server-side fences against 0.2.x writers on adopted (format 7) volumes.
/// Object names differ from the 0.2.x DDL, so an older `init` re-applying its
/// schema cannot replace them; the legacy deletion fence stays as shipped.
const FORMAT_FENCES: Migration = Migration {
    component: FORMAT_FENCES_COMPONENT,
    version: 1,
    name: "fence 0.2.x writers on adopted volumes",
    phase: MigrationPhase::Additive,
    sql: "CREATE OR REPLACE FUNCTION studiofs_fence_deletion_claim_volume_format()
          RETURNS TRIGGER LANGUAGE plpgsql AS $$
          BEGIN
              IF NEW.state = 'delete_pending' AND NEW.reconcile_owner IS NOT NULL
                 AND (TG_OP = 'INSERT' OR NEW.reconcile_fence IS DISTINCT FROM OLD.reconcile_fence)
                 AND EXISTS (
                     SELECT 1 FROM metadata_state
                     WHERE vol = NEW.vol AND metadata_format_version >= 7
                       AND current_setting('studiofs.deletion_volume_format', true)
                           IS DISTINCT FROM metadata_format_version::text
                 )
              THEN
                  RAISE EXCEPTION 'deletion claim requires current volume format capability';
              END IF;
              RETURN NEW;
          END $$;
          DROP TRIGGER IF EXISTS studiofs_deletion_claim_volume_format_fence ON snapshot_payload_upload_objects;
          CREATE TRIGGER studiofs_deletion_claim_volume_format_fence
              BEFORE INSERT OR UPDATE ON snapshot_payload_upload_objects
              FOR EACH ROW EXECUTE FUNCTION studiofs_fence_deletion_claim_volume_format();
          CREATE OR REPLACE FUNCTION studiofs_fence_adopted_format_downgrade()
          RETURNS TRIGGER LANGUAGE plpgsql AS $$
          BEGIN
              -- Only adopted or 0.2.5-created volumes have a policy row.
              IF EXISTS (SELECT 1 FROM volume_protocol WHERE vol = NEW.vol) THEN
                  RAISE EXCEPTION 'volume format 7 cannot be replaced by an older format';
              END IF;
              RETURN NEW;
          END $$;
          DROP TRIGGER IF EXISTS studiofs_adopted_format_downgrade_fence ON metadata_state;
          CREATE TRIGGER studiofs_adopted_format_downgrade_fence
              BEFORE INSERT OR UPDATE OF metadata_format_version ON metadata_state
              FOR EACH ROW WHEN (NEW.metadata_format_version < 7)
              EXECUTE FUNCTION studiofs_fence_adopted_format_downgrade();",
};

/// Checks the epoch on the same tuple PostgreSQL rechecks after waiting for a
/// concurrent policy update. A subquery against volume_protocol in the bump
/// would still see its original statement snapshot.
const EVENT_PUBLICATION_FENCE: Migration = Migration {
    component: EVENT_PUBLICATION_FENCE_COMPONENT,
    version: crate::version::POSTGRES_EVENT_PUBLICATION_FENCE_SCHEMA.current as i32,
    name: "fence metadata event publication",
    phase: MigrationPhase::Additive,
    sql: super::schema::EVENT_PUBLICATION_FENCE_SQL,
};

const MAINTENANCE_FENCE: Migration = Migration {
    component: MAINTENANCE_FENCE_COMPONENT,
    version: 1,
    name: "fence destructive maintenance claims",
    phase: MigrationPhase::Additive,
    sql: super::schema::MAINTENANCE_FENCE_SQL,
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
pub const MIGRATIONS: &[Migration] = &[
    BASELINE,
    CLIENT_SESSIONS,
    CHECKPOINT_ENCODINGS,
    FORMAT_FENCES,
    EVENT_PUBLICATION_FENCE,
    MAINTENANCE_FENCE,
];
/// Every migration this binary knows, grouped by component in version order.
#[cfg(feature = "test-successor")]
pub const MIGRATIONS: &[Migration] = &[
    BASELINE,
    SUCCESSOR_PROBE,
    CLIENT_SESSIONS,
    CHECKPOINT_ENCODINGS,
    FORMAT_FENCES,
    EVENT_PUBLICATION_FENCE,
    MAINTENANCE_FENCE,
];

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

/// Puts idempotent version-1 component migrations first when a repair was asked
/// for, nothing is built above them, and they were not already applied in this
/// run.
fn with_repair(
    recorded: &[RecordedComponent],
    mut pending: Vec<Migration>,
    applied: &[Migration],
    repair: bool,
) -> Vec<Migration> {
    if !repair {
        return pending;
    }
    let mut repairs = Vec::new();
    for step in [
        BASELINE,
        CLIENT_SESSIONS,
        CHECKPOINT_ENCODINGS,
        EVENT_PUBLICATION_FENCE,
        MAINTENANCE_FENCE,
    ] {
        let at_v1 = recorded
            .iter()
            .any(|row| row.component == step.component && row.version == step.version);
        if at_v1 && !pending.contains(&step) && !applied.contains(&step) {
            repairs.push(step);
        }
    }
    if !repairs.is_empty() {
        repairs.extend(pending);
        pending = repairs;
    }
    pending
}

pub(crate) async fn migrate_with(
    client: &mut tokio_postgres::Client,
    registry: &[Migration],
    repair_baseline: bool,
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
        let next = with_repair(&recorded, pending, &applied, repair_baseline)
            .into_iter()
            .next();
        let Some(step) = next else {
            tx.commit()
                .await
                .map_err(|error| pg_failure("finish schema migration", &error))?;
            return Ok(applied);
        };
        if step.phase == MigrationPhase::Contract {
            let blockers = contract_database_volume_blockers(&tx, None).await?;
            if let Some((vol, detail)) = blockers.first() {
                return Err(MetaError::Storage(format!(
                    "cannot apply contract migration {step}: volume '{vol}' {detail}"
                )));
            }
        }
        apply(&tx, &step, registry).await?;
        tx.commit()
            .await
            .map_err(|error| pg_failure("commit schema migration", &error))?;
        applied.push(step);
    }
}

/// Checks every volume in the database before a `Contract` transition or DDL step.
///
/// Returns `(volume_name, reason)` for every volume that lacks a `volume_protocol`
/// policy or (when `feature` is `Some`) still has `feature` in `compatible` or
/// `required` state.
pub async fn contract_database_volume_blockers<C>(
    client: &C,
    feature: Option<&str>,
) -> MetaResult<Vec<(String, String)>>
where
    C: GenericClient + Sync,
{
    let has_volumes: bool = client
        .query_one(
            "SELECT to_regclass('volumes') IS NOT NULL
                AND to_regclass('volume_protocol') IS NOT NULL
                AND to_regclass('metadata_state') IS NOT NULL",
            &[],
        )
        .await
        .map_err(|error| pg_failure("check database volumes for contract", &error))?
        .get(0);
    if !has_volumes {
        return Ok(Vec::new());
    }
    let rows = client
        .query(
            "SELECT v.name, p.policy_epoch, m.write_requirement, p.requirements::TEXT
             FROM volumes v
             LEFT JOIN metadata_state m ON m.vol = v.vol
             LEFT JOIN volume_protocol p ON p.vol = v.vol
             ORDER BY v.vol",
            &[],
        )
        .await
        .map_err(|error| pg_failure("inspect database volumes for contract", &error))?;
    let mut blockers = Vec::new();
    for row in rows {
        let name: String = row.get(0);
        let epoch: Option<i64> = row.get(1);
        let write_requirement: Option<i16> = row.get(2);
        let requirements: Option<String> = row.get(3);
        let (Some(epoch), Some(write_requirement), Some(requirements)) =
            (epoch, write_requirement, requirements)
        else {
            blockers.push((
                name,
                "has no volume_protocol policy; run `studiofs upgrade` on it first".to_owned(),
            ));
            continue;
        };
        let protocol = crate::volume_protocol::VolumeProtocol::decode(
            epoch,
            write_requirement,
            &requirements,
        )?;
        if let Some(cap) = feature {
            let state = protocol.requirements.features.get(cap).copied();
            let in_required_set = protocol.requirements.read.contains(cap)
                || protocol.requirements.write.contains(cap)
                || protocol.requirements.maintain.contains(cap);
            if in_required_set
                || matches!(
                    state,
                    Some(
                        crate::volume_protocol::FeatureState::Compatible
                            | crate::volume_protocol::FeatureState::Required
                    )
                )
            {
                blockers.push((
                    name,
                    format!("still needs feature `{cap}` (state: {state:?})"),
                ));
            }
        }
    }
    Ok(blockers)
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
    let pending = with_repair(&recorded, pending, &[], repair_baseline);
    Ok((recorded, pending))
}

/// Check a backup's schema revisions against the compiled migration registry.
pub fn validate_restore_revisions(
    revisions: &std::collections::BTreeMap<String, i32>,
) -> MetaResult<()> {
    for (component, &found) in revisions {
        let known = if component == PAYLOAD_GC_COMPONENT {
            i32::from(POSTGRES_PAYLOAD_GC_SCHEMA.current)
        } else {
            latest_version(MIGRATIONS, component)
        };
        if found > known {
            return Err(MetaError::UpgradeRequired {
                reason: UpgradeRequiredReason::SchemaComponentNewer {
                    component: component.clone(),
                    found,
                    known,
                },
            });
        }
        if found <= 0 {
            return Err(MetaError::Serialization(format!(
                "invalid PostgreSQL schema revision for {component}"
            )));
        }
    }
    for component in [CORE_COMPONENT, PAYLOAD_GC_COMPONENT] {
        if !revisions.contains_key(component) {
            return Err(MetaError::Serialization(format!(
                "dump misses PostgreSQL schema revision for {component}"
            )));
        }
    }
    Ok(())
}

/// Read schema revisions from the migration ledger without running DDL.
pub async fn recorded_schema_revisions<C: GenericClient + Sync>(
    client: &C,
) -> MetaResult<std::collections::BTreeMap<String, i32>> {
    let (recorded, _) = read_recorded(client).await?;
    Ok(recorded
        .into_iter()
        .map(|row| (row.component, row.version))
        .collect())
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
        let mut by_component: std::collections::BTreeMap<&str, Vec<i32>> =
            std::collections::BTreeMap::new();
        for step in MIGRATIONS {
            by_component
                .entry(step.component)
                .or_default()
                .push(step.version);
        }
        assert_eq!(
            by_component.keys().copied().collect::<Vec<_>>(),
            vec![
                CHECKPOINT_ENCODINGS_COMPONENT,
                CLIENT_SESSIONS_COMPONENT,
                CORE_COMPONENT,
                EVENT_PUBLICATION_FENCE_COMPONENT,
                FORMAT_FENCES_COMPONENT,
                MAINTENANCE_FENCE_COMPONENT,
            ]
        );
        for (component, versions) in &by_component {
            assert!(
                versions
                    .iter()
                    .zip(1..)
                    .all(|(version, expected)| *version == expected),
                "component {component} versions must be 1..=N: {versions:?}"
            );
        }
        assert_eq!(
            latest_version(MIGRATIONS, CORE_COMPONENT),
            i32::from(crate::version::POSTGRES_CORE_SCHEMA.current)
        );
        assert_eq!(
            latest_version(MIGRATIONS, CLIENT_SESSIONS_COMPONENT),
            i32::from(crate::version::POSTGRES_CLIENT_SESSIONS_SCHEMA.current)
        );
        assert_eq!(
            latest_version(MIGRATIONS, CHECKPOINT_ENCODINGS_COMPONENT),
            i32::from(crate::version::POSTGRES_CHECKPOINT_ENCODINGS_SCHEMA.current)
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
            "f3261295118fbd878ec17853fa436034baf5f48c4308d8155bab1fd475d6eb9f"
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
            payload_gc.clone(),
        ];
        assert!(matches!(
            plan(&stale, MIGRATIONS, false),
            Err(MetaError::RepairRequired(message)) if message.contains("studiofs upgrade")
        ));
        assert_eq!(plan(&stale, MIGRATIONS, true).unwrap()[0], BASELINE);
        // An explicit repair re-lists a verified baseline exactly once.
        let repaired = with_repair(
            &verified,
            plan(&verified, MIGRATIONS, true).unwrap(),
            &[],
            true,
        );
        assert_eq!((repaired[0], repaired.len()), (BASELINE, later + 1));
        let full_recorded = [
            row(CORE_COMPONENT, 1, MIGRATIONS),
            row(CLIENT_SESSIONS_COMPONENT, 1, MIGRATIONS),
            row(CHECKPOINT_ENCODINGS_COMPONENT, 1, MIGRATIONS),
            payload_gc.clone(),
        ];
        let full_repaired = with_repair(
            &full_recorded,
            plan(&full_recorded, MIGRATIONS, true).unwrap(),
            &[],
            true,
        );
        assert_eq!(full_repaired[0], BASELINE);
        assert!(full_repaired.contains(&CLIENT_SESSIONS));
        assert!(full_repaired.contains(&CHECKPOINT_ENCODINGS));
        let reconciled = with_repair(&stale, plan(&stale, MIGRATIONS, true).unwrap(), &[], true);
        assert_eq!(
            reconciled.iter().filter(|step| **step == BASELINE).count(),
            1
        );
    }
}
