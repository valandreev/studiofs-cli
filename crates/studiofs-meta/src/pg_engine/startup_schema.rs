//! Capability-scoped PostgreSQL schema validation at mount startup (SFS-1006).
//!
//! A mount checks only the schema components it uses, through their
//! `studiofs_schema_components` rows, plus the tables and columns its
//! bootstrap, replication and publication paths touch. Optional features
//! (snapshot creation, policies, artifacts, snapshot mounts, Janitor sweeps)
//! check their own objects when used. Missing snapshot objects surface as
//! redacted retryable backend errors and leave the mount serving. A newer
//! component version or an unknown component is additive for a mount. The
//! Janitor still requires its exact `payload_gc` version before deleting.

use std::collections::BTreeMap;
use std::time::Duration;

use super::{PgMetaEngine, PgTimeoutContext, StartupSchemaValidation};
use crate::compat::ReasonCode;
use crate::engine::{MetaError, MetaResult};
use crate::pg_transport::connect_studiofs_postgres;
use crate::version::{POSTGRES_CORE_SCHEMA, POSTGRES_PAYLOAD_GC_SCHEMA, VersionIdentity};

/// One schema component a mount uses, with the objects it relies on.
struct MountComponent {
    /// Row name in `studiofs_schema_components`.
    name: &'static str,
    identity: VersionIdentity,
    tables: &'static [&'static str],
    columns: &'static [(&'static str, &'static str)],
}

const MOUNT_COMPONENTS: [MountComponent; 2] = [
    // Bootstrap, replay, publication, checkpoints, file locks and the event
    // log prune floor, which reads the snapshot source fences.
    MountComponent {
        name: super::migrations::CORE_COMPONENT,
        identity: POSTGRES_CORE_SCHEMA,
        tables: &[
            "volumes",
            "counters",
            "event_log",
            "metadata_state",
            "bootstrap_checkpoint_leases",
            "acl_rules",
            "file_lock_leases",
            "trash_entry_authority",
            "trash_publication_receipts",
            "snapshot_catalog",
            "snapshot_source_checkpoints",
        ],
        columns: &[
            ("metadata_state", "metadata_format_version"),
            ("metadata_state", "write_requirement"),
            ("metadata_state", "total_event_payload_bytes"),
            ("metadata_state", "checkpoint_total_event_bytes"),
            ("metadata_state", "checkpoint_sha256"),
            ("metadata_state", "checkpoint_publication_state"),
            ("metadata_state", "checkpoint_generation"),
            ("metadata_state", "checkpoint_updated_at"),
            ("metadata_state", "event_log_prune_floor"),
            ("metadata_state", "event_log_pruned_rows"),
        ],
    },
    // Publication records every payload it uploads or retires in this ledger.
    MountComponent {
        name: "payload_gc",
        identity: POSTGRES_PAYLOAD_GC_SCHEMA,
        tables: &[
            "snapshot_current_references",
            "snapshot_object_reachability",
            "snapshot_reachability_batches",
            "snapshot_reachability_bootstrap",
            "snapshot_payload_upload_objects",
            "snapshot_payload_upload_fragments",
            "snapshot_payload_reconcile_cursors",
            "snapshot_payload_upload_watermarks",
            "snapshot_payload_gc_candidates",
            "snapshot_artifact_payload_proofs",
        ],
        columns: &[],
    },
];

// Catalog-only, so it runs before the registry table is known to exist.
const MISSING_OBJECTS_SQL: &str = r#"
    SELECT to_regclass(quote_ident(current_schema()) || '.studiofs_schema_components') IS NOT NULL,
        ARRAY(
            SELECT required.component || ':' || required.object
            FROM unnest($1::TEXT[], $2::TEXT[], $3::TEXT[])
                AS required(component, table_name, object)
            WHERE NOT EXISTS (
                SELECT 1 FROM information_schema.columns AS info
                WHERE info.table_schema = current_schema()
                  AND info.table_name = required.table_name
                  AND (required.object = required.table_name
                       OR info.column_name = split_part(required.object, '.', 2))
            )
            ORDER BY 1
        )
"#;

/// Mount requirements as the parallel arrays [`MISSING_OBJECTS_SQL`] takes:
/// component, table and object (`table` or `table.column`).
fn required_objects() -> [Vec<String>; 3] {
    let mut arrays: [Vec<String>; 3] = Default::default();
    for component in &MOUNT_COMPONENTS {
        let objects = component
            .tables
            .iter()
            .map(|table| (*table, (*table).to_owned()))
            .chain(
                component
                    .columns
                    .iter()
                    .map(|(table, column)| (*table, format!("{table}.{column}"))),
            );
        for (table, object) in objects {
            arrays[0].push(component.name.to_owned());
            arrays[1].push(table.to_owned());
            arrays[2].push(object);
        }
    }
    arrays
}

/// Diagnostic for every mount component that is absent, too old, or missing
/// an object; `None` when the mount can serve. `recorded` maps component rows
/// to versions; `missing` holds `component:object` entries.
fn mount_schema_refusal(recorded: &BTreeMap<String, i32>, missing: &[String]) -> Option<String> {
    let mut repair_requirements = Vec::new();
    let problems: Vec<String> = MOUNT_COMPONENTS
        .iter()
        .filter_map(|component| {
            let id = component.identity;
            let lacks: Vec<&str> = missing
                .iter()
                .filter_map(|entry| entry.strip_prefix(component.name)?.strip_prefix(':'))
                .collect();
            let (reason, subject) = match recorded.get(component.name) {
                None => (ReasonCode::SchemaComponentMissing, id.name.to_owned()),
                Some(&found) if found < i32::from(id.min_supported) => (
                    ReasonCode::SchemaComponentOutdated,
                    format!("{} {found}, needs {}", id.name, id.min_supported),
                ),
                // A newer version is additive for a mount; only missing objects refuse it.
                Some(_) if lacks.is_empty() => return None,
                Some(&found) => {
                    if found > i32::from(id.current) {
                        repair_requirements.push(format!("{} {found} or newer", id.name));
                    }
                    (
                        ReasonCode::SchemaComponentOutdated,
                        format!("{} {found} lacks {}", id.name, lacks.join(", ")),
                    )
                }
            };
            Some(format!(
                "{} ({subject}): {}",
                reason.as_str(),
                reason.action()
            ))
        })
        .collect();
    (!problems.is_empty()).then(|| {
        let mut repair = "stop mounts and janitors first, then run `studiofs upgrade --repair-schema` to restore dropped objects".to_owned();
        if !repair_requirements.is_empty() {
            repair.push_str(" with a binary that supports ");
            repair.push_str(&repair_requirements.join(" and "));
        }
        format!(
            "postgres schema cannot serve this mount: {}; {repair}",
            problems.join("; ")
        )
    })
}

impl PgMetaEngine {
    /// Validates the components this mount uses. Unreachable PostgreSQL and
    /// bounded timeouts fall back to local-first startup; a proven schema
    /// problem or a rejected probe statement fails startup.
    pub(super) fn validate_postgres_schema(
        postgres_url: &str,
        volume_id: i64,
        connect_timeout: Duration,
        command_timeout: Duration,
    ) -> MetaResult<StartupSchemaValidation> {
        let postgres_url = postgres_url.to_string();
        Self::run_async_once(async move {
            let (client, connection) = match tokio::time::timeout(
                connect_timeout,
                connect_studiofs_postgres(&postgres_url),
            )
            .await
            {
                Ok(Ok(connection)) => connection,
                Ok(Err(error)) if error.is_reachability_error() => {
                    return Ok(StartupSchemaValidation::Fallback(error));
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    return Ok(StartupSchemaValidation::Fallback(Self::pg_timeout_error(
                        PgTimeoutContext::Connection("validate postgres schema connect"),
                        connect_timeout,
                    )));
                }
            };
            let connection_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let result = tokio::time::timeout(command_timeout, async move {
                let [components, tables, objects] = required_objects();
                let row = client
                    .query_one(MISSING_OBJECTS_SQL, &[&components, &tables, &objects])
                    .await
                    .map_err(|error| {
                        Self::classify_pg_error("postgres schema validation failed", error)
                    })?;
                let has_registry: bool = row.get(0);
                let missing: Vec<String> = row.get(1);
                let mut recorded = BTreeMap::new();
                if has_registry {
                    let names: Vec<&str> = MOUNT_COMPONENTS.iter().map(|c| c.name).collect();
                    let rows = client
                        .query(
                            "SELECT component, version FROM studiofs_schema_components WHERE component = ANY($1::TEXT[])",
                            &[&names],
                        )
                        .await
                        .map_err(|error| {
                            Self::classify_pg_error("postgres schema component query failed", error)
                        })?;
                    for row in rows {
                        recorded.insert(row.get::<_, String>(0), row.get::<_, i32>(1));
                    }
                }
                if let Some(message) = mount_schema_refusal(&recorded, &missing) {
                    return Err(MetaError::Storage(message));
                }

                let row = client
                    .query_opt(
                        "SELECT metadata_format_version FROM metadata_state WHERE vol = $1",
                        &[&volume_id],
                    )
                    .await
                    .map_err(|error| {
                        Self::classify_pg_error("postgres metadata format query failed", error)
                    })?;
                let found = row.and_then(|row| row.get::<_, Option<i16>>(0));
                crate::ensure_supported_pg_sync_format_version(found)?;
                Ok(())
            })
            .await
            .map_err(|_| {
                Self::pg_timeout_error(
                    PgTimeoutContext::Command("validate postgres schema"),
                    command_timeout,
                )
            });
            connection_task.abort();
            let _ = connection_task.await;
            match result {
                Ok(Ok(())) => Ok(StartupSchemaValidation::Available),
                Ok(Err(error)) if error.is_reachability_error() => {
                    Ok(StartupSchemaValidation::Fallback(error))
                }
                Ok(Err(error)) => Err(error),
                Err(error) => Ok(StartupSchemaValidation::Fallback(error)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline() -> BTreeMap<String, i32> {
        BTreeMap::from([("core".to_owned(), 1), ("payload_gc".to_owned(), 2)])
    }

    #[test]
    fn startup_schema_accepts_baseline_and_newer_additive_components() {
        assert_eq!(mount_schema_refusal(&baseline(), &[]), None);
        let mut successor = baseline();
        successor.insert("core".to_owned(), 7);
        successor.insert("payload_gc".to_owned(), 3);
        assert_eq!(mount_schema_refusal(&successor, &[]), None);
    }

    #[test]
    fn startup_schema_refuses_absent_outdated_or_incomplete_components() {
        let absent = mount_schema_refusal(&BTreeMap::new(), &[]).unwrap();
        assert!(absent.contains("schema_component_missing (PostgreSQL core schema)"));
        assert!(absent.contains("schema_component_missing (PostgreSQL payload GC schema)"));
        assert!(absent.contains("studiofs upgrade"));

        let mut outdated = baseline();
        outdated.insert("payload_gc".to_owned(), 1);
        let message = mount_schema_refusal(&outdated, &[]).unwrap();
        assert!(
            message.contains("schema_component_outdated (PostgreSQL payload GC schema 1, needs 2)")
        );
        assert!(!message.contains("core schema"));

        // A newer component still refuses when an object the mount uses is gone.
        let mut newer = baseline();
        newer.insert("core".to_owned(), 9);
        let missing = [
            "core:file_lock_leases".to_owned(),
            "core:metadata_state.checkpoint_sha256".to_owned(),
        ];
        let message = mount_schema_refusal(&newer, &missing).unwrap();
        assert!(
            message.contains(
                "PostgreSQL core schema 9 lacks file_lock_leases, metadata_state.checkpoint_sha256"
            ),
            "{message}"
        );
        assert!(
            message.contains("with a binary that supports PostgreSQL core schema 9 or newer"),
            "{message}"
        );
    }

    #[test]
    fn startup_schema_requires_only_mount_objects() {
        let [components, tables, objects] = required_objects();
        assert_eq!(components.len(), objects.len());
        assert_eq!(tables.len(), objects.len());
        assert!(objects.iter().any(|object| object == "file_lock_leases"));
        assert!(
            objects
                .iter()
                .any(|object| object == "metadata_state.event_log_pruned_rows")
        );
        assert!(
            objects
                .iter()
                .any(|object| object == "metadata_state.write_requirement")
        );
        for optional in [
            "snapshot_policies",
            "snapshot_creation_jobs",
            "snapshot_artifacts",
            "snapshot_mount_leases",
            "snapshot_payload_sweep_checkpoints",
        ] {
            assert!(!tables.iter().any(|table| table == optional), "{optional}");
        }
        // Every required object is created by the baseline DDL.
        let ddl = super::super::schema::studiofs_pg_schema_sql();
        for table in &tables {
            assert!(
                ddl.contains(&format!("CREATE TABLE IF NOT EXISTS {table} ")),
                "{table}"
            );
        }
    }
}
