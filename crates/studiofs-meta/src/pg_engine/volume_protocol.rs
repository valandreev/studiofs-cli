//! PostgreSQL storage of the per-volume protocol policy (SFS-1007).
//!
//! The policy record is `volume_protocol` (epoch, required sets, checkpoint
//! identity and feature states). Its write requirement lives in the
//! `metadata_state` row that `BUMP_METADATA_VERSION` already updates.
//! Publication checks it and the mirrored epoch in the same UPDATE. A policy
//! update locks that row to serialize with publication and appends to
//! `volume_protocol_history`.

use tokio_postgres::GenericClient;

use super::migrations::pg_failure;
use super::{PgMetaEngine, sql};
use crate::compat::{CheckpointDescriptor, ClientCapabilities, Operation, VolumeRequirements};
use crate::engine::{MetaError, MetaResult, UpgradeRequiredReason};
use crate::version::SHARED_WRITE_REQUIREMENT;
use crate::volume_protocol::{
    CutoverDryRunReport, CutoverLocalState, CutoverRequest, CutoverSharedState, ProtocolRejection,
    VolumeProtocol, check_update, evaluate_cutover_readiness,
};

const SELECT_PROTOCOL: &str = "
    SELECT p.policy_epoch, m.write_requirement, p.requirements::TEXT,
           m.metadata_format_version, m.version
    FROM metadata_state m LEFT JOIN volume_protocol p ON p.vol = m.vol
    WHERE m.vol = $1";

const INSERT_HISTORY: &str = "
    INSERT INTO volume_protocol_history (vol, policy_epoch, write_requirement, requirements, changed_by)
    VALUES ($1, $2, $3, $4::TEXT::JSONB, $5)";

pub(super) async fn check_maintenance_policy<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
) -> MetaResult<()> {
    evaluate_maintenance(&read_volume_protocol(client, volume).await?)
}

fn evaluate_maintenance(policy: &VolumeProtocol) -> MetaResult<()> {
    let mut requirements = VolumeRequirements::baseline();
    policy.apply_to(&mut requirements);
    let decision = crate::compat::evaluate(
        &ClientCapabilities::current(),
        &requirements,
        &Default::default(),
    );
    if let Some(refusal) = decision.refusal(Operation::Maintenance) {
        return Err(ProtocolRejection::MaintenanceForbidden {
            reason: refusal.reason.as_str(),
            capability: refusal.subject.clone(),
        }
        .into());
    }
    Ok(())
}

/// Authorizes and stamps maintenance on the volume in the caller's transaction.
///
/// The locked metadata row supplies the
/// epoch. If we waited behind activation, the joined policy can come from an
/// older statement snapshot, so re-read it under the acquired lock.
///
/// # Errors
///
/// Returns the policy refusal if this binary lacks a required capability, or a
/// classified error if the policy cannot be locked, read, or stamped.
pub async fn fence_maintenance<C: GenericClient + Sync>(tx: &C, volume: i64) -> MetaResult<()> {
    let classify = |error| PgMetaEngine::classify_pg_error("fence maintenance", error);
    let capabilities: Vec<&str> = ClientCapabilities::current().maintain.into_iter().collect();
    let row = tx
        .query_opt(
            "WITH locked AS MATERIALIZED (
             SELECT vol, publication_epoch, write_requirement, metadata_format_version
             FROM metadata_state WHERE vol = $1 FOR SHARE
         )
         SELECT m.publication_epoch, p.policy_epoch, m.write_requirement,
                p.requirements::text, m.metadata_format_version,
                set_config('studiofs.maintenance_epoch', m.publication_epoch::text, true),
                set_config('studiofs.maintenance_capabilities', to_jsonb($2::text[])::text, true),
                set_config('studiofs.deletion_format', '6', true),
                set_config('studiofs.deletion_volume_format', $3, true)
         FROM locked m LEFT JOIN volume_protocol p ON p.vol = m.vol",
            &[
                &volume,
                &capabilities,
                &crate::version::SHARED_VOLUME_FORMAT.current.to_string(),
            ],
        )
        .await
        .map_err(classify)?
        .ok_or(ProtocolRejection::Missing)?;
    let epoch: i64 = row.get(0);
    let policy_epoch: Option<i64> = row.get(1);
    let policy = if policy_epoch != Some(epoch) {
        read_volume_protocol(tx, volume).await?
    } else {
        decode_volume_policy(row.get(4), policy_epoch, row.get(2), row.get(3))?
    };
    evaluate_maintenance(&policy)
}

fn epoch_i64(epoch: u64) -> MetaResult<i64> {
    i64::try_from(epoch).map_err(|_| MetaError::Storage("volume policy epoch overflow".into()))
}

fn requirement_i16(value: u16) -> MetaResult<i16> {
    i16::try_from(value)
        .map_err(|_| MetaError::Storage("write requirement exceeds SMALLINT".into()))
}

/// Writes the baseline policy for a volume that has none; never changes an
/// existing policy. `init` and `load` call it in the transaction that creates
/// the volume, and the format 5/6 adoption (SFS-1132) reuses it.
///
/// # Errors
///
/// A classified PostgreSQL error.
pub async fn insert_initial_volume_protocol<C>(
    client: &C,
    volume: i64,
    actor: &str,
) -> MetaResult<()>
where
    C: GenericClient + Sync,
{
    insert_restored_volume_protocol(client, volume, &VolumeProtocol::baseline(), actor).await
}

/// Install a validated restore policy as the first policy of a fresh volume.
pub async fn insert_restored_volume_protocol<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
    protocol: &VolumeProtocol,
    actor: &str,
) -> MetaResult<()> {
    protocol.validate()?;
    let requirements = protocol.encode_requirements()?;
    let epoch = epoch_i64(protocol.epoch)?;
    let write_requirement = requirement_i16(protocol.write_requirement)?;
    client
        .execute(
            "WITH inserted AS (
                INSERT INTO volume_protocol (vol, policy_epoch, requirements, updated_by)
                VALUES ($1, $2, $3::TEXT::JSONB, $4) ON CONFLICT (vol) DO NOTHING
                RETURNING vol, policy_epoch, requirements, updated_by
            )
            INSERT INTO volume_protocol_history
                (vol, policy_epoch, write_requirement, requirements, changed_by)
            SELECT vol, policy_epoch, $5, requirements, updated_by FROM inserted",
            &[&volume, &epoch, &requirements, &actor, &write_requirement],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("insert volume protocol", error))?;
    Ok(())
}

/// Reads a volume's policy.
///
/// # Errors
///
/// `VolumeProtocolRejected(Missing)` without a policy, `UpgradeRequired` for
/// a policy written by a newer binary, or a classified PostgreSQL error.
pub async fn read_volume_protocol<C>(client: &C, volume: i64) -> MetaResult<VolumeProtocol>
where
    C: GenericClient + Sync,
{
    read_volume_policy_and_head(client, volume)
        .await
        .map(|(policy, _)| policy)
}

pub(super) async fn read_volume_policy_and_head<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
) -> MetaResult<(VolumeProtocol, i64)> {
    let row = client
        .query_opt(SELECT_PROTOCOL, &[&volume])
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("read volume protocol", error))?;
    let Some(row) = row else {
        return Err(ProtocolRejection::Missing.into());
    };
    let protocol = decode_volume_policy(row.get(3), row.get(0), row.get(1), row.get(2))?;
    Ok((protocol, row.get(4)))
}

pub(super) fn decode_volume_policy(
    format: Option<i16>,
    epoch: Option<i64>,
    write_requirement: i16,
    requirements: Option<&str>,
) -> MetaResult<VolumeProtocol> {
    crate::check_pg_volume_format(format)?;
    let (Some(epoch), Some(requirements)) = (epoch, requirements) else {
        return Err(ProtocolRejection::Missing.into());
    };
    VolumeProtocol::decode(epoch, write_requirement, requirements)
}

async fn lock_volume_for_protocol_update(
    tx: &tokio_postgres::Transaction<'_>,
    volume: i64,
    context: &str,
) -> MetaResult<()> {
    let failed = |error| PgMetaEngine::classify_pg_error(context, error);
    // Use the same lock order as load: its volume lock conflicts with the
    // foreign-key lock needed by the history insert.
    let volume_row = tx
        .query_opt(
            "SELECT vol FROM volumes WHERE vol = $1 FOR KEY SHARE",
            &[&volume],
        )
        .await
        .map_err(failed)?;
    if volume_row.is_none() {
        return Err(ProtocolRejection::Missing.into());
    }
    let state_row = tx
        .query_opt(
            "SELECT vol FROM metadata_state WHERE vol = $1 FOR UPDATE",
            &[&volume],
        )
        .await
        .map_err(failed)?;
    if state_row.is_none() {
        return Err(ProtocolRejection::Missing.into());
    }
    Ok(())
}

async fn apply_locked_volume_protocol_update(
    tx: &tokio_postgres::Transaction<'_>,
    volume: i64,
    current: &VolumeProtocol,
    proposed: &VolumeProtocol,
    actor_id: &str,
    context: &str,
) -> MetaResult<VolumeProtocol> {
    let failed = |error| PgMetaEngine::classify_pg_error(context, error);
    if current.requirements.maintain != proposed.requirements.maintain {
        // Janitors hold this lock across S3 requests. Try it without waiting
        // because Janitors acquire it before taking the metadata row lock.
        let idle: bool = tx
            .query_one(
                "SELECT pg_try_advisory_xact_lock($1)",
                &[&super::janitor::pg_janitor_lock_key(volume)],
            )
            .await
            .map_err(failed)?
            .get(0);
        if !idle {
            return Err(ProtocolRejection::MaintenanceInProgress.into());
        }
        // Claims and renewals are fenced by the policy epoch under the metadata
        // row lock. A cancelled or timed-out S3 DELETE sent before activation may
        // still land afterward. Its target was proven unreachable under the old policy.
        let leased: bool = tx
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM snapshot_cleanup_leases
                    WHERE vol = $1 AND owner <> '' AND lease_expires_at > clock_timestamp()
                 ) OR EXISTS (
                    SELECT 1 FROM snapshot_payload_upload_objects
                    WHERE vol = $1 AND state = 'delete_pending' AND reconcile_owner IS NOT NULL
                        AND reconcile_lease_expires_at > clock_timestamp()
                 )",
                &[&volume],
            )
            .await
            .map_err(failed)?
            .get(0);
        if leased {
            return Err(ProtocolRejection::MaintenanceInProgress.into());
        }
    }
    let next = VolumeProtocol {
        epoch: current
            .epoch
            .checked_add(1)
            .ok_or_else(|| MetaError::Storage("volume policy epoch overflow".into()))?,
        ..proposed.clone()
    };
    let epoch = epoch_i64(next.epoch)?;
    let write_requirement = requirement_i16(next.write_requirement)?;
    let requirements = next.encode_requirements()?;
    tx.execute(
        "UPDATE volume_protocol SET policy_epoch = $2, requirements = $3::TEXT::JSONB,
                updated_by = $4, updated_at = now() WHERE vol = $1",
        &[&volume, &epoch, &requirements, &actor_id],
    )
    .await
    .map_err(failed)?;
    tx.execute(
        "UPDATE metadata_state SET write_requirement = $2 WHERE vol = $1",
        &[&volume, &write_requirement],
    )
    .await
    .map_err(failed)?;
    tx.execute(
        INSERT_HISTORY,
        &[
            &volume,
            &epoch,
            &write_requirement,
            &requirements,
            &actor_id,
        ],
    )
    .await
    .map_err(failed)?;
    Ok(next)
}

/// Replaces the policy with `proposed` (its `epoch` is ignored) when the
/// stored epoch is `expected_epoch` and [`check_update`] accepts the change
/// for a binary declaring `actor`. The epoch rises by one, the write
/// requirement and an audit row are written in the same transaction. A retry
/// whose change already committed returns the stored policy; a change that
/// alters nothing keeps the epoch.
///
/// # Errors
///
/// `VolumeProtocolRejected` (`EpochConflict`, `Missing` or a violated rule),
/// `UpgradeRequired` for a policy written by a newer binary, or a classified
/// PostgreSQL error.
pub async fn update_volume_protocol(
    tx: &tokio_postgres::Transaction<'_>,
    volume: i64,
    expected_epoch: u64,
    proposed: &VolumeProtocol,
    actor: &ClientCapabilities,
    actor_id: &str,
) -> MetaResult<VolumeProtocol> {
    lock_volume_for_protocol_update(tx, volume, "update volume protocol").await?;
    // Under READ COMMITTED a statement that waited for a row lock can still
    // have a stale snapshot of other tables. Read the policy in a new statement.
    let current = read_volume_protocol(tx, volume).await?;
    let same = current.write_requirement == proposed.write_requirement
        && current.requirements == proposed.requirements;
    if current.epoch != expected_epoch {
        if same {
            return Ok(current);
        }
        return Err(ProtocolRejection::EpochConflict {
            expected: expected_epoch,
            found: current.epoch,
        }
        .into());
    }
    if !check_update(&current, proposed, actor)? {
        return Ok(current);
    }
    apply_locked_volume_protocol_update(
        tx,
        volume,
        &current,
        proposed,
        actor_id,
        "update volume protocol",
    )
    .await
}

/// `SHARED_WRITE_REQUIREMENT` as the `$4` parameter of `BUMP_METADATA_VERSION`.
pub(super) const PUBLICATION_WRITE_REQUIREMENT: i16 =
    SHARED_WRITE_REQUIREMENT.current_pg_smallint();

/// Explains a `BUMP_METADATA_VERSION` that matched no row: an unsupported
/// format, raised write requirement, or changed epoch is a typed refusal,
/// so the caller keeps its journal as unpublished local work.
pub(super) async fn refused_publication<C>(client: &C, volume: i64) -> MetaError
where
    C: GenericClient + Sync,
{
    let row = match client
        .query_opt(sql::SELECT_PUBLICATION_GATES, &[&volume])
        .await
    {
        Ok(row) => row,
        Err(error) => {
            return PgMetaEngine::classify_pg_error(
                "postgres publication gate revalidation failed",
                error,
            );
        }
    };
    let Some(row) = row else {
        return ProtocolRejection::Missing.into();
    };
    if let Err(error) = crate::check_pg_volume_format(row.get(0)) {
        return error;
    }
    let found: i16 = row.get(1);
    if found > PUBLICATION_WRITE_REQUIREMENT {
        return MetaError::UpgradeRequired {
            reason: UpgradeRequiredReason::WriteRequirementNewer {
                found: found.unsigned_abs(),
                supported: SHARED_WRITE_REQUIREMENT.current,
            },
        };
    }
    let epoch: i64 = row.get(2);
    if let Some(expected) = row.get::<_, Option<i64>>(3)
        && expected != epoch
    {
        let (Ok(expected), Ok(found)) = (u64::try_from(expected), u64::try_from(epoch)) else {
            return MetaError::Storage("postgres policy epoch is negative".into());
        };
        return ProtocolRejection::EpochConflict { expected, found }.into();
    }
    MetaError::Storage("postgres metadata version bump requires a current policy stamp".into())
}

pub(super) fn is_volume_read_policy_rejection(error: &MetaError) -> bool {
    matches!(
        error,
        MetaError::UnsupportedFormatVersion { .. }
            | MetaError::UpgradeRequired { .. }
            | MetaError::NeedsMigration { .. }
            | MetaError::VolumeProtocolRejected(_)
            | MetaError::RepairRequired(_)
    )
}

/// Verify the read policy at bootstrap or snapshot acquisition.
pub async fn check_volume_read_policy<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
    checkpoint: Option<&crate::BootstrapCheckpointDescriptor>,
) -> MetaResult<()> {
    let protocol = read_volume_protocol(client, volume).await?;
    check_read_operations(&protocol, checkpoint)
}

pub(super) fn check_read_operations(
    protocol: &VolumeProtocol,
    checkpoint: Option<&crate::BootstrapCheckpointDescriptor>,
) -> MetaResult<()> {
    protocol.check_operations(
        VolumeRequirements::baseline(),
        checkpoint.map(|descriptor| {
            descriptor.checkpoint_encoding.map_or(
                crate::compat::CheckpointDescriptor::BASELINE,
                |encoding| crate::compat::CheckpointDescriptor {
                    format: encoding.format as u16,
                    schema: encoding.schema as u16,
                },
            )
        }),
        &Default::default(),
        &[Operation::Bootstrap, Operation::Replay],
    )
}

/// Reads the stored policy plus head, checkpoint and maintenance state in
/// bounded queries, for cutover readiness and the operator compatibility report.
pub async fn collect_cutover_shared_state<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
) -> MetaResult<(VolumeProtocol, CutoverSharedState)> {
    let failed = |error| PgMetaEngine::classify_pg_error("collect cutover state", error);
    let decode = |error| pg_failure("decode cutover state", &error);
    let current = read_volume_protocol(client, volume).await?;
    let state_row = client
        .query_opt(
            "SELECT version, checkpoint_version, octet_length(snapshot) > 0
             FROM metadata_state WHERE vol = $1",
            &[&volume],
        )
        .await
        .map_err(failed)?
        .ok_or(ProtocolRejection::Missing)?;
    let remote_head_version: i64 = state_row.try_get(0).map_err(decode)?;
    let checkpoint_version: i64 = state_row.try_get(1).map_err(decode)?;
    let has_baseline_snapshot: Option<bool> = state_row.try_get(2).map_err(decode)?;
    let checkpoint_ready = checkpoint_version > 0 && has_baseline_snapshot.unwrap_or(false);
    let mut available_checkpoints = Vec::new();
    if checkpoint_ready {
        available_checkpoints.push(CheckpointDescriptor::BASELINE);
    }
    let has_encodings_table: bool = client
        .query_one(
            "SELECT to_regclass('serving_checkpoint_encodings') IS NOT NULL",
            &[],
        )
        .await
        .map_err(failed)?
        .try_get(0)
        .map_err(decode)?;
    if has_encodings_table {
        let rows = client
            .query(
                "SELECT DISTINCT checkpoint_format, checkpoint_schema
                 FROM serving_checkpoint_encodings WHERE vol = $1",
                &[&volume],
            )
            .await
            .map_err(failed)?;
        for row in rows {
            let format: i16 = row.try_get(0).map_err(decode)?;
            let schema: i16 = row.try_get(1).map_err(decode)?;
            let (Ok(format), Ok(schema)) = (u16::try_from(format), u16::try_from(schema)) else {
                return Err(MetaError::Storage(format!(
                    "serving checkpoint encoding has invalid format {format} or schema {schema}"
                )));
            };
            let desc = CheckpointDescriptor { format, schema };
            if !available_checkpoints.contains(&desc) {
                available_checkpoints.push(desc);
            }
        }
    }

    let maint_row = client
        .query_one(
            "SELECT
                EXISTS (
                    SELECT 1 FROM snapshot_cleanup_leases
                    WHERE vol = $1 AND owner <> '' AND lease_expires_at > clock_timestamp()
                ),
                EXISTS (
                    SELECT 1 FROM snapshot_payload_upload_objects
                    WHERE vol = $1 AND state = 'delete_pending' AND reconcile_owner IS NOT NULL
                      AND reconcile_lease_expires_at > clock_timestamp()
                ),
                EXISTS (
                    SELECT 1 FROM snapshot_creation_jobs
                    WHERE vol = $1 AND state IN ('queued', 'running', 'retry_wait')
                )",
            &[&volume],
        )
        .await
        .map_err(failed)?;
    let cleanup_active: bool = maint_row.try_get(0).map_err(decode)?;
    let gc_active: bool = maint_row.try_get(1).map_err(decode)?;
    let snapshot_active: bool = maint_row.try_get(2).map_err(decode)?;
    let snapshot_or_gc_active = cleanup_active || gc_active || snapshot_active;
    let active_maintenance_detail = if snapshot_active {
        Some("snapshot creation job is queued, running or waiting to retry".to_owned())
    } else if cleanup_active {
        Some("snapshot cleanup lease is active".to_owned())
    } else if gc_active {
        Some("payload GC delete_pending lease is active".to_owned())
    } else {
        None
    };

    Ok((
        current,
        CutoverSharedState {
            remote_head_version,
            checkpoint_version,
            checkpoint_ready,
            available_checkpoints,
            snapshot_or_gc_active,
            active_maintenance_detail,
        },
    ))
}

/// Inspects cutover readiness for `volume` without modifying any state.
///
/// # Errors
///
/// Returns `VolumeProtocolRejected(Missing)` if the volume has no policy, or a
/// classified PostgreSQL error.
pub async fn inspect_cutover_dry_run<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
    request: &CutoverRequest,
    actor: &ClientCapabilities,
    local: &CutoverLocalState,
) -> MetaResult<CutoverDryRunReport> {
    let (current, shared) = collect_cutover_shared_state(client, volume).await?;
    let sessions = super::client_sessions::read_client_sessions(client, volume).await?;
    Ok(evaluate_cutover_readiness(
        &current, request, actor, &sessions, local, &shared,
    ))
}

/// Atomically validates readiness and applies an explicit feature cutover
/// under the same `volumes` / `metadata_state` row locks used by publication.
///
/// # Errors
///
/// Returns `EpochConflict` if `expected_epoch` does not match,
/// `CutoverRefused` with the blocker list if readiness fails, or a classified
/// PostgreSQL error.
pub async fn execute_feature_cutover(
    client: &mut tokio_postgres::Client,
    volume: i64,
    expected_epoch: Option<u64>,
    request: &CutoverRequest,
    actor: &ClientCapabilities,
    actor_id: &str,
    local: &CutoverLocalState,
) -> MetaResult<CutoverDryRunReport> {
    let failed = |error| PgMetaEngine::classify_pg_error("execute feature cutover", error);
    let tx = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
        .start()
        .await
        .map_err(failed)?;
    lock_volume_for_protocol_update(&tx, volume, "execute feature cutover").await?;
    let (current, shared) = collect_cutover_shared_state(&tx, volume).await?;
    if let Some(expected) = expected_epoch
        && current.epoch != expected
    {
        return Err(ProtocolRejection::EpochConflict {
            expected,
            found: current.epoch,
        }
        .into());
    }
    let sessions = super::client_sessions::read_client_sessions(&tx, volume).await?;
    let report = evaluate_cutover_readiness(&current, request, actor, &sessions, local, &shared);
    if !report.is_ready() {
        let summary = report
            .blockers
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        return Err(ProtocolRejection::CutoverRefused(format!(
            "`{}` ({}) blocked: {summary}",
            request.feature,
            request.phase.as_str()
        ))
        .into());
    }
    if let Some(proposed) = &report.proposed
        && report.next_epoch != report.current_epoch
    {
        apply_locked_volume_protocol_update(
            &tx,
            volume,
            &current,
            proposed,
            actor_id,
            "execute feature cutover",
        )
        .await?;
    }
    tx.commit().await.map_err(failed)?;
    Ok(report)
}
