//! PostgreSQL storage of the per-volume protocol policy (SFS-1007).
//!
//! The policy record is `volume_protocol` (epoch plus required sets and
//! feature states); its write requirement lives in `metadata_state`, the row
//! `BUMP_METADATA_VERSION` already updates, so publication checks it without
//! an extra lock or round trip. Every change locks that row first, which
//! serializes it with publication, and appends `volume_protocol_history`.

use tokio_postgres::GenericClient;

use super::{PgMetaEngine, sql};
use crate::compat::ClientCapabilities;
use crate::engine::{MetaError, MetaResult, UpgradeRequiredReason};
use crate::version::SHARED_WRITE_REQUIREMENT;
use crate::volume_protocol::{ProtocolRejection, VolumeProtocol, check_update};

const SELECT_PROTOCOL: &str = "
    SELECT p.policy_epoch, m.write_requirement, p.requirements::TEXT
    FROM metadata_state m LEFT JOIN volume_protocol p ON p.vol = m.vol
    WHERE m.vol = $1";

const INSERT_HISTORY: &str = "
    INSERT INTO volume_protocol_history (vol, policy_epoch, write_requirement, requirements, changed_by)
    VALUES ($1, $2, $3, $4::TEXT::JSONB, $5)
    ON CONFLICT (vol, policy_epoch) DO NOTHING";

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
    let baseline = VolumeProtocol::baseline();
    let requirements = baseline.encode_requirements()?;
    let epoch = epoch_i64(baseline.epoch)?;
    let write_requirement = requirement_i16(baseline.write_requirement)?;
    let inserted = client
        .execute(
            "INSERT INTO volume_protocol (vol, policy_epoch, requirements, updated_by)
             VALUES ($1, $2, $3::TEXT::JSONB, $4) ON CONFLICT (vol) DO NOTHING",
            &[&volume, &epoch, &requirements, &actor],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("insert volume protocol", error))?;
    if inserted == 1 {
        client
            .execute(
                INSERT_HISTORY,
                &[&volume, &epoch, &write_requirement, &requirements, &actor],
            )
            .await
            .map_err(|error| PgMetaEngine::classify_pg_error("record volume protocol", error))?;
    }
    Ok(())
}

async fn select_protocol<C>(client: &C, volume: i64, lock: bool) -> MetaResult<VolumeProtocol>
where
    C: GenericClient + Sync,
{
    let query = if lock {
        format!("{SELECT_PROTOCOL} FOR UPDATE OF m")
    } else {
        SELECT_PROTOCOL.to_owned()
    };
    let row = client
        .query_opt(&query, &[&volume])
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("read volume protocol", error))?;
    let Some(row) = row else {
        return Err(ProtocolRejection::Missing.into());
    };
    let (Some(epoch), Some(requirements)) = (
        row.get::<_, Option<i64>>(0),
        row.get::<_, Option<String>>(2),
    ) else {
        return Err(ProtocolRejection::Missing.into());
    };
    VolumeProtocol::decode(epoch, row.get(1), &requirements)
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
    select_protocol(client, volume, false).await
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
    let current = select_protocol(tx, volume, true).await?;
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
    let failed = |error| PgMetaEngine::classify_pg_error("update volume protocol", error);
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

/// `SHARED_WRITE_REQUIREMENT` as the `$4` parameter of `BUMP_METADATA_VERSION`.
pub(super) const PUBLICATION_WRITE_REQUIREMENT: i16 =
    SHARED_WRITE_REQUIREMENT.current_pg_smallint();

/// Explains a `BUMP_METADATA_VERSION` that matched no row: an unsupported
/// format or a raised write requirement is a typed refusal, so the caller
/// keeps its journal as unpublished local work.
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
    if let Err(error) =
        crate::ensure_supported_pg_sync_format_version(row.as_ref().and_then(|row| row.get(0)))
    {
        return error;
    }
    let found = row.map_or(0, |row| row.get::<_, i16>(1));
    if found > PUBLICATION_WRITE_REQUIREMENT {
        return MetaError::UpgradeRequired {
            reason: UpgradeRequiredReason::WriteRequirementNewer {
                found: found.unsigned_abs(),
                supported: SHARED_WRITE_REQUIREMENT.current,
            },
        };
    }
    MetaError::Storage(
        "postgres metadata version bump did not update the current-format row".into(),
    )
}
