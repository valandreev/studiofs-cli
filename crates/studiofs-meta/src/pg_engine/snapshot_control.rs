use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;

use super::PgMetaEngine;
use crate::connect_studiofs_postgres;
use crate::engine::{MetaError, MetaResult};
use crate::snapshot::{
    SnapshotCatalogEntry, SnapshotCreationState, SnapshotCut, SnapshotId, SnapshotLifecycle,
    SnapshotOrigin, SnapshotRetention, SnapshotVolumeContext, VolumeId,
};

/// Inputs captured by one PostgreSQL Snapshot v2 registration transaction.
#[derive(Debug, Clone)]
pub(crate) struct SnapshotCaptureRequest {
    /// Verified PostgreSQL volume identifier.
    pub(crate) volume_id: VolumeId,
    /// Explicit manual or automatic origin.
    pub(crate) origin: SnapshotOrigin,
    /// Retention beginning at the later Ready transition.
    pub(crate) retention: SnapshotRetention,
    /// Timestamp supplied by the control-plane clock.
    pub(crate) captured_at: DateTime<Utc>,
}

impl PgMetaEngine {
    /// Registers a manual or automatic Snapshot v2 cut in PostgreSQL.
    ///
    /// PostgreSQL owns the short transaction, captured cut, immutable source
    /// checkpoint, public ID allocation, catalog row, and durable queued job.
    /// The method does not perform artifact-store I/O or report the snapshot
    /// Ready before a later worker publication CAS.
    ///
    /// # Errors
    ///
    /// Returns a redacted metadata error for connection, transaction, or
    /// control-plane failures.
    pub fn create_snapshot_v2(
        &self,
        origin: SnapshotOrigin,
        retention: SnapshotRetention,
    ) -> MetaResult<SnapshotCatalogEntry> {
        let postgres_url = self.postgres_url.clone();
        let request = SnapshotCaptureRequest {
            volume_id: self.volume_id,
            origin,
            retention,
            captured_at: Utc::now(),
        };

        Self::run_async_once(async move {
            let (mut client, connection) = connect_studiofs_postgres(&postgres_url)
                .await
                .map_err(|_| snapshot_control_error("connect to PostgreSQL"))?;
            let connection_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let result = async {
                let tx = client
                    .transaction()
                    .await
                    .map_err(|_| snapshot_control_error("begin snapshot transaction"))?;
                let entry = capture_snapshot_in_transaction(&tx, &request).await?;
                tx.commit()
                    .await
                    .map_err(|_| snapshot_control_error("commit snapshot transaction"))?;
                Ok(entry)
            }
            .await;
            connection_task.abort();
            result
        })
    }
}

/// Captures the current published metadata version through a generic PG client.
///
/// The helper intentionally participates in the caller's transaction. A
/// transaction caller therefore keeps the cut and its durable job under one
/// commit boundary, while a plain client remains useful for focused adapter
/// queries and compile-time GenericClient coverage.
///
/// # Errors
///
/// Returns a redacted metadata error when the published checkpoint is absent,
/// invalid, or cannot be read.
pub(crate) async fn capture_snapshot_cut<C>(
    client: &C,
    volume_id: VolumeId,
    captured_at: DateTime<Utc>,
) -> MetaResult<SnapshotCut>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "SELECT version FROM metadata_state WHERE vol = $1 FOR SHARE",
            &[&volume_id],
        )
        .await
        .map_err(|_| snapshot_control_error("read published checkpoint"))?
        .ok_or_else(|| snapshot_control_error("published checkpoint is missing"))?;
    let cut_version: i64 = row.get(0);
    SnapshotCut::new(volume_id, cut_version, captured_at)
        .map_err(|_| snapshot_control_error("published checkpoint is invalid"))
}

/// Registers one immutable Snapshot v2 cut and its durable queued job.
///
/// The caller owns the transaction and must commit it after this function
/// returns. The volume row is locked before ID allocation, which serializes
/// same-volume captures without a client-side collision retry. The source
/// checkpoint bytes, catalog row, and creation job are written before the same
/// transaction can commit; no artifact-store I/O occurs here.
///
/// # Errors
///
/// Returns a redacted metadata error for PostgreSQL failures or invalid volume,
/// retention, identity, or checkpoint data. The caller's transaction remains
/// responsible for rollback.
pub(crate) async fn capture_snapshot_in_transaction<C>(
    client: &C,
    request: &SnapshotCaptureRequest,
) -> MetaResult<SnapshotCatalogEntry>
where
    C: GenericClient + Sync,
{
    let volume_row = client
        .query_opt(
            "SELECT name, uuid FROM volumes WHERE vol = $1 FOR UPDATE",
            &[&request.volume_id],
        )
        .await
        .map_err(|_| snapshot_control_error("lock snapshot volume"))?
        .ok_or_else(|| snapshot_control_error("snapshot volume is missing"))?;
    let volume_name: String = volume_row.get(0);
    let volume_uuid: String = volume_row.get(1);
    let volume = SnapshotVolumeContext::new(request.volume_id, volume_name, volume_uuid)
        .map_err(|_| snapshot_control_error("snapshot volume identity is invalid"))?;

    let cut = capture_snapshot_cut(client, request.volume_id, request.captured_at).await?;
    let checkpoint_row = client
        .query_opt(
            "SELECT snapshot FROM metadata_state WHERE vol = $1 FOR SHARE",
            &[&request.volume_id],
        )
        .await
        .map_err(|_| snapshot_control_error("read snapshot checkpoint"))?
        .ok_or_else(|| snapshot_control_error("snapshot checkpoint is missing"))?;
    let checkpoint_bytes: Vec<u8> = checkpoint_row.get(0);
    let base_id = SnapshotId::from_cut(&volume.volume_name, request.captured_at)
        .map_err(|_| snapshot_control_error("snapshot ID allocation failed"))?;
    let snapshot_id = allocate_snapshot_id(
        client,
        request.volume_id,
        &volume.volume_name,
        request.captured_at,
        &base_id,
    )
    .await?;

    let retention_seconds = request
        .retention
        .ttl()
        .map(|duration| {
            i64::try_from(duration.as_secs())
                .map_err(|_| snapshot_control_error("snapshot retention is too large"))
        })
        .transpose()?;
    let origin = request.origin.as_str();
    client
        .execute(
            "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle, retention_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &request.volume_id,
                &snapshot_id.as_str(),
                &origin,
                &cut.cut_version(),
                &cut.captured_at(),
                &SnapshotLifecycle::Creating.as_str(),
                &retention_seconds,
            ],
        )
        .await
        .map_err(|_| snapshot_control_error("insert snapshot catalog row"))?;

    let checkpoint_sha256 = Sha256::digest(&checkpoint_bytes).to_vec();
    client
        .execute(
            "INSERT INTO snapshot_source_checkpoints (vol, snapshot_id, cut_version, checkpoint_version, checkpoint_format_version, checkpoint_schema_version, checkpoint_bytes, checkpoint_sha256) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &request.volume_id,
                &snapshot_id.as_str(),
                &cut.cut_version(),
                &cut.cut_version(),
                &1_i16,
                &1_i16,
                &checkpoint_bytes,
                &checkpoint_sha256,
            ],
        )
        .await
        .map_err(|_| snapshot_control_error("retain snapshot checkpoint"))?;

    client
        .execute(
            "INSERT INTO snapshot_creation_jobs (vol, snapshot_id, state) VALUES ($1, $2, $3)",
            &[
                &request.volume_id,
                &snapshot_id.as_str(),
                &SnapshotCreationState::Queued.as_str(),
            ],
        )
        .await
        .map_err(|_| snapshot_control_error("insert snapshot creation job"))?;

    Ok(SnapshotCatalogEntry {
        snapshot_id,
        volume,
        origin: request.origin,
        cut,
        lifecycle: SnapshotLifecycle::Creating,
        creation_state: SnapshotCreationState::Queued,
        retention: request.retention,
        ready_at: None,
        expires_at: None,
        artifact: None,
        failure: None,
    })
}

async fn allocate_snapshot_id<C>(
    client: &C,
    volume_id: VolumeId,
    volume_name: &str,
    captured_at: DateTime<Utc>,
    base_id: &SnapshotId,
) -> MetaResult<SnapshotId>
where
    C: GenericClient + Sync,
{
    let prefix = format!("{}-%", escape_like(base_id.as_str()));
    let rows = client
        .query(
            "SELECT id FROM snapshot_catalog WHERE vol = $1 AND (id = $2 OR id LIKE $3 ESCAPE '\\') FOR UPDATE",
            &[&volume_id, &base_id.as_str(), &prefix],
        )
        .await
        .map_err(|_| snapshot_control_error("allocate snapshot ID"))?;
    let existing = rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<std::collections::HashSet<_>>();

    for ordinal in 1..=existing.len().saturating_add(1) as u64 {
        let candidate = if ordinal == 1 {
            base_id.clone()
        } else {
            SnapshotId::with_collision_suffix(volume_name, captured_at, ordinal)
                .map_err(|_| snapshot_control_error("allocate snapshot ID"))?
        };
        if !existing.contains(candidate.as_str()) {
            return Ok(candidate);
        }
    }

    Err(snapshot_control_error("allocate snapshot ID"))
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn snapshot_control_error(operation: &'static str) -> MetaError {
    MetaError::RemoteUnavailable(format!(
        "snapshot control plane operation failed: {operation}"
    ))
}
