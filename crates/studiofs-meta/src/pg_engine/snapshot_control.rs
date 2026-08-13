use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;

use super::{OverlayConflictSets, RemoteApplyCoordinator, RemoteReplayApplyContext, sql};
use super::{PgMetaEngine, janitor::pg_janitor_lock_key};
use crate::engine::{MetaError, MetaInodes, MetaResult};
use crate::redb_engine::RedbMetaEngine;
use crate::snapshot::{
    ServingCheckpoint, SnapshotCatalogEntry, SnapshotCreationState, SnapshotCut, SnapshotId,
    SnapshotLifecycle, SnapshotOrigin, SnapshotRetention, SnapshotSha256, SnapshotVolumeContext,
    VolumeId,
};
use crate::snapshot_ports::{
    SnapshotPortError, SnapshotPortFuture, SnapshotReconstruction, SnapshotSourceDescriptor,
    SnapshotSourceReader,
};
use std::path::Path;
use std::time::Duration;

/// Inputs captured by one PostgreSQL Snapshot v2 registration transaction.
#[derive(Debug, Clone)]
pub(crate) struct SnapshotCaptureRequest {
    /// Verified PostgreSQL volume identifier.
    pub(crate) volume_id: VolumeId,
    /// Explicit manual or automatic origin.
    pub(crate) origin: SnapshotOrigin,
    /// Retention beginning at the later Ready transition.
    pub(crate) retention: SnapshotRetention,
}

struct RawSnapshotSource {
    cut_version: i64,
    captured_at: DateTime<Utc>,
    checkpoint_version: i64,
    checkpoint_format_version: i16,
    checkpoint_schema_version: i16,
    checkpoint_bytes: Vec<u8>,
    checkpoint_sha256: Vec<u8>,
    event_start_seq: Option<i64>,
    event_end_seq: Option<i64>,
    event_start_version: Option<i64>,
    event_end_version: Option<i64>,
    event_row_count: i64,
    event_rows: Vec<tokio_postgres::Row>,
}

struct LoadedSnapshotSource {
    cut: SnapshotCut,
    descriptor: SnapshotSourceDescriptor,
    checkpoint_bytes: Vec<u8>,
    event_rows: Vec<tokio_postgres::Row>,
}

enum SnapshotSourceError {
    Retryable,
    Permanent,
}

impl SnapshotSourceError {
    fn retryable(_: MetaError) -> Self {
        Self::Retryable
    }

    fn permanent(_: MetaError) -> Self {
        Self::Permanent
    }

    fn local(error: MetaError) -> Self {
        if matches!(error, MetaError::Serialization(_)) {
            Self::Permanent
        } else {
            Self::Retryable
        }
    }

    fn into_port_error(self) -> SnapshotPortError {
        match self {
            Self::Retryable => SnapshotPortError::retryable(
                "snapshot source reconstruct",
                "snapshot source is temporarily unavailable",
            ),
            Self::Permanent => SnapshotPortError::permanent(
                "invalid_source",
                "snapshot source is invalid or incomplete",
            ),
        }
    }
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
        };

        Self::run_async_once(async move {
            Self::with_postgres_client_timeout_once(
                postgres_url,
                Self::default_connect_timeout(),
                Self::default_command_timeout(),
                "create snapshot",
                |mut client| async move {
                    let tx = client
                        .transaction()
                        .await
                        .map_err(|_| snapshot_control_error("begin snapshot transaction"))?;
                    let entry = capture_snapshot_in_transaction(&tx, &request).await?;
                    tx.commit()
                        .await
                        .map_err(|_| snapshot_control_error("commit snapshot transaction"))?;
                    Ok(entry)
                },
            )
            .await
        })
    }

    async fn load_snapshot_source(
        postgres_url: String,
        volume_id: VolumeId,
        command_timeout: Duration,
        snapshot_id_text: String,
        expected_cut: Option<SnapshotCut>,
        expected_descriptor: Option<SnapshotSourceDescriptor>,
        load_event_rows: bool,
    ) -> Result<LoadedSnapshotSource, SnapshotSourceError> {
        let query_snapshot_id = snapshot_id_text.clone();
        let raw = Self::with_postgres_client_timeout_once(
            postgres_url,
            Self::default_connect_timeout(),
            command_timeout,
            "reconstruct snapshot source",
            move |client| async move {
                let Some(source) = client
                    .query_opt(
                        sql::SELECT_SNAPSHOT_SOURCE,
                        &[&volume_id, &query_snapshot_id],
                    )
                    .await
                    .map_err(|_| snapshot_control_error("read snapshot source checkpoint"))?
                else {
                    return Ok(None);
                };

                let cut_version: i64 = source.get(0);
                let captured_at: DateTime<Utc> = source.get(1);
                let checkpoint_version: i64 = source.get(2);
                let checkpoint_format_version: i16 = source.get(3);
                let checkpoint_schema_version: i16 = source.get(4);
                let checkpoint_bytes: Vec<u8> = source.get(5);
                let checkpoint_sha256: Vec<u8> = source.get(6);
                let event_start_seq: Option<i64> = source.get(7);
                let event_end_seq: Option<i64> = source.get(8);
                let event_start_version: Option<i64> = source.get(9);
                let event_end_version: Option<i64> = source.get(10);
                let event_row_count: i64 = source.get(11);
                let event_rows = if load_event_rows {
                    client
                        .query(
                            sql::SELECT_SNAPSHOT_EVENT_ROWS,
                            &[
                                &volume_id,
                                &checkpoint_version,
                                &cut_version,
                                &event_start_seq,
                                &event_end_seq,
                            ],
                        )
                        .await
                        .map_err(|_| snapshot_control_error("read snapshot event range"))?
                } else {
                    Vec::new()
                };
                Ok(Some(RawSnapshotSource {
                    cut_version,
                    captured_at,
                    checkpoint_version,
                    checkpoint_format_version,
                    checkpoint_schema_version,
                    checkpoint_bytes,
                    checkpoint_sha256,
                    event_start_seq,
                    event_end_seq,
                    event_start_version,
                    event_end_version,
                    event_row_count,
                    event_rows,
                }))
            },
        )
        .await
        .map_err(SnapshotSourceError::retryable)?
        .ok_or_else(|| {
            SnapshotSourceError::permanent(snapshot_control_error(
                "snapshot source checkpoint is missing",
            ))
        })?;

        let cut = SnapshotCut::new(volume_id, raw.cut_version, raw.captured_at).map_err(|_| {
            SnapshotSourceError::permanent(snapshot_control_error("snapshot source cut is invalid"))
        })?;
        if raw.checkpoint_version < 0
            || raw.checkpoint_version > cut.cut_version()
            || raw.checkpoint_format_version != ServingCheckpoint::FORMAT_VERSION
            || raw.checkpoint_schema_version != ServingCheckpoint::SCHEMA_VERSION
            || raw.checkpoint_sha256.as_slice() != Sha256::digest(&raw.checkpoint_bytes).as_slice()
        {
            return Err(SnapshotSourceError::permanent(snapshot_control_error(
                "snapshot source checkpoint is invalid",
            )));
        }
        let replay_proof = SnapshotSha256::from_bytes(
            raw.checkpoint_sha256.as_slice().try_into().map_err(|_| {
                SnapshotSourceError::permanent(snapshot_control_error(
                    "snapshot source checksum is invalid",
                ))
            })?,
        );
        let descriptor = SnapshotSourceDescriptor {
            checkpoint_id: snapshot_id_text,
            checkpoint_version: raw.checkpoint_version,
            checkpoint_format_version: raw.checkpoint_format_version,
            checkpoint_schema_version: raw.checkpoint_schema_version,
            event_start_seq: raw.event_start_seq,
            event_end_seq: raw.event_end_seq,
            event_start_version: raw.event_start_version,
            event_end_version: raw.event_end_version,
            event_row_count: raw.event_row_count,
            replay_proof,
        };
        if expected_cut.is_some_and(|expected| expected != cut)
            || expected_descriptor
                .as_ref()
                .is_some_and(|expected| expected != &descriptor)
        {
            return Err(SnapshotSourceError::permanent(snapshot_control_error(
                "snapshot source descriptor does not match retained source",
            )));
        }
        if load_event_rows {
            validate_snapshot_event_rows(&raw, &cut, &descriptor)?;
        }

        Ok(LoadedSnapshotSource {
            cut,
            descriptor,
            checkpoint_bytes: raw.checkpoint_bytes,
            event_rows: raw.event_rows,
        })
    }

    fn reconstruct_loaded_snapshot_source(
        loaded: LoadedSnapshotSource,
        destination: &Path,
    ) -> Result<SnapshotReconstruction, SnapshotSourceError> {
        let checkpoint: ServingCheckpoint = postcard::from_bytes(&loaded.checkpoint_bytes)
            .map_err(|_| {
                SnapshotSourceError::permanent(snapshot_control_error(
                    "decode snapshot source checkpoint",
                ))
            })?;
        let local = RedbMetaEngine::open(destination).map_err(SnapshotSourceError::retryable)?;
        local
            .import_serving_checkpoint(&checkpoint)
            .map_err(SnapshotSourceError::local)?;
        let outcome = RemoteApplyCoordinator::new(&local)
            .apply_event_rows(
                loaded.event_rows,
                None,
                "\0studiofs-snapshot-replay",
                RemoteReplayApplyContext {
                    last_seq: loaded.descriptor.checkpoint_version,
                    current_namespace_version: loaded.descriptor.checkpoint_version,
                    flushed_event_seq: 0,
                    next_event_seq: local
                        .get_counter("nextEventSeq")
                        .map_err(SnapshotSourceError::local)?,
                    initial_overlay: OverlayConflictSets::default(),
                },
            )
            .map_err(SnapshotSourceError::local)?;
        if outcome.max_version != loaded.cut.cut_version() {
            return Err(SnapshotSourceError::permanent(snapshot_control_error(
                "snapshot replay did not reach captured cut",
            )));
        }

        Ok(SnapshotReconstruction {
            path: destination.to_path_buf(),
        })
    }
}

fn validate_snapshot_event_rows(
    raw: &RawSnapshotSource,
    cut: &SnapshotCut,
    descriptor: &SnapshotSourceDescriptor,
) -> Result<(), SnapshotSourceError> {
    if raw.event_row_count < 0 {
        return Err(SnapshotSourceError::permanent(snapshot_control_error(
            "snapshot event count is invalid",
        )));
    }
    let actual_count = i64::try_from(raw.event_rows.len()).map_err(|_| {
        SnapshotSourceError::permanent(snapshot_control_error("snapshot event count is too large"))
    })?;
    if actual_count != raw.event_row_count {
        return Err(SnapshotSourceError::permanent(snapshot_control_error(
            "snapshot event range is incomplete",
        )));
    }
    if raw.event_row_count == 0 {
        if cut.cut_version() != raw.checkpoint_version
            || descriptor.event_start_seq.is_some()
            || descriptor.event_end_seq.is_some()
            || descriptor.event_start_version.is_some()
            || descriptor.event_end_version.is_some()
        {
            return Err(SnapshotSourceError::permanent(snapshot_control_error(
                "empty snapshot event range is invalid",
            )));
        }
        return Ok(());
    }

    let (min_seq, max_seq, min_version, max_version) = raw.event_rows.iter().fold(
        (i64::MAX, i64::MIN, i64::MAX, i64::MIN),
        |(min_seq, max_seq, min_version, max_version), row| {
            let batch_version: i64 = row.get(0);
            let seq_id: i64 = row.get(1);
            (
                min_seq.min(seq_id),
                max_seq.max(seq_id),
                min_version.min(batch_version),
                max_version.max(batch_version),
            )
        },
    );
    if descriptor.event_start_seq != Some(min_seq)
        || descriptor.event_end_seq != Some(max_seq)
        || descriptor.event_start_version != Some(min_version)
        || descriptor.event_end_version != Some(max_version)
        || max_version != cut.cut_version()
    {
        return Err(SnapshotSourceError::permanent(snapshot_control_error(
            "snapshot event range does not reach captured cut",
        )));
    }
    Ok(())
}

impl SnapshotSourceReader for PgMetaEngine {
    fn load_descriptor<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
    ) -> SnapshotPortFuture<'a, SnapshotSourceDescriptor> {
        Box::pin(async move {
            let loaded = Self::load_snapshot_source(
                self.postgres_url.clone(),
                self.volume_id,
                self.command_timeout,
                snapshot_id.to_string(),
                None,
                None,
                false,
            )
            .await
            .map_err(SnapshotSourceError::into_port_error)?;
            Ok(loaded.descriptor)
        })
    }

    fn reconstruct<'a>(
        &'a self,
        cut: &'a SnapshotCut,
        descriptor: &'a SnapshotSourceDescriptor,
        destination: &'a Path,
    ) -> SnapshotPortFuture<'a, SnapshotReconstruction> {
        Box::pin(async move {
            if cut.volume_id() != self.volume_id
                || descriptor.checkpoint_version < 0
                || descriptor.checkpoint_version > cut.cut_version()
                || descriptor.checkpoint_format_version != ServingCheckpoint::FORMAT_VERSION
                || descriptor.checkpoint_schema_version != ServingCheckpoint::SCHEMA_VERSION
            {
                return Err(SnapshotPortError::permanent(
                    "invalid_source",
                    "snapshot source descriptor is invalid",
                ));
            }
            let snapshot_id = SnapshotId::parse(&descriptor.checkpoint_id).map_err(|_| {
                SnapshotPortError::permanent(
                    "invalid_source",
                    "snapshot source descriptor is invalid",
                )
            })?;
            let loaded = Self::load_snapshot_source(
                self.postgres_url.clone(),
                self.volume_id,
                self.command_timeout,
                snapshot_id.to_string(),
                Some(*cut),
                Some(descriptor.clone()),
                true,
            )
            .await
            .map_err(SnapshotSourceError::into_port_error)?;
            let destination = destination.to_path_buf();
            tokio::task::spawn_blocking(move || {
                Self::reconstruct_loaded_snapshot_source(loaded, &destination)
                    .map_err(SnapshotSourceError::into_port_error)
            })
            .await
            .map_err(|_| {
                SnapshotPortError::retryable(
                    "snapshot source reconstruct",
                    "snapshot reconstruction task failed",
                )
            })?
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
#[cfg(test)]
pub(crate) async fn capture_snapshot_cut<C>(
    client: &C,
    volume_id: VolumeId,
) -> MetaResult<SnapshotCut>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "SELECT version, clock_timestamp() FROM metadata_state WHERE vol = $1 FOR SHARE",
            &[&volume_id],
        )
        .await
        .map_err(|_| snapshot_control_error("read published checkpoint"))?
        .ok_or_else(|| snapshot_control_error("published checkpoint is missing"))?;
    let cut_version: i64 = row.get(0);
    let captured_at: DateTime<Utc> = row.get(1);
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
    request
        .retention
        .validate_for_origin(request.origin)
        .map_err(|_| snapshot_control_error("snapshot retention does not match origin"))?;

    let volume_row = client
        .query_opt(
            "WITH snapshot_lock AS (SELECT pg_advisory_xact_lock($1)) SELECT volumes.name, volumes.uuid FROM volumes CROSS JOIN snapshot_lock WHERE volumes.vol = $2 FOR UPDATE",
            &[&pg_janitor_lock_key(request.volume_id), &request.volume_id],
        )
        .await
        .map_err(|_| snapshot_control_error("lock snapshot janitor lease and volume"))?
        .ok_or_else(|| snapshot_control_error("snapshot volume is missing"))?;
    let volume_name: String = volume_row.get(0);
    let volume_uuid: String = volume_row.get(1);
    let volume = SnapshotVolumeContext::new(request.volume_id, volume_name, volume_uuid)
        .map_err(|_| snapshot_control_error("snapshot volume identity is invalid"))?;

    let checkpoint_row = client
        .query_opt(
            "SELECT version, checkpoint_version, clock_timestamp(), snapshot FROM metadata_state WHERE vol = $1 FOR SHARE",
            &[&request.volume_id],
        )
        .await
        .map_err(|_| snapshot_control_error("read published checkpoint"))?
        .ok_or_else(|| snapshot_control_error("snapshot checkpoint is missing"))?;
    let cut_version: i64 = checkpoint_row.get(0);
    let checkpoint_version: i64 = checkpoint_row.get(1);
    let captured_at: DateTime<Utc> = checkpoint_row.get(2);
    let cut = SnapshotCut::new(request.volume_id, cut_version, captured_at)
        .map_err(|_| snapshot_control_error("published checkpoint is invalid"))?;
    let checkpoint_bytes: Vec<u8> = checkpoint_row.get(3);
    if checkpoint_version < 0 || checkpoint_version > cut_version {
        return Err(snapshot_control_error(
            "published checkpoint version is invalid",
        ));
    }
    postcard::from_bytes::<ServingCheckpoint>(&checkpoint_bytes)
        .map_err(|_| snapshot_control_error("published checkpoint payload is invalid"))?;
    let event_bounds = client
            .query_one(
            "SELECT min(seq_id), max(seq_id), min(batch_version), max(batch_version), count(*)::BIGINT FROM event_log WHERE vol = $1 AND batch_version > $2 AND batch_version <= $3",
            &[&request.volume_id, &checkpoint_version, &cut.cut_version()],
        )
        .await
        .map_err(|_| snapshot_control_error("read snapshot event range"))?;
    let event_start_seq: Option<i64> = event_bounds.get(0);
    let event_end_seq: Option<i64> = event_bounds.get(1);
    let event_start_version: Option<i64> = event_bounds.get(2);
    let event_end_version: Option<i64> = event_bounds.get(3);
    let event_row_count: i64 = event_bounds.get(4);
    let base_id = SnapshotId::from_cut(&volume.volume_name, cut.captured_at())
        .map_err(|_| snapshot_control_error("snapshot ID allocation failed"))?;
    let snapshot_id = allocate_snapshot_id(
        client,
        request.volume_id,
        &volume.volume_name,
        cut.captured_at(),
        &base_id,
    )
    .await?;
    let snapshot_id_text = snapshot_id.to_string();

    let retention_seconds = request
        .retention
        .ttl()
        .map(|duration| {
            i64::try_from(duration.as_secs())
                .map_err(|_| snapshot_control_error("snapshot retention is too large"))
        })
        .transpose()?;
    let origin = request.origin.as_str().to_string();
    let lifecycle = SnapshotLifecycle::Creating.as_str().to_string();
    let creation_state = SnapshotCreationState::Queued.as_str().to_string();
    client
        .execute(
            "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle, retention_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &request.volume_id,
                &snapshot_id_text,
                &origin,
                &cut.cut_version(),
                &cut.captured_at(),
                &lifecycle,
                &retention_seconds,
            ],
        )
        .await
        .map_err(|_| snapshot_control_error("insert snapshot catalog row"))?;

    let checkpoint_sha256 = Sha256::digest(&checkpoint_bytes).to_vec();
    client
        .execute(
            "INSERT INTO snapshot_source_checkpoints (vol, snapshot_id, cut_version, checkpoint_version, checkpoint_format_version, checkpoint_schema_version, checkpoint_bytes, checkpoint_sha256, event_start_seq, event_end_seq, event_start_version, event_end_version, event_row_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            &[
                &request.volume_id,
                &snapshot_id_text,
                &cut.cut_version(),
                &checkpoint_version,
                &ServingCheckpoint::FORMAT_VERSION,
                &ServingCheckpoint::SCHEMA_VERSION,
                &checkpoint_bytes,
                &checkpoint_sha256,
                &event_start_seq,
                &event_end_seq,
                &event_start_version,
                &event_end_version,
                &event_row_count,
            ],
        )
        .await
        .map_err(|_| snapshot_control_error("retain snapshot checkpoint"))?;

    client
        .execute(
            "INSERT INTO snapshot_creation_jobs (vol, snapshot_id, state) VALUES ($1, $2, $3)",
            &[&request.volume_id, &snapshot_id_text, &creation_state],
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

pub(crate) async fn allocate_snapshot_id<C>(
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
    let base_id_text = base_id.to_string();
    let rows = client
        .query(
            "SELECT id FROM snapshot_catalog WHERE vol = $1 AND (id = $2 OR id LIKE $3 ESCAPE '\\') FOR UPDATE",
            &[&volume_id, &base_id_text, &prefix],
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
    let message = format!("snapshot control plane operation failed: {operation}");
    match operation {
        "published checkpoint is missing"
        | "snapshot volume is missing"
        | "snapshot checkpoint is missing" => MetaError::Storage(message),
        _ => MetaError::RemoteUnavailable(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_control_plane_rows_are_permanent() {
        for operation in [
            "published checkpoint is missing",
            "snapshot volume is missing",
            "snapshot checkpoint is missing",
        ] {
            assert!(matches!(
                snapshot_control_error(operation),
                MetaError::Storage(_)
            ));
        }

        assert!(matches!(
            snapshot_control_error("read published checkpoint"),
            MetaError::RemoteUnavailable(_)
        ));
    }
}
