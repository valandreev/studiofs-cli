//! Owns remote metadata snapshot reads and local checkpoint import.

#[cfg(test)]
use super::RemoteApplyCoordinator;
use super::{
    PG_EVENT_INSERT_CHUNK_SIZE, PgMetaEngine, PgSnapshotBackend, SnapshotBackendAccess,
    StartupSnapshotImportMode, runtime, sql,
};
use crate::engine::{
    BootstrapCheckpointDescriptor, CheckpointDescriptorFailure, CheckpointEncoding, MetaError,
    MetaResult,
};
use crate::events::{MetaEvent, encode_event};
use crate::snapshot::ServingCheckpoint;
use crate::version::{SERVING_CHECKPOINT_FORMAT, SERVING_CHECKPOINT_SCHEMA, SHARED_VOLUME_FORMAT};
use crate::volume_protocol::ProtocolRejection;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

const MISSING_RETAINED_PUBLICATION_STATE: &str =
    "lock retained publication: metadata state missing";

fn descriptor_row_error(error: tokio_postgres::Error) -> MetaError {
    MetaError::Storage(format!(
        "postgres bootstrap checkpoint descriptor row is invalid: {error}"
    ))
}

fn try_descriptor_from_row(row: &tokio_postgres::Row) -> MetaResult<BootstrapCheckpointDescriptor> {
    Ok(BootstrapCheckpointDescriptor {
        volume_id: row
            .try_get::<_, Option<i64>>("vol")
            .map_err(descriptor_row_error)?
            .unwrap_or_default(),
        volume_uuid: row
            .try_get::<_, Option<String>>("uuid")
            .map_err(descriptor_row_error)?
            .unwrap_or_default(),
        metadata_format_version: row
            .try_get::<_, Option<i16>>("metadata_format_version")
            .map_err(descriptor_row_error)?
            .and_then(|version| u16::try_from(version).ok())
            .unwrap_or_default(),
        remote_head_version: row
            .try_get::<_, Option<i64>>("version")
            .map_err(descriptor_row_error)?
            .unwrap_or(-1),
        checkpoint_version: row
            .try_get::<_, Option<i64>>("checkpoint_version")
            .map_err(descriptor_row_error)?
            .unwrap_or(-1),
        checkpoint_encoding: row
            .try_get::<_, Option<i16>>("checkpoint_format")
            .map_err(descriptor_row_error)?
            .zip(
                row.try_get::<_, Option<i16>>("checkpoint_schema")
                    .map_err(descriptor_row_error)?,
            )
            .map(|(format, schema)| CheckpointEncoding { format, schema }),
        checkpoint_bytes: row
            .try_get::<_, Option<Vec<u8>>>("snapshot")
            .map_err(descriptor_row_error)?
            .unwrap_or_default(),
        checkpoint_sha256: row
            .try_get::<_, Option<Vec<u8>>>("checkpoint_sha256")
            .map_err(descriptor_row_error)?
            .unwrap_or_default(),
        format_info_bytes: row
            .try_get::<_, Option<Vec<u8>>>("format_info")
            .map_err(descriptor_row_error)?
            .unwrap_or_default(),
        publication_state: row
            .try_get::<_, Option<String>>("checkpoint_publication_state")
            .map_err(descriptor_row_error)?
            .unwrap_or_default(),
        publication_generation: row
            .try_get::<_, Option<i64>>("checkpoint_generation")
            .map_err(descriptor_row_error)?
            .unwrap_or_default(),
    })
}

const MAX_ALTERNATIVE_CHECKPOINT_RETRIES: usize = 8;

#[derive(Clone, Copy, Eq, PartialEq)]
struct ExcludedCheckpoint {
    encoding: CheckpointEncoding,
    version: i64,
}

#[derive(Clone, Copy)]
enum CheckpointSelection<'a> {
    Compatible { excluded: &'a [ExcludedCheckpoint] },
    BaselineOnly,
}

fn check_bootstrap_descriptor_compatibility(
    descriptor: &BootstrapCheckpointDescriptor,
    replay_floor: i64,
    incompatible_alternative: bool,
) -> MetaResult<()> {
    crate::check_volume_format(descriptor.metadata_format_version)?;
    if !descriptor.format_info_bytes.is_empty()
        && let Err(
            error @ (MetaError::UnsupportedFormatVersion { .. }
            | MetaError::UpgradeRequired { .. }
            | MetaError::NeedsMigration { .. }),
        ) = crate::decode_format_info(&descriptor.format_info_bytes)
    {
        return Err(error);
    }
    if incompatible_alternative
        || (descriptor.checkpoint_version >= 0 && descriptor.checkpoint_version < replay_floor)
    {
        return Err(MetaError::NeedCompatibleCheckpoint { replay_floor });
    }
    Ok(())
}

pub(super) async fn read_bootstrap_checkpoint_and_decode_with_pool(
    command_pool: Arc<runtime::PgConnectionPool>,
    volume_id: i64,
    command_timeout: std::time::Duration,
) -> MetaResult<(BootstrapCheckpointDescriptor, ServingCheckpoint)> {
    PgMetaEngine::with_postgres_client_timeout(
        command_pool,
        command_timeout,
        "bootstrap checkpoint descriptor",
        |client| {
            Box::pin(async move {
                let tx = client
                    .build_transaction()
                    .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
                    .read_only(true)
                    .start()
                    .await
                    .map_err(descriptor_row_error)?;
                let read = async {
                    let (descriptor, checkpoint) = read_checkpoint(&tx, volume_id).await?;
                    super::check_volume_read_policy(&tx, volume_id, Some(&descriptor)).await?;
                    Ok((descriptor, checkpoint))
                }
                .await;
                runtime::finish_pooled_transaction(
                    tx,
                    read,
                    |error| {
                        matches!(
                            error,
                            MetaError::InvalidCheckpointDescriptor { .. }
                                | MetaError::BootstrapCheckpointDescriptorMissing
                                | MetaError::NeedCompatibleCheckpoint { .. }
                                | MetaError::UnsupportedCheckpointEncoding { .. }
                        ) || super::volume_protocol::is_volume_read_policy_rejection(error)
                    },
                    descriptor_row_error,
                )
                .await
            })
        },
    )
    .await?
}

/// Select and validate a serving checkpoint using the caller's PostgreSQL snapshot.
pub async fn read_compatible_checkpoint<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    volume_id: i64,
) -> MetaResult<BootstrapCheckpointDescriptor> {
    let (descriptor, _) = read_checkpoint(client, volume_id).await?;
    super::check_volume_read_policy(client, volume_id, Some(&descriptor)).await?;
    Ok(descriptor)
}

/// Validate the frozen replay tail without advancing any local cursor.
pub async fn validate_checkpoint_replay(
    client: &tokio_postgres::Transaction<'_>,
    descriptor: &BootstrapCheckpointDescriptor,
) -> MetaResult<()> {
    let mut cursor = descriptor.checkpoint_version;
    let target = descriptor.remote_head_version;
    while cursor < target {
        let page = PgMetaEngine::query_foreign_event_page(
            client,
            descriptor.volume_id,
            cursor,
            target,
            64,
            8 * 1024 * 1024,
        )
        .await?;
        let batches = super::RemoteApplyCoordinator::decode_event_rows(page.rows, "")?;
        let Some(last) = batches.last() else {
            return Err(MetaError::ReplayGap {
                local_version: cursor,
                first_available_version: cursor,
                target_version: target,
            });
        };
        super::RemoteApplyCoordinator::validate_decoded_batch_continuity(cursor, &batches, target)?;
        cursor = last.batch_version;
    }
    Ok(())
}

async fn read_checkpoint<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    volume_id: i64,
) -> MetaResult<(BootstrapCheckpointDescriptor, ServingCheckpoint)> {
    let has_encodings: bool = client
        .query_one(sql::CHECKPOINT_ENCODINGS_AVAILABLE, &[])
        .await
        .map_err(descriptor_row_error)?
        .get(0);
    let mut excluded = Vec::new();
    loop {
        let selection = if has_encodings && excluded.len() < MAX_ALTERNATIVE_CHECKPOINT_RETRIES {
            CheckpointSelection::Compatible {
                excluded: &excluded,
            }
        } else {
            CheckpointSelection::BaselineOnly
        };
        let descriptor = select_bootstrap_checkpoint(client, volume_id, selection).await?;
        match descriptor.validate_and_decode() {
            Err(
                MetaError::InvalidCheckpointDescriptor {
                    reason:
                        CheckpointDescriptorFailure::Checksum
                        | CheckpointDescriptorFailure::CheckpointBytes
                        | CheckpointDescriptorFailure::EnvelopeIdentity,
                }
                | MetaError::UnsupportedCheckpointEncoding { .. },
            ) if descriptor.checkpoint_encoding.is_some() => {
                if let Some(encoding) = descriptor.checkpoint_encoding {
                    let rejected = ExcludedCheckpoint {
                        encoding,
                        version: descriptor.checkpoint_version,
                    };
                    if !excluded.contains(&rejected) {
                        excluded.push(rejected);
                        continue;
                    }
                }
                // The baseline and alternatives share the caller's read snapshot.
                let baseline = select_bootstrap_checkpoint(
                    client,
                    volume_id,
                    CheckpointSelection::BaselineOnly,
                )
                .await?;
                let checkpoint = baseline.validate_and_decode()?;
                return Ok((baseline, checkpoint));
            }
            result => return result.map(|checkpoint| (descriptor, checkpoint)),
        }
    }
}

async fn select_bootstrap_checkpoint<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    volume_id: i64,
    selection: CheckpointSelection<'_>,
) -> MetaResult<BootstrapCheckpointDescriptor> {
    let row = checked_bootstrap_row(
        if let CheckpointSelection::Compatible { excluded } = selection {
            let excluded_formats: Vec<_> =
                excluded.iter().map(|item| item.encoding.format).collect();
            let excluded_schemas: Vec<_> =
                excluded.iter().map(|item| item.encoding.schema).collect();
            let excluded_versions: Vec<_> = excluded.iter().map(|item| item.version).collect();
            client
                .query_opt(
                    sql::SELECT_COMPATIBLE_CHECKPOINT_DESCRIPTOR,
                    &[
                        &volume_id,
                        &(SERVING_CHECKPOINT_FORMAT.min_supported as i16),
                        &ServingCheckpoint::FORMAT_VERSION,
                        &(SERVING_CHECKPOINT_SCHEMA.min_supported as i16),
                        &ServingCheckpoint::SCHEMA_VERSION,
                        &excluded_formats,
                        &excluded_schemas,
                        &excluded_versions,
                    ],
                )
                .await
        } else {
            client
                .query_opt(sql::SELECT_BOOTSTRAP_CHECKPOINT_DESCRIPTOR, &[&volume_id])
                .await
        },
    )?;
    let descriptor = try_descriptor_from_row(&row)?;
    let replay_floor: i64 = row
        .try_get("event_log_prune_floor")
        .map_err(descriptor_row_error)?;
    let incompatible_alternative: bool = row
        .try_get("incompatible_alternative_present")
        .map_err(descriptor_row_error)?;
    check_bootstrap_descriptor_compatibility(&descriptor, replay_floor, incompatible_alternative)?;
    Ok(descriptor)
}

fn checked_bootstrap_row(
    row: Result<Option<tokio_postgres::Row>, tokio_postgres::Error>,
) -> MetaResult<tokio_postgres::Row> {
    row.map_err(|error| {
        PgMetaEngine::classify_pg_error("postgres bootstrap checkpoint select failed", error)
    })?
    .ok_or(MetaError::BootstrapCheckpointDescriptorMissing)
}

async fn read_bootstrap_checkpoint_with_pool(
    command_pool: Arc<runtime::PgConnectionPool>,
    volume_id: i64,
    command_timeout: std::time::Duration,
) -> MetaResult<BootstrapCheckpointDescriptor> {
    let (descriptor, _) =
        read_bootstrap_checkpoint_and_decode_with_pool(command_pool, volume_id, command_timeout)
            .await?;
    Ok(descriptor)
}

impl PgSnapshotBackend {
    /// Reads and validates the one-row PostgreSQL bootstrap checkpoint record.
    pub fn read_bootstrap_checkpoint(&self) -> MetaResult<BootstrapCheckpointDescriptor> {
        PgMetaEngine::run_async_once(read_bootstrap_checkpoint_with_pool(
            self.snapshot_command_pool(),
            self.snapshot_volume_id(),
            self.snapshot_command_timeout(),
        ))
    }
}

pub(super) struct ForeignEventPage {
    pub(super) rows: Vec<tokio_postgres::Row>,
    pub(super) first_batch_version: Option<i64>,
    pub(super) last_batch_version: Option<i64>,
    pub(super) batch_count: i64,
    pub(super) event_count: i64,
    pub(super) response_bytes: i64,
    pub(super) has_more: bool,
    pub(super) oversized: bool,
}

struct EncodedRemoteEventBatch {
    events: Vec<MetaEvent>,
    payloads: Vec<Vec<u8>>,
    payload_bytes: i64,
}

async fn read_remote_metadata_head_version_with_pool(
    command_pool: Arc<runtime::PgConnectionPool>,
    volume_id: i64,
    command_timeout: std::time::Duration,
) -> MetaResult<i64> {
    PgMetaEngine::with_postgres_client_timeout(
        command_pool,
        command_timeout,
        "metadata head version",
        move |client| {
            Box::pin(async move {
                let row = client
                    .query_opt(sql::SELECT_METADATA_VERSION, &[&volume_id])
                    .await
                    .map_err(|e| {
                        PgMetaEngine::classify_pg_error(
                            "postgres metadata head version select failed",
                            e,
                        )
                    })?;
                let head = match row {
                    Some(row) => row
                        .try_get::<_, Option<i64>>(0)
                        .map_err(|e| {
                            PgMetaEngine::classify_pg_error(
                                "postgres metadata head version row parse failed",
                                e,
                            )
                        })?
                        .unwrap_or(0),
                    None => 0,
                };
                Ok(head)
            })
        },
    )
    .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RemoteHeadCheckpointState {
    remote_head_version: i64,
    checkpoint_version: i64,
    replay_floor: i64,
}

fn should_download_startup_checkpoint(
    local_version: i64,
    head: RemoteHeadCheckpointState,
) -> MetaResult<bool> {
    if head.remote_head_version <= local_version {
        return Ok(false);
    }
    if head.checkpoint_version < local_version {
        if local_version < head.replay_floor {
            return Err(MetaError::NeedCompatibleCheckpoint {
                replay_floor: head.replay_floor,
            });
        }
        return Ok(false);
    }
    Ok(true)
}

async fn read_remote_metadata_head_and_checkpoint_version_with_pool(
    command_pool: Arc<runtime::PgConnectionPool>,
    volume_id: i64,
    command_timeout: std::time::Duration,
) -> MetaResult<RemoteHeadCheckpointState> {
    PgMetaEngine::with_postgres_client_timeout(
        command_pool,
        command_timeout,
        "metadata head and checkpoint version",
        move |client| {
            Box::pin(async move {
                let row = match client
                    .query_opt(
                        sql::SELECT_METADATA_HEAD_AND_COMPATIBLE_CHECKPOINT_VERSION,
                        &[
                            &volume_id,
                            &(SERVING_CHECKPOINT_FORMAT.min_supported as i16),
                            &ServingCheckpoint::FORMAT_VERSION,
                            &(SERVING_CHECKPOINT_SCHEMA.min_supported as i16),
                            &ServingCheckpoint::SCHEMA_VERSION,
                        ],
                    )
                    .await
                {
                    Err(error)
                        if error.code()
                            == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE) =>
                    {
                        client
                            .query_opt(
                                sql::SELECT_METADATA_HEAD_AND_CHECKPOINT_VERSION,
                                &[&volume_id],
                            )
                            .await
                            .map_err(|e| {
                                PgMetaEngine::classify_pg_error(
                                    "postgres metadata head and checkpoint version select failed",
                                    e,
                                )
                            })?
                    }
                    result => result.map_err(|e| {
                        PgMetaEngine::classify_pg_error(
                            "postgres metadata head and checkpoint version select failed",
                            e,
                        )
                    })?,
                };
                let Some(row) = row else {
                    return Ok(RemoteHeadCheckpointState {
                        remote_head_version: 0,
                        checkpoint_version: -1,
                        replay_floor: 0,
                    });
                };
                let head = row
                    .try_get::<_, Option<i64>>(0)
                    .map_err(|e| {
                        PgMetaEngine::classify_pg_error(
                            "postgres metadata head version row parse failed",
                            e,
                        )
                    })?
                    .unwrap_or(0);
                let checkpoint = row
                    .try_get::<_, Option<i64>>(1)
                    .map_err(|e| {
                        PgMetaEngine::classify_pg_error(
                            "postgres checkpoint version row parse failed",
                            e,
                        )
                    })?
                    .unwrap_or(-1);
                let replay_floor = row
                    .try_get::<_, Option<i64>>(2)
                    .map_err(|e| {
                        PgMetaEngine::classify_pg_error(
                            "postgres event-log prune floor row parse failed",
                            e,
                        )
                    })?
                    .unwrap_or(0);
                Ok(RemoteHeadCheckpointState {
                    remote_head_version: head,
                    checkpoint_version: checkpoint,
                    replay_floor,
                })
            })
        },
    )
    .await
}

#[derive(Clone, Copy)]
pub(super) struct CheckpointFreshnessObservation {
    pub(super) head_version: u64,
    pub(super) checkpoint_version: u64,
    pub(super) total_event_payload_bytes: u64,
    pub(super) format_info_present: bool,
    pub(super) event_payload_bytes: u64,
    pub(super) checkpoint_age: std::time::Duration,
}

fn nonnegative_counter(value: i64, name: &str) -> MetaResult<u64> {
    u64::try_from(value)
        .map_err(|_| MetaError::Storage(format!("postgres {name} must be nonnegative")))
}

fn row_i64(row: &tokio_postgres::Row, index: usize, name: &str) -> MetaResult<i64> {
    row.try_get(index).map_err(|error| {
        PgMetaEngine::classify_pg_error(format!("postgres {name} row parse failed"), error)
    })
}

pub(super) fn checkpoint_freshness_observation_from_row(
    row: &tokio_postgres::Row,
) -> MetaResult<CheckpointFreshnessObservation> {
    let head_version = nonnegative_counter(
        row_i64(row, 0, "checkpoint freshness head")?,
        "head version",
    )?;
    let checkpoint_version = nonnegative_counter(
        row_i64(row, 1, "checkpoint freshness checkpoint")?,
        "checkpoint version",
    )?;
    let total_event_payload_bytes = nonnegative_counter(
        row_i64(row, 2, "checkpoint freshness total byte counter")?,
        "total event payload bytes",
    )?;
    let checkpoint_total_event_bytes = nonnegative_counter(
        row_i64(row, 3, "checkpoint freshness checkpoint byte counter")?,
        "checkpoint total event payload bytes",
    )?;
    let event_payload_bytes = total_event_payload_bytes
        .checked_sub(checkpoint_total_event_bytes)
        .ok_or_else(|| {
            MetaError::Storage(
                "postgres checkpoint byte counters are not monotonically ordered".to_string(),
            )
        })?;
    let checkpoint_age_ms = nonnegative_counter(
        row_i64(row, 4, "checkpoint freshness age")?,
        "checkpoint age",
    )?;
    let format_info_present = row.try_get(5).map_err(|error| {
        PgMetaEngine::classify_pg_error(
            "postgres checkpoint freshness format-info flag parse failed",
            error,
        )
    })?;
    let metadata_format_version = row.try_get::<_, Option<i16>>(6).map_err(|error| {
        PgMetaEngine::classify_pg_error(
            "postgres checkpoint freshness format version parse failed",
            error,
        )
    })?;
    crate::check_pg_volume_format(metadata_format_version)?;

    Ok(CheckpointFreshnessObservation {
        head_version,
        checkpoint_version,
        total_event_payload_bytes,
        format_info_present,
        event_payload_bytes,
        checkpoint_age: std::time::Duration::from_millis(checkpoint_age_ms),
    })
}

async fn read_checkpoint_freshness_with_pool(
    command_pool: Arc<runtime::PgConnectionPool>,
    volume_id: i64,
    command_timeout: std::time::Duration,
) -> MetaResult<CheckpointFreshnessObservation> {
    PgMetaEngine::with_postgres_client_timeout(
        command_pool,
        command_timeout,
        "checkpoint freshness observation",
        move |client| {
            Box::pin(async move {
                let row = client
                    .query_opt(sql::SELECT_CHECKPOINT_FRESHNESS, &[&volume_id])
                    .await
                    .map_err(|e| {
                        PgMetaEngine::classify_pg_error(
                            "postgres checkpoint freshness select failed",
                            e,
                        )
                    })?
                    .ok_or_else(|| {
                        MetaError::Storage(
                            "postgres metadata_state row missing during checkpoint freshness observation"
                                .to_string(),
                        )
                    })?;
                checkpoint_freshness_observation_from_row(&row)
            })
        },
    )
    .await
}

// Match the page query's sizing expression: two BIGINT columns plus a
// conservative four-byte varlena header for each TEXT column.
const PG_EVENT_FIXED_RESPONSE_BYTES: i64 = 8 + 8 + 4 + 4;

fn encoded_event_response_bytes(
    event_type: &str,
    payload_len: usize,
    client_id: &str,
) -> MetaResult<i64> {
    let payload_len = i64::try_from(payload_len)
        .map_err(|_| MetaError::Storage("event payload length exceeds BIGINT".to_string()))?;
    let event_type_len = i64::try_from(event_type.len())
        .map_err(|_| MetaError::Storage("event type length exceeds BIGINT".to_string()))?;
    let client_id_len = i64::try_from(client_id.len())
        .map_err(|_| MetaError::Storage("client ID length exceeds BIGINT".to_string()))?;

    PG_EVENT_FIXED_RESPONSE_BYTES
        .checked_add(event_type_len)
        .and_then(|bytes| bytes.checked_add(client_id_len))
        .and_then(|bytes| bytes.checked_add(payload_len))
        .ok_or_else(|| MetaError::Storage("event response size exceeds BIGINT".to_string()))
}

impl ForeignEventPage {
    fn empty(rows: Vec<tokio_postgres::Row>) -> Self {
        Self {
            rows,
            first_batch_version: None,
            last_batch_version: None,
            batch_count: 0,
            event_count: 0,
            response_bytes: 0,
            has_more: false,
            oversized: false,
        }
    }
}

impl PgMetaEngine {
    pub(super) fn validate_replay_cursor_against_floor(
        local_version: i64,
        retained_floor: i64,
        checkpoint_version: i64,
        target_version: i64,
    ) -> MetaResult<()> {
        if local_version < 0
            || retained_floor < 0
            || checkpoint_version < 0
            || target_version < 0
            || retained_floor > checkpoint_version
        {
            return Err(MetaError::Storage(format!(
                "postgres replay state has invalid cursor ({local_version}), retention floor ({retained_floor}), checkpoint ({checkpoint_version}), or target ({target_version})"
            )));
        }
        if local_version < retained_floor {
            return Err(MetaError::RebootstrapRequired {
                local_version,
                retained_floor,
                checkpoint_version,
                target_version,
            });
        }
        Ok(())
    }

    pub(super) fn read_remote_checkpoint_freshness(
        &self,
    ) -> MetaResult<CheckpointFreshnessObservation> {
        self.run_async(read_checkpoint_freshness_with_pool(
            self.command_pool.clone(),
            self.volume_id,
            self.command_timeout,
        ))
    }

    pub(super) fn validate_foreign_event_page_limits(
        max_batch_count: i64,
        max_response_bytes: i64,
    ) -> MetaResult<()> {
        if max_batch_count <= 0 || max_response_bytes <= 0 {
            return Err(MetaError::Storage(
                "postgres foreign event page limits must be positive".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn pull_snapshot_locked(
        &self,
        import_mode: StartupSnapshotImportMode,
    ) -> MetaResult<bool> {
        self.ensure_remote_format_validated()?;
        let result = self.pull_snapshot_locked_inner(import_mode);
        self.record_remote_operation_result(result)
    }

    fn pull_snapshot_locked_inner(
        &self,
        import_mode: StartupSnapshotImportMode,
    ) -> MetaResult<bool> {
        let local_version = self.local_version.load(Ordering::SeqCst);
        let head = self.read_remote_metadata_head_and_checkpoint_version_unrecorded()?;
        if !should_download_startup_checkpoint(local_version, head)? {
            return Ok(false);
        }
        let (descriptor, checkpoint) =
            self.run_async(read_bootstrap_checkpoint_and_decode_with_pool(
                self.command_pool.clone(),
                self.volume_id,
                self.command_timeout,
            ))?;
        if descriptor.remote_head_version <= local_version {
            return Ok(false);
        }
        if descriptor.checkpoint_version < local_version {
            return Ok(false);
        }

        let replay_start_version = Self::snapshot_replay_start_version(
            descriptor.checkpoint_version,
            descriptor.remote_head_version,
        );
        self.import_serving_checkpoint_locked(import_mode, &checkpoint, replay_start_version)?;
        Ok(true)
    }

    pub(super) fn import_serving_checkpoint_locked(
        &self,
        import_mode: StartupSnapshotImportMode,
        checkpoint: &ServingCheckpoint,
        replay_start_version: i64,
    ) -> MetaResult<()> {
        let (_position_guard, _pending_state) = self.lock_local_position_and_pending_state()?;
        let _active_open_inodes_guard =
            if import_mode == StartupSnapshotImportMode::PreserveLocalState {
                Some(self.active_open_inodes.lock().map_err(|_| {
                    MetaError::Storage("pg active open inodes lock poisoned".to_string())
                })?)
            } else {
                None
            };
        let active_open_inodes = _active_open_inodes_guard
            .as_ref()
            .map(|active| active.keys().copied().collect::<HashSet<_>>())
            .unwrap_or_default();
        #[cfg(test)]
        self.run_snapshot_import_after_active_open_capture_hook();
        match import_mode {
            StartupSnapshotImportMode::ReplaceLocalState => {
                self.local.import_serving_checkpoint(checkpoint)?;
                self.reset_local_event_progress_after_state_replacement_locked()?;
            }
            StartupSnapshotImportMode::ReplaceLocalStatePreservingEventLog => {
                self.local
                    .import_serving_checkpoint_preserving_local_event_log(checkpoint)?;
            }
            StartupSnapshotImportMode::PreserveLocalState => {
                self.local
                    .import_serving_checkpoint_preserving_local_state(
                        checkpoint,
                        &active_open_inodes,
                    )?;
            }
        }
        self.reconcile_imported_snapshot_regular_files_with_active_open_inodes(
            (import_mode == StartupSnapshotImportMode::PreserveLocalState)
                .then_some(&active_open_inodes),
        )?;
        self.persist_local_version_locked(replay_start_version)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn set_snapshot_import_after_active_open_capture_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        *self
            .snapshot_import_after_active_open_capture_hook
            .lock()
            .expect("snapshot import hook lock") = Some(hook);
    }

    #[cfg(test)]
    fn run_snapshot_import_after_active_open_capture_hook(&self) {
        let hook = self
            .snapshot_import_after_active_open_capture_hook
            .lock()
            .expect("snapshot import hook lock")
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(super) fn read_remote_metadata_head_version_unrecorded(&self) -> MetaResult<i64> {
        self.run_async(read_remote_metadata_head_version_with_pool(
            self.command_pool.clone(),
            self.volume_id,
            self.command_timeout,
        ))
    }

    fn read_remote_metadata_head_and_checkpoint_version_unrecorded(
        &self,
    ) -> MetaResult<RemoteHeadCheckpointState> {
        self.run_async(read_remote_metadata_head_and_checkpoint_version_with_pool(
            self.command_pool.clone(),
            self.volume_id,
            self.command_timeout,
        ))
    }

    pub(super) fn remote_metadata_head_version(&self) -> MetaResult<i64> {
        self.validate_remote_mount_policy(true)?.ok_or_else(|| {
            MetaError::Storage("postgres mount policy observation is missing the head".into())
        })
    }

    #[cfg(test)]
    pub(super) fn push_event_log_batch_locked(&self, events: &[MetaEvent]) -> MetaResult<i64> {
        let projected = RemoteApplyCoordinator::project_local_batch(events);
        let mut oversized_event_index = None;
        self.push_projected_event_log_batch_locked(projected.events, &mut oversized_event_index)
    }

    pub(super) fn push_projected_event_log_batch_locked(
        &self,
        events: Vec<MetaEvent>,
        oversized_event_index: &mut Option<usize>,
    ) -> MetaResult<i64> {
        if events
            .iter()
            .any(|event| matches!(event, MetaEvent::SetVolumeSettings { .. }))
        {
            return Err(MetaError::Storage(
                "volume settings require the online authoritative compare-and-set".into(),
            ));
        }
        if events.is_empty() {
            return Ok(self.local_version.load(Ordering::SeqCst));
        }

        self.ensure_remote_format_validated()?;

        let client_id = self.current_client_id()?;
        let retained_digest = super::retained::publication_digest(&client_id, &events)?;
        let mut batches = Self::split_projected_event_batches(
            events,
            &client_id,
            self.sync_max_response_bytes_per_page,
            oversized_event_index,
        )?;
        // Publication fence: an unclean shutdown keeps only durable commits, so
        // make the published journal durable first. A recovered image then
        // holds every event peers can see and stays this client's authority.
        self.local.sync_local_metadata()?;
        // Local rejections (oversized event, failed fence) must win before PostgreSQL is contacted.
        let mut session = match self.acquire_publication_session() {
            Ok(session) => session,
            Err(error) => return self.record_remote_operation_result(Err(error)),
        };
        let expected_metadata_format_version = i16::try_from(SHARED_VOLUME_FORMAT.current)
            .map_err(|_| {
                MetaError::Storage(
                    "metadata format version exceeds PostgreSQL SMALLINT".to_string(),
                )
            })?;

        let mut attempt = 0_u8;
        let result = loop {
            let command_pool = self.command_pool.clone();
            let volume_id = self.volume_id;
            let now_ms = Self::now_millis();
            let command_timeout = self.command_timeout;
            let current_session = session.clone();
            let current_client_id = client_id.clone();
            let current_retained_digest = retained_digest.clone();
            let current_batches = batches;
            let outcome = self.run_async(async move {
                Self::with_postgres_client_timeout(
                    command_pool,
                    command_timeout,
                    "push event log batch",
                    move |client| {
                        Box::pin(async move {
                            let tx = client
                                .transaction()
                                .await
                                .map_err(|e| Self::classify_pg_error("postgres begin tx failed", e))?;

                            let mut retry = false;
                            let outcome = async {
                                let first_batch = current_batches.first().ok_or_else(|| {
                                    MetaError::Storage("encoded event batches unexpectedly empty".into())
                                })?;
                                let publishable = crate::events::ensure_publishable(
                                    current_batches.iter().flat_map(|batch| &batch.events),
                                    current_session.protocol(),
                                );
                                // Negotiate every event before writing. The SQL stamp accepts
                                // exactly the epoch that this encoding check used.
                                let first_row = if publishable.is_err() {
                                    current_session.stamp_checked(&tx, volume_id).await.map(|()| None)
                                } else if current_retained_digest.is_some() {
                                    current_session.stamp_and_lock_retained(&tx, volume_id).await
                                } else {
                                    current_session.stamp_and_bump(
                                        &tx, volume_id, expected_metadata_format_version, first_batch.payload_bytes,
                                    ).await
                                };
                                let mut first_row = match first_row {
                                    Ok(row) => row,
                                    Err(error @ (super::client_sessions::StampError::SessionInactive
                                        | super::client_sessions::StampError::PolicyStale { .. })) => {
                                        retry = true;
                                        return Err(error.into_meta_error());
                                    }
                                    Err(super::client_sessions::StampError::Other(error)) => {
                                        return Err(error);
                                    }
                                };
                                publishable?;

                                let mut last_batch_version = None;
                                let mut retained_locked = std::collections::HashMap::new();
                                let mut last_observation = None;
                                if let Some(digest) = &current_retained_digest {
                                    // After locking, a fresh statement snapshot must see any
                                    // receipt committed by the previous lock holder.
                                    let row = first_row.take().ok_or_else(|| {
                                        MetaError::Storage(MISSING_RETAINED_PUBLICATION_STATE.to_owned())
                                    })?;
                                    crate::check_pg_volume_format(row.get::<_, Option<i16>>("metadata_format_version"))?;
                                    if let Some(receipt) = tx.query_opt(
                                        "SELECT version FROM trash_publication_receipts WHERE vol=$1 AND digest=$2",
                                        &[&volume_id, digest],
                                    ).await.map_err(|e| Self::classify_pg_error("read retained publication receipt", e))? {
                                        let observation = checkpoint_freshness_observation_from_row(&row)?;
                                        let version = receipt.get::<_, i64>(0);
                                        return Ok((version, observation));
                                    }
                                }
                                for (index, batch) in current_batches.iter().enumerate() {
                                    let batch_version_row = if index == 0 && current_retained_digest.is_none() {
                                        first_row.take()
                                    } else {
                                        tx
                                        .query_opt(
                                            sql::BUMP_METADATA_VERSION,
                                            &[
                                                &volume_id,
                                                &expected_metadata_format_version,
                                                &batch.payload_bytes,
                                                &super::volume_protocol::PUBLICATION_WRITE_REQUIREMENT,
                                            ],
                                        )
                                        .await
                                        .map_err(|e| {
                                            Self::classify_pg_error(
                                                "postgres metadata_state bump failed",
                                                e,
                                            )
                                        })?
                                    };
                                    let Some(batch_version_row) = batch_version_row else {
                                        let error =
                                            super::volume_protocol::refused_publication(&tx, volume_id).await;
                                        if matches!(
                                            error,
                                            MetaError::VolumeProtocolRejected(ProtocolRejection::EpochConflict { .. })
                                        ) {
                                            retry = true;
                                        }
                                        return Err(error);
                                    };
                                    let batch_version = row_i64(
                                        &batch_version_row,
                                        0,
                                        "postgres metadata batch version",
                                    )?;
                                    let batch_observation =
                                        checkpoint_freshness_observation_from_row(&batch_version_row)?;
                                    last_observation = Some(batch_observation);

                                    super::retained::apply_retained_authority(&tx, volume_id, &batch.events, true, &mut retained_locked).await?;

                                    for (event_chunk, payload_chunk) in batch
                                        .events
                                        .chunks(PG_EVENT_INSERT_CHUNK_SIZE)
                                        .zip(batch.payloads.chunks(PG_EVENT_INSERT_CHUNK_SIZE))
                                    {
                                        let event_types: Vec<&str> =
                                            event_chunk.iter().map(|event| event.kind()).collect();
                                        let payloads = payload_chunk;
                                        tx.execute(
                                            sql::INSERT_EVENT_LOG_BATCH,
                                            &[
                                                &volume_id,
                                                &batch_version,
                                                &current_client_id,
                                                &now_ms,
                                                &event_types,
                                                &payloads,
                                            ],
                                        )
                                        .await
                                        .map_err(|e| {
                                            Self::classify_pg_error(
                                                "postgres event_log batch insert failed",
                                                e,
                                            )
                                        })?;
                                    }

                                    for event in &batch.events {
                                        match event {
                                            MetaEvent::SetAclRule {
                                                ino,
                                                principal,
                                                access_level,
                                            } => {
                                                tx.execute(
                                                    r#"
                                                    INSERT INTO acl_rules (vol, ino, principal, access_level, updated_at)
                                                    VALUES ($1, $2, $3, $4, now())
                                                    ON CONFLICT (vol, ino, principal) DO UPDATE
                                                    SET access_level = EXCLUDED.access_level,
                                                        updated_at = now()
                                                    "#,
                                                    &[&volume_id, &(*ino as i64), principal, &(*access_level as i16)],
                                                )
                                                .await
                                                .map_err(|e| {
                                                    Self::classify_pg_error(
                                                        "postgres acl_rules upsert failed",
                                                        e,
                                                    )
                                                })?;
                                            }
                                            MetaEvent::RemoveAclRule { ino, principal } => {
                                                tx.execute(
                                                    "DELETE FROM acl_rules WHERE vol=$1 AND ino=$2 AND principal=$3",
                                                    &[&volume_id, &(*ino as i64), principal],
                                                )
                                                .await
                                                .map_err(|e| {
                                                    Self::classify_pg_error(
                                                        "postgres acl_rules delete failed",
                                                        e,
                                                    )
                                                })?;
                                            }
                                            _ => {}
                                        }
                                    }

                                    super::snapshot_reachability::apply_object_lifetime_changes(
                                        &tx,
                                        volume_id,
                                        batch_version,
                                        &batch.events,
                                    )
                                    .await?;
                                    last_batch_version = Some(batch_version);
                                }

                                let last_batch_version = last_batch_version.ok_or_else(|| {
                                    MetaError::Storage("encoded event batches unexpectedly empty".to_string())
                                })?;

                                let channel = Self::metadata_notify_channel(volume_id);
                                if let Some(digest) = &current_retained_digest {
                                    tx.execute(
                                        "INSERT INTO trash_publication_receipts(vol,digest,version) VALUES($1,$2,$3)",
                                        &[&volume_id, digest, &last_batch_version],
                                    ).await.map_err(|e| Self::classify_pg_error("record retained publication receipt", e))?;
                                }
                                let _ = tx
                                    .execute(
                                        "SELECT pg_notify($1, $2)",
                                        &[&channel, &last_batch_version.to_string()],
                                    )
                                    .await;

                                let observation = last_observation.ok_or_else(|| {
                                    MetaError::Storage(
                                        "metadata publication did not return checkpoint freshness state"
                                            .to_string(),
                                    )
                                })?;

                                Ok((last_batch_version, observation))
                            }.await;
                            let outcome = runtime::finish_pooled_transaction(
                                tx, outcome, |error| {
                                    retry
                                        || super::volume_protocol::is_volume_read_policy_rejection(error)
                                        || super::retained::is_retained_publication_rejection(error)
                                        || matches!(error, MetaError::Storage(message)
                                            if matches!(message.as_str(),
                                                super::snapshot_reachability::PAYLOAD_PUBLICATION_FENCED
                                                    | MISSING_RETAINED_PUBLICATION_STATE))
                                },
                                |error| Self::classify_pg_error("finish PostgreSQL publication", error),
                            ).await?;
                            match outcome {
                                Err(error) if retry => Ok(Err((error, current_batches))),
                                outcome => Ok(Ok(outcome)),
                            }
                        })
                    },
                )
                .await
            });
            match outcome {
                Ok(Ok(done)) => break done,
                Ok(Err((retry_error, returned_batches))) => {
                    if attempt == 0 {
                        attempt += 1;
                        batches = returned_batches;
                        match self.force_refresh_publication_session(&session) {
                            Ok(refreshed) => {
                                session = refreshed;
                                continue;
                            }
                            Err(error) => break Err(error),
                        }
                    }
                    break Err(retry_error);
                }
                Err(error) => break Err(error),
            }
        };
        let result = self.record_remote_operation_result(result);
        if let Ok((_, observation)) = &result {
            self.record_publication_success();
            self.record_checkpoint_freshness_observation(*observation);
        }
        result.map(|(version, _)| version)
    }

    fn projected_child_purge_group_len(events: &[MetaEvent], start: usize) -> usize {
        let Some(MetaEvent::SetXattr {
            ino, name, value, ..
        }) = events.get(start)
        else {
            return 1;
        };
        if name != crate::trash::RETAINED_RECEIPT_XATTR {
            return 1;
        }
        let Ok(entry) = crate::redb_engine::RedbMetaEngine::decode_retained_entry(*ino, value)
        else {
            return 1;
        };
        if entry.state != crate::trash::RetainedEntryState::Retained {
            return 1;
        }
        let receipt = entry.receipt;
        let Some(tail) = events.get(start + 1..) else {
            return 1;
        };
        match tail {
            [
                MetaEvent::SetAttr { attr: p_attr },
                MetaEvent::RemoveAttr { ino: c_ino },
                MetaEvent::RemoveEntry { parent, .. },
                ..,
            ] if p_attr.kind == crate::FileType::Directory
                && *parent == p_attr.ino
                && *c_ino != receipt =>
            {
                4
            }
            [
                MetaEvent::RemoveEntry { parent, .. },
                MetaEvent::SetAttr { attr: c_attr },
                MetaEvent::SetAttr { attr: p_attr },
                ..,
            ] if *parent == p_attr.ino
                && p_attr.kind == crate::FileType::Directory
                && c_attr.ino != *parent =>
            {
                4
            }
            [
                MetaEvent::RemoveEntry { parent, .. },
                MetaEvent::RemoveAttr { .. },
                MetaEvent::SetAttr { attr: p_attr },
                ..,
            ] if *parent == p_attr.ino && p_attr.kind == crate::FileType::Directory => 4,
            [
                MetaEvent::RemoveEntry { parent, .. },
                MetaEvent::PublishRegularFileBaseSlices { ino: c1, .. },
                MetaEvent::SetAttr { attr: c2 },
                ..,
            ] if *c1 == c2.ino && *parent != receipt => 4,
            [
                MetaEvent::PublishRegularFileBaseSlices { ino: c1, .. },
                MetaEvent::SetAttr { attr: c2 },
                ..,
            ] if *c1 == c2.ino => 3,
            _ => 1,
        }
    }

    fn split_projected_event_batches(
        events: Vec<MetaEvent>,
        client_id: &str,
        max_response_bytes: i64,
        oversized_event_index: &mut Option<usize>,
    ) -> MetaResult<Vec<EncodedRemoteEventBatch>> {
        let mut batches = Vec::new();
        let batch_capacity = events.len().min(PG_EVENT_INSERT_CHUNK_SIZE);
        let mut batch_events = Vec::with_capacity(batch_capacity);
        let mut batch_payloads = Vec::with_capacity(batch_capacity);
        let mut batch_response_bytes = 0_i64;
        let mut batch_payload_bytes = 0_i64;

        let mut event_index = 0usize;
        while event_index < events.len() {
            let group_len = Self::projected_child_purge_group_len(&events, event_index);
            let mut group_encoded = Vec::with_capacity(group_len);
            let mut group_response_bytes = 0_i64;
            let mut group_payload_bytes = 0_i64;
            for offset in 0..group_len {
                let idx = event_index + offset;
                let event = &events[idx];
                let payload = encode_event(event)?;
                let payload_len = i64::try_from(payload.len()).map_err(|_| {
                    MetaError::Storage("event payload length exceeds BIGINT".to_string())
                })?;
                let event_response_bytes =
                    encoded_event_response_bytes(event.kind(), payload.len(), client_id)?;
                if event_response_bytes > max_response_bytes {
                    *oversized_event_index = Some(idx);
                    return Err(MetaError::EventTooLarge {
                        estimated: event_response_bytes,
                        limit: max_response_bytes,
                    });
                }
                group_response_bytes = group_response_bytes
                    .checked_add(event_response_bytes)
                    .ok_or_else(|| {
                        MetaError::Storage("event response size exceeds BIGINT".to_string())
                    })?;
                group_payload_bytes =
                    group_payload_bytes
                        .checked_add(payload_len)
                        .ok_or_else(|| {
                            MetaError::Storage("event payload size exceeds BIGINT".to_string())
                        })?;
                group_encoded.push(payload);
            }
            if group_response_bytes > max_response_bytes {
                *oversized_event_index = Some(event_index);
                return Err(MetaError::EventTooLarge {
                    estimated: group_response_bytes,
                    limit: max_response_bytes,
                });
            }

            let would_exceed = batch_response_bytes
                .checked_add(group_response_bytes)
                .is_none_or(|bytes| bytes > max_response_bytes);
            if !batch_events.is_empty() && would_exceed {
                batches.push(EncodedRemoteEventBatch {
                    events: std::mem::take(&mut batch_events),
                    payloads: std::mem::take(&mut batch_payloads),
                    payload_bytes: batch_payload_bytes,
                });
                batch_response_bytes = 0;
                batch_payload_bytes = 0;
            }

            batch_response_bytes = batch_response_bytes
                .checked_add(group_response_bytes)
                .ok_or_else(|| {
                    MetaError::Storage("event response size exceeds BIGINT".to_string())
                })?;
            batch_payload_bytes = batch_payload_bytes
                .checked_add(group_payload_bytes)
                .ok_or_else(|| {
                    MetaError::Storage("event payload size exceeds BIGINT".to_string())
                })?;
            for (offset, payload) in group_encoded.into_iter().enumerate() {
                batch_events.push(events[event_index + offset].clone());
                batch_payloads.push(payload);
            }
            event_index += group_len;
        }

        if !batch_events.is_empty() {
            batches.push(EncodedRemoteEventBatch {
                events: batch_events,
                payloads: batch_payloads,
                payload_bytes: batch_payload_bytes,
            });
        }
        Ok(batches)
    }

    fn validate_foreign_event_page_continuity(
        cursor_batch_version: i64,
        first_batch_version: i64,
        target_batch_version: i64,
    ) -> MetaResult<()> {
        let expected_first_batch_version =
            cursor_batch_version.checked_add(1).ok_or_else(|| {
                MetaError::Storage("postgres metadata replay cursor exceeds BIGINT".to_string())
            })?;
        if first_batch_version != expected_first_batch_version {
            return Err(MetaError::ReplayGap {
                local_version: cursor_batch_version,
                first_available_version: first_batch_version,
                target_version: target_batch_version,
            });
        }
        Ok(())
    }

    fn empty_foreign_event_page_gap<T>(
        cursor_batch_version: i64,
        target_batch_version: i64,
    ) -> MetaResult<T> {
        let first_available_version = cursor_batch_version.checked_add(1).ok_or_else(|| {
            MetaError::Storage("postgres metadata replay cursor exceeds BIGINT".to_string())
        })?;
        Err(MetaError::ReplayGap {
            local_version: cursor_batch_version,
            first_available_version,
            target_version: target_batch_version,
        })
    }

    pub(super) async fn query_foreign_event_page<C>(
        client: &C,
        volume_id: i64,
        cursor_batch_version: i64,
        target_batch_version: i64,
        max_batch_count: i64,
        max_response_bytes: i64,
    ) -> MetaResult<ForeignEventPage>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        Self::validate_foreign_event_page_limits(max_batch_count, max_response_bytes)?;
        if target_batch_version <= cursor_batch_version {
            return Ok(ForeignEventPage::empty(Vec::new()));
        }

        let rows = client
            .query(
                sql::SELECT_FOREIGN_EVENT_ROWS,
                &[
                    &volume_id,
                    &cursor_batch_version,
                    &target_batch_version,
                    &max_batch_count,
                    &max_response_bytes,
                ],
            )
            .await
            .map_err(|e| Self::classify_pg_error("postgres event_log page select failed", e))?;

        let Some(first_row) = rows.first() else {
            return Self::empty_foreign_event_page_gap(cursor_batch_version, target_batch_version);
        };

        let decode_err =
            |error| Self::classify_pg_error("postgres event page metadata decode failed", error);
        let first_batch_version = first_row
            .try_get::<_, Option<i64>>("first_batch_version")
            .map_err(decode_err)?
            .ok_or_else(|| {
                MetaError::Storage(
                    "postgres foreign event page missing first batch version".to_string(),
                )
            })?;
        let last_batch_version = first_row
            .try_get::<_, Option<i64>>("last_batch_version")
            .map_err(decode_err)?;
        let batch_count = first_row
            .try_get::<_, i64>("batch_count")
            .map_err(decode_err)?;
        let event_count = first_row
            .try_get::<_, i64>("event_count")
            .map_err(decode_err)?;
        let response_bytes = first_row
            .try_get::<_, i64>("response_bytes")
            .map_err(decode_err)?;
        let has_more = first_row
            .try_get::<_, bool>("has_more")
            .map_err(decode_err)?;
        let oversized = first_row
            .try_get::<_, bool>("oversized")
            .map_err(decode_err)?;
        Self::validate_foreign_event_page_continuity(
            cursor_batch_version,
            first_batch_version,
            target_batch_version,
        )?;

        Ok(ForeignEventPage {
            first_batch_version: Some(first_batch_version),
            last_batch_version,
            batch_count,
            event_count,
            response_bytes,
            has_more,
            oversized,
            rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        ChunkId, PayloadRef, PayloadRefState, SliceId, SliceOrder, SliceRange, SliceRecord,
    };

    #[test]
    fn encoded_payload_byte_accounting_matches_event_payloads() {
        let events = vec![
            MetaEvent::RemoveAttr { ino: 1 },
            MetaEvent::RemoveAttr { ino: 2 },
            MetaEvent::RemoveAttr { ino: 3 },
        ];
        let expected = events
            .iter()
            .map(|event| encode_event(event).expect("test event should encode").len() as i64)
            .sum::<i64>();

        let mut oversized_event_index = None;
        let batches = PgMetaEngine::split_projected_event_batches(
            events,
            "client-slot:1",
            i64::MAX,
            &mut oversized_event_index,
        )
        .expect("test events should fit in one batch");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].payload_bytes, expected);
    }

    #[test]
    fn large_regular_file_publication_fits_the_default_replay_byte_cap() {
        let slices = (0..200_000_u64)
            .map(|index| SliceRecord {
                slice_id: SliceId(index),
                order: SliceOrder(index),
                range: SliceRange::new(index as u32, index as u32 + 1)
                    .expect("fixture slice range"),
                payload: Some(PayloadRef::new_full(index, 1)),
                payload_state: PayloadRefState::Published,
            })
            .collect();
        let event = MetaEvent::PublishRegularFileBaseSlices {
            ino: 42,
            chunk_slices: vec![(ChunkId(0), slices)],
        };
        let payload = encode_event(&event).expect("publication event should encode");
        let response_bytes =
            encoded_event_response_bytes(event.kind(), payload.len(), "client-slot:1")
                .expect("publication response size should fit BIGINT");
        assert!(response_bytes > 2 * 1024 * 1024);
        assert!(response_bytes <= 8 * 1024 * 1024);

        let mut oversized_event_index = None;
        let batches = PgMetaEngine::split_projected_event_batches(
            vec![event],
            "client-slot:1",
            super::super::DEFAULT_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE,
            &mut oversized_event_index,
        )
        .expect("a valid publication event must fit the default replay cap");
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn foreign_event_page_rejects_a_missing_next_batch() {
        assert!(matches!(
            PgMetaEngine::validate_foreign_event_page_continuity(5, 51, 60),
            Err(MetaError::ReplayGap {
                local_version: 5,
                first_available_version: 51,
                target_version: 60,
            })
        ));
        assert!(PgMetaEngine::validate_foreign_event_page_continuity(5, 6, 60).is_ok());
        assert!(PgMetaEngine::validate_foreign_event_page_continuity(5, 0, 60).is_err());
        assert!(PgMetaEngine::validate_foreign_event_page_continuity(5, -1, 60).is_err());
    }

    #[test]
    fn empty_foreign_event_page_rejects_a_lagging_cursor() {
        let error = PgMetaEngine::empty_foreign_event_page_gap::<()>(5, 60)
            .expect_err("lagging empty page must fail");
        assert!(matches!(
            error,
            MetaError::ReplayGap {
                local_version: 5,
                first_available_version: 6,
                target_version: 60,
            }
        ));
    }

    #[test]
    fn empty_foreign_event_page_handles_the_maximum_version_boundary() {
        let error = PgMetaEngine::empty_foreign_event_page_gap::<()>(i64::MAX - 1, i64::MAX)
            .expect_err("maximum cursor boundary must produce a replay gap");
        assert!(matches!(
            error,
            MetaError::ReplayGap {
                local_version: version,
                first_available_version: first,
                target_version: target,
            } if version == i64::MAX - 1
                && first == i64::MAX
                && target == i64::MAX
        ));
    }

    #[test]
    fn replay_cursor_below_retained_floor_requires_rebootstrap() {
        let error = PgMetaEngine::validate_replay_cursor_against_floor(5, 8, 8, 12)
            .expect_err("a cursor below retained history must require rebootstrap");

        assert!(matches!(
            error,
            MetaError::RebootstrapRequired {
                local_version: 5,
                retained_floor: 8,
                checkpoint_version: 8,
                target_version: 12,
            }
        ));
    }

    #[test]
    fn replay_cursor_at_retained_floor_continues_normal_replay() {
        assert!(PgMetaEngine::validate_replay_cursor_against_floor(8, 8, 8, 12).is_ok());
    }

    #[test]
    fn negative_replay_cursor_is_invalid_state() {
        assert!(matches!(
            PgMetaEngine::validate_replay_cursor_against_floor(-1, 0, 0, 12),
            Err(MetaError::Storage(message))
                if message == "postgres replay state has invalid cursor (-1), retention floor (0), checkpoint (0), or target (12)"
        ));
    }

    #[test]
    fn negative_replay_target_is_invalid_state() {
        assert!(matches!(
            PgMetaEngine::validate_replay_cursor_against_floor(0, 0, 0, -1),
            Err(MetaError::Storage(message))
                if message == "postgres replay state has invalid cursor (0), retention floor (0), checkpoint (0), or target (-1)"
        ));
    }

    #[test]
    fn invalid_replay_state_reports_version_coordinates() {
        assert!(matches!(
            PgMetaEngine::validate_replay_cursor_against_floor(5, 10, 8, 12),
            Err(MetaError::Storage(message))
                if message == "postgres replay state has invalid cursor (5), retention floor (10), checkpoint (8), or target (12)"
        ));
    }

    #[test]
    fn invalid_replay_history_boundaries_are_rejected() {
        for (local_version, retained_floor, checkpoint_version) in
            [(5, 10, 8), (5, -1, 8), (5, 0, -1)]
        {
            assert!(matches!(
                PgMetaEngine::validate_replay_cursor_against_floor(
                    local_version,
                    retained_floor,
                    checkpoint_version,
                    12,
                ),
                Err(MetaError::Storage(_))
            ));
        }
    }

    #[test]
    fn bootstrap_compatibility_preserves_format_and_upgrade_errors_before_need_checkpoint() {
        let mut descriptor = BootstrapCheckpointDescriptor {
            volume_id: 1,
            volume_uuid: "uuid".to_string(),
            format_info_bytes: Vec::new(),
            metadata_format_version: SHARED_VOLUME_FORMAT.current + 1,
            remote_head_version: 10,
            checkpoint_version: 2,
            checkpoint_encoding: None,
            checkpoint_bytes: Vec::new(),
            checkpoint_sha256: Vec::new(),
            publication_state: "pending".to_string(),
            publication_generation: 1,
        };
        assert!(matches!(
            check_bootstrap_descriptor_compatibility(&descriptor, 8, true),
            Err(MetaError::UnsupportedFormatVersion { found, expected })
                if found == SHARED_VOLUME_FORMAT.current + 1
                    && expected == SHARED_VOLUME_FORMAT.current
        ));

        descriptor.metadata_format_version = SHARED_VOLUME_FORMAT.current;
        let mut json =
            serde_json::to_value(crate::baseline_fixtures::fixture_format_info()).unwrap();
        json["successor_field"] = serde_json::json!(true);
        descriptor.format_info_bytes = serde_json::to_vec(&json).unwrap();
        assert!(matches!(
            check_bootstrap_descriptor_compatibility(&descriptor, 8, true),
            Err(MetaError::UpgradeRequired { .. })
        ));

        descriptor.format_info_bytes.clear();
        assert!(matches!(
            check_bootstrap_descriptor_compatibility(&descriptor, 8, true),
            Err(MetaError::NeedCompatibleCheckpoint { replay_floor: 8 })
        ));
    }

    #[test]
    fn startup_checkpoint_decision_skips_stale_only_when_above_replay_floor() {
        assert!(
            !should_download_startup_checkpoint(
                1,
                RemoteHeadCheckpointState {
                    remote_head_version: 2,
                    checkpoint_version: 0,
                    replay_floor: 0,
                }
            )
            .unwrap()
        );
        assert!(matches!(
            should_download_startup_checkpoint(
                2,
                RemoteHeadCheckpointState {
                    remote_head_version: 6,
                    checkpoint_version: 0,
                    replay_floor: 5,
                }
            ),
            Err(MetaError::NeedCompatibleCheckpoint { replay_floor: 5 })
        ));
        assert!(
            should_download_startup_checkpoint(
                2,
                RemoteHeadCheckpointState {
                    remote_head_version: 6,
                    checkpoint_version: 5,
                    replay_floor: 5,
                }
            )
            .unwrap()
        );
    }
}
