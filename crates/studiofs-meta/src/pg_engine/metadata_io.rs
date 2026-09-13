//! Owns remote metadata snapshot reads and local checkpoint import.

#[cfg(test)]
use super::RemoteApplyCoordinator;
use super::{
    PG_EVENT_INSERT_CHUNK_SIZE, PgMetaEngine, PgSnapshotBackend, SnapshotBackendAccess,
    StartupSnapshotImportMode, runtime, sql,
};
use crate::engine::{BootstrapCheckpointDescriptor, MetaError, MetaFormat, MetaResult};
use crate::events::{MetaEvent, encode_event};
use crate::schema::CURRENT_FORMAT_VERSION;
use crate::snapshot::ServingCheckpoint;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::Ordering;

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

async fn read_bootstrap_checkpoint_and_decode_with_pool(
    command_pool: Arc<runtime::PgConnectionPool>,
    volume_id: i64,
    command_timeout: std::time::Duration,
) -> MetaResult<(BootstrapCheckpointDescriptor, ServingCheckpoint)> {
    let row = PgMetaEngine::with_postgres_client_timeout(
        command_pool,
        command_timeout,
        "bootstrap checkpoint descriptor",
        move |client| {
            Box::pin(async move {
                let row = client
                    .query_opt(sql::SELECT_BOOTSTRAP_CHECKPOINT_DESCRIPTOR, &[&volume_id])
                    .await
                    .map_err(|e| {
                        PgMetaEngine::classify_pg_error(
                            "postgres bootstrap checkpoint select failed",
                            e,
                        )
                    })?
                    .ok_or(MetaError::BootstrapCheckpointDescriptorMissing)?;
                Ok(row)
            })
        },
    )
    .await?;
    let descriptor = try_descriptor_from_row(&row)?;
    let checkpoint = descriptor.validate_and_decode()?;
    Ok((descriptor, checkpoint))
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

async fn read_remote_metadata_head_and_checkpoint_version_with_pool(
    command_pool: Arc<runtime::PgConnectionPool>,
    volume_id: i64,
    command_timeout: std::time::Duration,
) -> MetaResult<(i64, i64)> {
    PgMetaEngine::with_postgres_client_timeout(
        command_pool,
        command_timeout,
        "metadata head and checkpoint version",
        move |client| {
            Box::pin(async move {
                let row = client
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
                    })?;
                let Some(row) = row else {
                    return Ok((0, -1));
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
                Ok((head, checkpoint))
            })
        },
    )
    .await
}

#[derive(Clone, Copy)]
pub(super) struct CheckpointFreshnessObservation {
    pub(super) head_version: u64,
    pub(super) checkpoint_version: u64,
    pub(super) event_payload_bytes: u64,
    pub(super) checkpoint_age: std::time::Duration,
}

#[derive(Clone, Copy)]
pub(super) struct CheckpointCapture {
    pub(super) head_version: u64,
    pub(super) total_event_payload_bytes: u64,
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

fn checkpoint_freshness_observation_from_row(
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

    Ok(CheckpointFreshnessObservation {
        head_version,
        checkpoint_version,
        event_payload_bytes,
        checkpoint_age: std::time::Duration::from_millis(checkpoint_age_ms),
    })
}

fn checkpoint_capture_from_row(row: &tokio_postgres::Row) -> MetaResult<CheckpointCapture> {
    Ok(CheckpointCapture {
        head_version: nonnegative_counter(
            row_i64(row, 0, "checkpoint capture head")?,
            "head version",
        )?,
        total_event_payload_bytes: nonnegative_counter(
            row_i64(row, 1, "checkpoint capture total byte counter")?,
            "total event payload bytes",
        )?,
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

async fn read_checkpoint_capture_with_pool(
    command_pool: Arc<runtime::PgConnectionPool>,
    volume_id: i64,
    command_timeout: std::time::Duration,
) -> MetaResult<CheckpointCapture> {
    PgMetaEngine::with_postgres_client_timeout(
        command_pool,
        command_timeout,
        "checkpoint capture",
        move |client| {
            Box::pin(async move {
                let row = client
                    .query_opt(sql::SELECT_CHECKPOINT_CAPTURE, &[&volume_id])
                    .await
                    .map_err(|error| {
                        PgMetaEngine::classify_pg_error(
                            "postgres checkpoint capture select failed",
                            error,
                        )
                    })?
                    .ok_or_else(|| {
                        MetaError::Storage(
                            "postgres metadata_state row missing during checkpoint capture"
                                .to_string(),
                        )
                    })?;
                checkpoint_capture_from_row(&row)
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
    pub(super) fn read_remote_checkpoint_freshness(
        &self,
    ) -> MetaResult<CheckpointFreshnessObservation> {
        self.run_async(read_checkpoint_freshness_with_pool(
            self.command_pool.clone(),
            self.volume_id,
            self.command_timeout,
        ))
    }

    pub(super) fn read_remote_checkpoint_capture(&self) -> MetaResult<CheckpointCapture> {
        self.run_async(read_checkpoint_capture_with_pool(
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
        let (remote_head_version, checkpoint_version) =
            self.read_remote_metadata_head_and_checkpoint_version_unrecorded()?;
        if remote_head_version <= local_version || checkpoint_version < local_version {
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
        match import_mode {
            StartupSnapshotImportMode::ReplaceLocalState => {
                self.local.import_serving_checkpoint(&checkpoint)?;
            }
            StartupSnapshotImportMode::ReplaceLocalStatePreservingEventLog => {
                self.local
                    .import_serving_checkpoint_preserving_local_event_log(&checkpoint)?;
            }
            StartupSnapshotImportMode::PreserveLocalState => {
                self.local
                    .import_serving_checkpoint_preserving_local_state(&checkpoint)?;
            }
        }
        self.reconcile_imported_snapshot_regular_files()?;
        self.persist_local_version(replay_start_version)?;
        Ok(true)
    }

    fn read_remote_metadata_head_version_unrecorded(&self) -> MetaResult<i64> {
        self.run_async(read_remote_metadata_head_version_with_pool(
            self.command_pool.clone(),
            self.volume_id,
            self.command_timeout,
        ))
    }

    fn read_remote_metadata_head_and_checkpoint_version_unrecorded(
        &self,
    ) -> MetaResult<(i64, i64)> {
        self.run_async(read_remote_metadata_head_and_checkpoint_version_with_pool(
            self.command_pool.clone(),
            self.volume_id,
            self.command_timeout,
        ))
    }

    pub(super) fn remote_metadata_head_version(&self) -> MetaResult<i64> {
        self.ensure_remote_format_validated()?;
        let result = self.read_remote_metadata_head_version_unrecorded();
        self.record_remote_operation_result(result)
    }

    #[cfg(test)]
    pub(super) fn push_event_log_batch_locked(&self, events: &[MetaEvent]) -> MetaResult<i64> {
        let projected = RemoteApplyCoordinator::project_local_batch(events);
        self.push_projected_event_log_batch_locked(projected.events)
    }

    pub(super) fn push_projected_event_log_batch_locked(
        &self,
        events: Vec<MetaEvent>,
    ) -> MetaResult<i64> {
        if events.is_empty() {
            return Ok(self.local_version.load(Ordering::SeqCst));
        }

        self.ensure_remote_format_validated()?;

        let client_id = self.current_client_id()?;
        let batches = Self::split_projected_event_batches(
            events,
            &client_id,
            self.sync_max_response_bytes_per_page,
        )?;
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let now_ms = Self::now_millis();
        let expected_metadata_format_version =
            i16::try_from(CURRENT_FORMAT_VERSION).map_err(|_| {
                MetaError::Storage(
                    "metadata format version exceeds PostgreSQL SMALLINT".to_string(),
                )
            })?;
        let command_timeout = self.command_timeout;

        let result = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "push event log batch",
                move |client| Box::pin(async move {
                    let tx = client
                        .transaction()
                        .await
                        .map_err(|e| Self::classify_pg_error("postgres begin tx failed", e))?;

                    let mut last_batch_version = None;
                    let mut last_observation = None;
                    for batch in batches {
                        let batch_version_row = tx
                            .query_opt(
                                sql::BUMP_METADATA_VERSION,
                                &[
                                    &volume_id,
                                    &expected_metadata_format_version,
                                    &batch.payload_bytes,
                                ],
                            )
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres metadata_state bump failed",
                                    e,
                                )
                            })?;
                        let Some(batch_version_row) = batch_version_row else {
                            let format_row = tx
                                .query_opt(sql::SELECT_METADATA_FORMAT_VERSION, &[&volume_id])
                                .await
                                .map_err(|e| {
                                    Self::classify_pg_error(
                                        "postgres metadata format revalidation failed",
                                        e,
                                    )
                                })?;
                            let found = format_row
                                .as_ref()
                                .and_then(|row| row.get::<_, Option<i16>>(0));
                            crate::ensure_supported_pg_sync_format_version(found)?;
                            return Err(MetaError::Storage(
                                "postgres metadata version bump did not update the current-format row"
                                    .to_string(),
                            ));
                        };
                        let batch_observation = checkpoint_freshness_observation_from_row(
                            &batch_version_row,
                        )?;
                        let batch_version = i64::try_from(batch_observation.head_version)
                            .map_err(|_| {
                                MetaError::Storage(
                                    "postgres metadata head version exceeds BIGINT".to_string(),
                                )
                            })?;
                        last_observation = Some(batch_observation);

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
                                    &client_id,
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

                    tx.commit()
                        .await
                        .map_err(|e| Self::classify_pg_error("postgres commit failed", e))?;
                    Ok((last_batch_version, observation))
                }),
            )
            .await
        });
        let result = self.record_remote_operation_result(result);
        if let Ok((_, observation)) = &result {
            self.record_checkpoint_freshness_observation(*observation);
        }
        result.map(|(version, _)| version)
    }

    fn split_projected_event_batches(
        events: Vec<MetaEvent>,
        client_id: &str,
        max_response_bytes: i64,
    ) -> MetaResult<Vec<EncodedRemoteEventBatch>> {
        let mut batches = Vec::new();
        let mut batch_events = Vec::new();
        let mut batch_payloads = Vec::new();
        let mut batch_response_bytes = 0_i64;
        let mut batch_payload_bytes = 0_i64;

        for event in events {
            let payload = encode_event(&event)?;
            let payload_len = i64::try_from(payload.len()).map_err(|_| {
                MetaError::Storage("event payload length exceeds BIGINT".to_string())
            })?;
            let event_response_bytes =
                encoded_event_response_bytes(event.kind(), payload.len(), client_id)?;
            if event_response_bytes > max_response_bytes {
                return Err(MetaError::EventTooLarge {
                    estimated: event_response_bytes,
                    limit: max_response_bytes,
                });
            }

            let would_exceed = batch_response_bytes
                .checked_add(event_response_bytes)
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
                .checked_add(event_response_bytes)
                .ok_or_else(|| {
                    MetaError::Storage("event response size exceeds BIGINT".to_string())
                })?;
            batch_payload_bytes =
                batch_payload_bytes
                    .checked_add(payload_len)
                    .ok_or_else(|| {
                        MetaError::Storage("event payload size exceeds BIGINT".to_string())
                    })?;
            batch_events.push(event);
            batch_payloads.push(payload);
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

    pub(super) async fn query_foreign_event_page<C>(
        client: &C,
        volume_id: i64,
        last_batch_version: i64,
        target_batch_version: i64,
        max_batch_count: i64,
        max_response_bytes: i64,
    ) -> MetaResult<ForeignEventPage>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        Self::validate_foreign_event_page_limits(max_batch_count, max_response_bytes)?;
        if target_batch_version <= last_batch_version {
            return Ok(ForeignEventPage::empty(Vec::new()));
        }

        let rows = client
            .query(
                sql::SELECT_FOREIGN_EVENT_ROWS,
                &[
                    &volume_id,
                    &last_batch_version,
                    &target_batch_version,
                    &max_batch_count,
                    &max_response_bytes,
                ],
            )
            .await
            .map_err(|e| Self::classify_pg_error("postgres event_log page select failed", e))?;

        let Some(first_row) = rows.first() else {
            return Ok(ForeignEventPage::empty(rows));
        };

        let decode_err =
            |error| Self::classify_pg_error("postgres event page metadata decode failed", error);
        let first_batch_version = first_row
            .try_get::<_, Option<i64>>("first_batch_version")
            .map_err(decode_err)?;
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

        Ok(ForeignEventPage {
            first_batch_version,
            last_batch_version,
            batch_count,
            event_count,
            response_bytes,
            has_more,
            oversized,
            rows,
        })
    }

    pub(super) fn write_checkpoint_locked(&self) -> MetaResult<()> {
        self.ensure_remote_format_validated()?;
        let capture = self.read_remote_checkpoint_capture()?;
        let local_version =
            u64::try_from(self.local_version.load(Ordering::SeqCst)).map_err(|_| {
                MetaError::Storage("local metadata version must be nonnegative".to_string())
            })?;
        if capture.head_version > local_version {
            return Ok(());
        }

        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let payload = self.local.export_serving_checkpoint_bytes()?;
        let format_info = self.local.get_format_info()?;
        let metadata_format_version = i16::try_from(format_info.format_version).map_err(|_| {
            MetaError::Storage("metadata format version exceeds PostgreSQL SMALLINT".to_string())
        })?;
        let checkpoint_sha256 = Sha256::digest(&payload);
        let format_info = serde_json::to_vec(&format_info).map_err(|error| {
            MetaError::Serialization(format!("format info encode failed: {error}"))
        })?;
        let version = i64::try_from(local_version).map_err(|_| {
            MetaError::Storage("local metadata version exceeds PostgreSQL BIGINT".to_string())
        })?;
        let checkpoint_total_event_bytes = i64::try_from(capture.total_event_payload_bytes)
            .map_err(|_| {
                MetaError::Storage(
                    "checkpoint payload byte counter exceeds PostgreSQL BIGINT".to_string(),
                )
            })?;
        let command_timeout = self.command_timeout;

        let result = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "write checkpoint",
                move |client| {
                    Box::pin(async move {
                        let upserted = client
                            .query_opt(
                                sql::UPSERT_METADATA_CHECKPOINT,
                                &[
                                    &volume_id,
                                    &metadata_format_version,
                                    &version,
                                    &version,
                                    &checkpoint_total_event_bytes,
                                    &payload,
                                    &checkpoint_sha256.as_slice(),
                                    &format_info,
                                ],
                            )
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error("postgres checkpoint upsert failed", e)
                            })?;

                        if upserted.is_none() {
                            let row = client
                                .query_one(
                                    "SELECT metadata_format_version FROM metadata_state WHERE vol = $1",
                                    &[&volume_id],
                                )
                                .await
                                .map_err(|e| {
                                    Self::classify_pg_error(
                                        "postgres checkpoint format revalidation failed",
                                        e,
                                    )
                                })?;
                            crate::ensure_supported_pg_sync_format_version(row.get(0))?;
                        }

                        Ok(())
                    })
                },
            )
            .await
        });
        let result = self.record_remote_operation_result(result);
        if result.is_ok() {
            self.observe_checkpoint_freshness();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let batches =
            PgMetaEngine::split_projected_event_batches(events, "client-slot:1", i64::MAX)
                .expect("test events should fit in one batch");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].payload_bytes, expected);
    }
}
