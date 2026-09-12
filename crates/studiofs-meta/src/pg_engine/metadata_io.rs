//! Owns remote metadata snapshot reads and local checkpoint import.

#[cfg(test)]
use super::RemoteApplyCoordinator;
use super::{PG_EVENT_INSERT_CHUNK_SIZE, PgMetaEngine, StartupSnapshotImportMode, sql};
use crate::engine::{MetaError, MetaFormat, MetaResult};
use crate::events::{MetaEvent, encode_event};
use crate::schema::CURRENT_FORMAT_VERSION;
use crate::snapshot::ServingCheckpoint;
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;

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
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let local_version = self.local_version.load(Ordering::SeqCst);
        let command_timeout = self.command_timeout;

        let row = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "pull snapshot",
                move |client| {
                    Box::pin(async move {
                        let version_row = client
                            .query_opt(sql::SELECT_METADATA_VERSION, &[&volume_id])
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres snapshot version select failed",
                                    e,
                                )
                            })?;

                        let Some(version_row) = version_row else {
                            return Ok(None);
                        };
                        let version: i64 = version_row.get(0);
                        if version <= local_version {
                            return Ok(None);
                        }

                        let row = client
                            .query_one(sql::SELECT_METADATA_SNAPSHOT, &[&volume_id])
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres serving checkpoint select failed",
                                    e,
                                )
                            })?;

                        Ok(Some(row))
                    })
                },
            )
            .await
        })?;

        let Some(row) = row else {
            return Ok(false);
        };

        let version: i64 = row.get(0);
        let checkpoint_version: i64 = row.get(1);
        let bytes: Vec<u8> = row.get(2);
        let metadata_format_version = row.get::<_, Option<i16>>(3);
        let checkpoint_sha256: Option<Vec<u8>> = row.get(4);
        let checkpoint_publication_state: Option<String> = row.get(5);
        let checkpoint_generation: Option<i64> = row.get(6);

        crate::ensure_supported_pg_sync_format_version(metadata_format_version)?;
        crate::validate_checkpoint_descriptor(
            &bytes,
            checkpoint_sha256.as_deref(),
            checkpoint_publication_state.as_deref(),
            checkpoint_generation,
        )?;

        if checkpoint_version < 0 || checkpoint_version > version {
            return Err(MetaError::Storage(
                "postgres serving checkpoint version is invalid".to_string(),
            ));
        }
        if checkpoint_version < local_version {
            return Ok(false);
        }

        let checkpoint: ServingCheckpoint = postcard::from_bytes(&bytes).map_err(|e| {
            MetaError::Serialization(format!("serving checkpoint decode failed: {e}"))
        })?;
        let replay_start_version = Self::snapshot_replay_start_version(checkpoint_version, version);
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

    pub(super) fn remote_metadata_head_version(&self) -> MetaResult<i64> {
        self.ensure_remote_format_validated()?;
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;

        let result = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "metadata head version",
                move |client| {
                    Box::pin(async move {
                        let row = client
                            .query_opt(sql::SELECT_METADATA_VERSION, &[&volume_id])
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres metadata head version select failed",
                                    e,
                                )
                            })?;
                        Ok(row.map(|row| row.get(0)).unwrap_or(0))
                    })
                },
            )
            .await
        });
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
                    for batch in batches {
                        let batch_version_row = tx
                            .query_opt(
                                sql::BUMP_METADATA_VERSION,
                                &[&volume_id, &expected_metadata_format_version],
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
                        let batch_version: i64 = batch_version_row.get(0);

                        for (event_chunk, payload_chunk) in batch
                            .events
                            .chunks(PG_EVENT_INSERT_CHUNK_SIZE)
                            .zip(batch.payloads.chunks(PG_EVENT_INSERT_CHUNK_SIZE))
                        {
                            let event_types: Vec<&str> =
                                event_chunk.iter().map(|event| event.kind()).collect();
                            let payloads = payload_chunk.to_vec();
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

                    tx.commit()
                        .await
                        .map_err(|e| Self::classify_pg_error("postgres commit failed", e))?;
                    Ok(last_batch_version)
                }),
            )
            .await
        });
        self.record_remote_operation_result(result)
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

        for event in events {
            let payload = encode_event(&event)?;
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
                });
                batch_response_bytes = 0;
            }

            batch_response_bytes = batch_response_bytes
                .checked_add(event_response_bytes)
                .ok_or_else(|| {
                    MetaError::Storage("event response size exceeds BIGINT".to_string())
                })?;
            batch_events.push(event);
            batch_payloads.push(payload);
        }

        if !batch_events.is_empty() {
            batches.push(EncodedRemoteEventBatch {
                events: batch_events,
                payloads: batch_payloads,
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
        let version = self.local_version.load(Ordering::SeqCst);
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
        self.record_remote_operation_result(result)
    }
}
