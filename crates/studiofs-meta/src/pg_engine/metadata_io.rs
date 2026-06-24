//! Owns remote metadata snapshot reads and local snapshot import.

#[cfg(test)]
use super::RemoteApplyCoordinator;
use super::{PG_EVENT_INSERT_CHUNK_SIZE, PgMetaEngine, StartupSnapshotImportMode, sql};
use crate::engine::{MetaError, MetaResult};
use crate::events::MetaEvent;
use std::sync::atomic::Ordering;

impl PgMetaEngine {
    pub(super) fn pull_snapshot_locked(
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
                                MetaError::Storage(format!(
                                    "postgres snapshot version select failed: {e}"
                                ))
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
                                MetaError::Storage(format!(
                                    "postgres snapshot payload select failed: {e}"
                                ))
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
        let bytes: Vec<u8> = row.get(1);

        let snapshot = Self::decode_snapshot(&bytes)?;
        let replay_start_version =
            Self::snapshot_replay_start_version(&snapshot, version, local_version);
        match import_mode {
            StartupSnapshotImportMode::ReplaceLocalState => {
                self.local.import_remote_snapshot(&snapshot)?;
            }
            StartupSnapshotImportMode::PreserveLocalState => {
                self.local
                    .import_remote_snapshot_preserving_local_state(&snapshot)?;
            }
        }
        self.reconcile_imported_snapshot_regular_files()?;
        self.persist_local_version(replay_start_version)?;
        Ok(true)
    }

    pub(super) fn remote_metadata_head_version(&self) -> MetaResult<i64> {
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
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
                                MetaError::Storage(format!(
                                    "postgres metadata head version select failed: {e}"
                                ))
                            })?;
                        Ok(row.map(|row| row.get(0)).unwrap_or(0))
                    })
                },
            )
            .await
        })
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

        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let now_ms = Self::now_millis();
        let events_vec = events;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "push event log batch",
                move |client| Box::pin(async move {
                    let tx = client.transaction().await.map_err(|e| {
                        MetaError::Storage(format!("postgres begin tx failed: {e}"))
                    })?;

                    let batch_version: i64 = tx
                        .query_one(sql::BUMP_METADATA_VERSION, &[&volume_id])
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!("postgres metadata_state bump failed: {e}"))
                        })?
                        .get(0);

                    for chunk in events_vec.chunks(PG_EVENT_INSERT_CHUNK_SIZE) {
                        let event_types: Vec<&str> =
                            chunk.iter().map(|event| event.kind()).collect();
                        let payloads: Vec<Vec<u8>> = chunk
                            .iter()
                            .map(|event| {
                                postcard::to_stdvec(event).map_err(|e| {
                                    MetaError::Serialization(format!(
                                        "postcard encode failed: {e}"
                                    ))
                                })
                            })
                            .collect::<MetaResult<Vec<_>>>()?;
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
                            MetaError::Storage(format!(
                                "postgres event_log batch insert failed: {e}"
                            ))
                        })?;
                    }

                    for event in &events_vec {
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
                                    MetaError::Storage(format!(
                                        "postgres acl_rules upsert failed: {e}"
                                    ))
                                })?;
                            }
                            MetaEvent::RemoveAclRule { ino, principal } => {
                                tx.execute(
                                    "DELETE FROM acl_rules WHERE vol=$1 AND ino=$2 AND principal=$3",
                                    &[&volume_id, &(*ino as i64), principal],
                                )
                                .await
                                .map_err(|e| {
                                    MetaError::Storage(format!(
                                        "postgres acl_rules delete failed: {e}"
                                    ))
                                })?;
                            }
                            MetaEvent::CreateSnapshot { snapshot } => {
                                let pinned: Vec<i64> = snapshot
                                    .pinned_object_ids
                                    .iter()
                                    .map(|id| {
                                        i64::try_from(*id).map_err(|_| {
                                            MetaError::Storage(format!(
                                                "snapshot pinned object id overflow: {id}"
                                            ))
                                        })
                                    })
                                    .collect::<MetaResult<Vec<_>>>()?;
                                tx.execute(
                                    r#"
                                    INSERT INTO snapshots (
                                        vol,
                                        id,
                                        root_ino,
                                        created_at,
                                        state,
                                        pinned_object_ids,
                                        snapshot_payload,
                                        updated_at
                                    )
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, now())
                                    ON CONFLICT (vol, id) DO UPDATE
                                    SET root_ino = EXCLUDED.root_ino,
                                        created_at = EXCLUDED.created_at,
                                        state = EXCLUDED.state,
                                        pinned_object_ids = EXCLUDED.pinned_object_ids,
                                        snapshot_payload = EXCLUDED.snapshot_payload,
                                        updated_at = now()
                                    "#,
                                    &[
                                        &volume_id,
                                        &snapshot.id,
                                        &(snapshot.root_ino as i64),
                                        &snapshot.created_at,
                                        &(snapshot.state as i16),
                                        &pinned,
                                        &snapshot.snapshot_payload,
                                    ],
                                )
                                .await
                                .map_err(|e| {
                                    MetaError::Storage(format!(
                                        "postgres snapshots upsert failed: {e}"
                                    ))
                                })?;
                            }
                            MetaEvent::UpdateSnapshotState { id, state } => {
                                tx.execute(
                                    r#"
                                    UPDATE snapshots
                                    SET state = $3,
                                        updated_at = now()
                                    WHERE vol = $1 AND id = $2
                                    "#,
                                    &[&volume_id, id, &(*state as i16)],
                                )
                                .await
                                .map_err(|e| {
                                    MetaError::Storage(format!(
                                        "postgres snapshots state update failed: {e}"
                                    ))
                                })?;
                            }
                            _ => {}
                        }
                    }

                    let channel = Self::metadata_notify_channel(volume_id);
                    let _ = tx
                        .execute(
                            "SELECT pg_notify($1, $2)",
                            &[&channel, &batch_version.to_string()],
                        )
                        .await;

                    tx.commit()
                        .await
                        .map_err(|e| MetaError::Storage(format!("postgres commit failed: {e}")))?;
                    Ok(batch_version)
                }),
            )
            .await
        })
    }

    pub(super) async fn query_foreign_event_rows<C>(
        client: &C,
        volume_id: i64,
        last_seq: i64,
    ) -> MetaResult<Vec<tokio_postgres::Row>>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        client
            .query(sql::SELECT_FOREIGN_EVENT_ROWS, &[&volume_id, &last_seq])
            .await
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres event_log select failed: {}",
                    Self::format_pg_error(e)
                ))
            })
    }

    pub(super) fn write_checkpoint_locked(&self) -> MetaResult<()> {
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let snapshot = self.local.export_remote_snapshot()?;
        let payload = serde_json::to_vec(&snapshot)
            .map_err(|e| MetaError::Serialization(format!("snapshot encode failed: {e}")))?;
        let version = self.local_version.load(Ordering::SeqCst);
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "write checkpoint",
                move |client| {
                    Box::pin(async move {
                        client
                            .execute(
                                sql::UPSERT_METADATA_CHECKPOINT,
                                &[&volume_id, &version, &payload],
                            )
                            .await
                            .map_err(|e| {
                                MetaError::Storage(format!(
                                    "postgres checkpoint upsert failed: {e}"
                                ))
                            })?;

                        Ok(())
                    })
                },
            )
            .await
        })
    }
}
