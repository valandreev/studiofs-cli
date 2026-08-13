//! PostgreSQL-authoritative payload reachability and upload-fragment ledger.
//!
//! The current-reference relation is the published after-image for each
//! regular-file inode. The reachability relation records the version interval
//! during which an immutable payload object had at least one current reference.
//! Snapshot rows never copy object identifiers.

use super::PgMetaEngine;
use crate::engine::{MetaError, MetaResult};
use crate::events::MetaEvent;
use crate::schema::PayloadRefState;
use crate::snapshot::VolumeId;
use crate::snapshot_ports::{
    UploadedFragmentLedger, UploadedFragmentLedgerEntry, UploadedFragmentReconciliationClaim,
    UploadedFragmentReconciliationOutcome, UploadedObjectDeletionClaim,
    UploadedObjectDeletionOutcome,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use tokio_postgres::GenericClient;

/// Maximum number of uploaded fragment rows one reconciliation cycle may claim.
pub const SNAPSHOT_UPLOADED_FRAGMENT_SCAN_LIMIT: i64 = 256;

/// Lease duration used by uploaded-fragment reconciliation.
pub const SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS: i64 = 60;

/// Applies one ordered published metadata batch to the PostgreSQL reachability
/// ledger inside the caller's transaction.
pub(crate) async fn apply_object_lifetime_changes<C>(
    client: &C,
    volume_id: VolumeId,
    batch_version: i64,
    events: &[MetaEvent],
) -> MetaResult<()>
where
    C: GenericClient + Sync,
{
    if events.is_empty() {
        return Ok(());
    }

    let applied = client
        .execute(
            "INSERT INTO snapshot_reachability_batches (vol, batch_version) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[&volume_id, &batch_version],
        )
        .await
        .map_err(|error| {
            PgMetaEngine::classify_pg_error(
                "postgres snapshot reachability batch marker failed",
                error,
            )
        })?;
    if applied == 0 {
        return Ok(());
    }

    let affected_inodes = affected_inodes(events);
    if affected_inodes.is_empty() {
        return Ok(());
    }

    let mut before_by_inode = BTreeMap::new();
    for inode in affected_inodes.iter().copied() {
        let before = load_current_references(client, volume_id, inode).await?;
        before_by_inode.insert(inode, before);
    }

    let mut after_by_inode = before_by_inode.clone();
    fold_published_events(&mut after_by_inode, events);

    let object_ids = before_by_inode
        .values()
        .chain(after_by_inode.values())
        .flat_map(|objects| objects.iter().copied())
        .collect::<BTreeSet<_>>();
    let before_counts = count_references(&before_by_inode);
    let after_counts = count_references(&after_by_inode);

    for object_id in object_ids.iter().copied() {
        if after_counts.get(&object_id).copied().unwrap_or(0) == 0 {
            continue;
        }
        let object_id_i64 = checked_i64(object_id, "object id")?;
        let state = client
            .query_opt(
                "SELECT state FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2 FOR UPDATE",
                &[&volume_id, &object_id_i64],
            )
            .await
            .map_err(|error| {
                PgMetaEngine::classify_pg_error(
                    "postgres snapshot upload publication fence read failed",
                    error,
                )
            })?
            .map(|row| row.get::<_, String>(0));
        if state.as_deref() == Some("delete_pending") {
            return Err(MetaError::Storage(
                "snapshot payload publication requires a new object id while deletion is pending"
                    .to_owned(),
            ));
        }
    }

    for inode in affected_inodes {
        let inode_i64 = checked_i64(inode, "inode")?;
        client
            .execute(
                "DELETE FROM snapshot_current_references WHERE vol = $1 AND ino = $2",
                &[&volume_id, &inode_i64],
            )
            .await
            .map_err(|error| {
                PgMetaEngine::classify_pg_error(
                    "postgres snapshot current-reference delete failed",
                    error,
                )
            })?;

        if let Some(objects) = after_by_inode.get(&inode) {
            for object_id in objects {
                let object_id = checked_i64(*object_id, "object id")?;
                client
                    .execute(
                        "INSERT INTO snapshot_current_references (vol, ino, object_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                        &[&volume_id, &inode_i64, &object_id],
                    )
                    .await
                    .map_err(|error| {
                        PgMetaEngine::classify_pg_error(
                            "postgres snapshot current-reference insert failed",
                            error,
                        )
                    })?;
            }
        }
    }

    for object_id in object_ids {
        let before_count = before_counts.get(&object_id).copied().unwrap_or(0);
        let after_count = after_counts.get(&object_id).copied().unwrap_or(0);
        let object_id_i64 = checked_i64(object_id, "object id")?;

        match (before_count, after_count) {
            (0, count) if count > 0 => {
                client
                    .execute(
                        "INSERT INTO snapshot_object_reachability (vol, object_id, start_version) VALUES ($1, $2, $3)",
                        &[&volume_id, &object_id_i64, &batch_version],
                    )
                    .await
                    .map_err(|error| {
                        PgMetaEngine::classify_pg_error(
                            "postgres snapshot reachability interval open failed",
                            error,
                        )
                    })?;
            }
            (count, 0) if count > 0 => {
                client
                    .execute(
                        "UPDATE snapshot_object_reachability SET end_version = $3 WHERE vol = $1 AND object_id = $2 AND end_version IS NULL",
                        &[&volume_id, &object_id_i64, &batch_version],
                    )
                    .await
                    .map_err(|error| {
                        PgMetaEngine::classify_pg_error(
                            "postgres snapshot reachability interval close failed",
                            error,
                        )
                    })?;
            }
            _ => {}
        }

        if after_count > 0 {
            client
                .execute(
                    "UPDATE snapshot_payload_upload_objects SET state = 'published', publish_batch_version = $3, closed_at = NULL, last_error = NULL WHERE vol = $1 AND object_id = $2 AND state IN ('uploading', 'uploaded', 'retry_wait')",
                    &[&volume_id, &object_id_i64, &batch_version],
                )
                .await
                .map_err(|error| {
                    PgMetaEngine::classify_pg_error(
                        "postgres snapshot upload publication state update failed",
                        error,
                    )
                })?;
        }
    }

    Ok(())
}

fn affected_inodes(events: &[MetaEvent]) -> BTreeSet<u64> {
    events
        .iter()
        .flat_map(|event| match event {
            MetaEvent::RemoveAttr { ino }
            | MetaEvent::RemoveRegularFileGraph { ino }
            | MetaEvent::PublishRegularFileBaseSlices { ino, .. } => vec![*ino],
            MetaEvent::RenameEntry {
                displaced_remove_attr_ino,
                ..
            } => displaced_remove_attr_ino.iter().copied().collect(),
            MetaEvent::SetRegularFileOverlaySlices { .. } => Vec::new(),
            _ => Vec::new(),
        })
        .collect()
}

fn fold_published_events(after_by_inode: &mut BTreeMap<u64, BTreeSet<u64>>, events: &[MetaEvent]) {
    for event in events {
        match event {
            MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices } => {
                let objects = chunk_slices
                    .iter()
                    .flat_map(|(_, slices)| slices.iter())
                    .filter_map(|slice| {
                        (slice.payload_state == PayloadRefState::Published)
                            .then_some(slice.payload.as_ref()?.object_id)
                    })
                    .collect();
                after_by_inode.insert(*ino, objects);
            }
            MetaEvent::RemoveRegularFileGraph { ino } | MetaEvent::RemoveAttr { ino } => {
                after_by_inode.insert(*ino, BTreeSet::new());
            }
            MetaEvent::RenameEntry {
                displaced_remove_attr_ino: Some(ino),
                ..
            } => {
                after_by_inode.insert(*ino, BTreeSet::new());
            }
            _ => {}
        }
    }
}

fn count_references(references: &BTreeMap<u64, BTreeSet<u64>>) -> BTreeMap<u64, usize> {
    let mut counts = BTreeMap::new();
    for objects in references.values() {
        for object_id in objects {
            *counts.entry(*object_id).or_insert(0) += 1;
        }
    }
    counts
}

async fn load_current_references<C>(
    client: &C,
    volume_id: VolumeId,
    inode: u64,
) -> MetaResult<BTreeSet<u64>>
where
    C: GenericClient + Sync,
{
    let inode = checked_i64(inode, "inode")?;
    let rows = client
        .query(
            "SELECT object_id FROM snapshot_current_references WHERE vol = $1 AND ino = $2",
            &[&volume_id, &inode],
        )
        .await
        .map_err(|error| {
            PgMetaEngine::classify_pg_error(
                "postgres snapshot current-reference query failed",
                error,
            )
        })?;

    rows.into_iter()
        .map(|row| {
            let object_id: i64 = row.get(0);
            u64::try_from(object_id).map_err(|_| {
                MetaError::Storage("snapshot current-reference object id is invalid".to_owned())
            })
        })
        .collect()
}

fn checked_i64(value: u64, kind: &str) -> MetaResult<i64> {
    i64::try_from(value)
        .map_err(|_| MetaError::Storage(format!("snapshot {kind} does not fit PostgreSQL")))
}

impl PgMetaEngine {
    pub(super) fn load_pg_current_reference_object_ids(&self) -> MetaResult<HashSet<u64>> {
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let rows = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "janitor current-reference query",
                move |client| {
                    Box::pin(async move {
                        client
                            .query(
                                "SELECT DISTINCT object_id FROM snapshot_current_references WHERE vol = $1",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres janitor current-reference query failed",
                                    error,
                                )
                            })
                    })
                },
            )
            .await
        })?;

        rows.into_iter()
            .map(|row| {
                u64::try_from(row.get::<_, i64>(0)).map_err(|_| {
                    MetaError::Storage("snapshot current-reference object id is invalid".to_owned())
                })
            })
            .collect()
    }

    pub(super) fn backfill_current_reference_ledger(&self) -> MetaResult<()> {
        let references = self.local.list_published_object_references()?;
        let objects = references
            .iter()
            .map(|(_, object_id)| *object_id)
            .collect::<BTreeSet<_>>();
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "bootstrap snapshot reachability",
                move |client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot reachability bootstrap transaction failed",
                                error,
                            )
                        })?;
                        let version: i64 = tx
                            .query_one(
                                "SELECT version FROM metadata_state WHERE vol = $1 FOR UPDATE",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap version query failed",
                                    error,
                                )
                            })?
                            .get(0);
                        let inserted = tx
                            .execute(
                                "INSERT INTO snapshot_reachability_bootstrap (vol, initialized_version) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                                &[&volume_id, &version],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap marker failed",
                                    error,
                                )
                            })?;
                        if inserted == 0 {
                            tx.commit().await.map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap noop commit failed",
                                    error,
                                )
                            })?;
                            return Ok(());
                        }

                        for (inode, object_id) in &references {
                            let inode = checked_i64(*inode, "inode")?;
                            let object_id = checked_i64(*object_id, "object id")?;
                            tx.execute(
                                "INSERT INTO snapshot_current_references (vol, ino, object_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                                &[&volume_id, &inode, &object_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap reference insert failed",
                                    error,
                                )
                            })?;
                        }
                        for object_id in objects {
                            let object_id = checked_i64(object_id, "object id")?;
                            tx.execute(
                                "INSERT INTO snapshot_object_reachability (vol, object_id, start_version) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                                &[&volume_id, &object_id, &version],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap interval insert failed",
                                    error,
                                )
                            })?;
                        }
                        tx.execute(
                            "INSERT INTO snapshot_object_reachability (vol, object_id, start_version, end_version)
                             SELECT proofs.vol, proofs.object_id, catalog.cut_version, catalog.cut_version + 1
                             FROM snapshot_artifact_payload_proofs AS proofs
                             JOIN snapshot_catalog AS catalog
                               ON catalog.vol = proofs.vol AND catalog.id = proofs.snapshot_id
                             WHERE proofs.vol = $1
                               AND catalog.lifecycle IN ('creating', 'ready', 'deleting')
                               AND catalog.cut_version < 9223372036854775807
                             ON CONFLICT DO NOTHING",
                            &[&volume_id],
                        )
                        .await
                        .map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot reachability historical bootstrap failed",
                                error,
                            )
                        })?;
                        tx.commit().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot reachability bootstrap commit failed",
                                error,
                            )
                        })?;
                        Ok(())
                    })
                },
            )
            .await
        })
    }

    fn fragment_ledger_volume_id(&self) -> MetaResult<VolumeId> {
        Ok(self.volume_id)
    }

    fn validate_fragment_entry_volume(
        &self,
        entry: &UploadedFragmentLedgerEntry,
    ) -> MetaResult<()> {
        if entry.volume_id != self.volume_id {
            return Err(MetaError::Storage(
                "snapshot upload volume identity mismatch".to_owned(),
            ));
        }
        Ok(())
    }

    fn claim_uploaded_fragment_reconciliation_sync(
        &self,
        owner: &str,
    ) -> MetaResult<Vec<UploadedFragmentReconciliationClaim>> {
        let owner = owner.to_owned();
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "claim snapshot upload reconciliation",
                move |client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload reconciliation transaction failed",
                                error,
                            )
                        })?;
                        tx.execute(
                            "INSERT INTO snapshot_payload_reconcile_cursors (vol) VALUES ($1) ON CONFLICT DO NOTHING",
                            &[&volume_id],
                        )
                        .await
                        .map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload reconciliation cursor create failed",
                                error,
                            )
                        })?;
                        let cursor = tx
                            .query_one(
                                "SELECT object_id, fragment_index FROM snapshot_payload_reconcile_cursors WHERE vol = $1 FOR UPDATE",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload reconciliation cursor read failed",
                                    error,
                                )
                            })?;
                        let cursor_object_id: i64 = cursor.get(0);
                        let cursor_fragment_index: i32 = cursor.get(1);
                        let rows = tx
                            .query(
                                "SELECT fragment.object_id, fragment.fragment_index, fragment.object_key, fragment.expected_byte_length, fragment.expected_sha256
                                 FROM snapshot_payload_upload_fragments AS fragment
                                 JOIN snapshot_payload_upload_objects AS object
                                   ON object.vol = fragment.vol AND object.object_id = fragment.object_id
                                 WHERE fragment.vol = $1
                                   AND (
                                       object.state IN ('uploading', 'retry_wait')
                                       OR (
                                           object.state = 'uploaded'
                                           AND fragment.uploaded_at >= clock_timestamp() - interval '60 seconds'
                                       )
                                   )
                                   AND (object.reconcile_lease_expires_at IS NULL OR object.reconcile_lease_expires_at <= clock_timestamp())
                                   AND (object.next_retry_at IS NULL OR object.next_retry_at <= clock_timestamp())
                                   AND (fragment.object_id, fragment.fragment_index) > ($2, $3)
                                 ORDER BY fragment.object_id, fragment.fragment_index
                                 LIMIT $4
                                 FOR UPDATE OF object, fragment SKIP LOCKED",
                                &[
                                    &volume_id,
                                    &cursor_object_id,
                                    &cursor_fragment_index,
                                    &SNAPSHOT_UPLOADED_FRAGMENT_SCAN_LIMIT,
                                ],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload reconciliation claim failed",
                                    error,
                                )
                            })?;

                        let mut claims = Vec::with_capacity(rows.len());
                        let mut fencing_tokens = HashMap::new();
                        for row in rows {
                            let object_id: i64 = row.get(0);
                            let fragment_index: i32 = row.get(1);
                            let expected_byte_length: i64 = row.get(3);
                            let digest: Vec<u8> = row.get(4);
                            let expected_sha256: [u8; 32] = digest.try_into().map_err(|_| {
                                MetaError::Storage(
                                    "snapshot upload fragment digest is invalid".to_owned(),
                                )
                            })?;
                            let fencing_token = if let Some(fencing_token) =
                                fencing_tokens.get(&object_id).copied()
                            {
                                fencing_token
                            } else {
                                let fencing_token: i64 = tx
                                    .query_one(
                                        "UPDATE snapshot_payload_upload_objects SET reconcile_owner = $3, reconcile_lease_expires_at = clock_timestamp() + ($4 * interval '1 second'), reconcile_fence = reconcile_fence + 1 WHERE vol = $1 AND object_id = $2 RETURNING reconcile_fence",
                                        &[
                                            &volume_id,
                                            &object_id,
                                            &owner,
                                            &SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS,
                                        ],
                                    )
                                    .await
                                    .map_err(|error| {
                                        Self::classify_pg_error(
                                            "postgres snapshot upload reconciliation lease failed",
                                            error,
                                        )
                                    })?
                                    .get(0);
                                fencing_tokens.insert(object_id, fencing_token);
                                fencing_token
                            };
                            claims.push(UploadedFragmentReconciliationClaim {
                                entry: UploadedFragmentLedgerEntry {
                                    volume_id,
                                    object_id: u64::try_from(object_id).map_err(|_| {
                                        MetaError::Storage(
                                            "snapshot upload object id is invalid".to_owned(),
                                        )
                                    })?,
                                    fragment_index: u32::try_from(fragment_index).map_err(|_| {
                                        MetaError::Storage(
                                            "snapshot upload fragment index is invalid".to_owned(),
                                        )
                                    })?,
                                    object_key: row.get(2),
                                    expected_byte_length: u64::try_from(expected_byte_length)
                                        .map_err(|_| {
                                            MetaError::Storage(
                                                "snapshot upload fragment length is invalid"
                                                    .to_owned(),
                                            )
                                        })?,
                                    expected_sha256,
                                },
                                fencing_token,
                            });
                        }

                        if let Some(last) = claims.last() {
                            let object_id = checked_i64(last.entry.object_id, "object id")?;
                            let fragment_index = i32::try_from(last.entry.fragment_index).map_err(|_| {
                                MetaError::Storage("snapshot fragment index is invalid".to_owned())
                            })?;
                            tx.execute(
                                "UPDATE snapshot_payload_reconcile_cursors SET object_id = $2, fragment_index = $3, updated_at = clock_timestamp() WHERE vol = $1",
                                &[&volume_id, &object_id, &fragment_index],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload reconciliation cursor update failed",
                                    error,
                                )
                            })?;
                        } else {
                            tx.execute(
                                "UPDATE snapshot_payload_reconcile_cursors SET object_id = 0, fragment_index = 0, updated_at = clock_timestamp() WHERE vol = $1",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload reconciliation cursor reset failed",
                                    error,
                                )
                            })?;
                        }
                        tx.commit().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload reconciliation commit failed",
                                error,
                            )
                        })?;
                        Ok(claims)
                    })
                },
            )
            .await
        })
    }

    fn finish_uploaded_fragment_reconciliation_sync(
        &self,
        owner: &str,
        claim: &UploadedFragmentReconciliationClaim,
        outcome: UploadedFragmentReconciliationOutcome,
    ) -> MetaResult<()> {
        let object_id = checked_i64(claim.entry.object_id, "object id")?;
        let fragment_index = i32::try_from(claim.entry.fragment_index)
            .map_err(|_| MetaError::Storage("snapshot fragment index is invalid".to_owned()))?;
        let expected_byte_length =
            checked_i64(claim.entry.expected_byte_length, "fragment length")?;
        let expected_sha256 = claim.entry.expected_sha256.to_vec();
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let owner = owner.to_owned();
        let fencing_token = claim.fencing_token;
        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "finish snapshot upload reconciliation",
                move |client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload reconciliation completion transaction failed",
                                error,
                            )
                        })?;
                        match outcome {
                            UploadedFragmentReconciliationOutcome::RemoteConfirmed => {
                                let updated = tx
                                    .execute(
                                        "UPDATE snapshot_payload_upload_fragments SET confirmed_at = clock_timestamp(), confirmation_state = 'confirmed', last_error = NULL WHERE vol = $1 AND object_id = $2 AND fragment_index = $3 AND expected_byte_length = $4 AND expected_sha256 = $5 AND EXISTS (SELECT 1 FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2 AND reconcile_owner = $6 AND reconcile_fence = $7 AND state IN ('uploading', 'uploaded', 'retry_wait'))",
                                        &[
                                            &volume_id,
                                            &object_id,
                                            &fragment_index,
                                            &expected_byte_length,
                                            &expected_sha256,
                                            &owner,
                                            &fencing_token,
                                        ],
                                    )
                                    .await
                                    .map_err(|error| {
                                        Self::classify_pg_error(
                                            "postgres snapshot upload reconciliation fragment confirmation failed",
                                            error,
                                        )
                                    })?;
                                if updated == 1 {
                                    let pending = tx
                                        .query_one(
                                            "SELECT EXISTS (SELECT 1 FROM snapshot_payload_upload_fragments WHERE vol = $1 AND object_id = $2 AND confirmation_state <> 'confirmed')",
                                            &[&volume_id, &object_id],
                                        )
                                        .await
                                        .map_err(|error| {
                                            Self::classify_pg_error(
                                                "postgres snapshot upload reconciliation pending-state query failed",
                                                error,
                                            )
                                        })?
                                        .get::<_, bool>(0);
                                    if pending {
                                        tx.execute(
                                            "UPDATE snapshot_payload_upload_objects SET next_retry_at = NULL, last_error = NULL WHERE vol = $1 AND object_id = $2 AND reconcile_owner = $3 AND reconcile_fence = $4",
                                            &[&volume_id, &object_id, &owner, &fencing_token],
                                        )
                                        .await
                                    } else {
                                        tx.execute(
                                            "UPDATE snapshot_payload_upload_objects SET state = CASE WHEN state = 'uploading' THEN 'uploaded' ELSE 'delete_pending' END, closed_at = CASE WHEN state = 'uploading' THEN closed_at ELSE clock_timestamp() END, next_retry_at = NULL, last_error = NULL, reconcile_owner = NULL, reconcile_lease_expires_at = NULL WHERE vol = $1 AND object_id = $2 AND reconcile_owner = $3 AND reconcile_fence = $4 AND state IN ('uploading', 'uploaded', 'retry_wait')",
                                            &[&volume_id, &object_id, &owner, &fencing_token],
                                        )
                                        .await
                                    }
                                    .map_err(|error| {
                                        Self::classify_pg_error(
                                            "postgres snapshot upload reconciliation object update failed",
                                            error,
                                        )
                                    })?;
                                }
                            }
                            UploadedFragmentReconciliationOutcome::Retryable => {
                                tx
                                    .execute(
                                        "UPDATE snapshot_payload_upload_objects SET state = CASE WHEN state = 'uploading' THEN 'uploading' ELSE 'retry_wait' END, next_retry_at = clock_timestamp() + ($5 * interval '1 second'), last_error = 'remote proof retry required', reconcile_owner = NULL, reconcile_lease_expires_at = NULL WHERE vol = $1 AND object_id = $2 AND reconcile_owner = $3 AND reconcile_fence = $4 AND state IN ('uploading', 'uploaded', 'retry_wait')",
                                        &[
                                            &volume_id,
                                            &object_id,
                                            &owner,
                                            &fencing_token,
                                            &SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS,
                                        ],
                                    )
                                    .await
                                    .map_err(|error| {
                                        Self::classify_pg_error(
                                            "postgres snapshot upload reconciliation retry update failed",
                                            error,
                                        )
                                    })?;
                            }
                        }
                        tx.commit().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload reconciliation completion commit failed",
                                error,
                            )
                        })?;
                        Ok(())
                    })
                },
            )
            .await
        })
    }

    fn begin_fragment_upload_sync(&self, entry: &UploadedFragmentLedgerEntry) -> MetaResult<()> {
        self.validate_fragment_entry_volume(entry)?;
        let object_id = checked_i64(entry.object_id, "object id")?;
        let fragment_index = i32::try_from(entry.fragment_index)
            .map_err(|_| MetaError::Storage("snapshot fragment index is invalid".to_owned()))?;
        let expected_byte_length = checked_i64(entry.expected_byte_length, "fragment length")?;
        let key = entry.object_key.clone();
        let digest = entry.expected_sha256.to_vec();
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "begin snapshot fragment upload",
                move |client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload begin transaction failed",
                                error,
                            )
                        })?;
                        tx
                            .execute(
                                "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, $2, 'uploading') ON CONFLICT (vol, object_id) DO UPDATE SET state = CASE WHEN snapshot_payload_upload_objects.state = 'deleted' THEN 'uploading' ELSE snapshot_payload_upload_objects.state END, closed_at = NULL",
                                &[&volume_id, &object_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload object begin failed",
                                    error,
                                )
                            })?;
                        tx
                            .execute(
                                "INSERT INTO snapshot_payload_upload_fragments (vol, object_id, fragment_index, object_key, expected_byte_length, expected_sha256, confirmation_state) VALUES ($1, $2, $3, $4, $5, $6, 'pending') ON CONFLICT (vol, object_id, fragment_index) DO UPDATE SET object_key = EXCLUDED.object_key, expected_byte_length = EXCLUDED.expected_byte_length, expected_sha256 = EXCLUDED.expected_sha256, uploaded_at = now(), confirmed_at = NULL, confirmation_state = 'pending', last_error = NULL",
                                &[&volume_id, &object_id, &fragment_index, &key, &expected_byte_length, &digest],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload fragment begin failed",
                                    error,
                                )
                            })?;
                        tx.commit().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload begin commit failed",
                                error,
                            )
                        })?;
                        Ok(())
                    })
                },
            )
            .await
        })
    }

    fn confirm_fragment_uploaded_sync(
        &self,
        entry: &UploadedFragmentLedgerEntry,
    ) -> MetaResult<()> {
        self.validate_fragment_entry_volume(entry)?;
        let object_id = checked_i64(entry.object_id, "object id")?;
        let fragment_index = i32::try_from(entry.fragment_index)
            .map_err(|_| MetaError::Storage("snapshot fragment index is invalid".to_owned()))?;
        let expected_byte_length = checked_i64(entry.expected_byte_length, "fragment length")?;
        let digest = entry.expected_sha256.to_vec();
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "confirm snapshot fragment upload",
                move |client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload confirmation transaction failed",
                                error,
                            )
                        })?;
                        let updated = tx
                            .execute(
                                "UPDATE snapshot_payload_upload_fragments SET expected_byte_length = $4, expected_sha256 = $5, confirmed_at = now(), confirmation_state = 'confirmed', last_error = NULL WHERE vol = $1 AND object_id = $2 AND fragment_index = $3",
                                &[&volume_id, &object_id, &fragment_index, &expected_byte_length, &digest],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload fragment confirmation failed",
                                    error,
                                )
                            })?;
                        if updated != 1 {
                            return Err(MetaError::Storage(
                                "snapshot upload fragment confirmation row is missing".to_owned(),
                            ));
                        }
                        let pending = tx
                            .query_one(
                                "SELECT EXISTS (SELECT 1 FROM snapshot_payload_upload_fragments WHERE vol = $1 AND object_id = $2 AND confirmation_state <> 'confirmed')",
                                &[&volume_id, &object_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload confirmation state query failed",
                                    error,
                                )
                            })?
                            .get::<_, bool>(0);
                        if !pending {
                            tx
                            .execute(
                                "UPDATE snapshot_payload_upload_objects SET state = 'uploaded' WHERE vol = $1 AND object_id = $2 AND state = 'uploading'",
                                &[&volume_id, &object_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload object confirmation failed",
                                    error,
                                )
                            })?;
                        }
                        tx.commit().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload confirmation commit failed",
                                error,
                            )
                        })?;
                        Ok(())
                    })
                },
            )
            .await
        })
    }

    fn claim_uploaded_object_deletion_sync(
        &self,
        object_id: u64,
        owner: &str,
    ) -> MetaResult<UploadedObjectDeletionClaim> {
        let object_id = checked_i64(object_id, "object id")?;
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let owner = owner.to_owned();
        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "claim snapshot upload object deletion",
                move |client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload deletion claim transaction failed",
                                error,
                            )
                        })?;
                        let row = tx
                            .query_opt(
                                "SELECT state, (reconcile_lease_expires_at IS NOT NULL AND reconcile_lease_expires_at > clock_timestamp()) FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2 FOR UPDATE",
                                &[&volume_id, &object_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload deletion claim row read failed",
                                    error,
                                )
                            })?;
                        let claim = if let Some(row) = row {
                            let state: String = row.get(0);
                            let lease_active: bool = row.get(1);
                            if lease_active {
                                UploadedObjectDeletionClaim::Protected
                            } else {
                                let referenced = tx
                                    .query_one(
                                        "SELECT EXISTS (SELECT 1 FROM snapshot_current_references WHERE vol = $1 AND object_id = $2)",
                                        &[&volume_id, &object_id],
                                    )
                                    .await
                                    .map_err(|error| {
                                        Self::classify_pg_error(
                                            "postgres snapshot upload deletion reference check failed",
                                            error,
                                        )
                                    })?
                                    .get::<_, bool>(0);
                                if referenced {
                                    UploadedObjectDeletionClaim::Protected
                                } else if state == "deleted" {
                                    UploadedObjectDeletionClaim::Untracked
                                } else if !matches!(
                                    state.as_str(),
                                    "uploaded" | "published" | "retry_wait" | "delete_pending"
                                ) {
                                    UploadedObjectDeletionClaim::Protected
                                } else {
                                    tx.query_opt(
                                        "UPDATE snapshot_payload_upload_objects SET state = 'delete_pending', reconcile_owner = $3, reconcile_lease_expires_at = clock_timestamp() + ($4 * interval '1 second'), reconcile_fence = reconcile_fence + 1, next_retry_at = NULL, last_error = NULL WHERE vol = $1 AND object_id = $2 AND state IN ('uploaded', 'published', 'retry_wait', 'delete_pending') AND (reconcile_lease_expires_at IS NULL OR reconcile_lease_expires_at <= clock_timestamp()) AND NOT EXISTS (SELECT 1 FROM snapshot_current_references WHERE vol = $1 AND object_id = $2) RETURNING reconcile_fence",
                                        &[
                                            &volume_id,
                                            &object_id,
                                            &owner,
                                            &SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS,
                                        ],
                                    )
                                    .await
                                    .map_err(|error| {
                                        Self::classify_pg_error(
                                            "postgres snapshot upload deletion lease failed",
                                            error,
                                        )
                                    })?
                                    .map(|row| UploadedObjectDeletionClaim::Claimed {
                                        fencing_token: row.get(0),
                                    })
                                    .unwrap_or(UploadedObjectDeletionClaim::Protected)
                                }
                            }
                        } else {
                            UploadedObjectDeletionClaim::Untracked
                        };
                        tx.commit().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload deletion claim commit failed",
                                error,
                            )
                        })?;
                        Ok(claim)
                    })
                },
            )
            .await
        })
    }

    fn finish_uploaded_object_deletion_sync(
        &self,
        object_id: u64,
        owner: &str,
        fencing_token: i64,
        outcome: UploadedObjectDeletionOutcome,
    ) -> MetaResult<()> {
        let object_id = checked_i64(object_id, "object id")?;
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let owner = owner.to_owned();
        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "finish snapshot upload object deletion",
                move |client| {
                    Box::pin(async move {
                        let (state, message) = match outcome {
                            UploadedObjectDeletionOutcome::Deleted => {
                                ("deleted", None)
                            }
                            UploadedObjectDeletionOutcome::Retryable => {
                                ("retry_wait", Some("remote deletion retry required"))
                            }
                        };
                        client
                            .execute(
                                "UPDATE snapshot_payload_upload_objects SET state = $5, closed_at = CASE WHEN $5 = 'deleted' THEN clock_timestamp() ELSE closed_at END, next_retry_at = CASE WHEN $5 = 'retry_wait' THEN clock_timestamp() + ($6 * interval '1 second') ELSE NULL END, last_error = $7, reconcile_owner = NULL, reconcile_lease_expires_at = NULL WHERE vol = $1 AND object_id = $2 AND reconcile_owner = $3 AND reconcile_fence = $4 AND state = 'delete_pending'",
                                &[
                                    &volume_id,
                                    &object_id,
                                    &owner,
                                    &fencing_token,
                                    &state,
                                    &SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS,
                                    &message,
                                ],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload deletion completion failed",
                                    error,
                                )
                            })?;
                        Ok(())
                    })
                },
            )
            .await
        })
    }
}

impl UploadedFragmentLedger for PgMetaEngine {
    fn fragment_ledger_volume_id(&self) -> MetaResult<VolumeId> {
        self.fragment_ledger_volume_id()
    }

    fn claim_uploaded_fragment_reconciliation(
        &self,
        owner: &str,
    ) -> MetaResult<Vec<UploadedFragmentReconciliationClaim>> {
        self.claim_uploaded_fragment_reconciliation_sync(owner)
    }

    fn finish_uploaded_fragment_reconciliation(
        &self,
        owner: &str,
        claim: &UploadedFragmentReconciliationClaim,
        outcome: UploadedFragmentReconciliationOutcome,
    ) -> MetaResult<()> {
        self.finish_uploaded_fragment_reconciliation_sync(owner, claim, outcome)
    }

    fn claim_uploaded_object_deletion(
        &self,
        object_id: u64,
        owner: &str,
    ) -> MetaResult<UploadedObjectDeletionClaim> {
        self.claim_uploaded_object_deletion_sync(object_id, owner)
    }

    fn finish_uploaded_object_deletion(
        &self,
        object_id: u64,
        owner: &str,
        fencing_token: i64,
        outcome: UploadedObjectDeletionOutcome,
    ) -> MetaResult<()> {
        self.finish_uploaded_object_deletion_sync(object_id, owner, fencing_token, outcome)
    }

    fn begin_fragment_upload(&self, entry: &UploadedFragmentLedgerEntry) -> MetaResult<()> {
        self.begin_fragment_upload_sync(entry)
    }

    fn confirm_fragment_uploaded(&self, entry: &UploadedFragmentLedgerEntry) -> MetaResult<()> {
        self.confirm_fragment_uploaded_sync(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ChunkId, PayloadRef, SliceId, SliceOrder, SliceRange, SliceRecord};

    fn published_slice(object_id: u64) -> SliceRecord {
        SliceRecord {
            slice_id: SliceId(1),
            order: SliceOrder(1),
            range: SliceRange::new(0, 1).expect("valid range"),
            payload: Some(PayloadRef::new_full(object_id, 1)),
            payload_state: PayloadRefState::Published,
        }
    }

    #[test]
    fn published_after_image_ignores_unpublished_and_tombstone_slices() {
        let mut after = BTreeMap::from([(7, BTreeSet::from([11]))]);
        let mut unpublished = published_slice(12);
        unpublished.payload_state = PayloadRefState::Uploaded;
        let tombstone = SliceRecord::tombstone(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(1, 2).expect("valid range"),
        );
        let events = [MetaEvent::PublishRegularFileBaseSlices {
            ino: 7,
            chunk_slices: vec![(
                ChunkId(0),
                vec![published_slice(13), unpublished, tombstone],
            )],
        }];

        fold_published_events(&mut after, &events);

        assert_eq!(after[&7], BTreeSet::from([13]));
    }

    #[test]
    fn repeated_publish_and_remove_events_use_the_final_after_image() {
        let mut after = BTreeMap::from([(7, BTreeSet::from([11]))]);
        let events = [
            MetaEvent::PublishRegularFileBaseSlices {
                ino: 7,
                chunk_slices: vec![(ChunkId(0), vec![published_slice(12)])],
            },
            MetaEvent::RemoveRegularFileGraph { ino: 7 },
            MetaEvent::PublishRegularFileBaseSlices {
                ino: 7,
                chunk_slices: vec![(ChunkId(0), vec![published_slice(13)])],
            },
        ];

        fold_published_events(&mut after, &events);

        assert_eq!(after[&7], BTreeSet::from([13]));
    }

    #[test]
    fn reference_counts_keep_overlapping_inode_lifetimes_open() {
        let references = BTreeMap::from([(1, BTreeSet::from([7, 8])), (2, BTreeSet::from([7]))]);

        assert_eq!(count_references(&references)[&7], 2);
        assert_eq!(count_references(&references)[&8], 1);
    }
}
