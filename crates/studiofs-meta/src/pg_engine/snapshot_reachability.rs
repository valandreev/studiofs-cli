//! PostgreSQL-authoritative payload reachability and upload-fragment ledger.
//!
//! The current-reference relation is the published after-image for each
//! regular-file inode. The reachability relation records the version interval
//! during which an immutable payload object had at least one current reference.
//! Snapshot rows never copy object identifiers.

use super::{LOCAL_PG_VERSION_COUNTER, PgMetaEngine};
use crate::engine::{MetaError, MetaInodes, MetaResult};
use crate::events::MetaEvent;
use crate::redb_engine::RedbMetaEngine;
use crate::schema::PayloadRefState;
use crate::snapshot::VolumeId;
use crate::snapshot_ports::{
    UploadedFragmentLedger, UploadedFragmentLedgerEntry, UploadedFragmentReconciliationClaim,
    UploadedFragmentReconciliationOutcome, UploadedObjectDeletionClaim,
    UploadedObjectDeletionOutcome,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;
use tokio_postgres::GenericClient;
use tracing::debug;

/// Maximum number of uploaded fragment rows one reconciliation cycle may claim.
pub const SNAPSHOT_UPLOADED_FRAGMENT_SCAN_LIMIT: i64 = 256;

/// Lease duration used by uploaded-fragment reconciliation.
pub const SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS: i64 = 60;

const UPLOAD_LEDGER_REPLAY_BATCH_SIZE: usize = 256;
const SNAPSHOT_REACHABILITY_BOOTSTRAP_BATCH_SIZE: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReachabilityBootstrapOutcome {
    Completed,
    AlreadyInitialized,
    VersionMismatch,
    SnapshotCreationInProgress,
}

type FragmentUploadColumns = (Vec<i64>, Vec<i32>, Vec<String>, Vec<i64>, Vec<Vec<u8>>);

fn replay_local_upload_ledger_once<B, C>(
    local: &RedbMetaEngine,
    mut begin_remote: B,
    mut confirm_remote: C,
) -> MetaResult<()>
where
    B: FnMut(&[UploadedFragmentLedgerEntry]) -> MetaResult<()>,
    C: FnMut(&[UploadedFragmentLedgerEntry]) -> MetaResult<()>,
{
    let pending = local.pending_local_upload_ledger(UPLOAD_LEDGER_REPLAY_BATCH_SIZE)?;
    if pending.is_empty() {
        return Ok(());
    }

    let begins = pending
        .iter()
        .filter(|record| !record.begin_replicated)
        .map(|record| record.entry.clone())
        .collect::<Vec<_>>();
    if !begins.is_empty() {
        begin_remote(&begins)?;
        local.mark_local_upload_ledger_begun(&begins)?;
    }

    let confirmations = local
        .pending_local_upload_ledger(UPLOAD_LEDGER_REPLAY_BATCH_SIZE)?
        .into_iter()
        .filter(|record| record.begin_replicated && record.confirmed)
        .map(|record| record.entry)
        .collect::<Vec<_>>();
    if !confirmations.is_empty() {
        confirm_remote(&confirmations)?;
        local.remove_local_upload_ledger_confirmed(&confirmations)?;
    }
    Ok(())
}

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

    let affected_inode_ids = affected_inodes
        .iter()
        .copied()
        .map(|inode| checked_i64(inode, "inode"))
        .collect::<MetaResult<Vec<_>>>()?;
    let current_rows = client
        .query(
            "SELECT ino, object_id FROM snapshot_current_references WHERE vol = $1 AND ino = ANY($2::BIGINT[])",
            &[&volume_id, &affected_inode_ids],
        )
        .await
        .map_err(|error| {
            PgMetaEngine::classify_pg_error(
                "postgres snapshot current-reference query failed",
                error,
            )
        })?;
    let mut before_by_inode = affected_inodes
        .iter()
        .copied()
        .map(|inode| (inode, BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for row in current_rows {
        let inode: i64 = row.get(0);
        let object_id: i64 = row.get(1);
        let inode = u64::try_from(inode).map_err(|_| {
            MetaError::Storage("snapshot current-reference inode is invalid".to_owned())
        })?;
        let object_id = u64::try_from(object_id).map_err(|_| {
            MetaError::Storage("snapshot current-reference object id is invalid".to_owned())
        })?;
        before_by_inode.entry(inode).or_default().insert(object_id);
    }

    let mut after_by_inode = before_by_inode.clone();
    fold_published_events(&mut after_by_inode, events);

    let object_ids = before_by_inode
        .values()
        .chain(after_by_inode.values())
        .flat_map(|objects| objects.iter().copied())
        .collect::<BTreeSet<_>>();
    let object_id_values = object_ids
        .iter()
        .copied()
        .map(|object_id| checked_i64(object_id, "object id"))
        .collect::<MetaResult<Vec<_>>>()?;
    let global_count_rows = if object_id_values.is_empty() {
        Vec::new()
    } else {
        client
            .query(
                "SELECT object_id, count(*)::BIGINT FROM snapshot_current_references WHERE vol = $1 AND object_id = ANY($2::BIGINT[]) GROUP BY object_id",
                &[&volume_id, &object_id_values],
            )
            .await
            .map_err(|error| {
                PgMetaEngine::classify_pg_error(
                    "postgres snapshot object reference count query failed",
                    error,
                )
            })?
    };
    let mut before_counts = BTreeMap::new();
    for row in global_count_rows {
        let object_id: i64 = row.get(0);
        let count: i64 = row.get(1);
        before_counts.insert(
            u64::try_from(object_id).map_err(|_| {
                MetaError::Storage("snapshot reference object id is invalid".to_owned())
            })?,
            usize::try_from(count).map_err(|_| {
                MetaError::Storage("snapshot reference count is invalid".to_owned())
            })?,
        );
    }
    let mut after_counts = before_counts.clone();
    for (inode, before_objects) in &before_by_inode {
        let after_objects = after_by_inode.get(inode).ok_or_else(|| {
            MetaError::Storage("snapshot after-image inode is missing".to_owned())
        })?;
        for object_id in before_objects {
            let count = after_counts.get_mut(object_id).ok_or_else(|| {
                MetaError::Storage("snapshot reference count is missing".to_owned())
            })?;
            *count = count.checked_sub(1).ok_or_else(|| {
                MetaError::Storage("snapshot reference count underflow".to_owned())
            })?;
        }
        for object_id in after_objects {
            *after_counts.entry(*object_id).or_default() += 1;
        }
    }

    let published_object_ids = after_counts
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(object_id, _)| checked_i64(*object_id, "object id"))
        .collect::<MetaResult<Vec<_>>>()?;
    if !published_object_ids.is_empty() {
        let states = client
            .query(
                "SELECT object_id, state FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = ANY($2::BIGINT[]) FOR UPDATE",
                &[&volume_id, &published_object_ids],
            )
            .await
            .map_err(|error| {
                PgMetaEngine::classify_pg_error(
                    "postgres snapshot upload publication fence read failed",
                    error,
                )
            })?;
        if states.iter().any(|row| {
            matches!(
                row.get::<_, String>(1).as_str(),
                "delete_pending" | "deleted"
            )
        }) {
            return Err(MetaError::Storage(
                "snapshot payload publication requires a new object id after deletion has started"
                    .to_owned(),
            ));
        }
    }

    client
        .execute(
            "DELETE FROM snapshot_current_references WHERE vol = $1 AND ino = ANY($2::BIGINT[])",
            &[&volume_id, &affected_inode_ids],
        )
        .await
        .map_err(|error| {
            PgMetaEngine::classify_pg_error(
                "postgres snapshot current-reference delete failed",
                error,
            )
        })?;

    let mut reference_inodes = Vec::new();
    let mut reference_object_ids = Vec::new();
    for (inode, objects) in &after_by_inode {
        let inode = checked_i64(*inode, "inode")?;
        for object_id in objects {
            reference_inodes.push(inode);
            reference_object_ids.push(checked_i64(*object_id, "object id")?);
        }
    }
    if !reference_inodes.is_empty() {
        client
            .execute(
                "INSERT INTO snapshot_current_references (vol, ino, object_id) SELECT $1, refs.ino, refs.object_id FROM UNNEST($2::BIGINT[], $3::BIGINT[]) AS refs(ino, object_id) ON CONFLICT DO NOTHING",
                &[&volume_id, &reference_inodes, &reference_object_ids],
            )
            .await
            .map_err(|error| {
                PgMetaEngine::classify_pg_error(
                    "postgres snapshot current-reference insert failed",
                    error,
                )
            })?;
    }

    let mut opened_object_ids = Vec::new();
    let mut closed_object_ids = Vec::new();
    for object_id in object_ids.iter().copied() {
        let before_count = before_counts.get(&object_id).copied().unwrap_or(0);
        let after_count = after_counts.get(&object_id).copied().unwrap_or(0);
        match (before_count, after_count) {
            (0, count) if count > 0 => opened_object_ids.push(checked_i64(object_id, "object id")?),
            (count, 0) if count > 0 => closed_object_ids.push(checked_i64(object_id, "object id")?),
            _ => {}
        }
    }

    if !opened_object_ids.is_empty() {
        client
            .execute(
                "INSERT INTO snapshot_object_reachability (vol, object_id, start_version) SELECT $1, objects.object_id, $3 FROM UNNEST($2::BIGINT[]) AS objects(object_id)",
                &[&volume_id, &opened_object_ids, &batch_version],
            )
            .await
            .map_err(|error| {
                PgMetaEngine::classify_pg_error(
                    "postgres snapshot reachability interval open failed",
                    error,
                )
            })?;
    }
    if !closed_object_ids.is_empty() {
        client
            .execute(
                "UPDATE snapshot_object_reachability SET end_version = $3 WHERE vol = $1 AND object_id = ANY($2::BIGINT[]) AND end_version IS NULL",
                &[&volume_id, &closed_object_ids, &batch_version],
            )
            .await
            .map_err(|error| {
                PgMetaEngine::classify_pg_error(
                    "postgres snapshot reachability interval close failed",
                    error,
                )
            })?;
    }
    if !published_object_ids.is_empty() {
        client
            .execute(
                "UPDATE snapshot_payload_upload_objects SET state = 'published', publish_batch_version = $3, closed_at = NULL, last_error = NULL WHERE vol = $1 AND object_id = ANY($2::BIGINT[]) AND state IN ('uploading', 'uploaded', 'retry_wait')",
                &[&volume_id, &published_object_ids, &batch_version],
            )
            .await
            .map_err(|error| {
                PgMetaEngine::classify_pg_error(
                    "postgres snapshot upload publication state update failed",
                    error,
                )
            })?;
    }

    Ok(())
}

fn affected_inodes(events: &[MetaEvent]) -> BTreeSet<u64> {
    events
        .iter()
        .filter_map(|event| match event {
            MetaEvent::RemoveAttr { ino }
            | MetaEvent::RemoveRegularFileGraph { ino }
            | MetaEvent::PublishRegularFileBaseSlices { ino, .. } => Some(*ino),
            MetaEvent::RenameEntry {
                displaced_remove_attr_ino,
                ..
            } => *displaced_remove_attr_ino,
            MetaEvent::SetRegularFileOverlaySlices { .. } => None,
            _ => None,
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

#[cfg(test)]
fn count_references(references: &BTreeMap<u64, BTreeSet<u64>>) -> BTreeMap<u64, usize> {
    let mut counts = BTreeMap::new();
    for objects in references.values() {
        for object_id in objects {
            *counts.entry(*object_id).or_insert(0) += 1;
        }
    }
    counts
}

fn checked_i64(value: u64, kind: &str) -> MetaResult<i64> {
    i64::try_from(value)
        .map_err(|_| MetaError::Storage(format!("snapshot {kind} does not fit PostgreSQL")))
}

impl PgMetaEngine {
    pub(super) fn replay_local_upload_ledger(&self) -> MetaResult<()> {
        replay_local_upload_ledger_once(
            &self.local,
            |entries| self.begin_fragment_uploads_sync(entries),
            |entries| self.confirm_fragment_uploads_sync(entries),
        )
    }

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

    /// Attempt one bounded reachability-bootstrap transaction when needed.
    ///
    /// A version mismatch or active snapshot creation is a normal deferral:
    /// the bootstrap flag remains clear and the next eligible replication
    /// cycle may retry it. PostgreSQL and local-storage failures remain typed
    /// metadata errors.
    pub(super) fn backfill_current_reference_ledger_if_needed(&self) -> MetaResult<()> {
        if self
            .snapshot_reachability_bootstrapped
            .load(Ordering::Acquire)
        {
            return Ok(());
        }

        let _guard = self
            .snapshot_reachability_bootstrap_lock
            .lock()
            .map_err(|_| {
                MetaError::Storage("snapshot reachability bootstrap lock poisoned".to_owned())
            })?;
        if self
            .snapshot_reachability_bootstrapped
            .load(Ordering::Acquire)
        {
            return Ok(());
        }

        match self.backfill_current_reference_ledger_once()? {
            ReachabilityBootstrapOutcome::Completed
            | ReachabilityBootstrapOutcome::AlreadyInitialized => {
                self.snapshot_reachability_bootstrapped
                    .store(true, Ordering::Release);
            }
            ReachabilityBootstrapOutcome::VersionMismatch => {
                debug!(
                    volume_id = self.volume_id,
                    "snapshot reachability bootstrap deferred until metadata catches up"
                );
            }
            ReachabilityBootstrapOutcome::SnapshotCreationInProgress => {
                debug!(
                    volume_id = self.volume_id,
                    "snapshot reachability bootstrap deferred until snapshot creation completes"
                );
            }
        }
        Ok(())
    }

    fn backfill_current_reference_ledger_once(&self) -> MetaResult<ReachabilityBootstrapOutcome> {
        let local = self.local.clone();
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "bootstrap snapshot reachability",
                move |client| {
                    let local = local.clone();
                    Box::pin(async move {
                        let fast_path = client
                            .query_opt(
                                "SELECT metadata.version,
                                        EXISTS (
                                            SELECT 1
                                            FROM snapshot_reachability_bootstrap AS bootstrap
                                            WHERE bootstrap.vol = $1
                                        )
                                 FROM metadata_state AS metadata
                                 WHERE metadata.vol = $1",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap marker/version fast-path query failed",
                                    error,
                                )
                            })?;
                        let Some(fast_path) = fast_path else {
                            return Err(MetaError::Storage(
                                "postgres snapshot reachability bootstrap metadata state row missing"
                                    .to_owned(),
                            ));
                        };
                        let remote_version: i64 = fast_path.get(0);
                        if fast_path.get::<_, bool>(1) {
                            return Ok(ReachabilityBootstrapOutcome::AlreadyInitialized);
                        }

                        let local_version_before_scan = checked_i64(
                            local.get_counter(LOCAL_PG_VERSION_COUNTER)?,
                            "local metadata version",
                        )?;
                        // This non-locking version read is only an early cutoff;
                        // the transaction below rechecks the authoritative row.
                        if local_version_before_scan != remote_version {
                            return Ok(ReachabilityBootstrapOutcome::VersionMismatch);
                        }

                        let references = local.list_published_object_references()?;
                        let local_version_after_scan = checked_i64(
                            local.get_counter(LOCAL_PG_VERSION_COUNTER)?,
                            "local metadata version",
                        )?;
                        // The references must belong to one stable local version.
                        if local_version_before_scan != local_version_after_scan {
                            return Ok(ReachabilityBootstrapOutcome::VersionMismatch);
                        }

                        let mut reference_inodes = Vec::with_capacity(references.len());
                        let mut reference_object_ids = Vec::with_capacity(references.len());
                        let mut unique_object_ids = Vec::with_capacity(references.len());
                        for (inode, object_id) in references {
                            let inode = checked_i64(inode, "inode")?;
                            let object_id = checked_i64(object_id, "object id")?;
                            reference_inodes.push(inode);
                            reference_object_ids.push(object_id);
                            unique_object_ids.push(object_id);
                        }
                        unique_object_ids.sort_unstable();
                        unique_object_ids.dedup();

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
                        let initialized = tx
                            .query_opt(
                                "SELECT initialized_version FROM snapshot_reachability_bootstrap WHERE vol = $1",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap marker query failed",
                                    error,
                                )
                            })?;
                        if initialized.is_some() {
                            tx.commit().await.map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap noop commit failed",
                                    error,
                                )
                            })?;
                            return Ok(ReachabilityBootstrapOutcome::AlreadyInitialized);
                        }

                        let creating_snapshot = tx
                            .query_opt(
                                "SELECT 1 FROM snapshot_catalog WHERE vol = $1 AND lifecycle = 'creating' LIMIT 1",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability creating snapshot check failed",
                                    error,
                                )
                            })?;
                        if creating_snapshot.is_some() {
                            tx.rollback().await.map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap snapshot-deferred rollback failed",
                                    error,
                                )
                            })?;
                            return Ok(ReachabilityBootstrapOutcome::SnapshotCreationInProgress);
                        }

                        // Only this row-locked version can authorize the
                        // prepared local references for publication.
                        let local_version_after_lock = checked_i64(
                            local.get_counter(LOCAL_PG_VERSION_COUNTER)?,
                            "local metadata version",
                        )?;
                        if local_version_after_scan != local_version_after_lock
                            || local_version_after_lock != version
                        {
                            tx.rollback().await.map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap version-mismatch rollback failed",
                                    error,
                                )
                            })?;
                            return Ok(ReachabilityBootstrapOutcome::VersionMismatch);
                        }

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
                        if inserted != 1 {
                            tx.rollback().await.map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap marker-conflict rollback failed",
                                    error,
                                )
                            })?;
                            return Err(MetaError::Storage(
                                "postgres snapshot reachability bootstrap marker was not inserted"
                                    .to_owned(),
                            ));
                        }

                        for (inode_chunk, object_id_chunk) in reference_inodes
                            .chunks(SNAPSHOT_REACHABILITY_BOOTSTRAP_BATCH_SIZE)
                            .zip(reference_object_ids.chunks(SNAPSHOT_REACHABILITY_BOOTSTRAP_BATCH_SIZE))
                        {
                            tx.execute(
                                "INSERT INTO snapshot_current_references (vol, ino, object_id) SELECT $1, refs.ino, refs.object_id FROM UNNEST($2::BIGINT[], $3::BIGINT[]) AS refs(ino, object_id) ON CONFLICT DO NOTHING",
                                &[&volume_id, &inode_chunk, &object_id_chunk],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot reachability bootstrap reference insert failed",
                                    error,
                                )
                            })?;
                        }

                        for object_id_chunk in
                            unique_object_ids.chunks(SNAPSHOT_REACHABILITY_BOOTSTRAP_BATCH_SIZE)
                        {
                            tx.execute(
                                "INSERT INTO snapshot_object_reachability (vol, object_id, start_version) SELECT $1, objects.object_id, $3 FROM UNNEST($2::BIGINT[]) AS objects(object_id) ON CONFLICT DO NOTHING",
                                &[&volume_id, &object_id_chunk, &version],
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
                               AND catalog.lifecycle IN ('ready', 'deleting')
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
                        Ok(ReachabilityBootstrapOutcome::Completed)
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
                                   AND object.state IN ('uploading', 'retry_wait')
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

                        let object_ids = rows
                            .iter()
                            .map(|row| row.get::<_, i64>(0))
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect::<Vec<_>>();
                        let lease_rows = tx
                            .query(
                                "UPDATE snapshot_payload_upload_objects SET reconcile_owner = $3, reconcile_lease_expires_at = clock_timestamp() + ($4 * interval '1 second'), reconcile_fence = reconcile_fence + 1 WHERE vol = $1 AND object_id = ANY($2::BIGINT[]) RETURNING object_id, reconcile_fence",
                                &[
                                    &volume_id,
                                    &object_ids,
                                    &owner,
                                    &(SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS as f64),
                                ],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload reconciliation lease failed",
                                    error,
                                )
                            })?;
                        let fencing_tokens = lease_rows
                            .into_iter()
                            .map(|row| (row.get::<_, i64>(0), row.get::<_, i64>(1)))
                            .collect::<HashMap<_, _>>();
                        let mut claims = Vec::with_capacity(rows.len());
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
                            let fencing_token = fencing_tokens.get(&object_id).copied().ok_or_else(|| {
                                MetaError::Storage(
                                    "snapshot upload reconciliation lease row is missing".to_owned(),
                                )
                            })?;
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
                                            &(SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS as f64),
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

    fn fragment_upload_columns(
        &self,
        entries: &[UploadedFragmentLedgerEntry],
    ) -> MetaResult<FragmentUploadColumns> {
        let mut object_ids = Vec::with_capacity(entries.len());
        let mut fragment_indices = Vec::with_capacity(entries.len());
        let mut object_keys = Vec::with_capacity(entries.len());
        let mut expected_byte_lengths = Vec::with_capacity(entries.len());
        let mut expected_sha256 = Vec::with_capacity(entries.len());
        for entry in entries {
            self.validate_fragment_entry_volume(entry)?;
            object_ids.push(checked_i64(entry.object_id, "object id")?);
            fragment_indices.push(i32::try_from(entry.fragment_index).map_err(|_| {
                MetaError::Storage("snapshot fragment index is invalid".to_owned())
            })?);
            object_keys.push(entry.object_key.clone());
            expected_byte_lengths.push(checked_i64(entry.expected_byte_length, "fragment length")?);
            expected_sha256.push(entry.expected_sha256.to_vec());
        }
        Ok((
            object_ids,
            fragment_indices,
            object_keys,
            expected_byte_lengths,
            expected_sha256,
        ))
    }

    fn begin_fragment_uploads_sync(
        &self,
        entries: &[UploadedFragmentLedgerEntry],
    ) -> MetaResult<()> {
        let (object_ids, fragment_indices, object_keys, expected_byte_lengths, expected_sha256) =
            self.fragment_upload_columns(entries)?;
        if entries.is_empty() {
            return Ok(());
        }
        let object_ids_for_objects = object_ids.clone();
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "begin snapshot fragment upload batch",
                move |client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload begin transaction failed",
                                error,
                            )
                        })?;
                        tx.execute(
                            "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) SELECT $1, object_id, 'uploading' FROM unnest($2::BIGINT[]) AS object_ids(object_id) GROUP BY object_id ON CONFLICT (vol, object_id) DO UPDATE SET state = CASE WHEN snapshot_payload_upload_objects.state = 'deleted' THEN 'uploading' ELSE snapshot_payload_upload_objects.state END, closed_at = NULL",
                            &[&volume_id, &object_ids_for_objects],
                        )
                        .await
                        .map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload object begin failed",
                                error,
                            )
                        })?;
                        tx.execute(
                            "INSERT INTO snapshot_payload_upload_fragments (vol, object_id, fragment_index, object_key, expected_byte_length, expected_sha256, confirmation_state) SELECT $1, object_id, fragment_index, object_key, expected_byte_length, expected_sha256, 'pending' FROM unnest($2::BIGINT[], $3::INT[], $4::TEXT[], $5::BIGINT[], $6::BYTEA[]) AS fragment_values(object_id, fragment_index, object_key, expected_byte_length, expected_sha256) ON CONFLICT (vol, object_id, fragment_index) DO UPDATE SET object_key = EXCLUDED.object_key, expected_byte_length = EXCLUDED.expected_byte_length, expected_sha256 = EXCLUDED.expected_sha256, uploaded_at = now(), confirmed_at = NULL, confirmation_state = 'pending', last_error = NULL",
                            &[
                                &volume_id,
                                &object_ids,
                                &fragment_indices,
                                &object_keys,
                                &expected_byte_lengths,
                                &expected_sha256,
                            ],
                        )
                        .await
                        .map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot fragment upload begin failed",
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

    fn confirm_fragment_uploads_sync(
        &self,
        entries: &[UploadedFragmentLedgerEntry],
    ) -> MetaResult<()> {
        let (object_ids, fragment_indices, _object_keys, expected_byte_lengths, expected_sha256) =
            self.fragment_upload_columns(entries)?;
        if entries.is_empty() {
            return Ok(());
        }
        let expected_row_count = u64::try_from(entries.len())
            .map_err(|_| MetaError::Storage("too many upload fragments".to_owned()))?;
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "confirm snapshot fragment upload batch",
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
                                "WITH entries AS (SELECT * FROM unnest($2::BIGINT[], $3::INT[], $4::BIGINT[], $5::BYTEA[]) AS fragment_values(object_id, fragment_index, expected_byte_length, expected_sha256)) UPDATE snapshot_payload_upload_fragments AS fragment SET expected_byte_length = entries.expected_byte_length, expected_sha256 = entries.expected_sha256, confirmed_at = now(), confirmation_state = 'confirmed', last_error = NULL FROM entries WHERE fragment.vol = $1 AND fragment.object_id = entries.object_id AND fragment.fragment_index = entries.fragment_index",
                                &[
                                    &volume_id,
                                    &object_ids,
                                    &fragment_indices,
                                    &expected_byte_lengths,
                                    &expected_sha256,
                                ],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres snapshot upload fragment confirmation failed",
                                    error,
                                )
                            })?;
                        if updated != expected_row_count {
                            return Err(MetaError::Storage(
                                "snapshot upload fragment confirmation row is missing".to_owned(),
                            ));
                        }
                        tx.execute(
                            "UPDATE snapshot_payload_upload_objects AS upload_object SET state = 'uploaded' WHERE upload_object.vol = $1 AND upload_object.object_id = ANY($2::BIGINT[]) AND upload_object.state = 'uploading' AND NOT EXISTS (SELECT 1 FROM snapshot_payload_upload_fragments AS fragment WHERE fragment.vol = upload_object.vol AND fragment.object_id = upload_object.object_id AND fragment.confirmation_state <> 'confirmed')",
                            &[&volume_id, &object_ids],
                        )
                        .await
                        .map_err(|error| {
                            Self::classify_pg_error(
                                "postgres snapshot upload object confirmation failed",
                                error,
                            )
                        })?;
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
                                            &(SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS as f64),
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
                                    &(SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS as f64),
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
        self.begin_fragment_uploads(std::slice::from_ref(entry))
    }

    fn begin_fragment_uploads(&self, entries: &[UploadedFragmentLedgerEntry]) -> MetaResult<()> {
        self.local.record_local_upload_ledger_begin(entries)?;
        self.signal_poll_wakeup();
        Ok(())
    }

    fn confirm_fragment_uploaded(&self, entry: &UploadedFragmentLedgerEntry) -> MetaResult<()> {
        self.confirm_fragment_uploads(std::slice::from_ref(entry))
    }

    fn confirm_fragment_uploads(&self, entries: &[UploadedFragmentLedgerEntry]) -> MetaResult<()> {
        self.local.record_local_upload_ledger_confirm(entries)?;
        self.signal_poll_wakeup();
        Ok(())
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

    #[test]
    fn local_upload_ledger_replay_orders_remote_begin_before_confirm() {
        let dir = studiofs_test_support::workspace_target_tempdir("upload-ledger-replay-");
        let path = dir.path().join("meta.redb");
        let engine = RedbMetaEngine::open(&path).expect("open local metadata");
        let entry = UploadedFragmentLedgerEntry {
            volume_id: 7,
            object_id: 42,
            fragment_index: 0,
            object_key: "objects/42".to_string(),
            expected_byte_length: 4,
            expected_sha256: [0xA5; 32],
        };
        engine
            .record_local_upload_ledger_begin(std::slice::from_ref(&entry))
            .expect("record local upload begin");
        engine
            .record_local_upload_ledger_confirm(std::slice::from_ref(&entry))
            .expect("record local upload confirmation");

        let calls = std::cell::RefCell::new(Vec::new());
        replay_local_upload_ledger_once(
            &engine,
            |entries| {
                assert_eq!(entries, std::slice::from_ref(&entry));
                calls.borrow_mut().push("begin");
                Ok(())
            },
            |entries| {
                assert_eq!(entries, std::slice::from_ref(&entry));
                calls.borrow_mut().push("confirm");
                Ok(())
            },
        )
        .expect("replay local upload ledger");

        assert_eq!(&*calls.borrow(), &["begin", "confirm"]);
        assert!(
            engine
                .pending_local_upload_ledger(16)
                .expect("read local upload ledger")
                .is_empty()
        );
    }

    #[test]
    fn failed_local_upload_ledger_replay_keeps_pending_confirmation() {
        let dir = studiofs_test_support::workspace_target_tempdir("upload-ledger-retry-");
        let path = dir.path().join("meta.redb");
        let engine = RedbMetaEngine::open(&path).expect("open local metadata");
        let entry = UploadedFragmentLedgerEntry {
            volume_id: 7,
            object_id: 42,
            fragment_index: 0,
            object_key: "objects/42".to_string(),
            expected_byte_length: 4,
            expected_sha256: [0xA5; 32],
        };
        engine
            .record_local_upload_ledger_begin(std::slice::from_ref(&entry))
            .expect("record local upload begin");
        engine
            .record_local_upload_ledger_confirm(std::slice::from_ref(&entry))
            .expect("record local upload confirmation");

        let result = replay_local_upload_ledger_once(
            &engine,
            |_| Err(MetaError::Storage("remote unavailable".to_string())),
            |_| panic!("confirmation must wait for a successful begin"),
        );

        assert!(result.is_err());
        let pending = engine
            .pending_local_upload_ledger(16)
            .expect("read local upload ledger");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].entry, entry);
        assert!(!pending[0].begin_replicated);
        assert!(pending[0].confirmed);
    }
}
