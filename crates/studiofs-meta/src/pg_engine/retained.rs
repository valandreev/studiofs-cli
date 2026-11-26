use super::*;
use crate::events::MetaEvent;
use crate::trash::{
    RETAINED_RECEIPT_XATTR, RetainedEntryState, RetainedPurgeReason, VolumeSettings,
};

/// Retry identity of one publication. The publisher is part of it: an identical
/// batch from a peer is its own publication, which must reach `event_log` under
/// that peer's client id rather than being absorbed by the first client's receipt.
pub(super) fn publication_digest(
    client_id: &str,
    events: &[MetaEvent],
) -> MetaResult<Option<Vec<u8>>> {
    use sha2::{Digest, Sha256};
    if !events.iter().any(
        |event| matches!(event, MetaEvent::SetXattr { name, .. } if name == RETAINED_RECEIPT_XATTR),
    ) {
        return Ok(None);
    }
    let mut digest = Sha256::new();
    digest.update(b"studiofs-retained-publication-v2");
    digest.update((client_id.len() as u64).to_le_bytes());
    digest.update(client_id.as_bytes());
    for event in events {
        let bytes = crate::events::encode_event(event)?;
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    Ok(Some(digest.finalize().to_vec()))
}

/// Called after the publication's format-fenced metadata row lock. All receipt
/// comparisons and their namespace events commit in the same PG transaction.
/// `validate_expiry` gates a fresh expiry against the current shared settings revision;
/// historical seeding passes false because its policy is not yet restored.
pub(super) async fn apply_retained_authority(
    tx: &tokio_postgres::Transaction<'_>,
    volume: i64,
    events: &[MetaEvent],
    validate_expiry: bool,
    locked: &mut HashMap<i64, crate::trash::RetainedEntry>,
) -> MetaResult<()> {
    for event in events {
        let MetaEvent::SetXattr {
            ino, name, value, ..
        } = event
        else {
            continue;
        };
        if name != RETAINED_RECEIPT_XATTR {
            continue;
        }
        let incoming = RedbMetaEngine::decode_retained_entry(*ino, value)?;
        let receipt = i64::try_from(*ino)
            .map_err(|_| MetaError::Storage("trash receipt exceeds shared inode range".into()))?;
        // This transaction already holds the row lock and knows its record: an identical
        // record (each child-purge fence) needs no further round trip under the state lock.
        if locked.get(&receipt) == Some(&incoming) {
            continue;
        }
        let row = tx
            .query_opt(
                "SELECT record FROM trash_entry_authority WHERE vol=$1 AND receipt=$2 FOR UPDATE",
                &[&volume, &receipt],
            )
            .await
            .map_err(|error| PgMetaEngine::classify_pg_error("read retained authority", error))?;
        if let Some(row) = row {
            let bytes: Vec<u8> = row.get(0);
            let current = RedbMetaEngine::decode_retained_entry(*ino, &bytes)?;
            if current == incoming {
                locked.insert(receipt, incoming);
                continue;
            }
            let mut transitioned = current.clone();
            transitioned.state = incoming.state.clone();
            transitioned.revision = incoming.revision;
            if current.state != RetainedEntryState::Retained
                || incoming.revision != 1
                || transitioned != incoming
            {
                return Err(MetaError::Storage(format!(
                    "retained entry {ino} has a conflicting shared transition; accepted local data and journal remain pending"
                )));
            }
            if validate_expiry {
                check_expiry_revision(tx, volume, *ino, &incoming.state).await?;
            }
            tx.execute(
                "UPDATE trash_entry_authority SET record=$3 WHERE vol=$1 AND receipt=$2",
                &[&volume, &receipt, value],
            )
            .await
            .map_err(|error| PgMetaEngine::classify_pg_error("settle retained authority", error))?;
            locked.insert(receipt, incoming);
        } else {
            if incoming.state != RetainedEntryState::Retained || incoming.revision != 0 {
                return Err(MetaError::Storage("retained transition has no shared admission; preserve this cache and retry publication".into()));
            }
            tx.execute(
                "INSERT INTO trash_entry_authority(vol,receipt,record) VALUES($1,$2,$3)",
                &[&volume, &receipt, value],
            )
            .await
            .map_err(|error| PgMetaEngine::classify_pg_error("admit retained authority", error))?;
            locked.insert(receipt, incoming);
        }
    }
    reject_receipt_removal(tx, volume, events).await
}

/// Reject an expiry judged under superseded shared settings. Revisions never
/// repeat, so a rejected proposal stays rejected even if the period returns to
/// its old value. The caller holds the `metadata_state` row lock, so the
/// revision cannot change between this read and the commit.
async fn check_expiry_revision(
    tx: &tokio_postgres::Transaction<'_>,
    volume: i64,
    receipt: Ino,
    state: &RetainedEntryState,
) -> MetaResult<()> {
    let RetainedEntryState::PurgeRequested {
        reason: RetainedPurgeReason::Expired { settings_revision },
    } = state
    else {
        return Ok(());
    };
    let row = tx
        .query_one(
            "SELECT volume_settings FROM metadata_state WHERE vol=$1",
            &[&volume],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("read retained expiry revision", error))?;
    let bytes: Option<Vec<u8>> = row.get(0);
    if shared_settings(bytes.as_deref())?.revision != *settings_revision {
        return Err(MetaError::Storage(format!(
            "retained entry {receipt} expiry was judged under superseded shared settings; the stale proposal is retired and the entry stays retained"
        )));
    }
    Ok(())
}

/// The shared settings (absent settings mean the disabled default).
fn shared_settings(bytes: Option<&[u8]>) -> MetaResult<VolumeSettings> {
    Ok(bytes
        .map(VolumeSettings::decode_json)
        .transpose()?
        .unwrap_or_default())
}

/// Rebuild the receipt authority projection inside the transaction that
/// replaces a volume's checkpoint and event log wholesale (`studiofs load`).
/// The caller must hold the volume lock and have proven the projection empty.
/// Checkpoint records are shared state at its cut; each later batch is
/// admitted by `apply_retained_authority`, exactly as on publication. It also
/// sets `metadata_state.volume_settings` (the caller has inserted the row) to
/// the checkpoint settings superseded by later tag-19 events.
/// `events` are `(batch_version, event_type, event_payload)` in log order.
pub async fn seed_retained_authority(
    tx: &tokio_postgres::Transaction<'_>,
    volume: i64,
    checkpoint: &[u8],
    checkpoint_version: i64,
    events: &[(i64, &str, &[u8])],
) -> MetaResult<()> {
    let checkpoint = crate::snapshot::ServingCheckpoint::decode_published(
        checkpoint,
        None,
        volume,
        checkpoint_version,
    )?;
    for (key, value) in &checkpoint.xattrs {
        let Some((ino, name)) = key.split_first_chunk::<8>() else {
            continue;
        };
        if name != RETAINED_RECEIPT_XATTR {
            continue;
        }
        let ino = u64::from_le_bytes(*ino);
        RedbMetaEngine::decode_retained_entry(ino, value)?;
        let receipt = i64::try_from(ino)
            .map_err(|_| MetaError::Storage("trash receipt exceeds shared inode range".into()))?;
        tx.execute(
            "INSERT INTO trash_entry_authority(vol,receipt,record) VALUES($1,$2,$3)",
            &[&volume, &receipt, value],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("seed retained authority", error))?;
    }
    let mut settings = checkpoint.volume_settings;
    let mut batch = Vec::new();
    let mut locked = HashMap::new();
    for (index, (version, event_type, payload)) in events.iter().enumerate() {
        if *version <= checkpoint_version {
            continue;
        }
        let event = crate::events::decode_event_with_type(event_type, payload)?;
        if let MetaEvent::SetVolumeSettings { settings: next } = &event
            && settings.is_superseded_by(next)?
        {
            settings = *next;
        }
        batch.push(event);
        if events.get(index + 1).is_none_or(|next| next.0 != *version) {
            apply_retained_authority(tx, volume, &batch, false, &mut locked).await?;
            batch.clear();
        }
    }
    // The trash-days CAS row (`set_shared_trash_days`) must continue from the
    // settled revision, not restart from the default.
    let settings = serde_json::to_vec(&settings)
        .map_err(|error| MetaError::Serialization(error.to_string()))?;
    let updated = tx
        .execute(
            "UPDATE metadata_state SET volume_settings=$2 WHERE vol=$1",
            &[&volume, &settings],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("seed volume settings", error))?;
    if updated != 1 {
        return Err(MetaError::Storage(
            "seed volume settings requires the loaded metadata_state row".into(),
        ));
    }
    Ok(())
}

/// A published receipt keeps its inode and record in base: a delayed client
/// proves its terminal record lost only from that replayed winner.
async fn reject_receipt_removal(
    tx: &tokio_postgres::Transaction<'_>,
    volume: i64,
    events: &[MetaEvent],
) -> MetaResult<()> {
    let removed_receipt_error = |ino: &dyn std::fmt::Display| {
        MetaError::Storage(format!(
            "retained entry {ino} receipt cannot be removed from shared metadata; accepted local data and journal remain pending"
        ))
    };
    let mut removed = Vec::new();
    for event in events {
        match event {
            MetaEvent::RemoveXattr { ino, name } if name == RETAINED_RECEIPT_XATTR => {
                return Err(removed_receipt_error(ino));
            }
            MetaEvent::RemoveAttr { ino }
            | MetaEvent::RenameEntry {
                displaced_remove_attr_ino: Some(ino),
                ..
            } => removed.push(i64::try_from(*ino).map_err(|_| {
                MetaError::Storage(format!("inode {ino} exceeds the shared BIGINT range"))
            })?),
            _ => {}
        }
    }
    if removed.is_empty() {
        return Ok(());
    }
    // Rows admitted earlier in this same batch are visible to this query.
    let row = tx
        .query_opt(
            "SELECT receipt FROM trash_entry_authority WHERE vol=$1 AND receipt=ANY($2::BIGINT[]) LIMIT 1",
            &[&volume, &removed],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("read removed retained receipts", error))?;
    match row {
        Some(row) => Err(removed_receipt_error(&row.get::<_, i64>(0))),
        None => Ok(()),
    }
}

/// Losing record indices and stale expiries; see `losing_retained_terminal_records`.
type RetainedLosses = (Vec<usize>, Vec<(Ino, u64)>);

/// What one [`PgMetaEngine::run_retained_lifecycle_once`] sweep did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetainedLifecycleSweep {
    /// Retained entries that expired into a local `PurgeRequested` record.
    pub expired: usize,
    /// Acknowledged purges whose `data` tree was unlinked locally.
    pub settled: usize,
    /// Acknowledged purges deferred by an open handle or a concurrent change.
    pub deferred: usize,
    /// Deferred orphans (acknowledged retained child purges with no open
    /// handle) finalized by this sweep.
    pub orphans_finalized: usize,
    /// Deferred orphans whose finalize failed this sweep (left for the next one).
    pub orphan_finalize_errors: usize,
}

/// Orphans retried per sweep, so one sweep stays bounded; the rest wait for
/// the next interval.
const ORPHAN_FINALIZE_BATCH: usize = 1_024;

/// The poll loop runs the sweep at most this often; expiry is whole-day.
pub(super) const RETAINED_LIFECYCLE_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(60);

impl PgMetaEngine {
    /// Background trash lifecycle. Candidates come from the shared authority
    /// rows, never from scanning local xattrs. An expired `Retained` row
    /// becomes a local `PurgeRequested(Expired)` transition, published (and
    /// conflict-checked against an offline restore) like a manual purge. A
    /// `PurgeRequested` row settles once base holds that replayed record; the
    /// local unlink is then published normally and the janitor reclaims the
    /// objects after its grace window. Idempotent: every step re-derives its
    /// state from shared rows, base and the local namespace.
    pub fn run_retained_lifecycle_once(
        &self,
        now_unix_secs: u64,
    ) -> MetaResult<RetainedLifecycleSweep> {
        use crate::trash::MetaVolumeSettings;
        let mut sweep = RetainedLifecycleSweep::default();
        let settings = self.local.volume_settings()?;
        let policy = settings.trash;
        // The last positive period never resets, so zero means no receipt exists.
        if policy.existing_entry_days() == 0 {
            return Ok(sweep);
        }
        let command_pool = self.command_pool.clone();
        let command_timeout = self.command_timeout;
        let volume_id = self.volume_id;
        let rows = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "read retained lifecycle candidates",
                move |client| {
                    Box::pin(async move {
                        client
                            .query(
                                "SELECT receipt, record FROM trash_entry_authority WHERE vol=$1 ORDER BY receipt",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error("read retained lifecycle candidates", error)
                            })
                    })
                },
            )
            .await
        })?;
        // ponytail: every sweep reads all authority rows (tombstones are kept); add a settled marker if this grows.
        for row in rows {
            let receipt = Ino::try_from(row.get::<_, i64>(0))
                .map_err(|_| MetaError::Storage("invalid shared trash receipt".into()))?;
            let shared = RedbMetaEngine::decode_retained_entry(receipt, &row.get::<_, Vec<u8>>(1))?;
            match shared.state {
                RetainedEntryState::Retained => {
                    let local = self.local.retained_entry(receipt)?;
                    if local.is_some_and(|local| local.state == RetainedEntryState::Retained)
                        && policy.is_expired(shared.deleted_at_unix_secs, now_unix_secs)
                    {
                        self.mutate(|local| {
                            local.request_retained_purge_at(
                                receipt,
                                crate::trash::RetainedPurgeReason::Expired {
                                    settings_revision: settings.revision,
                                },
                                now_unix_secs,
                            )
                        })?;
                        sweep.expired += 1;
                    }
                }
                RetainedEntryState::PurgeRequested { .. } => {
                    let open = self
                        .open_handle_inodes
                        .lock()
                        .map_err(|_| MetaError::Storage("open handle registry poisoned".into()))?
                        .keys()
                        .copied()
                        .collect::<std::collections::HashSet<_>>();
                    // ponytail: a handle opened through the private receipt path after this
                    // snapshot is unordered with the unlink; item 4 blocks that traversal.
                    match self.mutate(|local| {
                        local.settle_retained_purge(receipt, |ino| open.contains(&ino))
                    })? {
                        crate::redb_engine::RetainedSettlement::Settled => sweep.settled += 1,
                        crate::redb_engine::RetainedSettlement::Busy => sweep.deferred += 1,
                        crate::redb_engine::RetainedSettlement::NotAcknowledged
                        | crate::redb_engine::RetainedSettlement::NothingToSettle => {}
                    }
                }
                RetainedEntryState::Restored { .. } => {}
            }
        }
        self.finalize_deferred_orphans_batch(now_unix_secs, ORPHAN_FINALIZE_BATCH, &mut sweep)?;
        Ok(sweep)
    }

    fn take_fair_orphan_batch(
        cursor: &AtomicU64,
        candidates: &[crate::engine::DeferredDeleteRecord],
        batch_limit: usize,
    ) -> Vec<crate::engine::DeferredDeleteRecord> {
        if candidates.is_empty() || batch_limit == 0 {
            return Vec::new();
        }
        let mut batch = Vec::with_capacity(candidates.len().min(batch_limit));
        let _ = cursor.try_update(Ordering::AcqRel, Ordering::Acquire, |last_visited| {
            let start = candidates.partition_point(|record| record.ino <= last_visited);
            let start = if start < candidates.len() { start } else { 0 };
            batch.clear();
            batch.extend(
                candidates[start..]
                    .iter()
                    .chain(candidates[..start].iter())
                    .take(batch_limit)
                    .cloned(),
            );
            batch.last().map(|record| record.ino)
        });
        batch
    }

    pub(super) fn finalize_deferred_orphans_batch(
        &self,
        now_unix_secs: u64,
        batch_limit: usize,
        sweep: &mut RetainedLifecycleSweep,
    ) -> MetaResult<usize> {
        // A file purge stays a deferred delete until shared authority settles
        // it; finalize once acknowledged unless a source handle is still open
        // (its last close finalizes instead).
        let open = self
            .open_handle_inodes
            .lock()
            .map_err(|_| MetaError::Storage("open handle registry poisoned".into()))?
            .keys()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let mut candidates: Vec<_> =
            crate::engine::MetaRecovery::list_deferred_deletes(&*self.local)?
                .into_iter()
                .filter(|record| !open.contains(&record.ino))
                .collect();
        if !candidates.windows(2).all(|pair| pair[0].ino < pair[1].ino) {
            candidates.sort_by_key(|record| record.ino);
        }
        let batch =
            Self::take_fair_orphan_batch(&self.retained_orphan_cursor, &candidates, batch_limit);
        let visited = batch.len();
        let timestamp = i64::try_from(now_unix_secs).unwrap_or(i64::MAX);
        // One journal scan per pass, extended by each transaction's delta.
        let mut unacked = crate::redb_engine::UnackedChildPurges::default();
        for record in batch {
            // A failed finalize aborts only its own transaction (nothing is
            // deleted); the record stays for the next sweep.
            match self.mutate_with_hint(
                |local| {
                    // Recheck under the mutation lock: a last close registers
                    // before its own finalize and stays registered through its
                    // deferred barrier.
                    if self
                        .open_handle_inodes
                        .lock()
                        .map_err(|_| MetaError::Storage("open handle registry poisoned".into()))?
                        .contains_key(&record.ino)
                    {
                        return Ok(crate::engine::OrphanInodeFinalization::Deferred);
                    }
                    local.finalize_orphan_inode_guarded(record.ino, timestamp, Some(&mut unacked))
                },
                |_| PendingMutationHint::Unknown,
            ) {
                Ok(crate::engine::OrphanInodeFinalization::Removed { .. }) => {
                    sweep.orphans_finalized += 1;
                }
                Ok(_) => {}
                Err(error) => {
                    sweep.orphan_finalize_errors += 1;
                    tracing::warn!(ino = record.ino, %error, "deferred orphan finalize failed; retrying next sweep");
                }
            }
        }
        Ok(visited)
    }

    /// Local terminal receipt records that lost: a different shared terminal
    /// outcome, or an expiry judged under superseded shared settings. Only these
    /// records lose; the rest of the accepted local journal stays publishable and
    /// the shared row is kept. Stale expiries also return `(receipt, settings_revision)`,
    /// so the caller can discard their local overlay.
    ///
    /// `ranges` maps `events` (the selected, still-unpublished journal) back to
    /// journal sequences, so a losing child purge retires exactly its own span.
    pub(super) fn losing_retained_terminal_records(
        &self,
        events: &[MetaEvent],
        ranges: &[LocalEventRange],
    ) -> MetaResult<RetainedLosses> {
        enum Candidate {
            Terminal {
                index: usize,
                entry: crate::trash::RetainedEntry,
            },
            ChildPurge {
                start: usize,
                end: usize,
                entry: crate::trash::RetainedEntry,
            },
        }
        impl Candidate {
            fn entry(&self) -> &crate::trash::RetainedEntry {
                match self {
                    Self::Terminal { entry, .. } | Self::ChildPurge { entry, .. } => entry,
                }
            }
        }

        let mut candidates = Vec::new();
        for (index, event) in events.iter().enumerate() {
            if let MetaEvent::SetXattr {
                ino, name, value, ..
            } = event
                && name == RETAINED_RECEIPT_XATTR
            {
                let entry = RedbMetaEngine::decode_retained_entry(*ino, value)?;
                if entry.state != RetainedEntryState::Retained {
                    candidates.push(Candidate::Terminal { index, entry });
                } else if let Some(span) = RedbMetaEngine::child_purge_event_span(events, index) {
                    candidates.push(Candidate::ChildPurge {
                        start: index,
                        end: span.end,
                        entry,
                    });
                }
            }
        }
        if candidates.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let shared_revision = self.shared_settings_revision()?;
        let mut receipts = candidates
            .iter()
            .map(|candidate| {
                i64::try_from(candidate.entry().receipt).map_err(|_| {
                    MetaError::Storage("trash receipt exceeds shared inode range".into())
                })
            })
            .collect::<MetaResult<Vec<_>>>()?;
        receipts.sort_unstable();
        receipts.dedup();
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let rows = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "read retained authority conflicts",
                move |client| {
                    Box::pin(async move {
                        client
                            .query(
                                "SELECT receipt, record FROM trash_entry_authority WHERE vol=$1 AND receipt=ANY($2::BIGINT[])",
                                &[&volume_id, &receipts],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error("read retained authority conflicts", error)
                            })
                    })
                },
            )
            .await
        })?;
        let mut shared = std::collections::HashMap::new();
        for row in rows {
            let receipt = Ino::try_from(row.get::<_, i64>(0))
                .map_err(|_| MetaError::Storage("invalid shared trash receipt".into()))?;
            shared.insert(
                receipt,
                RedbMetaEngine::decode_retained_entry(receipt, &row.get::<_, Vec<u8>>(1))?,
            );
        }
        let mut losing_set = std::collections::BTreeSet::new();
        let mut stale_expiries = Vec::new();
        // Exact journal start of every losing child purge, per receipt. A span
        // journals atomically, so a split sequence mapping is not this span.
        let mut losing_child_purges: HashMap<Ino, std::collections::BTreeSet<u64>> = HashMap::new();
        for candidate in &candidates {
            if let Candidate::ChildPurge { start, end, entry } = candidate
                && shared
                    .get(&entry.receipt)
                    .is_some_and(|current| current.state != RetainedEntryState::Retained)
                && let (Some(first), Some(last)) = (
                    Self::selected_local_event_seq(ranges, *start),
                    Self::selected_local_event_seq(ranges, *end),
                )
                && last.checked_sub(first) == u64::try_from(end - start).ok()
            {
                losing_child_purges
                    .entry(entry.receipt)
                    .or_default()
                    .insert(first);
            }
        }
        let mut reconciled_child_purge_receipts = std::collections::HashSet::new();
        let needs_content_epochs = candidates.iter().any(|candidate| {
            matches!(candidate, Candidate::Terminal { .. })
                && shared
                    .get(&candidate.entry().receipt)
                    .is_some_and(|current| {
                        current != candidate.entry()
                            && matches!(current.state, RetainedEntryState::Restored { .. })
                    })
        });
        let shared_content_epochs = if needs_content_epochs {
            self.shared_retained_content_epochs(events)?
        } else {
            HashMap::new()
        };
        for candidate in candidates {
            match candidate {
                Candidate::Terminal { index, entry } => {
                    let Some(current) = shared.get(&entry.receipt) else {
                        continue;
                    };
                    if current.state != RetainedEntryState::Retained {
                        if current != &entry {
                            self.local.reconcile_losing_retained_restore(
                                &entry,
                                current,
                                self.flushed_event_seq.load(Ordering::SeqCst),
                                &shared_content_epochs,
                            )?;
                            losing_set.insert(index);
                        }
                    } else if let RetainedEntryState::PurgeRequested {
                        reason: RetainedPurgeReason::Expired { settings_revision },
                    } = entry.state
                        && settings_revision != shared_revision
                    {
                        losing_set.insert(index);
                        stale_expiries.push((entry.receipt, settings_revision));
                    }
                }
                Candidate::ChildPurge { start, end, entry } => {
                    let Some(current) = shared.get(&entry.receipt) else {
                        continue;
                    };
                    let Some(starts) = losing_child_purges.get(&entry.receipt) else {
                        continue;
                    };
                    if !starts.contains(
                        &Self::selected_local_event_seq(ranges, start).unwrap_or_default(),
                    ) {
                        continue;
                    }
                    if reconciled_child_purge_receipts.insert(entry.receipt) {
                        self.local.reconcile_losing_retained_child_purges(
                            entry.receipt,
                            current,
                            self.flushed_event_seq.load(Ordering::SeqCst),
                            &|seq| starts.contains(&seq),
                        )?;
                    }
                    losing_set.extend(start..=end);
                }
            }
        }
        Ok((losing_set.into_iter().collect(), stale_expiries))
    }

    /// Read epoch bounds at one validated checkpoint/head cut without changing local serving state.
    fn shared_retained_content_epochs(
        &self,
        events: &[MetaEvent],
    ) -> MetaResult<HashMap<Ino, u64>> {
        let inodes: std::collections::HashSet<_> = events
            .iter()
            .filter_map(|event| match event {
                MetaEvent::SetAttr { attr } if attr.kind == FileType::RegularFile => Some(attr.ino),
                _ => None,
            })
            .collect();
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let max_batches = self.sync_max_batches_per_page;
        let max_bytes = self.sync_max_response_bytes_per_page;
        self.run_async(async move {
            let (descriptor, checkpoint) =
                super::metadata_io::read_bootstrap_checkpoint_and_decode_with_pool(
                    command_pool.clone(),
                    volume_id,
                    command_timeout,
                )
                .await?;
            let mut epochs = HashMap::<Ino, u64>::new();
            for (ino, bytes) in checkpoint.inodes {
                if inodes.contains(&ino) {
                    let attr: InodeAttr = postcard::from_bytes(&bytes)
                        .map_err(|error| MetaError::Serialization(error.to_string()))?;
                    epochs.insert(ino, attr.content_epoch);
                }
            }
            let mut cursor = descriptor.checkpoint_version;
            while cursor < descriptor.remote_head_version {
                let head = descriptor.remote_head_version;
                let page = Self::with_postgres_client_timeout(
                    command_pool.clone(),
                    command_timeout,
                    "read retained winner content epochs",
                    move |client| {
                        Box::pin(async move {
                            Self::query_foreign_event_page(
                                client,
                                volume_id,
                                cursor,
                                head,
                                max_batches,
                                max_bytes,
                            )
                            .await
                        })
                    },
                )
                .await?;
                let batches = super::RemoteApplyCoordinator::decode_event_rows(page.rows, "")?;
                if batches.is_empty() {
                    return Err(MetaError::ReplayGap {
                        local_version: cursor,
                        first_available_version: cursor,
                        target_version: head,
                    });
                }
                super::RemoteApplyCoordinator::validate_decoded_batch_continuity(
                    cursor, &batches, head,
                )?;
                for batch in batches {
                    cursor = batch.batch_version;
                    for event in batch.events {
                        if let MetaEvent::SetAttr { attr } = event
                            && inodes.contains(&attr.ino)
                        {
                            let epoch = epochs.entry(attr.ino).or_default();
                            *epoch = (*epoch).max(attr.content_epoch);
                        }
                    }
                }
            }
            Ok(epochs)
        })
    }

    /// The shared settings revision, read outside the publication transaction.
    fn shared_settings_revision(&self) -> MetaResult<u64> {
        let command_pool = self.command_pool.clone();
        let command_timeout = self.command_timeout;
        let volume_id = self.volume_id;
        let bytes = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "read shared retention period",
                move |client| {
                    Box::pin(async move {
                        client
                            .query_one(
                                "SELECT volume_settings FROM metadata_state WHERE vol=$1",
                                &[&volume_id],
                            )
                            .await
                            .map(|row| row.get::<_, Option<Vec<u8>>>(0))
                            .map_err(|error| {
                                Self::classify_pg_error("read shared retention period", error)
                            })
                    })
                },
            )
            .await
        })?;
        Ok(shared_settings(bytes.as_deref())?.revision)
    }
}

/// Indices of local terminal receipt records that provably lost: base already
/// holds a different terminal record for the same receipt. The only shared
/// writer (`apply_retained_authority`) commits that winner with its own
/// publication, so base replays it before any later self batch is matched,
/// and no successful self batch can carry the loser. A rejected or still
/// unclassified record has no such proof and stays pending.
pub(super) fn lost_terminal_records_in_txn(
    txn: &crate::redb_engine::RedbWriteTransaction<'_>,
    events: &[MetaEvent],
) -> MetaResult<std::collections::BTreeSet<usize>> {
    let terminal = |ino, bytes: &[u8]| {
        RedbMetaEngine::decode_retained_entry(ino, bytes)
            .ok()
            .filter(|entry| entry.state != RetainedEntryState::Retained)
    };
    let mut lost = std::collections::BTreeSet::new();
    for (index, event) in events.iter().enumerate() {
        let MetaEvent::SetXattr {
            ino, name, value, ..
        } = event
        else {
            continue;
        };
        if name != RETAINED_RECEIPT_XATTR {
            continue;
        }
        if let Some(local) = terminal(*ino, value) {
            if RedbMetaEngine::base_retained_record_in_txn(txn, *ino)?
                .and_then(|bytes| terminal(*ino, &bytes))
                .is_some_and(|winner| winner != local)
            {
                lost.insert(index);
            }
        } else if let Some(span) = RedbMetaEngine::child_purge_event_span(events, index)
            && RedbMetaEngine::base_retained_record_in_txn(txn, span.receipt)?
                .and_then(|bytes| terminal(span.receipt, &bytes))
                .is_some()
        {
            for pos in index..=span.end {
                lost.insert(pos);
            }
        }
    }
    Ok(lost)
}
