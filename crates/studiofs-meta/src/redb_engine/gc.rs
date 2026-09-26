use super::*;
use redb::CompactionError;

impl RedbMetaEngine {
    fn record_local_event_log_prune(
        &self,
        result: &MetaResult<LocalEventLogPruneBatch>,
        elapsed: std::time::Duration,
    ) {
        let mut stats = self
            .local_event_log_prune
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        stats.last_duration_ns = Some(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
        match result {
            Ok(batch) => {
                let rows = u64::try_from(batch.rows).unwrap_or(u64::MAX);
                stats.rows_pruned = stats.rows_pruned.saturating_add(rows);
                stats.payload_bytes_pruned = stats
                    .payload_bytes_pruned
                    .saturating_add(batch.payload_bytes);
                stats.last_rows = Some(rows);
                if batch.rows > 0 {
                    stats.last_first_seq = batch.first_seq;
                    stats.last_last_seq = batch.last_seq;
                }
                stats.last_payload_bytes = Some(batch.payload_bytes);
            }
            Err(_) => {
                stats.last_rows = None;
                stats.last_payload_bytes = None;
            }
        }
    }

    pub(crate) fn prune_local_event_log_before(
        &self,
        exclusive_floor: u64,
        max_rows: usize,
    ) -> MetaResult<LocalEventLogPruneBatch> {
        let started = Instant::now();
        let result = (|| {
            if max_rows == 0
                || (exclusive_floor <= 1
                    && self
                        .get_counter(crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ)?
                        == 0)
            {
                return Ok(LocalEventLogPruneBatch::default());
            }

            let txn = self.begin_write_txn()?;
            let batch = Self::prune_local_event_log_batch_in_txn(&txn, exclusive_floor, max_rows)?;
            if batch.rows > 0 || batch.floor_updated {
                txn.commit()
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
            }
            Ok(batch)
        })();
        self.record_local_event_log_prune(&result, started.elapsed());
        result
    }

    pub(crate) fn prune_local_event_log_batch_in_txn(
        txn: &redb::WriteTransaction,
        exclusive_floor: u64,
        max_rows: usize,
    ) -> MetaResult<LocalEventLogPruneBatch> {
        let mut batch = LocalEventLogPruneBatch::default();
        if max_rows == 0 {
            return Ok(batch);
        }

        let mut counters = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let persisted_floor = counters
            .get(crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .map(|value| value.value());
        // The persisted floor is already proven safe, even if a caller's cursor is stale.
        let exclusive_floor = exclusive_floor.max(persisted_floor.unwrap_or(0).saturating_add(1));
        if exclusive_floor <= 1 {
            return Ok(batch);
        }
        let mut pruned_through = persisted_floor.unwrap_or(0);
        let mut events = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let first_seq = events
            .first()
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .map(|(seq, _)| seq.value());

        if persisted_floor.is_none() {
            if let Some(first_seq) = first_seq.filter(|seq| *seq <= exclusive_floor) {
                pruned_through = first_seq.saturating_sub(1);
                batch.floor_updated = pruned_through > 0;
            } else if first_seq.is_none() {
                pruned_through = exclusive_floor - 1;
                batch.floor_updated = pruned_through > 0;
            }
        }

        while batch.rows < max_rows {
            let next = events
                .first()
                .map_err(|error| MetaError::Storage(error.to_string()))?
                .map(|(seq, _)| seq.value());
            let Some(seq) = next else {
                break;
            };
            if seq >= exclusive_floor {
                break;
            }

            let Some((removed_seq, payload)) = events
                .pop_first()
                .map_err(|error| MetaError::Storage(error.to_string()))?
            else {
                break;
            };
            let removed_seq = removed_seq.value();
            batch.first_seq.get_or_insert(removed_seq);
            batch.last_seq = Some(removed_seq);
            batch.rows += 1;
            batch.payload_bytes = batch
                .payload_bytes
                .saturating_add(u64::try_from(payload.value().len()).unwrap_or(u64::MAX));
            pruned_through = removed_seq;
            batch.floor_updated = true;
        }

        let remaining_first_seq = events
            .first()
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .map(|(seq, _)| seq.value());
        if remaining_first_seq.is_none_or(|seq| seq >= exclusive_floor) {
            let acknowledged_through = exclusive_floor - 1;
            if pruned_through < acknowledged_through {
                pruned_through = acknowledged_through;
                batch.floor_updated = true;
            }
        }

        if batch.floor_updated {
            counters
                .insert(
                    crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ,
                    pruned_through,
                )
                .map_err(|error| MetaError::Storage(error.to_string()))?;
        }
        Ok(batch)
    }

    pub(super) fn add_pending_deletions_in_txn(
        txn: &redb::WriteTransaction,
        entries: &[(u64, i64)],
    ) -> MetaResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut pending_deletions = txn
            .open_table(PENDING_DELETIONS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for &(object_id, timestamp) in entries {
            pending_deletions
                .insert(object_id, timestamp)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        drop(pending_deletions);
        for &(object_id, timestamp) in entries {
            Self::append_event_in_txn(
                txn,
                &crate::events::MetaEvent::AddPendingDeletion {
                    object_id,
                    timestamp,
                },
            )?;
        }
        Ok(())
    }

    fn physical_file_size_bytes(&self) -> Option<u64> {
        std::fs::metadata(&self.metadata_path)
            .ok()
            .map(|metadata| metadata.len())
    }

    fn record_local_storage_compaction(
        &self,
        result: &MetaResult<LocalStorageCompactionResult>,
        skip_reason: Option<LocalStorageCompactionSkipReason>,
        elapsed: std::time::Duration,
        bytes_before: Option<u64>,
        bytes_after: Option<u64>,
    ) {
        let mut diagnostics = self
            .local_storage_compaction
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        diagnostics.attempts = diagnostics.attempts.saturating_add(1);
        diagnostics.last_result = result.as_ref().ok().copied();
        diagnostics.last_error = result.is_err();
        diagnostics.last_skip_reason = skip_reason;
        diagnostics.last_duration_ns = Some(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));

        match result {
            Ok(LocalStorageCompactionResult::Compacted) => {
                diagnostics.compacted = diagnostics.compacted.saturating_add(1);
                diagnostics.last_success_bytes_before = bytes_before;
                diagnostics.last_success_bytes_after = bytes_after;
            }
            Ok(LocalStorageCompactionResult::NoWork) => {
                diagnostics.no_work = diagnostics.no_work.saturating_add(1);
            }
            Ok(LocalStorageCompactionResult::SkippedBusy) => {
                diagnostics.skipped_busy = diagnostics.skipped_busy.saturating_add(1);
            }
            Err(_) => {
                diagnostics.errors = diagnostics.errors.saturating_add(1);
            }
        }
    }

    fn read_event_log_diagnostics(
        transaction: &redb::ReadTransaction,
        diagnostics: &mut LocalStorageDiagnostics,
    ) {
        let pruned_through_seq = Self::read_counter_from_read_txn(
            transaction,
            crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ,
        )
        .unwrap_or(0);
        diagnostics.local_event_log_pruned_through_seq =
            (pruned_through_seq > 0).then_some(pruned_through_seq);
        let Ok(events) = transaction.open_table(EVENT_LOG_TABLE) else {
            return;
        };
        let Ok(first) = events.first() else {
            return;
        };
        let first = first.map(|(seq, _)| seq.value());
        let Ok(last) = events.last() else {
            return;
        };
        let last = last.map(|(seq, _)| seq.value());
        let next =
            Self::read_counter_from_read_txn(transaction, crate::schema::counters::NEXT_EVENT_SEQ)
                .unwrap_or(0)
                .max(1);
        let Some(minimum_next) = last.map_or(Some(1), |last| last.checked_add(1)) else {
            diagnostics.first_local_event_seq = first;
            return;
        };
        let next = next.max(minimum_next);
        diagnostics.first_local_event_seq = first;
        diagnostics.next_local_event_seq = Some(next);
        diagnostics.retained_local_event_span =
            Some(first.map_or(0, |first| next.saturating_sub(first)));
    }

    fn cached_local_storage_page_stats(&self) -> Option<CachedLocalStoragePageStats> {
        *self
            .local_storage_page_stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn local_storage_page_stats_best_effort(&self) -> Option<LocalStoragePageStats> {
        self.local_storage_page_stats_best_effort_at(Instant::now())
    }

    pub(super) fn local_storage_page_stats_best_effort_at(
        &self,
        now: Instant,
    ) -> Option<LocalStoragePageStats> {
        let db = self.db.as_ref()?;
        let is_expired = |cached: Option<CachedLocalStoragePageStats>| {
            cached.is_none_or(|cached| {
                now.saturating_duration_since(cached.sampled_at)
                    >= LOCAL_STORAGE_PAGE_STATS_REFRESH_INTERVAL
            })
        };
        let cached = self.cached_local_storage_page_stats();
        if !is_expired(cached) {
            return cached.map(|cached| cached.stats);
        }

        let _refresh_guard = match self.local_storage_page_stats_refresh.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                return self
                    .cached_local_storage_page_stats()
                    .map(|cached| cached.stats);
            }
        };
        let cached = self.cached_local_storage_page_stats();
        if !is_expired(cached) {
            return cached.map(|cached| cached.stats);
        }

        let stats = if let Ok(_write_txn_guard) = self.write_txn_lock.try_lock()
            && let Ok(db) = db.try_read()
            && let Ok(transaction) = db.begin_write()
        {
            // redb 4.3 exposes page stats only on WriteTransaction; reuse this scan for five seconds.
            let stats = transaction.stats().ok().map(|stats| LocalStoragePageStats {
                allocated_pages: stats.allocated_pages(),
                leaf_pages: stats.leaf_pages(),
                branch_pages: stats.branch_pages(),
                tree_height: stats.tree_height(),
                stored_bytes: stats.stored_bytes(),
                metadata_bytes: stats.metadata_bytes(),
                fragmented_bytes: stats.fragmented_bytes(),
                page_size_bytes: u64::try_from(stats.page_size()).unwrap_or(u64::MAX),
            });
            let _ = transaction.abort();
            stats
        } else {
            None
        };
        if let Some(stats) = stats {
            *self
                .local_storage_page_stats
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(CachedLocalStoragePageStats {
                sampled_at: now,
                stats,
            });
            Some(stats)
        } else {
            cached.map(|cached| cached.stats)
        }
    }
}

impl MetaGc for RedbMetaEngine {
    fn reconcile_pending_regular_file_recoveries(&self) -> MetaResult<()> {
        RedbMetaEngine::reconcile_pending_regular_file_recoveries(self)
    }

    fn compact_local_storage(&self) -> MetaResult<LocalStorageCompactionResult> {
        let started = std::time::Instant::now();
        let mut bytes_before = None;
        let mut bytes_after = None;
        let mut skip_reason = None;
        let result = (|| {
            let Some(db) = self.db.as_ref() else {
                return Ok(LocalStorageCompactionResult::NoWork);
            };
            let mut db = match db.try_write() {
                Ok(db) => db,
                Err(std::sync::TryLockError::WouldBlock) => {
                    skip_reason = Some(LocalStorageCompactionSkipReason::DatabaseLockContended);
                    return Ok(LocalStorageCompactionResult::SkippedBusy);
                }
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(MetaError::Storage("redb lock poisoned".to_string()));
                }
            };
            bytes_before = self.physical_file_size_bytes();
            match db.compact() {
                Ok(true) => {
                    bytes_after = self.physical_file_size_bytes();
                    Ok(LocalStorageCompactionResult::Compacted)
                }
                Ok(false) => Ok(LocalStorageCompactionResult::NoWork),
                // ReadTransaction outlives our short RwLock read guard; redb
                // reports active transactions here.
                Err(CompactionError::TransactionInProgress) => {
                    skip_reason = Some(LocalStorageCompactionSkipReason::ActiveTransaction);
                    Ok(LocalStorageCompactionResult::SkippedBusy)
                }
                Err(error) => Err(MetaError::Storage(format!(
                    "failed to compact local redb: {error}"
                ))),
            }
        })();
        if matches!(&result, Ok(LocalStorageCompactionResult::Compacted)) {
            *self
                .local_storage_page_stats
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = None;
        }
        self.record_local_storage_compaction(
            &result,
            skip_reason,
            started.elapsed(),
            bytes_before,
            bytes_after,
        );
        result
    }

    fn local_storage_diagnostics(&self) -> MetaResult<Option<LocalStorageDiagnostics>> {
        let compaction = *self
            .local_storage_compaction
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let prune = *self
            .local_event_log_prune
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut diagnostics = LocalStorageDiagnostics {
            local_event_log_pruned_rows_total: prune.rows_pruned,
            local_event_log_pruned_payload_bytes_total: prune.payload_bytes_pruned,
            last_local_event_log_prune_rows: prune.last_rows,
            last_local_event_log_prune_first_seq: prune.last_first_seq,
            last_local_event_log_prune_last_seq: prune.last_last_seq,
            last_local_event_log_prune_payload_bytes: prune.last_payload_bytes,
            last_local_event_log_prune_duration_ns: prune.last_duration_ns,
            compaction_attempts: compaction.attempts,
            compaction_compacted: compaction.compacted,
            compaction_no_work: compaction.no_work,
            compaction_skipped_busy: compaction.skipped_busy,
            compaction_errors: compaction.errors,
            last_compaction_result: compaction.last_result,
            last_compaction_error: compaction.last_error,
            last_compaction_skip_reason: compaction.last_skip_reason,
            last_compaction_duration_ns: compaction.last_duration_ns,
            last_success_bytes_before: compaction.last_success_bytes_before,
            last_success_bytes_after: compaction.last_success_bytes_after,
            ..LocalStorageDiagnostics::default()
        };
        diagnostics.physical_file_bytes = self.physical_file_size_bytes();

        {
            let read_transaction = if let Some(db) = self.db.as_ref() {
                db.try_read().ok().and_then(|db| db.begin_read().ok())
            } else {
                self.read_only_db
                    .as_ref()
                    .and_then(|db| db.begin_read().ok())
            };
            if let Some(transaction) = read_transaction {
                Self::read_event_log_diagnostics(&transaction, &mut diagnostics);
            }
        }

        if let Some(stats) = self.local_storage_page_stats_best_effort() {
            diagnostics.allocated_pages = Some(stats.allocated_pages);
            diagnostics.leaf_pages = Some(stats.leaf_pages);
            diagnostics.branch_pages = Some(stats.branch_pages);
            diagnostics.tree_height = Some(stats.tree_height);
            diagnostics.stored_bytes = Some(stats.stored_bytes);
            diagnostics.metadata_bytes = Some(stats.metadata_bytes);
            diagnostics.fragmented_bytes = Some(stats.fragmented_bytes);
            diagnostics.page_size_bytes = Some(stats.page_size_bytes);
        }

        Ok(Some(diagnostics))
    }

    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()> {
        self.add_pending_deletions(&[(object_id, timestamp)])
    }

    fn add_pending_deletions(&self, entries: &[(u64, i64)]) -> MetaResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let txn = self.begin_write_txn()?;
        Self::add_pending_deletions_in_txn(&txn, entries)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        {
            let mut table = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .remove(object_id)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemovePendingDeletion { object_id },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>> {
        self.get_pending_deletions_limited(usize::MAX)
    }

    fn get_pending_deletions_limited(&self, limit: usize) -> MetaResult<Vec<(u64, i64)>> {
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(PENDING_DELETIONS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let deletions: Vec<(u64, i64)> = table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .take(limit)
            .filter_map(|r| r.ok())
            .map(|(k, v)| (k.value(), v.value()))
            .collect();

        Ok(deletions)
    }

    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>> {
        let txn = self.begin_read_txn()?;
        let mut refs = HashSet::new();
        Self::regular_file_graph_object_ids_from_table(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            &mut refs,
        )?;
        Self::regular_file_graph_object_ids_from_table(&txn, REGULAR_FILE_BASE_TABLE, &mut refs)?;
        Self::pending_recovery_object_ids_from_read_txn(&txn, &mut refs)?;
        Ok(refs)
    }
}
