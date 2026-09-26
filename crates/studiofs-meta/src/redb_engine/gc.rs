use super::*;
use redb::CompactionError;

impl RedbMetaEngine {
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
        let mut diagnostics = LocalStorageDiagnostics {
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
        Ok(refs)
    }
}
