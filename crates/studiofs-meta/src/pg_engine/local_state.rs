use super::{
    LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, LOCAL_PG_NAMESPACE_VERSION_COUNTER,
    LOCAL_PG_VERSION_COUNTER, LocalEventProgress, LocalEventRange, PendingReplicationState,
    PgMetaEngine, StartupSnapshotImportMode,
};
use crate::engine::{MetaError, MetaInodes, MetaResult};
use crate::redb_engine::RedbMetaEngine;
use std::sync::atomic::Ordering;

fn merge_local_event_ranges(ranges: &mut Vec<LocalEventRange>) {
    if ranges.len() < 2 {
        return;
    }
    ranges.sort_by_key(|range| (range.start, range.end));
    let mut merged: Vec<LocalEventRange> = Vec::with_capacity(ranges.len());
    for range in ranges.drain(..) {
        if let Some(previous) = merged.last_mut()
            && range.start <= previous.end
        {
            previous.end = previous.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    *ranges = merged;
}

fn trim_local_event_ranges_before(ranges: &mut Vec<LocalEventRange>, cursor: u64) {
    ranges.retain(|range| range.end > cursor);
    for range in ranges.iter_mut() {
        range.start = range.start.max(cursor);
    }
}

impl PgMetaEngine {
    /// Acquire the local replay position and pending mutation state together.
    ///
    /// When an operation needs both locks, it must acquire
    /// `local_position_lock` before `pending_state` and retain both until the
    /// coupled local cursor/serving-state transition is complete.
    pub(super) fn lock_local_position_and_pending_state(
        &self,
    ) -> MetaResult<(
        std::sync::MutexGuard<'_, ()>,
        std::sync::MutexGuard<'_, PendingReplicationState>,
    )> {
        let position_guard = self
            .local_position_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg local position lock poisoned".to_string()))?;
        #[cfg(test)]
        self.run_catch_up_fault_hook(super::CatchUpFaultPoint::AfterLocalPositionLockAcquired)?;
        let pending_state = self
            .pending_state
            .lock()
            .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
        Ok((position_guard, pending_state))
    }

    pub(super) fn load_persisted_local_version(local: &RedbMetaEngine) -> MetaResult<i64> {
        let version = local.get_counter(LOCAL_PG_VERSION_COUNTER)?;
        Ok(version.min(i64::MAX as u64) as i64)
    }

    pub(super) fn load_persisted_namespace_version(local: &RedbMetaEngine) -> MetaResult<i64> {
        let version = local.get_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER)?;
        Ok(version.min(i64::MAX as u64) as i64)
    }

    #[cfg(test)]
    pub(super) fn persist_local_version(&self, version: i64) -> MetaResult<()> {
        let _guard = self
            .local_position_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg local position lock poisoned".to_string()))?;
        self.persist_local_version_locked(version)
    }

    /// Persist the version while the caller holds `local_position_lock`.
    pub(super) fn persist_local_version_locked(&self, version: i64) -> MetaResult<()> {
        if version < 0 {
            return Ok(());
        }
        if version <= self.local_version.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.local
            .set_counter(LOCAL_PG_VERSION_COUNTER, version as u64)?;
        self.local_version.store(version, Ordering::SeqCst);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn persist_namespace_version(&self, version: i64) -> MetaResult<()> {
        if version < 0 {
            return Ok(());
        }
        let _guard = self
            .local_position_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg local position lock poisoned".to_string()))?;
        if version <= self.namespace_version.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.local
            .set_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER, version as u64)?;
        self.namespace_version.store(version, Ordering::SeqCst);
        Ok(())
    }

    pub(super) fn load_persisted_flushed_event_seq(local: &RedbMetaEngine) -> MetaResult<u64> {
        local.get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER)
    }

    #[cfg(test)]
    pub(super) fn persist_flushed_event_seq(&self, seq: u64) -> MetaResult<()> {
        let _guard = self
            .local_position_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg local position lock poisoned".to_string()))?;
        self.persist_flushed_event_seq_locked(seq)
    }

    pub(super) fn persist_flushed_event_seq_locked(&self, seq: u64) -> MetaResult<()> {
        if seq <= self.flushed_event_seq.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.local
            .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, seq)?;
        self.flushed_event_seq.store(seq, Ordering::SeqCst);
        Ok(())
    }

    pub(super) fn reject_local_event_ranges(&self, ranges: &[LocalEventRange]) -> MetaResult<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        let (_position_guard, _pending_state) = self.lock_local_position_and_pending_state()?;
        let cursor = self.flushed_event_seq.load(Ordering::SeqCst).max(1);
        let mut progress = self
            .local_event_progress
            .lock()
            .map_err(|_| MetaError::Storage("pg local event progress lock poisoned".to_string()))?;
        for range in ranges {
            let start = range.start.max(cursor);
            if start < range.end {
                progress.rejected.push(LocalEventRange {
                    start,
                    end: range.end,
                });
            }
        }
        merge_local_event_ranges(&mut progress.rejected);
        Ok(())
    }

    pub(super) fn mark_local_event_ranges_published(
        &self,
        ranges: &[LocalEventRange],
    ) -> MetaResult<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        let (_position_guard, _pending_state) = self.lock_local_position_and_pending_state()?;
        let mut progress = self
            .local_event_progress
            .lock()
            .map_err(|_| MetaError::Storage("pg local event progress lock poisoned".to_string()))?;
        let cursor = self.flushed_event_seq.load(Ordering::SeqCst).max(1);
        for range in ranges {
            let start = range.start.max(cursor);
            if start < range.end {
                progress.published.push(LocalEventRange {
                    start,
                    end: range.end,
                });
            }
        }
        merge_local_event_ranges(&mut progress.published);
        Ok(())
    }

    pub(super) fn complete_local_event_ranges(&self, ranges: &[LocalEventRange]) -> MetaResult<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        let (_position_guard, _pending_state) = self.lock_local_position_and_pending_state()?;
        self.complete_local_event_ranges_locked(ranges)
    }

    /// Complete ranges while the caller holds the local-position and pending-state locks.
    pub(super) fn complete_local_event_ranges_locked(
        &self,
        ranges: &[LocalEventRange],
    ) -> MetaResult<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        let mut progress = self
            .local_event_progress
            .lock()
            .map_err(|_| MetaError::Storage("pg local event progress lock poisoned".to_string()))?;
        let cursor = self.flushed_event_seq.load(Ordering::SeqCst).max(1);
        // Commit the in-memory ledger only after the matching durable cursor write succeeds.
        let mut completed_ranges = progress.completed.clone();
        for range in ranges {
            let start = range.start.max(cursor);
            if start < range.end {
                completed_ranges.push(LocalEventRange {
                    start,
                    end: range.end,
                });
            }
        }
        merge_local_event_ranges(&mut completed_ranges);

        let current = self.flushed_event_seq.load(Ordering::SeqCst);
        let mut next = current.max(1);
        let mut advanced = false;
        loop {
            if progress
                .rejected
                .iter()
                .any(|range| range.start <= next && next < range.end)
            {
                break;
            }
            let Some(completed) = completed_ranges
                .iter()
                .find(|range| range.start <= next && next < range.end)
            else {
                break;
            };
            next = completed.end;
            advanced = true;
        }
        if advanced && next > current {
            self.persist_flushed_event_seq_locked(next)?;
            trim_local_event_ranges_before(&mut completed_ranges, next);
            trim_local_event_ranges_before(&mut progress.rejected, next);
            trim_local_event_ranges_before(&mut progress.published, next);
        }
        progress.completed = completed_ranges;
        Ok(())
    }

    pub(super) fn trim_local_event_progress_before(&self, cursor: u64) -> MetaResult<()> {
        let mut progress = self
            .local_event_progress
            .lock()
            .map_err(|_| MetaError::Storage("pg local event progress lock poisoned".to_string()))?;
        trim_local_event_ranges_before(&mut progress.rejected, cursor);
        trim_local_event_ranges_before(&mut progress.completed, cursor);
        trim_local_event_ranges_before(&mut progress.published, cursor);
        Ok(())
    }

    pub(super) fn unprocessed_local_event_progress(&self) -> MetaResult<LocalEventProgress> {
        self.local_event_progress
            .lock()
            .map(|progress| progress.clone())
            .map_err(|_| MetaError::Storage("pg local event progress lock poisoned".to_string()))
    }

    #[cfg(test)]
    pub(super) fn persist_replay_position(
        &self,
        outcome: &super::RemoteReplayApplyOutcome,
    ) -> MetaResult<()> {
        if outcome.max_version > self.local_version.load(Ordering::SeqCst) {
            self.persist_local_version(outcome.max_version)?;
        }
        if outcome.max_namespace_version > self.namespace_version.load(Ordering::SeqCst) {
            self.persist_namespace_version(outcome.max_namespace_version)?;
        }
        if let Some(flushed_event_seq) = outcome.flushed_event_seq {
            self.persist_flushed_event_seq(flushed_event_seq)?;
        }
        Ok(())
    }

    pub(super) fn write_replay_position_in_txn(
        txn: &redb::WriteTransaction,
        outcome: &super::RemoteReplayApplyOutcome,
    ) -> MetaResult<()> {
        let positions = [
            (
                LOCAL_PG_VERSION_COUNTER,
                (outcome.max_version >= 0).then_some(outcome.max_version as u64),
            ),
            (
                LOCAL_PG_NAMESPACE_VERSION_COUNTER,
                (outcome.max_namespace_version >= 0)
                    .then_some(outcome.max_namespace_version as u64),
            ),
            (
                LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER,
                outcome.flushed_event_seq,
            ),
        ];
        RedbMetaEngine::update_counters_in_txn(txn, &positions)
    }

    pub(super) fn publish_replay_position(&self, outcome: &super::RemoteReplayApplyOutcome) {
        if outcome.max_version >= 0 {
            self.local_version
                .fetch_max(outcome.max_version, Ordering::SeqCst);
        }
        if outcome.max_namespace_version >= 0 {
            self.namespace_version
                .fetch_max(outcome.max_namespace_version, Ordering::SeqCst);
        }
        if let Some(flushed_event_seq) = outcome.flushed_event_seq {
            self.flushed_event_seq
                .fetch_max(flushed_event_seq, Ordering::SeqCst);
        }
    }

    pub(super) fn has_any_local_overlay_mutation(local: &RedbMetaEngine) -> MetaResult<bool> {
        Ok(!local.local_overlay_inodes()?.is_empty()
            || !local.local_overlay_attr_inodes()?.is_empty()
            || !local.local_overlay_dentries()?.is_empty()
            || !local.local_overlay_symlink_inodes()?.is_empty()
            || !local.local_overlay_xattr_keys()?.is_empty()
            || !local.local_overlay_acl_keys()?.is_empty())
    }

    pub(super) fn startup_snapshot_import_mode(
        local: &RedbMetaEngine,
        persisted_version: i64,
    ) -> MetaResult<Option<StartupSnapshotImportMode>> {
        if persisted_version > 0 {
            return Ok(None);
        }
        let has_unflushed_local_events = local.get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER)?
            < local.get_counter("nextEventSeq")?;
        if has_unflushed_local_events && Self::has_any_local_overlay_mutation(local)? {
            return Ok(Some(StartupSnapshotImportMode::PreserveLocalState));
        }
        Ok(Some(StartupSnapshotImportMode::ReplaceLocalState))
    }

    pub(super) fn snapshot_replay_start_version(
        checkpoint_version: i64,
        advertised_head_version: i64,
    ) -> i64 {
        checkpoint_version
            .max(0)
            .min(advertised_head_version.max(0))
    }
}
