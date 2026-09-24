use super::{
    LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, LOCAL_PG_NAMESPACE_VERSION_COUNTER,
    LOCAL_PG_VERSION_COUNTER, LocalEventRange, PendingReplicationState, PgMetaEngine,
    StartupSnapshotImportMode,
};
use crate::engine::{MetaError, MetaInodes, MetaResult};
use crate::redb_engine::RedbMetaEngine;
use std::sync::atomic::Ordering;

/// Sort ranges and merge overlaps or adjacency without replacing the vector.
fn merge_local_event_ranges(ranges: &mut Vec<LocalEventRange>) {
    if ranges.len() < 2 {
        return;
    }
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut write_index = 0;
    for read_index in 1..ranges.len() {
        let range = ranges[read_index];
        if range.start <= ranges[write_index].end {
            ranges[write_index].end = ranges[write_index].end.max(range.end);
        } else {
            write_index += 1;
            ranges[write_index] = range;
        }
    }
    ranges.truncate(write_index + 1);
}

/// Remove acknowledged intervals and clip a remaining interval at `cursor`.
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
        self.lock_local_position_and_pending_state_with_execution(None)
    }

    pub(super) fn lock_local_position_and_pending_state_with_execution(
        &self,
        execution: Option<&super::remote_apply::RemoteReplayExecution<'_>>,
    ) -> MetaResult<(
        std::sync::MutexGuard<'_, ()>,
        std::sync::MutexGuard<'_, PendingReplicationState>,
    )> {
        let position_guard = lock_with_replay_execution(
            &self.local_position_lock,
            execution,
            "pg local position lock poisoned",
        )?;
        #[cfg(test)]
        self.run_catch_up_fault_hook(super::CatchUpFaultPoint::AfterLocalPositionLockAcquired)?;
        let pending_state = lock_with_replay_execution(
            &self.pending_state,
            execution,
            "pg pending state lock poisoned",
        )?;
        if let Some(execution) = execution {
            execution.check()?;
        }
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

    /// Persist the cursor while the caller holds `local_position_lock`.
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

        let next =
            flushed_event_seq_after_completed_ranges(cursor, &completed_ranges, &progress.rejected);
        if next > cursor {
            self.persist_flushed_event_seq_locked(next)?;
            trim_local_event_ranges_before(&mut completed_ranges, next);
            trim_local_event_ranges_before(&mut progress.published, next);
        }
        progress.completed = completed_ranges;
        Ok(())
    }

    /// Return the cursor implied by completed ranges without persisting it.
    ///
    /// The caller holds `local_position_lock` and `pending_state`, so progress
    /// cannot change between this calculation and the replay transaction.
    pub(super) fn flushed_event_seq_after_completed_ranges_locked(
        &self,
        cursor: u64,
    ) -> MetaResult<u64> {
        let progress = self
            .local_event_progress
            .lock()
            .map_err(|_| MetaError::Storage("pg local event progress lock poisoned".to_string()))?;
        Ok(flushed_event_seq_after_completed_ranges(
            cursor,
            &progress.completed,
            &progress.rejected,
        ))
    }

    /// Trim acknowledged ranges while the caller holds `local_position_lock`
    /// and `pending_state`.
    pub(super) fn trim_local_event_progress_before_locked(&self, cursor: u64) -> MetaResult<()> {
        let mut progress = self
            .local_event_progress
            .lock()
            .map_err(|_| MetaError::Storage("pg local event progress lock poisoned".to_string()))?;
        let cursor = cursor.max(self.flushed_event_seq.load(Ordering::SeqCst));
        trim_local_event_ranges_before(&mut progress.rejected, cursor);
        trim_local_event_ranges_before(&mut progress.completed, cursor);
        trim_local_event_ranges_before(&mut progress.published, cursor);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn unprocessed_local_event_progress(&self) -> MetaResult<super::LocalEventProgress> {
        self.local_event_progress
            .lock()
            .map(|progress| progress.clone())
            .map_err(|_| MetaError::Storage("pg local event progress lock poisoned".to_string()))
    }

    /// Return published and completed ranges that no longer reserve local
    /// overlay conflicts. Rejected ranges intentionally remain local authority.
    pub(super) fn local_event_overlay_exclusions(&self) -> MetaResult<Vec<LocalEventRange>> {
        let progress = self
            .local_event_progress
            .lock()
            .map_err(|_| MetaError::Storage("pg local event progress lock poisoned".to_string()))?;
        let mut ranges = progress
            .published
            .iter()
            .chain(progress.completed.iter())
            .copied()
            .collect::<Vec<_>>();
        merge_local_event_ranges(&mut ranges);
        Ok(ranges)
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

fn lock_with_replay_execution<'a, T>(
    mutex: &'a std::sync::Mutex<T>,
    execution: Option<&super::remote_apply::RemoteReplayExecution<'_>>,
    poison_message: &'static str,
) -> MetaResult<std::sync::MutexGuard<'a, T>> {
    if let Some(execution) = execution {
        loop {
            execution.check()?;
            match mutex.try_lock() {
                Ok(guard) => return Ok(guard),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(MetaError::Storage(poison_message.to_string()));
                }
                Err(std::sync::TryLockError::WouldBlock) => execution.wait_for_lock_retry(),
            }
        }
    }

    mutex
        .lock()
        .map_err(|_| MetaError::Storage(poison_message.to_string()))
}

/// Advance through completed intervals without crossing any rejected interval.
fn flushed_event_seq_after_completed_ranges(
    cursor: u64,
    completed: &[LocalEventRange],
    rejected: &[LocalEventRange],
) -> u64 {
    let mut next = cursor.max(1);
    let mut completed_index = completed.partition_point(|range| range.end <= next);
    let mut rejected_index = rejected.partition_point(|range| range.end <= next);

    loop {
        if rejected
            .get(rejected_index)
            .is_some_and(|range| range.start <= next)
        {
            break;
        }
        let Some(completed_range) = completed.get(completed_index) else {
            break;
        };
        if completed_range.start > next {
            break;
        }
        if let Some(rejected_range) = rejected.get(rejected_index)
            && rejected_range.start < completed_range.end
        {
            next = rejected_range.start;
            break;
        }

        next = completed_range.end;
        completed_index += 1;
        while rejected
            .get(rejected_index)
            .is_some_and(|range| range.end <= next)
        {
            rejected_index += 1;
        }
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_local_event_ranges_reuses_capacity_and_coalesces_intervals() {
        let mut ranges = Vec::with_capacity(16);
        ranges.extend([
            LocalEventRange { start: 9, end: 12 },
            LocalEventRange { start: 1, end: 3 },
            LocalEventRange { start: 3, end: 5 },
            LocalEventRange { start: 4, end: 10 },
            LocalEventRange { start: 15, end: 17 },
        ]);
        let capacity = ranges.capacity();

        merge_local_event_ranges(&mut ranges);

        assert_eq!(
            ranges,
            vec![
                LocalEventRange { start: 1, end: 12 },
                LocalEventRange { start: 15, end: 17 },
            ]
        );
        assert_eq!(ranges.capacity(), capacity);
    }
}
