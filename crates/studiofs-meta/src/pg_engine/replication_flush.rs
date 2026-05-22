use super::{
    ClaimedPendingBatch, PendingReplicationState, PgMetaEngine, RecentFlush, RemoteApplyCoordinator,
};
use crate::engine::{MetaError, MetaInodes, MetaResult};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

impl PgMetaEngine {
    pub(super) fn clear_pending_state(state: &mut PendingReplicationState) {
        *state = PendingReplicationState::default();
    }

    fn finish_claimed_batch(&self, batch: ClaimedPendingBatch) {
        self.inflight_replication_ops
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(current.saturating_sub(batch.ops))
            })
            .ok();
    }

    pub(super) fn restore_pending_state_from_local_log(&self) -> MetaResult<()> {
        let event_start = self.flushed_event_seq.load(Ordering::SeqCst);
        let event_end = self.local.get_counter("nextEventSeq")?;
        let mut state = self
            .pending_state
            .lock()
            .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
        if event_end <= event_start {
            Self::clear_pending_state(&mut state);
            return Ok(());
        }

        let pending_events = self.local.read_events_range(event_start, event_end)?;
        self.local.apply_replica_serving_state(&pending_events)?;
        let projected = RemoteApplyCoordinator::project_local_batch(&pending_events);
        if projected.visible_ops == 0 {
            Self::clear_pending_state(&mut state);
            if projected.needs_local_replay {
                return Ok(());
            }
            drop(state);
            self.persist_flushed_event_seq(event_end)?;
            return Ok(());
        }

        let now = Instant::now();
        state.ops = projected.visible_ops;
        state.force_flush = true;
        state.event_start = Some(event_start);
        state.event_end = event_end;
        state.started_at = Some(now);
        state.last_mutation_at = Some(now);
        Ok(())
    }

    pub(super) fn mark_pending_after_mutation(
        &self,
        before_seq: u64,
        after_seq: u64,
    ) -> MetaResult<bool> {
        if after_seq <= before_seq {
            return Ok(false);
        }
        let now = Instant::now();
        let mut state = self
            .pending_state
            .lock()
            .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
        let event_start = state.event_start.unwrap_or(before_seq);
        let events = self.local.read_events_range(event_start, after_seq)?;
        let projected = RemoteApplyCoordinator::project_local_batch(&events);
        if projected.visible_ops == 0 {
            if projected.needs_local_replay {
                state.ops = 0;
                state.force_flush = false;
                state.event_start = Some(event_start);
                state.event_end = state.event_end.max(after_seq);
                state.last_mutation_at = Some(now);
                if state.started_at.is_none() {
                    state.started_at = Some(now);
                }
                return Ok(false);
            }
            if state.ops == 0 {
                if self.inflight_replication_ops.load(Ordering::SeqCst) > 0 {
                    return Ok(false);
                }
                Self::clear_pending_state(&mut state);
                drop(state);
                self.persist_flushed_event_seq(after_seq)?;
            } else {
                if state.event_start.is_none() {
                    state.event_start = Some(before_seq);
                }
                state.event_end = state.event_end.max(after_seq);
            }
            return Ok(false);
        }
        if state.ops == 0 {
            state.started_at = Some(now);
        }
        state.ops = projected.visible_ops;
        state.last_mutation_at = Some(now);
        state.event_start = Some(event_start);
        state.event_end = state.event_end.max(after_seq);
        state.force_flush = projected.needs_eager_flush || state.ops >= self.flush_ops_threshold;
        Ok(projected.needs_eager_flush)
    }

    pub(super) fn request_pending_flush(&self) {
        if let Ok(mut state) = self.pending_state.lock() {
            state.force_flush = true;
        }
    }

    pub(super) fn signal_poll_wakeup(&self) {
        let (wakeup_lock, wakeup_cv) = &*self.poll_wakeup;
        let mut wakeup_guard = wakeup_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *wakeup_guard = true;
        wakeup_cv.notify_one();
    }

    pub(super) fn should_flush_due_interval(
        state: &PendingReplicationState,
        idle_grace: Duration,
        interval: Duration,
    ) -> bool {
        if state.ops == 0 {
            return false;
        }
        if let Some(t0) = state.last_mutation_at {
            return t0.elapsed() >= idle_grace;
        }
        if let Some(t0) = state.started_at {
            return t0.elapsed() >= interval;
        }
        true
    }

    pub(super) fn claim_pending_batch(&self, force: bool) -> Option<ClaimedPendingBatch> {
        let mut state = self.pending_state.lock().ok()?;
        if state.ops == 0 {
            return None;
        }
        let should_flush = force
            || state.force_flush
            || Self::should_flush_due_interval(&state, self.flush_idle_grace, self.flush_interval);
        if !should_flush {
            return None;
        }
        let event_start = state.event_start?;
        let batch = ClaimedPendingBatch {
            ops: state.ops,
            started_at: state.started_at,
            last_mutation_at: state.last_mutation_at,
            force_flush: state.force_flush,
            event_start,
            event_end: state.event_end,
        };
        Self::clear_pending_state(&mut state);
        self.inflight_replication_ops
            .fetch_add(batch.ops, Ordering::SeqCst);
        Some(batch)
    }

    pub(super) fn restore_pending_batch(&self, batch: ClaimedPendingBatch) {
        self.finish_claimed_batch(batch);
        if let Ok(mut state) = self.pending_state.lock() {
            state.ops = state.ops.saturating_add(batch.ops);
            state.force_flush |= batch.force_flush;
            state.event_start = Some(
                state
                    .event_start
                    .map(|current| current.min(batch.event_start))
                    .unwrap_or(batch.event_start),
            );
            state.event_end = state.event_end.max(batch.event_end);
            state.started_at = match (state.started_at, batch.started_at) {
                (Some(current), Some(claimed)) => Some(current.min(claimed)),
                (Some(current), None) => Some(current),
                (None, Some(claimed)) => Some(claimed),
                (None, None) => None,
            };
            state.last_mutation_at = match (state.last_mutation_at, batch.last_mutation_at) {
                (Some(current), Some(claimed)) => Some(current.max(claimed)),
                (Some(current), None) => Some(current),
                (None, Some(claimed)) => Some(claimed),
                (None, None) => None,
            };
        }
    }

    fn flush_pending_claimed_batch(
        &self,
        batch: ClaimedPendingBatch,
    ) -> MetaResult<Option<RecentFlush>> {
        let events = self
            .local
            .read_events_range(batch.event_start, batch.event_end)?;
        if events.is_empty() {
            return Ok(None);
        }
        let projected = RemoteApplyCoordinator::project_local_batch(&events);
        if projected.events.is_empty() {
            self.persist_flushed_event_seq(batch.event_end)?;
            return Ok(None);
        }
        let batch_version = self.push_projected_event_log_batch_locked(projected.events)?;
        // Do not advance `local_version` here. The writer may have published a
        // newer batch_version while still missing earlier foreign batches.
        // `pull_foreign_events_locked()` must remain able to read the full gap.
        self.persist_flushed_event_seq(batch.event_end)?;
        Ok(Some(RecentFlush {
            batch_version,
            overlay: projected.overlay,
        }))
    }

    pub(super) fn flush_pending_claimed_batch_with_restore(
        &self,
        batch: ClaimedPendingBatch,
    ) -> MetaResult<Option<RecentFlush>> {
        match self.flush_pending_claimed_batch(batch) {
            Ok(recent_flush) => {
                self.finish_claimed_batch(batch);
                Ok(recent_flush)
            }
            Err(err) => {
                let recovered_from_remote = self
                    .pull_foreign_events_locked(None)
                    .map(|_| self.flushed_event_seq.load(Ordering::SeqCst) >= batch.event_end)
                    .unwrap_or(false);
                if recovered_from_remote {
                    self.finish_claimed_batch(batch);
                    return Ok(None);
                }
                self.restore_pending_batch(batch);
                Err(err)
            }
        }
    }

    pub(super) fn flush_due_pending(&self) -> MetaResult<()> {
        let _guard = self
            .replication_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
        let Some(batch) = self.claim_pending_batch(false) else {
            return Ok(());
        };
        let recent_flush = self.flush_pending_claimed_batch_with_restore(batch)?;
        let _ = self.pull_and_reconcile_foreign_events_locked(recent_flush.as_ref())?;
        Ok(())
    }
}
