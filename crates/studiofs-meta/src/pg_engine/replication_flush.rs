//! Owns local event replication backlog claims and flush completion.

use super::{
    ClaimedPendingBatch, PendingMutationHint, PendingReplicationState, PgMetaEngine, RecentFlush,
    RemoteApplyCoordinator,
};
use crate::engine::{MetaError, MetaInodes, MetaResult};
use crate::events::MetaEvent;
use crate::schema::{ChunkId, PayloadRefState, ROOT_INO, SliceRecord, UploadState};
use crate::types::{FileType, Ino, InodeAttr};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tracing::warn;

const REDB_REPAIR_EVENT_BATCH_SIZE: u64 = 10_000;
const LOCAL_ONLY_SUFFIX_CONTEXT_EVENTS: u64 = 16;

#[derive(Default)]
struct RepairedRegularFileOverlay {
    attr: Option<InodeAttr>,
    chunks: BTreeMap<ChunkId, Vec<SliceRecord>>,
}

impl RepairedRegularFileOverlay {
    fn reset(&mut self) {
        self.attr = None;
        self.chunks.clear();
    }

    fn has_uploaded_payload(&self) -> bool {
        self.chunks.values().flatten().any(|slice| {
            slice.payload.is_some() && slice.payload_state == PayloadRefState::Uploaded
        })
    }
}

fn attrs_match_repair_identity(left: &InodeAttr, right: &InodeAttr) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.size = 0;
    right.size = 0;
    left == right
}

fn attrs_match_repair_candidate(current: &InodeAttr, candidate: &InodeAttr) -> bool {
    current == candidate
        || (attrs_match_repair_identity(current, candidate)
            && current.size == candidate.committed_size)
}

fn preserve_repaired_uploaded_payload_state(
    previous: &[SliceRecord],
    incoming: &mut [SliceRecord],
) {
    for slice in incoming {
        let Some(payload) = slice.payload else {
            continue;
        };
        if !matches!(
            slice.payload_state,
            PayloadRefState::LocalOnly | PayloadRefState::Uploading | PayloadRefState::Recovering
        ) {
            continue;
        }
        if let Some(previous) = previous.iter().find(|previous| {
            previous.payload == Some(payload)
                && slice.range.start <= previous.range.start
                && slice.range.end >= previous.range.end
                && matches!(
                    previous.payload_state,
                    PayloadRefState::Uploaded | PayloadRefState::Published
                )
        }) {
            slice.payload_state = previous.payload_state;
        }
    }
}

fn push_repaired_overlay_events(
    events: &mut Vec<MetaEvent>,
    attr: InodeAttr,
    chunks: BTreeMap<ChunkId, Vec<SliceRecord>>,
) {
    let ino = attr.ino;
    events.push(MetaEvent::SetAttr { attr });
    events.push(MetaEvent::SetRegularFileOverlaySlices {
        ino,
        chunk_slices: chunks.into_iter().collect(),
    });
}

impl PgMetaEngine {
    pub(super) fn current_inode_is_unpublished_regular(&self, ino: Ino) -> MetaResult<bool> {
        if let Some(lifecycle) = self.local.get_inode_lifecycle(ino)? {
            return Ok(!lifecycle.has_published_base);
        }
        match self.local.get_attr(ino) {
            Ok(attr) => Ok(attr.kind == FileType::RegularFile && attr.is_unpublished_empty_draft()),
            Err(MetaError::InodeNotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn source_has_local_only_deleted_inode(source_events: &[MetaEvent], ino: Ino) -> bool {
        let mut has_graph_removal = false;
        let mut has_attr_removal = false;
        for event in source_events {
            match event {
                MetaEvent::RemoveRegularFileGraph { ino: event_ino } if *event_ino == ino => {
                    has_graph_removal = true;
                }
                MetaEvent::RemoveAttr { ino: event_ino } if *event_ino == ino => {
                    has_attr_removal = true;
                }
                MetaEvent::RemoveEntry { .. } => return false,
                _ => {}
            }
        }
        has_graph_removal && has_attr_removal
    }

    pub(super) fn projected_suffix_is_local_only(
        &self,
        source_events: &[MetaEvent],
        projected_events: &[MetaEvent],
    ) -> MetaResult<bool> {
        let mut graph_counts = HashMap::<Ino, usize>::new();
        let mut attr_counts = HashMap::<Ino, usize>::new();
        let mut source_is_only_file_deletions = true;
        for event in source_events {
            match event {
                MetaEvent::RemoveRegularFileGraph { ino } => {
                    *graph_counts.entry(*ino).or_default() += 1;
                }
                MetaEvent::RemoveAttr { ino } => {
                    *attr_counts.entry(*ino).or_default() += 1;
                }
                _ => {
                    source_is_only_file_deletions = false;
                    break;
                }
            }
        }
        if source_is_only_file_deletions
            && !attr_counts.is_empty()
            && attr_counts.iter().all(|(ino, attr_count)| {
                graph_counts.get(ino).copied().unwrap_or_default() >= *attr_count
            })
            && projected_events.len() == attr_counts.values().sum::<usize>()
            && projected_events.iter().all(|event| {
                matches!(event, MetaEvent::RemoveAttr { ino } if attr_counts.contains_key(ino))
            })
        {
            return Ok(true);
        }
        if projected_events.is_empty() {
            return Ok(false);
        }
        let unpublished_empty_inodes = source_events
            .iter()
            .filter_map(|event| match event {
                MetaEvent::SetAttr { attr } if attr.is_unpublished_empty_draft() => Some(attr.ino),
                _ => None,
            })
            .collect::<HashSet<_>>();
        for event in projected_events {
            let local_only = match event {
                MetaEvent::SetAttr { attr } => attr.is_unpublished_empty_draft(),
                MetaEvent::AddEntry { entry, .. } => {
                    entry.kind == FileType::RegularFile
                        && (unpublished_empty_inodes.contains(&entry.ino)
                            || self.current_inode_is_unpublished_regular(entry.ino)?
                            || Self::source_has_local_only_deleted_inode(source_events, entry.ino))
                }
                MetaEvent::RemoveAttr { ino } => {
                    Self::source_has_local_only_deleted_inode(source_events, *ino)
                }
                _ => false,
            };
            if !local_only {
                return Ok(false);
            }
        }
        Ok(true)
    }

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

    fn stale_published_setup_prefix_len(&self, events: &[MetaEvent]) -> MetaResult<usize> {
        let mut stale_inodes = HashSet::new();
        for (index, event) in events.iter().enumerate() {
            match event {
                MetaEvent::SetAttr { attr }
                    if attr.kind == FileType::RegularFile
                        && attr.upload_state == UploadState::Ready
                        && attr.size == 0
                        && attr.committed_size == 0
                        && attr.content_epoch == 0
                        && attr.committed_content_epoch == 0
                        && self
                            .local
                            .get_base_attr(attr.ino)?
                            .is_some_and(|base_attr| {
                                base_attr.kind == FileType::RegularFile
                                    && (base_attr.committed_size > attr.committed_size
                                        || base_attr.committed_content_epoch
                                            > attr.committed_content_epoch)
                            }) =>
                {
                    stale_inodes.insert(attr.ino);
                }
                MetaEvent::AddEntry { entry, .. }
                    if entry.kind == FileType::RegularFile && stale_inodes.contains(&entry.ino) => {
                }
                _ => return Ok(index),
            }
        }
        Ok(events.len())
    }

    fn initial_root_snapshot_prefix_len(event_start: u64, events: &[MetaEvent]) -> usize {
        if event_start != 0 {
            return 0;
        }

        matches!(
            events.first(),
            Some(MetaEvent::SetAttr { attr })
                if attr.ino == ROOT_INO
                    && attr.kind == FileType::Directory
                    && attr.parent == ROOT_INO
                    && attr.content_epoch == 0
                    && attr.committed_content_epoch == 0
                    && attr.upload_state == UploadState::Ready
        )
        .then_some(1)
        .unwrap_or_default()
    }

    pub(super) fn restore_pending_state_from_local_log(&self) -> MetaResult<()> {
        let mut state = self
            .pending_state
            .lock()
            .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
        self.restore_pending_state_from_local_log_with_state(&mut state)
    }

    pub(super) fn restore_pending_state_from_local_log_with_state(
        &self,
        state: &mut PendingReplicationState,
    ) -> MetaResult<()> {
        let mut event_start = self.flushed_event_seq.load(Ordering::SeqCst);
        let event_end = self.local.get_counter("nextEventSeq")?;
        if event_end <= event_start {
            Self::clear_pending_state(state);
            return Ok(());
        }

        let pending_events = self.local.read_events_range(event_start, event_end)?;
        // `studiofs init` publishes the initial root in the serving checkpoint,
        // while the local redb journal still contains the root setup event.
        // Treat that one event as acknowledged so every later local mutation
        // does not repeatedly re-project the startup baseline.
        let initial_root_prefix_len =
            Self::initial_root_snapshot_prefix_len(event_start, &pending_events);
        if initial_root_prefix_len > 0 {
            event_start = event_start
                .max(1)
                .saturating_add(initial_root_prefix_len as u64);
            self.persist_flushed_event_seq(event_start)?;
        }
        let pending_events = &pending_events[initial_root_prefix_len..];
        let stale_setup_prefix_len = self.stale_published_setup_prefix_len(pending_events)?;
        if stale_setup_prefix_len > 0 {
            event_start = event_start
                .max(1)
                .saturating_add(stale_setup_prefix_len as u64);
            self.persist_flushed_event_seq(event_start)?;
        }
        let pending_events = &pending_events[stale_setup_prefix_len..];
        if pending_events.is_empty() {
            Self::clear_pending_state(state);
            return Ok(());
        }
        self.local
            .apply_replica_serving_state_for_pending_replay(pending_events)?;
        let projected = RemoteApplyCoordinator::project_local_batch(pending_events);
        if projected.visible_ops == 0 {
            if projected.needs_local_replay {
                let now = Instant::now();
                state.ops = 0;
                state.force_flush = false;
                state.event_start = Some(event_start);
                state.event_end = event_end;
                state.started_at.get_or_insert(now);
                state.last_mutation_at = Some(now);
                return Ok(());
            }
            Self::clear_pending_state(state);
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

    /// Restore the local regular-file serving graph that redb recovery can lose
    /// after an upload has reached object storage but before its epoch publishes.
    ///
    /// Uploaded overlay transitions are deliberately acknowledged locally: they
    /// do not create a PostgreSQL metadata backlog.  A redb repair resets the
    /// remote replay position but must not turn that acknowledgement into a
    /// reason to discard the only durable local graph for the pending epoch.
    /// This method replays only those journal records into serving state; it
    /// never appends an event, moves the flushed cursor, or re-submits history.
    pub(super) fn restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair(
        &self,
    ) -> MetaResult<()> {
        let event_end = self.local.get_counter("nextEventSeq")?;
        let mut overlays = HashMap::<Ino, RepairedRegularFileOverlay>::new();

        let mut event_start = self.local.first_event_seq()?.unwrap_or(event_end);
        while event_start < event_end {
            let event_batch_end = event_start
                .saturating_add(REDB_REPAIR_EVENT_BATCH_SIZE)
                .min(event_end);
            for event in self.local.read_events_range(event_start, event_batch_end)? {
                match event {
                    MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                        let overlay = overlays.entry(ino).or_default();
                        for (chunk_id, slices) in chunk_slices {
                            let previous =
                                overlay.chunks.get(&chunk_id).cloned().unwrap_or_default();
                            let mut slices = slices;
                            preserve_repaired_uploaded_payload_state(&previous, &mut slices);
                            overlay.chunks.insert(chunk_id, slices);
                        }
                    }
                    MetaEvent::SetAttr { attr } if attr.kind == FileType::RegularFile => {
                        let overlay = overlays.entry(attr.ino).or_default();
                        if !attr.is_upload_pending() {
                            overlay.reset();
                            continue;
                        }
                        overlay.attr = Some(attr);
                    }
                    MetaEvent::SetAttr { attr } => {
                        overlays.remove(&attr.ino);
                    }
                    MetaEvent::PublishRegularFileBaseSlices { ino, .. }
                    | MetaEvent::RemoveRegularFileGraph { ino }
                    | MetaEvent::RemoveAttr { ino } => {
                        overlays.remove(&ino);
                    }
                    _ => {}
                }
            }
            event_start = event_batch_end;
        }

        let mut restored_inodes = 0usize;
        let mut restored_events = Vec::new();
        for (ino, overlay) in overlays {
            if overlay.chunks.is_empty() || !overlay.has_uploaded_payload() {
                continue;
            }
            let Some(attr) = overlay.attr else {
                continue;
            };

            let current = match self.local.get_attr(ino) {
                Ok(current) => current,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(error) => return Err(error),
            };
            if !attrs_match_repair_candidate(&current, &attr) {
                continue;
            }

            push_repaired_overlay_events(&mut restored_events, attr, overlay.chunks);
            restored_inodes = restored_inodes.saturating_add(1);
        }

        if restored_inodes > 0 {
            self.local.apply_replica_serving_state(&restored_events)?;
            self.local.rebuild_inode_lifecycle_table()?;
            tracing::warn!(
                restored_inodes,
                "redb repair restored acknowledged uploaded regular-file serving overlays"
            );
        }
        Ok(())
    }

    pub(super) fn mark_pending_after_mutation(
        &self,
        before_seq: u64,
        after_seq: u64,
        hint: PendingMutationHint,
    ) -> MetaResult<bool> {
        if after_seq <= before_seq {
            return Ok(false);
        }
        let now = Instant::now();
        let mut state = self
            .pending_state
            .lock()
            .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
        if state.ops > 0 && state.event_start.is_some() && after_seq <= state.event_end {
            // A later mutation may have published a higher event watermark
            // before this caller reacquired the pending-state lock. Its range
            // is already covered by that projection, so replaying the prefix
            // here would only add contention.
            return Ok(false);
        }
        if matches!(
            hint,
            PendingMutationHint::LocalOnly | PendingMutationHint::LocalOnlyDeletion
        ) {
            // The namespace wrapper has already established that this
            // mutation is local-only. Keep the event range for local serving-
            // state replay, but do not read and project it again on every
            // hot-path mutation. A local-only deletion is also allowed to
            // retire a fully canceled range immediately: there is no remote
            // work and no serving replay left once the unpublished inode has
            // been removed.
            let event_start = state.event_start.unwrap_or(before_seq);
            if hint == PendingMutationHint::LocalOnlyDeletion
                && state.ops == 0
                && self.inflight_replication_ops.load(Ordering::SeqCst) == 0
            {
                let events = self.local.read_events_range(event_start, after_seq)?;
                let projected = RemoteApplyCoordinator::project_local_batch(&events);
                if projected.visible_ops == 0 && !projected.needs_local_replay {
                    Self::clear_pending_state(&mut state);
                    drop(state);
                    self.persist_flushed_event_seq(after_seq)?;
                    return Ok(false);
                }
            }
            if state.event_start.is_none() {
                state.event_start = Some(event_start);
                if state.started_at.is_none() {
                    state.started_at = Some(now);
                }
            }
            state.last_mutation_at = Some(now);
            state.event_end = state.event_end.max(after_seq);
            return Ok(false);
        }
        if state.ops > 0 && state.event_start.is_some() && before_seq >= state.event_end {
            // A local-only suffix cannot change the already projected remote
            // operation count. Keep it in the range so a later flush can
            // restore serving state, but avoid re-projecting the whole older
            // prefix on every deferred namespace mutation.
            let context_start = before_seq.saturating_sub(LOCAL_ONLY_SUFFIX_CONTEXT_EVENTS);
            let events_with_context = self.local.read_events_range(context_start, after_seq)?;
            let suffix_len =
                usize::try_from(after_seq.saturating_sub(before_seq)).unwrap_or(usize::MAX);
            let context_len = events_with_context.len().saturating_sub(suffix_len);
            let suffix = &events_with_context[context_len..];
            let suffix_projected = RemoteApplyCoordinator::project_local_batch(suffix);
            let suffix_is_local_only = suffix_projected.visible_ops == 0
                || self.projected_suffix_is_local_only(
                    &events_with_context,
                    &suffix_projected.events,
                )?;
            if suffix_is_local_only {
                state.last_mutation_at = Some(now);
                state.event_end = after_seq;
                return Ok(false);
            }
        }
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
        self.background_workers.signal_poll_wakeup();
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

    fn pending_batch_is_due(&self) -> bool {
        let Ok(state) = self.pending_state.lock() else {
            return false;
        };
        state.ops > 0
            && state.event_start.is_some()
            && (state.force_flush
                || Self::should_flush_due_interval(
                    &state,
                    self.flush_idle_grace,
                    self.flush_interval,
                ))
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
            if !projected.needs_local_replay {
                self.persist_flushed_event_seq(batch.event_end)?;
            }
            self.restore_pending_state_from_local_log()?;
            return Ok(None);
        }
        let batch_version = self.push_projected_event_log_batch_locked(projected.events)?;
        // The replay pass acknowledges only the local prefix represented by the
        // projected batch. The remaining local-only suffix must stay replayable.
        self.restore_pending_state_from_local_log()?;
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
            Err(err @ MetaError::EventTooLarge { .. }) => {
                self.finish_claimed_batch(batch);
                warn!(
                    error = %err,
                    volume_id = self.volume_id,
                    event_start = batch.event_start,
                    event_end = batch.event_end,
                    "postgres flush rejected an oversized event; pending claim was not restored"
                );
                Err(err)
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
        self.replay_local_upload_ledger()?;
        if !self.pending_batch_is_due() {
            return Ok(());
        }
        self.ensure_remote_format_validated()?;
        let Some(batch) = self.claim_pending_batch(false) else {
            return Ok(());
        };
        let recent_flush = self.flush_pending_claimed_batch_with_restore(batch)?;
        let _ = self.pull_and_reconcile_foreign_events_locked(recent_flush.as_ref())?;
        Ok(())
    }
}

#[cfg(test)]
mod repair_tests {
    use super::*;

    #[test]
    fn repair_candidate_matches_exact_attr() {
        let attr = InodeAttr::new_file(42, 1, 0, 0, 0o644);

        assert!(attrs_match_repair_candidate(&attr, &attr));
    }

    #[test]
    fn repair_candidate_matches_remote_projection_size() {
        let mut candidate = InodeAttr::new_file(42, 1, 0, 0, 0o644);
        candidate.size = 128;
        candidate.committed_size = 64;
        let mut remote_projection = candidate.clone();
        remote_projection.size = candidate.committed_size;

        assert!(attrs_match_repair_candidate(&remote_projection, &candidate));
    }

    #[test]
    fn repair_identity_ignores_only_local_visible_size() {
        let left = InodeAttr::new_file(42, 1, 0, 0, 0o644);
        let mut right = left.clone();
        right.size = 128;

        assert!(attrs_match_repair_identity(&left, &right));

        right.committed_size = 64;
        assert!(!attrs_match_repair_identity(&left, &right));
    }

    #[test]
    fn repaired_overlay_events_preserve_attr_before_slices() {
        let attr = InodeAttr::new_file(42, 1, 0, 0, 0o644);
        let mut events = Vec::new();

        push_repaired_overlay_events(&mut events, attr.clone(), BTreeMap::new());

        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            MetaEvent::SetAttr { attr: actual } if actual == &attr
        ));
        assert!(matches!(
            &events[1],
            MetaEvent::SetRegularFileOverlaySlices { ino: 42, chunk_slices }
                if chunk_slices.is_empty()
        ));
    }
}
