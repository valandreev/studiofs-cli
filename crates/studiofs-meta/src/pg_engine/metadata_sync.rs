//! Owns foreign metadata catch-up orchestration and local replay application.

use super::{
    MetadataSyncSource, OverlayConflictSets, PgMetaEngine, RecentFlush, RemoteApplyCoordinator,
    RemoteReplayExecution, janitor::pg_event_log_prune_lock_key, sql,
};
use crate::engine::{MetaError, MetaInodes, MetaResult, PgSyncLifecycle, PgSyncPageDiagnostics};
use crate::types::{FileType, Ino};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tracing::warn;

pub(super) fn replay_cancelled(
    worker_shutdown: &AtomicBool,
    caller_shutdown: Option<&AtomicBool>,
) -> bool {
    worker_shutdown.load(Ordering::Acquire)
        || caller_shutdown.is_some_and(|shutdown| shutdown.load(Ordering::Acquire))
}

impl PgMetaEngine {
    pub(super) fn frozen_sync_target_version(&self) -> MetaResult<i64> {
        let current_target = self.metadata_startup_target_version.load(Ordering::Acquire);
        if current_target >= 0 {
            return Ok(current_target);
        }

        let observed_target = self.remote_metadata_head_version()?;
        self.metadata_remote_head_version
            .store(observed_target, Ordering::Release);
        if matches!(
            self.current_pg_sync_lifecycle(),
            PgSyncLifecycle::CatchingUp | PgSyncLifecycle::Degraded
        ) {
            let target = match self.metadata_startup_target_version.compare_exchange(
                -1,
                observed_target,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => observed_target,
                Err(existing) => existing,
            };
            return Ok(target);
        }
        Ok(observed_target)
    }

    fn active_open_inode_overlay(
        &self,
        active_open_inodes: Option<&HashSet<Ino>>,
    ) -> OverlayConflictSets {
        let mut overlay = OverlayConflictSets::default();
        if let Some(active_open_inodes) = active_open_inodes {
            overlay
                .remove_attr_inodes
                .extend(active_open_inodes.iter().copied());
        } else if let Ok(active) = self.active_open_inodes.lock() {
            overlay.remove_attr_inodes.extend(active.keys().copied());
        }
        overlay
    }

    pub(super) fn local_overlay_conflicts(&self) -> MetaResult<OverlayConflictSets> {
        self.local_overlay_conflicts_with_active_open_inodes(None, None)
    }

    fn local_overlay_conflicts_with_active_open_inodes(
        &self,
        active_open_inodes: Option<&HashSet<Ino>>,
        execution: Option<&RemoteReplayExecution<'_>>,
    ) -> MetaResult<OverlayConflictSets> {
        if let Some(execution) = execution {
            execution.check()?;
        }
        let active_open_overlay = self.active_open_inode_overlay(active_open_inodes);
        let start = self.flushed_event_seq.load(Ordering::SeqCst);
        let end = self.local.get_counter("nextEventSeq")?;
        if end <= start {
            return Ok(active_open_overlay);
        }

        let overlay_exclusions = self.local_event_overlay_exclusions()?;
        Ok(RemoteApplyCoordinator::merge_overlay_conflicts(
            &RemoteApplyCoordinator::overlay_conflicts_for_local_range(
                &self.local,
                start,
                end,
                &overlay_exclusions,
                execution,
            )?,
            &active_open_overlay,
        ))
    }

    pub(super) fn reconcile_imported_snapshot_regular_files_with_active_open_inodes(
        &self,
        active_open_inodes: Option<&HashSet<Ino>>,
    ) -> MetaResult<()> {
        let reconcile_inodes = self
            .local
            .list_inode_attrs()?
            .into_iter()
            .filter(|attr| attr.kind == FileType::RegularFile)
            .map(|attr| attr.ino)
            .collect::<HashSet<_>>();
        let overlay =
            self.local_overlay_conflicts_with_active_open_inodes(active_open_inodes, None)?;
        RemoteApplyCoordinator::new(&self.local)
            .reconcile_regular_file_serving_from_base_with_overlay(&reconcile_inodes, &overlay)?;
        Ok(())
    }

    /// Record aggregate sync state after a metadata operation.
    ///
    /// Replay helpers record their remote failure before returning, so callers
    /// pass `true` to avoid repeating those remote failure transitions.
    pub(super) fn record_metadata_sync_result(
        &self,
        source: MetadataSyncSource,
        result: &MetaResult<bool>,
        remote_operation_recorded: bool,
    ) {
        match result {
            Ok(changed) => {
                self.record_pg_sync_success();
                self.metadata_source_connected
                    .store(true, Ordering::Release);
                self.metadata_last_successful_sync_unix_ms
                    .store(Self::now_millis(), Ordering::Relaxed);
                self.observe_checkpoint_freshness();
                if source == MetadataSyncSource::FallbackPoll && *changed {
                    self.metadata_fallback_poll_discoveries
                        .fetch_add(1, Ordering::Relaxed);
                }
                if let Err(error) = self.try_complete_catchup_sync() {
                    self.record_pg_sync_failure(&error);
                    warn!(
                        error = %error,
                        volume_id = self.volume_id,
                        "metadata catch-up lifecycle recovery could not be completed"
                    );
                }
            }
            Err(error) => {
                if !matches!(error, MetaError::CatchUpCancelled) {
                    self.metadata_sync_errors.fetch_add(1, Ordering::Relaxed);
                }
                if !remote_operation_recorded {
                    self.record_pg_sync_failure(error);
                    if !matches!(
                        error,
                        MetaError::CatchUpCancelled | MetaError::CatchUpDeadlineExceeded
                    ) {
                        self.invalidate_remote_validation();
                    }
                    self.record_metadata_source_result(result);
                    if matches!(
                        error,
                        MetaError::ReplayGap { .. } | MetaError::RebootstrapRequired { .. }
                    ) {
                        self.mark_rebootstrap_required();
                    }
                }
            }
        }
    }

    pub(super) fn sync_metadata_with_source(&self, source: MetadataSyncSource) -> MetaResult<bool> {
        let mut remote_operation_recorded = false;
        let result = (|| {
            self.ensure_remote_format_validated()?;
            let _guard = self
                .replication_lock
                .lock()
                .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
            // `pull_foreign_events_locked` records every error after this
            // boundary through `record_remote_operation_result`.
            remote_operation_recorded = true;
            let changed = self.pull_foreign_events_locked(None)?;
            Ok(changed)
        })();
        self.record_metadata_sync_result(source, &result, remote_operation_recorded);
        result
    }

    pub(super) fn pull_foreign_events_locked(
        &self,
        recent_flush: Option<&RecentFlush>,
    ) -> MetaResult<bool> {
        let target_version = self.frozen_sync_target_version()?;
        self.pull_foreign_events_locked_to_target(recent_flush, target_version, None)
    }

    /// Pull and replay foreign events up to an already-frozen target version.
    ///
    /// The caller must hold `replication_lock`; keeping target resolution
    /// outside this helper lets a startup session reuse one target for all
    /// bounded replay pages.
    pub(super) fn pull_foreign_events_locked_to_target(
        &self,
        recent_flush: Option<&RecentFlush>,
        target_version: i64,
        caller_shutdown: Option<&AtomicBool>,
    ) -> MetaResult<bool> {
        let result = (|| {
            self.ensure_remote_format_validated()?;
            self.pull_foreign_events_locked_inner(recent_flush, target_version, caller_shutdown)
        })();
        self.record_remote_operation_result(result)
    }

    fn pull_foreign_events_locked_inner(
        &self,
        recent_flush: Option<&RecentFlush>,
        target_version: i64,
        caller_shutdown: Option<&AtomicBool>,
    ) -> MetaResult<bool> {
        let last_batch_version = self.local_version.load(Ordering::SeqCst);
        if last_batch_version >= target_version {
            return Ok(false);
        }

        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let client_id = self.current_client_id()?;
        let command_timeout = self.command_timeout;
        let sync_max_batches_per_page = self.sync_max_batches_per_page;
        let sync_max_response_bytes_per_page = self.sync_max_response_bytes_per_page;

        #[cfg(test)]
        self.run_catch_up_fault_hook(super::CatchUpFaultPoint::BeforePageFetch {
            cursor: last_batch_version,
        })?;

        let page_started = Instant::now();
        let page = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "pull foreign events",
                move |client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|e| {
                            Self::classify_pg_error("postgres begin pull transaction failed", e)
                        })?;
                        let lock_key = pg_event_log_prune_lock_key(volume_id);
                        tx.query_one(sql::ACQUIRE_METADATA_REPLAY_ADVISORY_LOCK, &[&lock_key])
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres metadata replay advisory lock failed",
                                    e,
                                )
                            })?;
                        let replay_state = tx
                            .query_opt(sql::SELECT_METADATA_REPLAY_STATE, &[&volume_id])
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres metadata replay state revalidation failed",
                                    e,
                                )
                            })?;
                        let replay_state = replay_state.ok_or_else(|| {
                            MetaError::Storage(
                                "postgres metadata replay state row missing".to_string(),
                            )
                        })?;
                        let format_version = replay_state
                            .try_get::<_, Option<i16>>("metadata_format_version")
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres metadata format version read failed",
                                    e,
                                )
                            })?;
                        crate::ensure_supported_pg_sync_format_version(format_version)?;
                        let checkpoint_version = replay_state
                            .try_get::<_, i64>("checkpoint_version")
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres checkpoint version read failed",
                                    e,
                                )
                            })?;
                        let retained_floor = replay_state
                            .try_get::<_, i64>("event_log_prune_floor")
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres event-log retention floor read failed",
                                    e,
                                )
                            })?;
                        Self::validate_replay_cursor_against_floor(
                            last_batch_version,
                            retained_floor,
                            checkpoint_version,
                            target_version,
                        )?;

                        let page = Self::query_foreign_event_page(
                            &tx,
                            volume_id,
                            last_batch_version,
                            target_version,
                            sync_max_batches_per_page,
                            sync_max_response_bytes_per_page,
                        )
                        .await?;
                        tx.commit().await.map_err(|e| {
                            Self::classify_pg_error("postgres pull transaction commit failed", e)
                        })?;
                        Ok(page)
                    })
                },
            )
            .await
        })?;
        let query_duration_us = elapsed_micros(page_started);

        #[cfg(test)]
        if !page.rows.is_empty() {
            self.run_catch_up_fault_hook(super::CatchUpFaultPoint::AfterPageFetch {
                cursor: last_batch_version,
                first_batch_version: page.first_batch_version,
                last_batch_version: page.last_batch_version,
            })?;
        }

        if page.rows.is_empty() {
            return Ok(false);
        }

        let page_diagnostics = PgSyncPageDiagnostics {
            first_batch_version: page
                .first_batch_version
                .and_then(|value| value.try_into().ok()),
            last_batch_version: page
                .last_batch_version
                .and_then(|value| value.try_into().ok()),
            batch_count: page.batch_count.try_into().unwrap_or_default(),
            event_count: page.event_count.try_into().unwrap_or_default(),
            response_bytes: page.response_bytes.try_into().unwrap_or_default(),
            has_more: page.has_more,
            oversized: page.oversized,
            query_duration_us,
            ..PgSyncPageDiagnostics::default()
        };
        let should_log = self.record_pg_sync_page_selected(page_diagnostics);
        if page.batch_count > 0 && should_log {
            tracing::debug!(
                volume_id = self.volume_id,
                local_applied_version = last_batch_version,
                target_version,
                remaining_version_distance = target_version
                    .saturating_sub(last_batch_version)
                    .max(0),
                first_batch_version = ?page.first_batch_version,
                last_batch_version = ?page.last_batch_version,
                batch_count = page.batch_count,
                event_count = page.event_count,
                response_bytes = page.response_bytes,
                has_more = page.has_more,
                oversized = page.oversized,
                "postgres foreign event page selected"
            );
        }
        if page.oversized {
            warn!(
                volume_id = self.volume_id,
                batch_version = ?page.first_batch_version,
                response_bytes = page.response_bytes,
                max_response_bytes = self.sync_max_response_bytes_per_page,
                "postgres foreign event page contains an oversized atomic batch"
            );
        }

        let batches = RemoteApplyCoordinator::decode_event_rows(page.rows, client_id.as_str())?;
        RemoteApplyCoordinator::validate_decoded_batch_continuity(
            last_batch_version,
            &batches,
            target_version,
        )?;
        // Keep the cursor snapshot, serving-state reconcile, and pending replay
        // atomic with local metadata mutations after decoding the remote page.
        let apply_started = Instant::now();
        let worker_shutdown = self.background_workers.poll_shutdown_ref();
        let cancelled = || replay_cancelled(worker_shutdown, caller_shutdown);
        // The PostgreSQL fetch has its own timeout; local replay gets a full budget.
        let deadline = apply_started.checked_add(command_timeout).ok_or_else(|| {
            MetaError::Storage("postgres catch-up replay deadline is out of range".to_string())
        })?;
        let execution = RemoteReplayExecution::new(deadline, &cancelled);
        execution.check()?;
        let (position_guard, mut pending_state) = self.lock_local_position_and_pending_state()?;
        execution.check()?;
        let flushed_event_seq = self.flushed_event_seq.load(Ordering::SeqCst);
        let next_event_seq = self.local.get_counter("nextEventSeq")?;
        let initial_overlay =
            self.local_overlay_conflicts_with_active_open_inodes(None, Some(&execution))?;
        let outcome = self.apply_replay_page_atomically_locked(
            batches,
            recent_flush,
            super::RemoteReplayApplyContext {
                last_seq: last_batch_version,
                current_namespace_version: self.namespace_version.load(Ordering::SeqCst),
                flushed_event_seq,
                next_event_seq,
                initial_overlay,
            },
            Some(&execution),
        )?;
        // Only an acknowledged self-batch advances the local journal cursor;
        // foreign batches update the base view without changing pending events.
        if outcome.flushed_event_seq.is_some() {
            self.restore_pending_state_from_local_log_with_state(&mut pending_state)?;
        }
        drop(pending_state);
        drop(position_guard);
        self.record_pg_sync_page_applied(PgSyncPageDiagnostics {
            apply_duration_us: elapsed_micros(apply_started),
            total_duration_us: elapsed_micros(page_started),
            ..page_diagnostics
        });
        Ok(outcome.changed)
    }

    #[cfg(test)]
    pub(super) fn apply_replay_page_atomically(
        &self,
        batches: Vec<super::remote_apply::DecodedReplayBatch>,
        recent_flush: Option<&super::RecentFlush>,
        context: super::RemoteReplayApplyContext,
    ) -> MetaResult<super::RemoteReplayApplyOutcome> {
        self.apply_replay_page_atomically_with_execution(batches, recent_flush, context, None)
    }

    #[cfg(test)]
    pub(super) fn apply_replay_page_atomically_with_execution(
        &self,
        batches: Vec<super::remote_apply::DecodedReplayBatch>,
        recent_flush: Option<&super::RecentFlush>,
        context: super::RemoteReplayApplyContext,
        execution: Option<&RemoteReplayExecution<'_>>,
    ) -> MetaResult<super::RemoteReplayApplyOutcome> {
        let (_position_guard, _pending_state) = self.lock_local_position_and_pending_state()?;
        self.apply_replay_page_atomically_locked(batches, recent_flush, context, execution)
    }

    /// Apply a page while the caller holds both locks in canonical order.
    fn apply_replay_page_atomically_locked(
        &self,
        batches: Vec<super::remote_apply::DecodedReplayBatch>,
        recent_flush: Option<&super::RecentFlush>,
        context: super::RemoteReplayApplyContext,
        execution: Option<&RemoteReplayExecution<'_>>,
    ) -> MetaResult<super::RemoteReplayApplyOutcome> {
        if batches.is_empty() {
            return Ok(super::RemoteReplayApplyOutcome {
                changed: false,
                max_version: context.last_seq,
                max_namespace_version: context.current_namespace_version,
                flushed_event_seq: None,
            });
        }
        let local_event_ack_barrier = self
            .local_event_progress
            .lock()
            .map_err(|_| MetaError::Storage("pg local event progress lock poisoned".to_string()))?
            .rejected
            .first()
            .map(|range| range.start);
        let local_event_overlay_exclusions = self.local_event_overlay_exclusions()?;
        let txn = self.local.begin_write_txn()?;
        let result = RemoteApplyCoordinator::new(&self.local)
            .with_local_event_ack_barrier(local_event_ack_barrier)
            .with_local_event_overlay_exclusions(local_event_overlay_exclusions)
            .apply_decoded_batches_in_txn(&txn, batches, recent_flush, context, execution);
        let mut outcome = match result {
            Ok(outcome) => outcome,
            Err(error) => {
                if let Err(abort_error) = txn.abort() {
                    tracing::warn!(
                        error = %abort_error,
                        "failed to abort redb write transaction during catch-up rollback"
                    );
                }
                return Err(error);
            }
        };
        if let Some(flushed_event_seq) = outcome.flushed_event_seq {
            outcome.flushed_event_seq =
                Some(self.flushed_event_seq_after_completed_ranges_locked(flushed_event_seq)?);
        }
        if let Err(error) = Self::write_replay_position_in_txn(&txn, &outcome) {
            if let Err(abort_error) = txn.abort() {
                tracing::warn!(
                    error = %abort_error,
                    "failed to abort redb write transaction during catch-up rollback"
                );
            }
            return Err(error);
        }

        #[cfg(test)]
        if let Err(error) =
            self.run_catch_up_fault_hook(super::CatchUpFaultPoint::BeforeLocalCommit {
                last_batch_version: outcome.max_version,
            })
        {
            if let Err(abort_error) = txn.abort() {
                tracing::warn!(
                    error = %abort_error,
                    "failed to abort redb write transaction during injected catch-up rollback"
                );
            }
            return Err(error);
        }
        if let Some(execution) = execution
            && let Err(error) = execution.check()
        {
            if let Err(abort_error) = txn.abort() {
                tracing::warn!(
                    error = %abort_error,
                    "failed to abort redb write transaction during catch-up rollback"
                );
            }
            return Err(error);
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))?;

        #[cfg(test)]
        self.run_catch_up_fault_hook(super::CatchUpFaultPoint::AfterLocalCommit {
            last_batch_version: outcome.max_version,
        })?;
        self.publish_replay_position(&outcome);
        if let Some(flushed_event_seq) = outcome.flushed_event_seq {
            self.trim_local_event_progress_before_locked(flushed_event_seq)?;
        }
        Ok(outcome)
    }

    pub(super) fn pull_and_reconcile_foreign_events_locked(
        &self,
        recent_flush: Option<&RecentFlush>,
    ) -> MetaResult<bool> {
        let mut changed = self.pull_foreign_events_locked(recent_flush)?;
        let _pending_state = self
            .pending_state
            .lock()
            .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
        let overlay = self.local_overlay_conflicts()?;
        changed |= RemoteApplyCoordinator::new(&self.local).reconcile_recent_flush(
            recent_flush,
            self.local_version.load(Ordering::SeqCst),
            &overlay,
        )?;
        Ok(changed)
    }
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}
