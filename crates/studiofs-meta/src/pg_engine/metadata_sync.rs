//! Owns foreign metadata catch-up orchestration and local replay application.

use super::{
    MetadataSyncSource, OverlayConflictSets, PgMetaEngine, RecentFlush, RemoteApplyCoordinator, sql,
};
use crate::engine::{MetaError, MetaInodes, MetaResult, PgSyncLifecycle, PgSyncPageDiagnostics};
use crate::types::FileType;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use tracing::warn;

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

    fn active_open_inode_overlay(&self) -> OverlayConflictSets {
        let mut overlay = OverlayConflictSets::default();
        let Ok(active) = self.active_open_inodes.lock() else {
            return overlay;
        };
        overlay.remove_attr_inodes.extend(active.keys().copied());
        overlay
    }

    pub(super) fn local_overlay_conflicts(&self) -> MetaResult<OverlayConflictSets> {
        let active_open_overlay = self.active_open_inode_overlay();
        let start = self.flushed_event_seq.load(Ordering::SeqCst);
        let end = self.local.get_counter("nextEventSeq")?;
        if end <= start {
            return Ok(active_open_overlay);
        }

        Ok(RemoteApplyCoordinator::merge_overlay_conflicts(
            &RemoteApplyCoordinator::overlay_conflicts_for_local_range(&self.local, start, end)?,
            &active_open_overlay,
        ))
    }

    pub(super) fn reconcile_imported_snapshot_regular_files(&self) -> MetaResult<()> {
        let reconcile_inodes = self
            .local
            .list_inode_attrs()?
            .into_iter()
            .filter(|attr| attr.kind == FileType::RegularFile)
            .map(|attr| attr.ino)
            .collect::<HashSet<_>>();
        let overlay = self.local_overlay_conflicts()?;
        RemoteApplyCoordinator::new(&self.local)
            .reconcile_regular_file_serving_from_base_with_overlay(&reconcile_inodes, &overlay)?;
        Ok(())
    }

    pub(super) fn record_metadata_sync_result(
        &self,
        source: MetadataSyncSource,
        result: &MetaResult<bool>,
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
                self.record_pg_sync_failure(error);
                self.invalidate_remote_validation();
                self.record_metadata_source_result(result);
                self.metadata_sync_errors.fetch_add(1, Ordering::Relaxed);
                if matches!(
                    error,
                    MetaError::ReplayGap { .. } | MetaError::RebootstrapRequired { .. }
                ) {
                    self.mark_rebootstrap_required();
                }
            }
        }
    }

    pub(super) fn sync_metadata_with_source(&self, source: MetadataSyncSource) -> MetaResult<bool> {
        let result = (|| {
            self.ensure_remote_format_validated()?;
            let _guard = self
                .replication_lock
                .lock()
                .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
            let changed = self.pull_foreign_events_locked(None)?;
            Ok(changed)
        })();
        self.record_metadata_sync_result(source, &result);
        result
    }

    pub(super) fn sync_metadata_to_current_head(&self) -> MetaResult<()> {
        // SFS-698: this bounded helper is reserved for explicit background
        // reachability/bootstrap work; engine construction must not call it.
        const MAX_STARTUP_CATCHUP_ROUNDS: usize = 16;

        self.ensure_remote_format_validated()?;
        let target_head = self.remote_metadata_head_version()?;
        for _ in 0..MAX_STARTUP_CATCHUP_ROUNDS {
            if self.local_version.load(Ordering::SeqCst) >= target_head {
                return Ok(());
            }

            let before = self.local_version.load(Ordering::SeqCst);
            self.sync_metadata_with_source(MetadataSyncSource::Manual)?;
            self.local.sync_local_metadata()?;
            let after = self.local_version.load(Ordering::SeqCst);
            if after >= target_head {
                return Ok(());
            }
            if after == before {
                return Err(MetaError::Storage(format!(
                    "postgres metadata catch-up stalled at local version {after} while remote head is {target_head}"
                )));
            }
        }

        Err(MetaError::Storage(
            "postgres metadata catch-up did not reach the frozen remote head".to_string(),
        ))
    }

    pub(super) fn pull_foreign_events_locked(
        &self,
        recent_flush: Option<&RecentFlush>,
    ) -> MetaResult<bool> {
        self.ensure_remote_format_validated()?;
        let target_version = self.frozen_sync_target_version()?;
        self.pull_foreign_events_locked_to_target(recent_flush, target_version)
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
    ) -> MetaResult<bool> {
        self.ensure_remote_format_validated()?;
        let result = self.pull_foreign_events_locked_inner(recent_flush, target_version);
        self.record_remote_operation_result(result)
    }

    fn pull_foreign_events_locked_inner(
        &self,
        recent_flush: Option<&RecentFlush>,
        target_version: i64,
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
                        let replay_state = tx
                            .query_opt(sql::SELECT_METADATA_REPLAY_STATE, &[&volume_id])
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres metadata replay state revalidation failed",
                                    e,
                                )
                            })?;
                        let found = replay_state
                            .as_ref()
                            .and_then(|row| row.get::<_, Option<i16>>(0));
                        crate::ensure_supported_pg_sync_format_version(found)?;
                        let replay_state = replay_state.ok_or_else(|| {
                            MetaError::Storage(
                                "postgres metadata replay state row missing".to_string(),
                            )
                        })?;
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
        };
        let should_log = self.record_pg_sync_page_selected(page_diagnostics);
        // Keep the local event position, serving-state reconcile, and pending
        // replay atomic with respect to local metadata mutations. The network
        // query above deliberately stays outside this critical section.
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
        let mut pending_state = self
            .pending_state
            .lock()
            .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
        let flushed_event_seq = self.flushed_event_seq.load(Ordering::SeqCst);
        let next_event_seq = self.local.get_counter("nextEventSeq")?;
        let initial_overlay = self.local_overlay_conflicts()?;
        let batches = RemoteApplyCoordinator::decode_event_rows(page.rows, client_id.as_str())?;
        RemoteApplyCoordinator::validate_decoded_batch_continuity(
            last_batch_version,
            &batches,
            target_version,
        )?;
        let outcome = self.apply_replay_page_atomically(
            batches,
            recent_flush,
            super::RemoteReplayApplyContext {
                last_seq: last_batch_version,
                current_namespace_version: self.namespace_version.load(Ordering::SeqCst),
                flushed_event_seq,
                next_event_seq,
                initial_overlay,
            },
        )?;
        if outcome.flushed_event_seq.is_some() {
            self.restore_pending_state_from_local_log_with_state(&mut pending_state)?;
        }
        drop(pending_state);
        self.record_pg_sync_page_applied(page_diagnostics);
        Ok(outcome.changed)
    }

    pub(super) fn apply_replay_page_atomically(
        &self,
        batches: Vec<super::remote_apply::DecodedReplayBatch>,
        recent_flush: Option<&super::RecentFlush>,
        context: super::RemoteReplayApplyContext,
    ) -> MetaResult<super::RemoteReplayApplyOutcome> {
        if batches.is_empty() {
            return Ok(super::RemoteReplayApplyOutcome {
                changed: false,
                max_version: context.last_seq,
                max_namespace_version: context.current_namespace_version,
                flushed_event_seq: None,
            });
        }
        let _position_guard = self
            .local_position_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg local position lock poisoned".to_string()))?;
        let txn = self.local.begin_write_txn()?;
        let result = RemoteApplyCoordinator::new(&self.local).apply_decoded_batches_in_txn(
            &txn,
            batches,
            recent_flush,
            context,
        );
        let outcome = match result {
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
        if let Err(error) = Self::write_replay_position_in_txn(&txn, &outcome) {
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
        self.publish_replay_position(&outcome);
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
