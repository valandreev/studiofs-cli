//! Owns foreign metadata catch-up orchestration and local replay application.

use super::{
    MetadataSyncSource, OverlayConflictSets, PgMetaEngine, RecentFlush, RemoteApplyCoordinator,
};
use crate::engine::{MetaError, MetaInodes, MetaReplication, MetaResult};
use crate::types::FileType;
use std::collections::HashSet;
use std::sync::atomic::Ordering;

impl PgMetaEngine {
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

    fn record_metadata_sync_result(&self, source: MetadataSyncSource, result: &MetaResult<bool>) {
        match result {
            Ok(changed) => {
                self.metadata_source_connected
                    .store(true, Ordering::Release);
                self.metadata_last_successful_sync_unix_ms
                    .store(Self::now_millis(), Ordering::Relaxed);
                if source == MetadataSyncSource::FallbackPoll && *changed {
                    self.metadata_fallback_poll_discoveries
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                self.metadata_source_connected
                    .store(false, Ordering::Release);
                self.metadata_sync_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn sync_metadata_with_source(&self, source: MetadataSyncSource) -> MetaResult<bool> {
        let result = (|| {
            let _guard = self
                .replication_lock
                .lock()
                .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
            let changed = self.pull_foreign_events_locked(None)?;
            let remote_head = self.remote_metadata_head_version()?;
            self.metadata_remote_head_version
                .store(remote_head, Ordering::Release);
            Ok(changed)
        })();
        self.record_metadata_sync_result(source, &result);
        result
    }

    pub(super) fn sync_metadata_to_current_head(&self) -> MetaResult<()> {
        const MAX_STARTUP_CATCHUP_ROUNDS: usize = 16;

        let mut target_head = self.remote_metadata_head_version()?;
        for _ in 0..MAX_STARTUP_CATCHUP_ROUNDS {
            if self.local_version.load(Ordering::SeqCst) >= target_head {
                return Ok(());
            }

            let before = self.local_version.load(Ordering::SeqCst);
            self.sync_metadata()?;
            let after = self.local_version.load(Ordering::SeqCst);
            target_head = self.remote_metadata_head_version()?;
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
            "postgres metadata catch-up did not reach the current remote head".to_string(),
        ))
    }

    pub(super) fn pull_foreign_events_locked(
        &self,
        recent_flush: Option<&RecentFlush>,
    ) -> MetaResult<bool> {
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let last_seq = self.local_version.load(Ordering::SeqCst);
        let command_timeout = self.command_timeout;

        let rows = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "pull foreign events",
                move |client| {
                    Box::pin(async move {
                        let client: &tokio_postgres::Client = client;
                        Self::query_foreign_event_rows(client, volume_id, last_seq).await
                    })
                },
            )
            .await
        })?;

        // Keep the local event position, serving-state reconcile, and pending
        // replay atomic with respect to local metadata mutations. The network
        // query above deliberately stays outside this critical section.
        let mut pending_state = self
            .pending_state
            .lock()
            .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
        let flushed_event_seq = self.flushed_event_seq.load(Ordering::SeqCst);
        let next_event_seq = self.local.get_counter("nextEventSeq")?;
        let initial_overlay = self.local_overlay_conflicts()?;
        let outcome = RemoteApplyCoordinator::new(&self.local).apply_event_rows(
            rows,
            recent_flush,
            client_id.as_str(),
            super::RemoteReplayApplyContext {
                last_seq,
                current_namespace_version: self.namespace_version.load(Ordering::SeqCst),
                flushed_event_seq,
                next_event_seq,
                initial_overlay,
            },
        )?;
        self.persist_replay_position(&outcome)?;
        if outcome.flushed_event_seq.is_some() {
            self.restore_pending_state_from_local_log_with_state(&mut pending_state)?;
        }
        Ok(outcome.changed)
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
