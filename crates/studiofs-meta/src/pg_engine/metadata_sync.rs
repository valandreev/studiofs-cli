use super::{
    MetadataSyncSource, OverlayConflictSets, PgMetaEngine, RecentFlush, RemoteApplyCoordinator,
};
use crate::engine::{MetaError, MetaInodes, MetaReplication, MetaResult};
use crate::types::FileType;
use std::collections::HashSet;
use std::sync::atomic::Ordering;

impl PgMetaEngine {
    pub(super) fn local_overlay_conflicts(&self) -> MetaResult<OverlayConflictSets> {
        let start = self.flushed_event_seq.load(Ordering::SeqCst);
        let end = self.local.get_counter("nextEventSeq")?;
        if end <= start {
            return Ok(OverlayConflictSets::default());
        }

        let events = self.local.read_events_range(start, end)?;
        Ok(RemoteApplyCoordinator::overlay_conflicts_for_events(
            &events,
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
        RemoteApplyCoordinator::new(self)
            .reconcile_regular_file_serving_from_base_with_overlay(&reconcile_inodes, &overlay)?;
        Ok(())
    }

    fn record_metadata_sync_result(&self, source: MetadataSyncSource, result: &MetaResult<bool>) {
        match result {
            Ok(changed) => {
                self.metadata_last_successful_sync_unix_ms
                    .store(Self::now_millis(), Ordering::Relaxed);
                if source == MetadataSyncSource::FallbackPoll && *changed {
                    self.metadata_fallback_poll_discoveries
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
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
            self.pull_foreign_events_locked(None)
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
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let last_seq = self.local_version.load(Ordering::SeqCst);
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;

        let rows = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "pull foreign events",
                move |client| async move {
                    Self::query_foreign_event_rows(&client, volume_id, last_seq).await
                },
            )
            .await
        })?;

        RemoteApplyCoordinator::new(self).apply_event_rows(
            rows,
            recent_flush,
            last_seq,
            client_id.as_str(),
        )
    }

    pub(super) fn pull_and_reconcile_foreign_events_locked(
        &self,
        recent_flush: Option<&RecentFlush>,
    ) -> MetaResult<bool> {
        let mut changed = self.pull_foreign_events_locked(recent_flush)?;
        changed |= RemoteApplyCoordinator::new(self).reconcile_recent_flush(recent_flush)?;
        Ok(changed)
    }
}
