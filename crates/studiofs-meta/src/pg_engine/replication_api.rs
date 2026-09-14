use super::{MetadataSyncSource, PgMetaEngine, sql};
use crate::engine::{
    MetaError, MetaReplication, MetaResult, MetadataAuthorityStatus,
    MetadataCheckpointFreshnessDiagnostics, MetadataCoherenceProgress,
    MetadataInvalidationDiagnostics, MetadataReplicationBacklog, PgSyncDiagnostics, PgSyncStatus,
};
use crate::types::Ino;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::warn;

impl MetaReplication for PgMetaEngine {
    fn take_invalidation_batches(&self) -> Vec<crate::coherence::InvalidationBatch> {
        self.local.take_invalidation_batches()
    }

    fn flush_metadata(&self) -> MetaResult<()> {
        {
            let _guard = self
                .replication_lock
                .lock()
                .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
            self.ensure_remote_format_validated()?;
            self.replay_local_upload_ledger()?;
            let recent_flush = if let Some(batch) = self.claim_pending_batch(true) {
                self.flush_pending_claimed_batch_with_restore(batch)?
            } else {
                None
            };
            let _ = self.pull_and_reconcile_foreign_events_locked(recent_flush.as_ref())?;
        }
        self.write_checkpoint_with_lease()?;
        self.local.flush_local_metadata()
    }

    fn flush_local_metadata(&self) -> MetaResult<()> {
        self.local.flush_local_metadata()
    }

    fn request_eager_flush(&self) {
        self.request_pending_flush();
        self.signal_poll_wakeup();
    }

    fn sync_metadata(&self) -> MetaResult<()> {
        let _ = self.sync_metadata_with_source(MetadataSyncSource::Manual)?;
        if let Err(error) = self.backfill_current_reference_ledger_if_needed() {
            warn!(
                error = %error,
                volume_id = self.volume_id,
                "postgres snapshot reachability bootstrap did not complete"
            );
        }
        self.local.sync_local_metadata()
    }

    fn sync_local_metadata(&self) -> MetaResult<()> {
        self.local.sync_local_metadata()
    }

    fn note_active_open_inode(&self, ino: Ino) {
        if let Ok(mut active) = self.active_open_inodes.lock() {
            *active.entry(ino).or_insert(0) += 1;
        }
    }

    fn note_inactive_open_inode(&self, ino: Ino) {
        if let Ok(mut active) = self.active_open_inodes.lock()
            && let Some(count) = active.get_mut(&ino)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.remove(&ino);
            }
        }
    }

    fn sync_metadata_for_invalidation_poll(&self) -> MetaResult<()> {
        let _ = self.sync_metadata_with_source(MetadataSyncSource::FallbackPoll)?;
        Ok(())
    }

    fn invalidation_sender_id(&self) -> Option<String> {
        self.current_client_id().ok()
    }

    fn metadata_invalidation_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
        Some(Arc::clone(&self.metadata_invalidation_notify))
    }

    fn metadata_invalidation_diagnostics(&self) -> MetadataInvalidationDiagnostics {
        let last_successful_sync_unix_ms = self
            .metadata_last_successful_sync_unix_ms
            .load(Ordering::Relaxed);
        MetadataInvalidationDiagnostics {
            notifications_received: self.metadata_notify_received.load(Ordering::Relaxed),
            listener_reconnects: self.metadata_notify_reconnects.load(Ordering::Relaxed),
            fallback_poll_discoveries: self
                .metadata_fallback_poll_discoveries
                .load(Ordering::Relaxed),
            sync_errors: self.metadata_sync_errors.load(Ordering::Relaxed),
            last_successful_sync_unix_ms: (last_successful_sync_unix_ms > 0)
                .then_some(last_successful_sync_unix_ms),
        }
    }

    fn metadata_checkpoint_freshness_diagnostics(&self) -> MetadataCheckpointFreshnessDiagnostics {
        self.checkpoint_freshness_diagnostics()
    }

    fn metadata_coherence_progress(&self) -> MetadataCoherenceProgress {
        let applied_version = u64::try_from(self.local_version.load(Ordering::Acquire)).ok();
        let remote_head_version =
            u64::try_from(self.metadata_remote_head_version.load(Ordering::Acquire)).ok();
        MetadataCoherenceProgress {
            applied_version,
            remote_head_version,
            source_connected: self.metadata_source_connected.load(Ordering::Acquire),
            remote_authoritative: true,
        }
    }

    fn metadata_version_token(&self) -> Option<String> {
        Some(self.local_version.load(Ordering::SeqCst).to_string())
    }

    fn namespace_version_token(&self) -> Option<String> {
        Some(self.namespace_version.load(Ordering::SeqCst).to_string())
    }

    fn replication_backlog(&self) -> MetadataReplicationBacklog {
        let pending_ops = self
            .pending_state
            .lock()
            .map(|state| state.ops)
            .unwrap_or(0);
        let inflight_ops = self.inflight_replication_ops.load(Ordering::SeqCst);
        MetadataReplicationBacklog {
            pending_ops,
            inflight_ops,
        }
    }

    fn metadata_authority_status(&self) -> MetadataAuthorityStatus {
        MetadataAuthorityStatus {
            inode_id_headroom: self.inode_id_headroom_available.load(Ordering::Acquire),
            object_id_headroom: self.object_id_headroom_available.load(Ordering::Acquire),
        }
    }

    fn pg_sync_status(&self) -> PgSyncStatus {
        PgSyncStatus::new(
            self.current_pg_sync_lifecycle(),
            self.metadata_authority_status(),
        )
    }

    fn pg_sync_diagnostics(&self) -> PgSyncDiagnostics {
        let status = self.pg_sync_status();
        let (current_page, last_page, last_sync_error, rebootstrap_required) =
            self.pg_sync_progress_snapshot();
        let local_applied_version = u64::try_from(self.local_version.load(Ordering::Acquire)).ok();
        let startup_target_version =
            u64::try_from(self.metadata_startup_target_version.load(Ordering::Acquire)).ok();
        let observed_remote_head_version =
            u64::try_from(self.metadata_remote_head_version.load(Ordering::Acquire)).ok();
        let checkpoint = self.metadata_checkpoint_freshness_diagnostics();
        let remote_head_version = observed_remote_head_version.or(checkpoint.head_version);
        let remaining_version_distance = startup_target_version
            .zip(local_applied_version)
            .map(|(target, local)| target.saturating_sub(local));
        let last_successful_sync_unix_ms = self
            .metadata_last_successful_sync_unix_ms
            .load(Ordering::Acquire);
        let last_successful_sync_unix_ms =
            (last_successful_sync_unix_ms > 0).then_some(last_successful_sync_unix_ms);

        PgSyncDiagnostics {
            local_applied_version,
            startup_target_version,
            remote_head_version,
            remaining_version_distance,
            current_page,
            last_page,
            last_sync_error,
            checkpoint,
            rebootstrap_required,
            source_connected: self.metadata_source_connected.load(Ordering::Acquire),
            sync_errors: self.metadata_sync_errors.load(Ordering::Relaxed),
            last_successful_sync_unix_ms,
            ..PgSyncDiagnostics::from_status(status)
        }
    }

    fn check_consistency(&self) -> MetaResult<()> {
        self.check_remote_identity()
    }

    fn check_consistency_for_mount_admission(&self) -> MetaResult<()> {
        if self.remote_transport_unavailable.load(Ordering::Acquire) {
            return Err(MetaError::RemoteUnavailable(
                "remote PostgreSQL transport is currently unavailable".to_string(),
            ));
        }
        self.check_consistency()
    }

    fn remote_metadata_size(&self) -> MetaResult<Option<u64>> {
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let size = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "fetch remote metadata size",
                move |client| {
                    Box::pin(async move {
                        let row = client
                            .query_one(sql::SELECT_REMOTE_METADATA_SIZE, &[&volume_id])
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error("postgres metadata size query failed", e)
                            })?;
                        let bytes: i64 = row.get(0);
                        Ok(bytes.max(0) as u64)
                    })
                },
            )
            .await
        })?;
        Ok(Some(size))
    }

    fn prepare_for_close(&self) -> MetaResult<()> {
        self.local.seal_relaxed_durability_for_close();
        let _start_guard = self.lock_worker_start_lock.lock().map_err(|_| {
            MetaError::Storage("pg background worker start state poisoned".to_string())
        })?;
        let join_timeout = Self::default_prepare_for_close_join_timeout();
        self.background_workers.shutdown(join_timeout)
    }
}
