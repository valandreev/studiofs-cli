use super::{
    LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, LOCAL_PG_NAMESPACE_VERSION_COUNTER,
    LOCAL_PG_VERSION_COUNTER, PgMetaEngine, StartupSnapshotImportMode,
};
use crate::engine::{MetaError, MetaInodes, MetaResult};
use crate::redb_engine::RedbMetaEngine;
use std::sync::atomic::Ordering;

impl PgMetaEngine {
    pub(super) fn load_persisted_local_version(local: &RedbMetaEngine) -> MetaResult<i64> {
        let version = local.get_counter(LOCAL_PG_VERSION_COUNTER)?;
        Ok(version.min(i64::MAX as u64) as i64)
    }

    pub(super) fn load_persisted_namespace_version(local: &RedbMetaEngine) -> MetaResult<i64> {
        let version = local.get_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER)?;
        Ok(version.min(i64::MAX as u64) as i64)
    }

    pub(super) fn persist_local_version(&self, version: i64) -> MetaResult<()> {
        if version < 0 {
            return Ok(());
        }
        let _guard = self
            .local_position_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg local position lock poisoned".to_string()))?;
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

    pub(super) fn persist_flushed_event_seq(&self, seq: u64) -> MetaResult<()> {
        let _guard = self
            .local_position_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg local position lock poisoned".to_string()))?;
        if seq <= self.flushed_event_seq.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.local
            .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, seq)?;
        self.flushed_event_seq.store(seq, Ordering::SeqCst);
        Ok(())
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
        &self,
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
