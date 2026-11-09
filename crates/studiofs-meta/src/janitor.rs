//! Metadata protection snapshot used by standalone object cleanup.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::{MetaGc, MetaInodes, RedbMetaEngine};
use std::path::Path;

/// A categorized error from the standalone janitor's read-only metadata gate.
#[derive(Debug, thiserror::Error)]
pub enum JanitorMetadataError {
    /// The available metadata does not prove that deletion is safe.
    #[error("unsafe_to_delete: {0}")]
    UnsafeToDelete(&'static str),
    /// A metadata backend or bounded query was unavailable.
    #[error("backend_unavailable: {0}")]
    BackendUnavailable(String),
}

fn backend_unavailable(error: impl fmt::Display) -> JanitorMetadataError {
    JanitorMetadataError::BackendUnavailable(error.to_string())
}

fn pg_backend_unavailable() -> JanitorMetadataError {
    JanitorMetadataError::BackendUnavailable("PostgreSQL janitor metadata is unavailable".into())
}

/// Read-only metadata session used only by the standalone CLI janitor.
///
/// Unlike the normal PostgreSQL metadata engine, opening this session does not
/// repair, synchronize, or create local metadata. PostgreSQL operations are
/// limited to the janitor advisory lock and read-only safety queries.
pub struct JanitorMetadataSession {
    local: RedbMetaEngine,
    format_info: crate::FormatInfo,
    postgres: crate::pg_engine::PgJanitorSession,
}

impl JanitorMetadataSession {
    /// Opens local metadata read-only and prepares the required PostgreSQL gate.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JanitorMetadataError> {
        let format_info = RedbMetaEngine::read_format_info_readonly(path.as_ref())
            .map_err(backend_unavailable)?;
        crate::ensure_supported_format_version(&format_info).map_err(|_| {
            JanitorMetadataError::UnsafeToDelete("local volume format is unsupported")
        })?;
        let local = RedbMetaEngine::open_read_only(path.as_ref()).map_err(backend_unavailable)?;
        let config = format_info
            .metadata
            .postgres
            .as_ref()
            .filter(|_| format_info.metadata.backend == crate::schema::MetadataBackend::Postgres)
            .ok_or(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL metadata is required for destructive janitor work",
            ))?;
        let postgres =
            crate::pg_engine::PgJanitorSession::new(config.url.clone(), config.volume_id);
        Ok(Self {
            local,
            format_info,
            postgres,
        })
    }

    /// The locally persisted format information used to configure object access.
    pub fn format_info(&self) -> &crate::FormatInfo {
        &self.format_info
    }

    /// Acquires the per-volume PostgreSQL janitor lock when metadata is shared.
    pub fn try_acquire_global_janitor_lock(&mut self) -> Result<bool, JanitorMetadataError> {
        self.postgres
            .try_acquire_global_janitor_lock()
            .map_err(|_| pg_backend_unavailable())
    }

    /// Releases the lock and closes its PostgreSQL session.
    pub fn release_global_janitor_lock(&mut self) -> Result<(), JanitorMetadataError> {
        self.postgres
            .release_global_janitor_lock()
            .map_err(|_| pg_backend_unavailable())
    }

    /// Reads a complete protection snapshot without mutating either backend.
    pub fn collect_protection(&mut self) -> Result<JanitorProtection, JanitorMetadataError> {
        if !self
            .local
            .pending_uploaded_regular_file_recoveries()
            .map_err(backend_unavailable)?
            .is_empty()
        {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "local metadata has pending file recovery state",
            ));
        }
        if !self.postgres.lock_is_held() {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        }
        let local_version = self
            .local
            .get_counter(crate::pg_engine::LOCAL_PG_VERSION_COUNTER)
            .map_err(backend_unavailable)?;
        let next_event_seq = self
            .local
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .map_err(backend_unavailable)?;
        let flushed_event_seq = self
            .local
            .get_counter(crate::schema::counters::LOCAL_PG_FLUSHED_EVENT_SEQ)
            .map_err(backend_unavailable)?;
        if self
            .local
            .get_counter(crate::pg_engine::LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)
            .map_err(backend_unavailable)?
            != 0
            || self
                .local
                .get_counter(crate::pg_engine::LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
                .map_err(backend_unavailable)?
                != 0
            || next_event_seq != flushed_event_seq
            || !self
                .local
                .pending_local_upload_ledger(1)
                .map_err(backend_unavailable)?
                .is_empty()
        {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "local PostgreSQL metadata has pending sync or recovery state",
            ));
        }
        let remote = self
            .postgres
            .read_protection(&self.format_info, local_version)?;
        let mut referenced = self
            .local
            .get_local_referenced_object_ids()
            .map_err(backend_unavailable)?;
        referenced.extend(remote.referenced);
        let pending_deletions = self
            .local
            .get_pending_deletions()
            .map_err(backend_unavailable)?
            .into_iter()
            .collect();
        Ok(JanitorProtection {
            referenced,
            snapshot_pins: remote.snapshot_pins,
            pending_deletions,
        })
    }
}

impl Drop for JanitorMetadataSession {
    fn drop(&mut self) {
        let _ = self.release_global_janitor_lock();
    }
}

/// Metadata facts that protect objects from standalone janitor deletion.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct JanitorProtection {
    /// Objects reachable from the current metadata namespace.
    pub referenced: HashSet<u64>,
    /// Objects retained by global snapshot or lifecycle pins.
    pub snapshot_pins: HashSet<u64>,
    /// Objects with pending deletion timestamps.
    pub pending_deletions: HashMap<u64, i64>,
}
