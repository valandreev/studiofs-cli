//! Bounded metadata eligibility checks used by standalone object cleanup.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::{MetaGc, MetaInodes, RedbMetaEngine};
use std::path::Path;

/// Maximum number of candidates accepted by one authoritative janitor query.
pub const MAX_JANITOR_ELIGIBILITY_BATCH: usize = 64;

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
    validated_local_version: Option<u64>,
    /// Local references read once per cycle; this process owns the redb file exclusively,
    /// and `validated_local_version` rejects any batch checked after local state moved.
    // ponytail: holds every locally referenced ID (8 B each); switch to an object-ID index if local graphs outgrow memory.
    local_referenced: HashSet<u64>,
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
            validated_local_version: None,
            local_referenced: HashSet::new(),
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

    /// Validates the metadata binding before the janitor lists object-store entries.
    pub fn validate_protection(&mut self) -> Result<(), JanitorMetadataError> {
        if self
            .local
            .has_pending_uploaded_regular_file_recoveries()
            .map_err(backend_unavailable)?
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
        self.postgres
            .validate_protection(&self.format_info, local_version)?;
        self.local_referenced = self
            .local
            .get_local_referenced_object_ids()
            .map_err(backend_unavailable)?;
        self.validated_local_version = Some(local_version);
        Ok(())
    }

    /// Checks only the supplied object IDs against authoritative local and shared metadata.
    pub fn check_eligibility(
        &mut self,
        object_ids: &[u64],
    ) -> Result<JanitorProtection, JanitorMetadataError> {
        if object_ids.len() > MAX_JANITOR_ELIGIBILITY_BATCH {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "janitor eligibility batch exceeds the configured limit",
            ));
        }
        if !self.postgres.lock_is_held() {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        }
        let pending_deletions = self
            .local
            .get_pending_deletions_for(object_ids)
            .map_err(backend_unavailable)?;
        let local_version = self
            .local
            .get_counter(crate::pg_engine::LOCAL_PG_VERSION_COUNTER)
            .map_err(backend_unavailable)?;
        let Some(validated_local_version) = self.validated_local_version else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "janitor protection was not validated",
            ));
        };
        if local_version != validated_local_version {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "local metadata changed during janitor cycle",
            ));
        }
        let remote =
            self.postgres
                .check_eligibility(&self.format_info, local_version, object_ids)?;
        let mut protection = HashMap::with_capacity(object_ids.len());
        for object_id in object_ids {
            let is_protected = self.local_referenced.contains(object_id)
                || remote.referenced.contains(object_id)
                || remote.snapshot_pins.contains(object_id);
            protection.insert(
                *object_id,
                if is_protected {
                    JanitorEligibility::Protected
                } else {
                    JanitorEligibility::Eligible {
                        pending_deletion: pending_deletions.get(object_id).copied(),
                    }
                },
            );
        }
        Ok(protection)
    }
}

impl Drop for JanitorMetadataSession {
    fn drop(&mut self) {
        let _ = self.release_global_janitor_lock();
    }
}

/// Authoritative outcome for one standalone janitor candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JanitorEligibility {
    /// Metadata still reaches or leases this object.
    Protected,
    /// Metadata proves the object is not reachable.
    Eligible {
        /// Timestamp of the local pending deletion marker, when one exists.
        pending_deletion: Option<i64>,
    },
    /// Metadata could not prove either result; callers must retain the object.
    Unknown,
}

/// Per-object authoritative results. Missing IDs are Unknown.
pub type JanitorProtection = HashMap<u64, JanitorEligibility>;
