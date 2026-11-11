//! Bounded metadata eligibility checks used by standalone object cleanup.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::{MetaGc, MetaInodes, RedbMetaEngine};
use std::path::Path;

/// Maximum number of candidates accepted by one authoritative janitor query: one
/// S3 listing page, so a scan pays one eligibility round trip per page.
pub const MAX_JANITOR_ELIGIBILITY_BATCH: usize = 1_000;

/// Seconds a deletion claim stays leased; an object's deletes must finish inside it.
pub const JANITOR_DELETION_CLAIM_LEASE_SECS: i64 =
    crate::pg_engine::snapshot_reachability::SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS;

/// One object's entry for [`JanitorMetadataSession::settle_deletions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JanitorDeletionSettlement {
    /// Records the remote deletion outcome of a claim. A confirmed deletion
    /// acknowledges the GC queue row; any other outcome defers it.
    Finished {
        /// Claimed object.
        object_id: u64,
        /// Token from [`JanitorMetadataSession::claim_deletions`].
        fencing_token: i64,
        /// Remote deletion outcome.
        outcome: crate::snapshot_ports::UploadedObjectDeletionOutcome,
    },
    /// Releases a claim whose deletes never started, as retryable, without
    /// deferring the GC queue row.
    Released {
        /// Claimed object.
        object_id: u64,
        /// Token from [`JanitorMetadataSession::claim_deletions`].
        fencing_token: i64,
    },
    /// Defers a protected object's GC queue row so it does not re-occupy the head
    /// of every later page.
    Deferred {
        /// Protected object.
        object_id: u64,
    },
}

impl JanitorDeletionSettlement {
    /// The settled object.
    pub fn object_id(&self) -> &u64 {
        match self {
            Self::Finished { object_id, .. }
            | Self::Released { object_id, .. }
            | Self::Deferred { object_id } => object_id,
        }
    }

    /// The claim's fencing token; `None` for a queue-only deferral.
    pub fn fencing_token(&self) -> Option<i64> {
        match self {
            Self::Finished { fencing_token, .. } | Self::Released { fencing_token, .. } => {
                Some(*fencing_token)
            }
            Self::Deferred { .. } => None,
        }
    }
}

/// Durable provider-independent position for one CLI orphan scan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JanitorSweepCheckpoint {
    /// Last listed key; pending object state retains unfinished canonical groups.
    pub after_key: Option<String>,
    /// Object whose listed keys are still being collected across bounded runs.
    pub pending_object_id: Option<u64>,
    /// Last canonical key observed for the pending object.
    pub pending_key: Option<String>,
}

/// Storage identity the persisted orphan scan belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JanitorSweepScope<'a> {
    /// Stable UUID from volume format metadata.
    pub volume_uuid: &'a str,
    /// Configured object storage endpoint.
    pub endpoint: &'a str,
    /// Configured bucket.
    pub bucket: &'a str,
    /// Normalized data key prefix.
    pub data_prefix: &'a str,
    /// Versioned provider-neutral listing strategy.
    pub listing_strategy: &'a str,
}

/// A categorized error from the standalone janitor's read-only metadata gate.
#[derive(Debug, thiserror::Error)]
pub enum JanitorMetadataError {
    /// The available metadata does not prove that deletion is safe.
    #[error("unsafe_to_delete: {0}")]
    UnsafeToDelete(&'static str),
    /// A metadata backend or bounded query was unavailable.
    #[error("backend_unavailable: {0}")]
    BackendUnavailable(String),
    /// PostgreSQL rejected a janitor statement for a missing table, column or privilege.
    #[error("schema_unavailable: {0}")]
    SchemaUnavailable(String),
    /// The deletion claim was fenced out before completion; the object is retried later.
    #[error("stale_claim: deletion claim is no longer current")]
    StaleClaim,
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
/// limited to the janitor advisory lock, read-only safety queries, retirement of
/// this client's abandoned uploads, and fenced object-level deletion claims in
/// the upload ledger.
pub struct JanitorMetadataSession {
    local: RedbMetaEngine,
    format_info: crate::FormatInfo,
    postgres: crate::pg_engine::PgJanitorSession,
    validated_local_version: Option<u64>,
    /// Local references read once per cycle; this process owns the redb file exclusively,
    /// and `validated_local_version` rejects any batch checked after local state moved.
    // ponytail: holds every locally referenced ID (8 B each); switch to an object-ID index if local graphs outgrow memory.
    local_referenced: HashSet<u64>,
    /// Owner recorded on object-level deletion claims made by this session.
    deletion_owner: String,
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
            deletion_owner: format!(
                "studiofs-cli-janitor:{}:{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ),
        })
    }

    /// The locally persisted format information used to configure object access.
    pub fn format_info(&self) -> &crate::FormatInfo {
        &self.format_info
    }

    /// Loads the versioned scan state after validating volume protection.
    pub fn load_sweep_checkpoint(
        &mut self,
        scope: JanitorSweepScope<'_>,
        persist_initial: bool,
    ) -> Result<JanitorSweepCheckpoint, JanitorMetadataError> {
        self.ensure_validated_cycle()?;
        self.postgres.load_sweep_checkpoint(scope, persist_initial)
    }

    /// Persists a completely processed scan boundary under the held volume lock.
    pub fn save_sweep_checkpoint(
        &mut self,
        scope: JanitorSweepScope<'_>,
        checkpoint: &JanitorSweepCheckpoint,
    ) -> Result<(), JanitorMetadataError> {
        self.ensure_validated_cycle()?;
        self.postgres.save_sweep_checkpoint(scope, checkpoint)
    }

    /// Acquires the per-volume PostgreSQL janitor lock when metadata is shared.
    ///
    /// The lock is released again when the deployed payload GC schema is
    /// missing, incomplete or of another version, so no cycle runs against it.
    pub fn try_acquire_global_janitor_lock(&mut self) -> Result<bool, JanitorMetadataError> {
        let acquired = self
            .postgres
            .try_acquire_global_janitor_lock()
            .map_err(|_| pg_backend_unavailable())?;
        if acquired && let Err(error) = self.postgres.verify_payload_gc_schema() {
            // The schema verdict matters more than a failed unlock; closing the
            // session drops the advisory lock either way.
            let _ = self.postgres.release_global_janitor_lock();
            return Err(error);
        }
        Ok(acquired)
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

    /// Retires the uploads this client abandoned before publication (written and
    /// deleted before their metadata reached PostgreSQL), so this cycle can delete
    /// them. Returns the retired count; a dry run only counts.
    ///
    /// Only the owning client can prove an upload abandoned: `validate_protection`
    /// showed its whole local journal acknowledged, and its pending-deletion
    /// markers are local-only. Unpublished uploads of other clients have no marker
    /// here and stay protected. A failure fails the cycle before any deletion.
    // ponytail: resends every unreferenced local marker each cycle (1,000 per transaction);
    // the read-only redb cannot drop them. Track retired IDs if marker sets grow large.
    pub fn retire_abandoned_uploads(&mut self, dry_run: bool) -> Result<u64, JanitorMetadataError> {
        self.ensure_validated_cycle()?;
        let marked: Vec<(u64, i64)> = self
            .local
            .get_pending_deletions()
            .map_err(backend_unavailable)?
            .into_iter()
            .filter(|(object_id, _)| !self.local_referenced.contains(object_id))
            .collect();
        if marked.is_empty() {
            return Ok(0);
        }
        self.postgres.retire_abandoned_uploads(&marked, dry_run)
    }

    /// Checks only the supplied object IDs against authoritative local and shared metadata.
    pub fn check_eligibility(
        &mut self,
        object_ids: &[u64],
        eligible_before: i64,
    ) -> Result<JanitorProtection, JanitorMetadataError> {
        if object_ids.len() > MAX_JANITOR_ELIGIBILITY_BATCH {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "janitor eligibility batch exceeds the configured limit",
            ));
        }
        let local_version = self.ensure_validated_cycle()?;
        let pending_deletions = self
            .local
            .get_pending_deletions_for(object_ids)
            .map_err(backend_unavailable)?;
        let remote = self.postgres.check_eligibility(
            &self.format_info,
            local_version,
            object_ids,
            eligible_before,
        )?;
        let mut protection = HashMap::with_capacity(object_ids.len());
        for object_id in object_ids {
            let is_protected = self.local_referenced.contains(object_id)
                || remote.referenced.contains(object_id)
                || remote.snapshot_pins.contains(object_id);
            let gc_candidate = remote.gc_candidates.get(object_id).copied();
            protection.insert(
                *object_id,
                if is_protected {
                    JanitorEligibility::Protected
                } else if gc_candidate.is_some_and(|(_, eligible)| !eligible) {
                    JanitorEligibility::Deferred
                } else {
                    let pending_deletion = pending_deletions.get(object_id).copied();
                    let pending_deletion = gc_candidate
                        .map(|(eligible_at, _)| {
                            pending_deletion.map_or(eligible_at, |local| local.max(eligible_at))
                        })
                        .or(pending_deletion);
                    JanitorEligibility::Eligible { pending_deletion }
                },
            );
        }
        Ok(protection)
    }

    /// Claims eligible objects immediately before their remote deletion, in two
    /// PostgreSQL round trips.
    ///
    /// Returns the fencing token of each claimed object for [`Self::settle_deletions`];
    /// an object missing from the result is now protected by metadata. Untracked
    /// objects are fenced too.
    pub fn claim_deletions(
        &mut self,
        object_ids: &[u64],
    ) -> Result<HashMap<u64, i64>, JanitorMetadataError> {
        self.ensure_validated_cycle()?;
        let unreferenced: Vec<u64> = object_ids
            .iter()
            .copied()
            .filter(|object_id| !self.local_referenced.contains(object_id))
            .collect();
        if unreferenced.is_empty() {
            return Ok(HashMap::new());
        }
        self.postgres
            .claim_object_deletions(&unreferenced, &self.deletion_owner)
    }

    /// Records remote deletion outcomes and updates the PG GC queue in one
    /// PostgreSQL round trip.
    ///
    /// Returns the objects whose claim PostgreSQL confirmed. A claim missing from
    /// the result was fenced out, so a caller never reports success that PostgreSQL
    /// did not confirm.
    pub fn settle_deletions(
        &mut self,
        settlements: &[JanitorDeletionSettlement],
    ) -> Result<HashSet<u64>, JanitorMetadataError> {
        if settlements.is_empty() {
            return Ok(HashSet::new());
        }
        self.postgres
            .settle_object_deletions(settlements, &self.deletion_owner)
    }

    /// Reads a bounded keyset page of durable PG GC candidates after the `(eligible_at_micros, object_id)` cursor.
    ///
    /// With `backfill`, it first enqueues legacy `delete_pending` ledger rows; dry runs pass `false`.
    pub fn gc_candidate_page(
        &mut self,
        after: Option<(i64, u64)>,
        limit: usize,
        eligible_before: i64,
        backfill: bool,
    ) -> Result<Vec<(u64, i64)>, JanitorMetadataError> {
        self.ensure_validated_cycle()?;
        self.postgres
            .gc_candidate_page(after, limit, eligible_before, backfill)
    }

    /// Prunes finished deletion fences below their payload namespace watermark.
    ///
    /// Runs under the held Janitor lock in bounded batches; returns the pruned row count.
    pub fn prune_deletion_fences(&mut self) -> Result<u64, JanitorMetadataError> {
        self.ensure_validated_cycle()?;
        self.postgres.prune_object_deletion_fences()
    }

    /// Requires the held lock and unchanged local state since `validate_protection`.
    fn ensure_validated_cycle(&self) -> Result<u64, JanitorMetadataError> {
        if !self.postgres.lock_is_held() {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        }
        let Some(validated_local_version) = self.validated_local_version else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "janitor protection was not validated",
            ));
        };
        let local_version = self
            .local
            .get_counter(crate::pg_engine::LOCAL_PG_VERSION_COUNTER)
            .map_err(backend_unavailable)?;
        if local_version != validated_local_version {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "local metadata changed during janitor cycle",
            ));
        }
        Ok(local_version)
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
    /// A durable PG GC candidate is too young or has a retry delay still active.
    Deferred,
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
