use super::{PgMetaEngine, sql};
use crate::engine::{MetaError, MetaFormat, MetaResult};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::warn;

pub(super) const DEFAULT_CHECKPOINT_LEASE_SECS: u64 = 300;
pub(super) const DEFAULT_CHECKPOINT_RETRY_MS: u64 = 1_000;
const CHECKPOINT_LEASE_SECS_ENV_VAR: &str = "STUDIOFS_PG_CHECKPOINT_LEASE_SECS";
const CHECKPOINT_RETRY_MS_ENV_VAR: &str = "STUDIOFS_PG_CHECKPOINT_RETRY_MS";

/// Coalesces freshness notifications until the coordinator consumes a request.
pub(super) struct CheckpointCoordinatorState {
    requested: AtomicBool,
}

impl CheckpointCoordinatorState {
    pub(super) fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
        }
    }

    pub(super) fn request(&self) -> bool {
        !self.requested.swap(true, Ordering::AcqRel)
    }

    pub(super) fn take_request(&self) -> bool {
        self.requested.swap(false, Ordering::AcqRel)
    }

    pub(super) fn clear(&self) {
        self.requested.store(false, Ordering::Release);
    }
}

#[derive(Clone, Copy)]
struct CheckpointSourceCut {
    version: i64,
    total_event_payload_bytes: i64,
}

struct BuiltCheckpoint {
    metadata_format_version: i16,
    payload: Vec<u8>,
    checkpoint_sha256: Vec<u8>,
    format_info: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(super) struct CheckpointLease {
    pub(super) fencing_token: i64,
}

impl PgMetaEngine {
    pub(super) fn default_checkpoint_lease_duration() -> Duration {
        Duration::from_secs(Self::env_positive_u64(
            CHECKPOINT_LEASE_SECS_ENV_VAR,
            DEFAULT_CHECKPOINT_LEASE_SECS,
        ))
    }

    pub(super) fn default_checkpoint_retry_interval() -> Duration {
        Self::env_duration_ms(CHECKPOINT_RETRY_MS_ENV_VAR, DEFAULT_CHECKPOINT_RETRY_MS)
    }

    pub(super) fn request_checkpoint_refresh(&self) {
        if self.checkpoint_coordinator_state.request() {
            self.background_workers.signal_checkpoint_wakeup();
        }
    }

    pub(super) fn start_background_checkpoint_coordinator(
        engine: Weak<Self>,
        shutdown: Arc<AtomicBool>,
        wakeup: Arc<(Mutex<bool>, Condvar)>,
        retry_interval: Duration,
        lease_duration: Duration,
    ) -> MetaResult<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("pg-meta-checkpoint".into())
            .spawn(move || {
                let mut retry_pending = false;
                loop {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }

                    let (wakeup_lock, wakeup_cv) = &*wakeup;
                    let mut wakeup_guard = wakeup_lock
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let wait_duration = if retry_pending {
                        retry_interval
                    } else {
                        Duration::from_secs(24 * 60 * 60)
                    };
                    let deadline = Instant::now() + wait_duration;
                    let mut timed_out = false;
                    while !*wakeup_guard {
                        if shutdown.load(Ordering::Acquire) {
                            return;
                        }
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            timed_out = true;
                            break;
                        }
                        let wait_result = wakeup_cv.wait_timeout(wakeup_guard, remaining);
                        match wait_result {
                            Ok((guard, timeout)) => {
                                wakeup_guard = guard;
                                if timeout.timed_out() {
                                    timed_out = true;
                                    break;
                                }
                            }
                            Err(poisoned) => {
                                wakeup_guard = poisoned.into_inner().0;
                            }
                        }
                    }
                    *wakeup_guard = false;
                    drop(wakeup_guard);

                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    let Some(engine) = engine.upgrade() else {
                        return;
                    };
                    let requested = engine.checkpoint_coordinator_state.take_request();
                    let retry_due =
                        timed_out && engine.checkpoint_freshness_diagnostics().refresh_needed;
                    if !requested && !retry_due {
                        retry_pending = false;
                        continue;
                    }

                    match engine.run_checkpoint_refresh_once(lease_duration) {
                        Ok(true) => {
                            retry_pending = false;
                        }
                        Ok(false) => {
                            engine.checkpoint_coordinator_state.request();
                            retry_pending = true;
                        }
                        Err(error) => {
                            warn!(
                                error = %error,
                                volume_id = engine.volume_id,
                                "postgres background checkpoint coordination did not complete"
                            );
                            engine.checkpoint_coordinator_state.request();
                            retry_pending = true;
                        }
                    }
                }
            })
            .map_err(|error| {
                MetaError::Storage(format!(
                    "failed to spawn pg-meta-checkpoint thread: {error}"
                ))
            })
    }

    fn run_checkpoint_refresh_once(&self, lease_duration: Duration) -> MetaResult<bool> {
        self.ensure_remote_format_validated()?;
        let owner = self.current_client_id()?;
        let Some(lease) = self.try_claim_checkpoint_lease(&owner, lease_duration)? else {
            return Ok(false);
        };

        let result = self.build_and_publish_checkpoint(&owner, lease);
        if let Err(error) = self.release_checkpoint_lease(&owner, lease) {
            warn!(
                error = %error,
                volume_id = self.volume_id,
                "postgres checkpoint lease release failed; lease expiry will recover ownership"
            );
        }
        result
    }

    fn build_and_publish_checkpoint(
        &self,
        owner: &str,
        lease: CheckpointLease,
    ) -> MetaResult<bool> {
        let Some(cut) = self.capture_checkpoint_source_cut()? else {
            return Ok(!self.checkpoint_freshness_diagnostics().refresh_needed);
        };

        // The local export is a consistent redb transaction. It intentionally
        // runs without replication_lock so ordinary replication can continue.
        let payload = self.local.export_serving_checkpoint_bytes()?;
        let format_info = self.local.get_format_info()?;
        let metadata_format_version = i16::try_from(format_info.format_version).map_err(|_| {
            MetaError::Storage("metadata format version exceeds PostgreSQL SMALLINT".to_string())
        })?;
        let format_info = serde_json::to_vec(&format_info).map_err(|error| {
            MetaError::Serialization(format!("format info encode failed: {error}"))
        })?;
        let checkpoint_sha256 = Sha256::digest(&payload).to_vec();

        let checkpoint = BuiltCheckpoint {
            metadata_format_version,
            payload,
            checkpoint_sha256,
            format_info,
        };
        let Some(observation) =
            self.publish_checkpoint_if_source_cut_is_current(owner, lease, cut, checkpoint)?
        else {
            return Ok(false);
        };

        self.record_checkpoint_freshness_observation(observation);
        Ok(true)
    }

    fn capture_checkpoint_source_cut(&self) -> MetaResult<Option<CheckpointSourceCut>> {
        let _guard = self
            .replication_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
        let freshness = self.read_remote_checkpoint_freshness()?;
        let local_version = self.local_version.load(Ordering::Acquire);
        let local_version_u64 = u64::try_from(local_version).map_err(|_| {
            MetaError::Storage("local metadata version must be nonnegative".to_string())
        })?;

        if freshness.head_version > local_version_u64 {
            self.record_checkpoint_freshness_observation(freshness);
            return Ok(None);
        }
        if freshness.checkpoint_version >= local_version_u64 && freshness.format_info_present {
            self.record_checkpoint_freshness_observation(freshness);
            return Ok(None);
        }

        let total_event_payload_bytes = i64::try_from(freshness.total_event_payload_bytes)
            .map_err(|_| {
                MetaError::Storage(
                    "checkpoint payload byte counter exceeds PostgreSQL BIGINT".to_string(),
                )
            })?;
        Ok(Some(CheckpointSourceCut {
            version: local_version,
            total_event_payload_bytes,
        }))
    }

    pub(super) fn try_claim_checkpoint_lease(
        &self,
        owner: &str,
        lease_for: Duration,
    ) -> MetaResult<Option<CheckpointLease>> {
        let lease_ms = i64::try_from(lease_for.as_millis()).map_err(|_| {
            MetaError::Storage("checkpoint lease duration exceeds PostgreSQL BIGINT".to_string())
        })?;
        if lease_ms <= 0 {
            return Err(MetaError::Storage(
                "checkpoint lease duration must be positive".to_string(),
            ));
        }
        let command_pool = Arc::clone(&self.command_pool);
        let command_timeout = self.command_timeout;
        let volume_id = self.volume_id;
        let owner = owner.to_string();
        let expected_owner = owner.clone();
        let result = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "claim bootstrap checkpoint lease",
                move |client| {
                    Box::pin(async move {
                        let row = client
                            .query_opt(
                                sql::CLAIM_BOOTSTRAP_CHECKPOINT_LEASE,
                                &[&volume_id, &owner, &lease_ms],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres bootstrap checkpoint lease claim failed",
                                    error,
                                )
                            })?;
                        let Some(row) = row else {
                            return Ok(None);
                        };
                        let actual_owner = row.try_get::<_, String>(0).map_err(|error| {
                            Self::classify_pg_error(
                                "postgres bootstrap checkpoint lease owner parse failed",
                                error,
                            )
                        })?;
                        if actual_owner != expected_owner {
                            return Err(MetaError::Storage(
                                "postgres bootstrap checkpoint lease owner changed during claim"
                                    .to_string(),
                            ));
                        }
                        let fencing_token = row.try_get::<_, i64>(2).map_err(|error| {
                            Self::classify_pg_error(
                                "postgres bootstrap checkpoint lease fence parse failed",
                                error,
                            )
                        })?;
                        Ok(Some(CheckpointLease { fencing_token }))
                    })
                },
            )
            .await
        });
        self.record_remote_operation_result(result)
    }

    fn release_checkpoint_lease(&self, owner: &str, lease: CheckpointLease) -> MetaResult<()> {
        let command_pool = Arc::clone(&self.command_pool);
        let command_timeout = self.command_timeout;
        let volume_id = self.volume_id;
        let owner = owner.to_string();
        let fencing_token = lease.fencing_token;
        let result = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "release bootstrap checkpoint lease",
                move |client| {
                    Box::pin(async move {
                        client
                            .execute(
                                sql::RELEASE_BOOTSTRAP_CHECKPOINT_LEASE,
                                &[&volume_id, &owner, &fencing_token],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres bootstrap checkpoint lease release failed",
                                    error,
                                )
                            })?;
                        Ok(())
                    })
                },
            )
            .await
        });
        self.record_remote_operation_result(result)
    }

    fn publish_checkpoint_if_source_cut_is_current(
        &self,
        owner: &str,
        lease: CheckpointLease,
        cut: CheckpointSourceCut,
        checkpoint: BuiltCheckpoint,
    ) -> MetaResult<Option<super::metadata_io::CheckpointFreshnessObservation>> {
        let _guard = self
            .replication_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
        if self.local_version.load(Ordering::Acquire) != cut.version {
            return Ok(None);
        }

        let command_pool = Arc::clone(&self.command_pool);
        let command_timeout = self.command_timeout;
        let volume_id = self.volume_id;
        let owner = owner.to_string();
        let fencing_token = lease.fencing_token;
        let result = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "publish fenced bootstrap checkpoint",
                move |client| {
                    Box::pin(async move {
                        let row = client
                            .query_opt(
                                sql::UPSERT_FENCED_METADATA_CHECKPOINT,
                                &[
                                    &volume_id,
                                    &checkpoint.metadata_format_version,
                                    &cut.version,
                                    &cut.version,
                                    &cut.total_event_payload_bytes,
                                    &checkpoint.payload,
                                    &checkpoint.checkpoint_sha256,
                                    &checkpoint.format_info,
                                    &owner,
                                    &fencing_token,
                                ],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres fenced checkpoint upsert failed",
                                    error,
                                )
                            })?;
                        row.as_ref()
                            .map(super::metadata_io::checkpoint_freshness_observation_from_row)
                            .transpose()
                    })
                },
            )
            .await
        });
        self.record_remote_operation_result(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_state_can_be_rearmed_after_consumption() {
        let state = CheckpointCoordinatorState::new();
        assert!(state.request());
        assert!(state.take_request());
        assert!(state.request());
        state.clear();
        assert!(!state.take_request());
    }
}
