//! Snapshot v2 materialization and publication worker.

use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use futures::{StreamExt, TryStreamExt, stream};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use studiofs_data::object::{
    CLOUD_FRAGMENT_COUNT, CLOUD_FRAGMENT_SIZE, format_object_fragment_keys, read_object,
    validate_cloud_fragment_layout,
};
use studiofs_data::snapshot_key::{
    SnapshotArtifactKey, SnapshotArtifactKeyError, SnapshotArtifactPrefix,
};
use studiofs_data::{DataStore, store::DataError};
use studiofs_meta::schema::{PayloadRef, PayloadRefState};
#[cfg(test)]
use studiofs_meta::snapshot::SnapshotCatalogEntry;
use studiofs_meta::snapshot::{
    SnapshotArtifactDescriptor, SnapshotCleanupClaim, SnapshotId, SnapshotLifecycle,
    SnapshotPolicyEvaluation, SnapshotSha256, SnapshotVolumeContext,
};
#[cfg(test)]
use studiofs_meta::snapshot_ports::SnapshotClaimOrFollow;
use studiofs_meta::snapshot_ports::{
    SnapshotArtifactBytes, SnapshotArtifactKind, SnapshotArtifactLocator, SnapshotArtifactScope,
    SnapshotArtifactStore, SnapshotCleanupStore, SnapshotClock, SnapshotFailure, SnapshotJobClaim,
    SnapshotJobStore, SnapshotPayloadBytes, SnapshotPayloadProof, SnapshotPayloadReader,
    SnapshotPolicyEvaluatorId, SnapshotPolicyStore, SnapshotPortError, SnapshotPortFuture,
    SnapshotReconstruction, SnapshotRetry, SnapshotSourceReader, SnapshotStoredArtifact,
    SnapshotWorkerId,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Format version written to the immutable metadata artifact.
pub const SNAPSHOT_ARTIFACT_FORMAT_VERSION: u16 = 1;
/// Schema version written to the immutable metadata artifact.
pub const SNAPSHOT_ARTIFACT_SCHEMA_VERSION: u16 = 1;
const SNAPSHOT_PAYLOAD_VERIFY_CONCURRENCY: usize = 16;
const SNAPSHOT_CLEANUP_LEASE_FOR: Duration = Duration::from_secs(60);
const SNAPSHOT_CLEANUP_RETRY_DELAY: Duration = Duration::from_secs(1);
const SNAPSHOT_CLEANUP_IDLE_DELAY: Duration = Duration::from_secs(1);
const SNAPSHOT_CLEANUP_EMPTY_LIST_CONFIRMATIONS: usize = 2;

/// Bounded worker configuration for one verified volume.
#[derive(Debug, Clone)]
pub struct SnapshotWorkerConfig {
    /// Lease duration used when claiming and renewing jobs.
    pub lease_for: Duration,
    /// Delay before retrying a transient materialization failure.
    pub retry_delay: Duration,
    /// Delay between empty-queue/catalog polls.
    pub idle_delay: Duration,
    /// Root for all local materializer files.
    pub materializer_root: PathBuf,
}

impl SnapshotWorkerConfig {
    /// Creates the bounded default configuration below `materializer_root`.
    pub fn new(materializer_root: impl Into<PathBuf>) -> Self {
        Self {
            lease_for: Duration::from_secs(60),
            retry_delay: Duration::from_secs(1),
            idle_delay: Duration::from_secs(1),
            materializer_root: materializer_root.into(),
        }
    }
}

/// Wall clock used by the tracked VFS worker.
pub struct SystemSnapshotClock;

impl SnapshotClock for SystemSnapshotClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        Utc::now()
    }

    fn sleep_until<'a>(
        &'a self,
        deadline: chrono::DateTime<Utc>,
        cancel: CancellationToken,
    ) -> SnapshotPortFuture<'a, ()> {
        Box::pin(async move {
            let delay = (deadline - Utc::now()).to_std().unwrap_or_default();
            tokio::select! {
                _ = cancel.cancelled() => Err(SnapshotPortError::Cancelled),
                _ = tokio::time::sleep(delay) => Ok(()),
            }
        })
    }
}

/// Snapshot artifact adapter backed by the configured encrypted DataStore.
pub struct DataStoreSnapshotArtifactStore {
    data_store: Arc<DataStore>,
    volume: SnapshotVolumeContext,
}

impl DataStoreSnapshotArtifactStore {
    /// Creates an adapter scoped to one verified volume identity.
    pub fn new(
        data_store: Arc<DataStore>,
        volume: SnapshotVolumeContext,
    ) -> Result<Self, SnapshotArtifactKeyError> {
        if data_store.volume_uuid() != volume.volume_uuid {
            return Err(SnapshotArtifactKeyError::InvalidVolume);
        }
        Ok(Self { data_store, volume })
    }

    fn key(
        &self,
        locator: &SnapshotArtifactLocator,
    ) -> Result<SnapshotArtifactKey, SnapshotPortError> {
        if locator.volume_id != self.volume.volume_id
            || locator.volume_uuid != self.volume.volume_uuid
        {
            return Err(SnapshotPortError::permanent(
                "invalid_artifact_key",
                "artifact volume identity does not match",
            ));
        }
        SnapshotArtifactKey::from_locator(self.data_store.data_prefix(), locator).map_err(|_| {
            SnapshotPortError::permanent("invalid_artifact_key", "artifact key rejected")
        })
    }

    fn map_data_error(operation: &'static str, error: DataError) -> SnapshotPortError {
        if error.is_retryable() {
            SnapshotPortError::retryable(operation, "artifact backend unavailable")
        } else {
            SnapshotPortError::permanent("artifact_operation", "artifact operation rejected")
        }
    }

    fn stored(
        locator: SnapshotArtifactLocator,
        key: &SnapshotArtifactKey,
        bytes: &[u8],
    ) -> SnapshotStoredArtifact {
        SnapshotStoredArtifact {
            object_key: key.as_str().to_owned(),
            locator,
            byte_length: bytes.len() as u64,
            sha256: digest(bytes),
        }
    }

    async fn list_artifact_locators(
        &self,
        scope: &SnapshotArtifactScope,
    ) -> Result<Vec<SnapshotArtifactLocator>, SnapshotPortError> {
        if scope.volume_id != self.volume.volume_id || scope.volume_uuid != self.volume.volume_uuid
        {
            return Err(SnapshotPortError::permanent(
                "invalid_artifact_scope",
                "artifact scope does not match volume",
            ));
        }
        let prefix = SnapshotArtifactPrefix::from_scope(self.data_store.data_prefix(), scope)
            .map_err(|_| {
                SnapshotPortError::permanent("invalid_artifact_scope", "artifact scope rejected")
            })?;
        let entries = self
            .data_store
            .list_snapshot_artifacts(&prefix)
            .await
            .map_err(|error| Self::map_data_error("list snapshot artifacts", error))?;
        let mut entries = Box::pin(entries);
        let mut locators = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry =
                entry.map_err(|error| Self::map_data_error("list snapshot artifacts", error))?;
            let locator = prefix.parse_key(&entry.key).map_err(|_| {
                SnapshotPortError::permanent("invalid_artifact_key", "listed artifact key rejected")
            })?;
            self.key(&locator)?;
            locators.push(locator);
        }
        Ok(locators)
    }
}

impl SnapshotArtifactStore for DataStoreSnapshotArtifactStore {
    fn put_attempt<'a>(
        &'a self,
        locator: &'a SnapshotArtifactLocator,
        source: &'a Path,
    ) -> SnapshotPortFuture<'a, SnapshotStoredArtifact> {
        Box::pin(async move {
            let key = self.key(locator)?;
            let bytes = Bytes::from(tokio::fs::read(source).await.map_err(|_| {
                SnapshotPortError::retryable(
                    "read snapshot artifact",
                    "artifact source unavailable",
                )
            })?);
            self.data_store
                .put_snapshot_artifact(&key, bytes.clone())
                .await
                .map_err(|error| Self::map_data_error("put snapshot attempt", error))?;
            Ok(Self::stored(locator.clone(), &key, &bytes))
        })
    }

    fn publish_final<'a>(
        &'a self,
        attempt: &'a SnapshotArtifactLocator,
        final_artifact: &'a SnapshotArtifactLocator,
        source: &'a Path,
        expected_sha256: &'a SnapshotSha256,
    ) -> SnapshotPortFuture<'a, SnapshotStoredArtifact> {
        Box::pin(async move {
            self.key(attempt)?;
            let final_key = self.key(final_artifact)?;
            let bytes = Bytes::from(tokio::fs::read(source).await.map_err(|_| {
                SnapshotPortError::retryable(
                    "read snapshot artifact",
                    "artifact source unavailable",
                )
            })?);
            let sha256 = digest(&bytes);
            if &sha256 != expected_sha256 {
                return Err(SnapshotPortError::permanent(
                    "artifact_checksum",
                    "snapshot attempt checksum mismatch",
                ));
            }
            self.data_store
                .put_snapshot_artifact(&final_key, bytes.clone())
                .await
                .map_err(|error| Self::map_data_error("publish snapshot artifact", error))?;
            Ok(Self::stored(final_artifact.clone(), &final_key, &bytes))
        })
    }

    fn get<'a>(
        &'a self,
        locator: &'a SnapshotArtifactLocator,
    ) -> SnapshotPortFuture<'a, SnapshotArtifactBytes> {
        Box::pin(async move {
            let key = self.key(locator)?;
            let bytes = self
                .data_store
                .get_snapshot_artifact(&key)
                .await
                .map_err(|error| Self::map_data_error("read snapshot artifact", error))?
                .to_bytes();
            Ok(SnapshotArtifactBytes {
                sha256: digest(&bytes),
                bytes: bytes.to_vec(),
            })
        })
    }

    fn delete<'a>(&'a self, locator: &'a SnapshotArtifactLocator) -> SnapshotPortFuture<'a, ()> {
        Box::pin(async move {
            let key = self.key(locator)?;
            match self.data_store.delete_snapshot_artifact(&key).await {
                Ok(()) => Ok(()),
                Err(DataError::OpenDal(error)) if error.kind() == opendal::ErrorKind::NotFound => {
                    Ok(())
                }
                Err(error) => Err(Self::map_data_error("delete snapshot artifact", error)),
            }
        })
    }

    fn list<'a>(
        &'a self,
        scope: &'a SnapshotArtifactScope,
    ) -> SnapshotPortFuture<'a, Vec<SnapshotStoredArtifact>> {
        Box::pin(async move {
            let locators = self.list_artifact_locators(scope).await?;
            let mut stored = Vec::with_capacity(locators.len());
            for locator in locators {
                let key = self.key(&locator)?;
                let bytes = self
                    .data_store
                    .get_snapshot_artifact(&key)
                    .await
                    .map_err(|error| Self::map_data_error("read listed artifact", error))?
                    .to_bytes();
                stored.push(Self::stored(locator, &key, &bytes));
            }
            Ok(stored)
        })
    }

    fn list_locators<'a>(
        &'a self,
        scope: &'a SnapshotArtifactScope,
    ) -> SnapshotPortFuture<'a, Vec<SnapshotArtifactLocator>> {
        Box::pin(self.list_artifact_locators(scope))
    }
}

/// Snapshot payload adapter that validates complete immutable cloud objects
/// before returning the exact referenced range.
pub struct DataStoreSnapshotPayloadReader {
    data_store: Arc<DataStore>,
}

impl DataStoreSnapshotPayloadReader {
    /// Creates a payload reader scoped to one verified volume identity.
    pub fn new(
        data_store: Arc<DataStore>,
        volume: SnapshotVolumeContext,
    ) -> Result<Self, SnapshotArtifactKeyError> {
        if data_store.volume_uuid() != volume.volume_uuid {
            return Err(SnapshotArtifactKeyError::InvalidVolume);
        }
        Ok(Self { data_store })
    }

    async fn read_object(&self, object_id: u64) -> Result<Vec<u8>, SnapshotPortError> {
        let keys = format_object_fragment_keys(self.data_store.data_prefix(), object_id);
        let mut assembled = vec![0_u8; keys.len() * CLOUD_FRAGMENT_SIZE];
        let mut lengths = vec![0_usize; CLOUD_FRAGMENT_COUNT];
        let fragments = stream::iter(keys.into_iter().enumerate().map(
            |(fragment_index, key)| async move {
                let object = self.data_store.get_by_key(&key).await.map_err(|error| {
                    DataStoreSnapshotArtifactStore::map_data_error("read snapshot payload", error)
                })?;
                let fragment = read_object(object).map_err(|_| {
                    SnapshotPortError::permanent(
                        "payload_object",
                        "snapshot payload object is invalid",
                    )
                })?;
                Ok::<_, SnapshotPortError>((fragment_index, fragment))
            },
        ))
        .buffer_unordered(CLOUD_FRAGMENT_COUNT)
        .try_collect::<Vec<_>>()
        .await?;
        for (fragment_index, fragment) in fragments {
            let start = fragment_index * CLOUD_FRAGMENT_SIZE;
            let end = start + fragment.len();
            if end > assembled.len() {
                return Err(SnapshotPortError::permanent(
                    "payload_object",
                    "snapshot payload fragment is too large",
                ));
            }
            assembled[start..end].copy_from_slice(&fragment);
            lengths[fragment_index] = fragment.len();
        }
        let total_len = validate_cloud_fragment_layout(&lengths).map_err(|_| {
            SnapshotPortError::permanent(
                "payload_object",
                "snapshot payload fragment layout is invalid",
            )
        })?;
        assembled.truncate(total_len);
        Ok(assembled)
    }

    fn payload_range(
        object: &[u8],
        payload: &PayloadRef,
    ) -> Result<SnapshotPayloadBytes, SnapshotPortError> {
        let start = usize::try_from(payload.offset).map_err(|_| {
            SnapshotPortError::permanent("payload_range", "snapshot payload range is invalid")
        })?;
        let length = usize::try_from(payload.length).map_err(|_| {
            SnapshotPortError::permanent("payload_range", "snapshot payload range is invalid")
        })?;
        let end = start.checked_add(length).ok_or_else(|| {
            SnapshotPortError::permanent("payload_range", "snapshot payload range is invalid")
        })?;
        if end > object.len() {
            return Err(SnapshotPortError::permanent(
                "payload_range",
                "snapshot payload range exceeds object",
            ));
        }
        let bytes = &object[start..end];
        Ok(SnapshotPayloadBytes {
            sha256: digest(bytes),
            bytes: bytes.to_vec(),
        })
    }
}

impl SnapshotPayloadReader for DataStoreSnapshotPayloadReader {
    fn stat_and_read<'a>(
        &'a self,
        payload: &'a PayloadRef,
    ) -> SnapshotPortFuture<'a, SnapshotPayloadBytes> {
        Box::pin(async move {
            let object = self.read_object(payload.object_id).await?;
            Self::payload_range(&object, payload)
        })
    }

    fn stat_and_read_batch<'a>(
        &'a self,
        payloads: &'a [PayloadRef],
    ) -> SnapshotPortFuture<'a, Vec<SnapshotPayloadBytes>> {
        Box::pin(async move {
            let Some(first) = payloads.first() else {
                return Ok(Vec::new());
            };
            if payloads
                .iter()
                .any(|payload| payload.object_id != first.object_id)
            {
                return Err(SnapshotPortError::permanent(
                    "payload_object",
                    "payload batch spans multiple objects",
                ));
            }
            let object = self.read_object(first.object_id).await?;
            payloads
                .iter()
                .map(|payload| Self::payload_range(&object, payload))
                .collect()
        })
    }
}

fn digest(bytes: &[u8]) -> SnapshotSha256 {
    SnapshotSha256::from_bytes(Sha256::digest(bytes).into())
}

fn materializer_attempt_root(
    materializer_root: &Path,
    snapshot_id: &SnapshotId,
    attempt: u64,
) -> Result<PathBuf, SnapshotWorkerError> {
    let snapshot_component = snapshot_id.as_str();
    if snapshot_component.is_empty()
        || snapshot_component == "."
        || snapshot_component == ".."
        || snapshot_component.contains('/')
        || snapshot_component.contains('\\')
    {
        return Err(SnapshotWorkerError::Integrity(
            SnapshotIntegrityError::InvalidMaterializerPath,
        ));
    }
    let snapshot_root = materializer_root.join(snapshot_component);
    if !snapshot_root.starts_with(materializer_root) {
        return Err(SnapshotWorkerError::Integrity(
            SnapshotIntegrityError::InvalidMaterializerPath,
        ));
    }
    Ok(snapshot_root.join("attempts").join(attempt.to_string()))
}

/// Integrity failures that must not publish a Ready snapshot.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum SnapshotIntegrityError {
    /// The reconstructed redb could not be opened or decoded.
    #[error("reconstructed snapshot metadata is invalid")]
    InvalidReconstructedMetadata,
    /// A payload offset and length did not form a valid range.
    #[error("snapshot payload range is invalid")]
    InvalidPayloadRange,
    /// The payload reader returned a different range length.
    #[error("snapshot payload length does not match")]
    PayloadLengthMismatch,
    /// The payload reader returned a different checksum.
    #[error("snapshot payload checksum does not match")]
    PayloadChecksumMismatch,
    /// The catalog identity did not match the worker's verified volume.
    #[error("snapshot catalog identity does not match")]
    CatalogIdentityMismatch,
    /// The retained source descriptor belongs to another snapshot.
    #[error("snapshot source identity does not match")]
    SourceIdentityMismatch,
    /// The artifact adapter returned metadata inconsistent with its attempt.
    #[error("snapshot artifact metadata does not match")]
    ArtifactMetadataMismatch,
    /// The snapshot identifier cannot be used as a local materializer component.
    #[error("snapshot materializer path is invalid")]
    InvalidMaterializerPath,
}

/// Errors returned by the reusable Snapshot v2 worker.
#[derive(Debug, Error)]
pub enum SnapshotWorkerError {
    /// A backend-independent port operation failed.
    #[error("snapshot worker port failure: {0}")]
    Port(#[source] SnapshotPortError),
    /// Reconstructed metadata or remote payload proof was invalid.
    #[error("snapshot worker integrity failure: {0}")]
    Integrity(#[source] SnapshotIntegrityError),
    /// Local I/O or blocking-task failure, with the source intentionally redacted.
    #[error("snapshot worker I/O failed: {operation}")]
    Io { operation: &'static str },
    /// The worker was cancelled before the operation completed.
    #[error("snapshot worker cancelled")]
    Cancelled,
    /// The worker configuration was not usable.
    #[error("snapshot worker configuration is invalid")]
    InvalidConfiguration,
}

const SNAPSHOT_POLICY_EVALUATOR_LEASE_FOR: Duration = Duration::from_secs(60);
const SNAPSHOT_POLICY_EVALUATOR_MAX_POLL: Duration = Duration::from_secs(30);

/// Errors returned by the mounted daily-policy evaluator.
#[derive(Debug, Error)]
pub enum SnapshotPolicyEvaluatorError {
    /// A backend-independent policy port operation failed.
    #[error("snapshot policy evaluator port failure: {0}")]
    Port(#[source] SnapshotPortError),
    /// The evaluator was cancelled during a port call or wait.
    #[error("snapshot policy evaluator cancelled")]
    Cancelled,
}

/// Evaluates one volume's durable daily snapshot policy.
pub struct SnapshotPolicyEvaluator {
    volume: SnapshotVolumeContext,
    evaluator: SnapshotPolicyEvaluatorId,
    policies: Arc<dyn SnapshotPolicyStore>,
    clock: Arc<dyn SnapshotClock>,
}

impl SnapshotPolicyEvaluator {
    /// Creates an evaluator scoped to one verified volume identity.
    pub fn new(
        volume: SnapshotVolumeContext,
        evaluator: SnapshotPolicyEvaluatorId,
        policies: Arc<dyn SnapshotPolicyStore>,
        clock: Arc<dyn SnapshotClock>,
    ) -> Self {
        Self {
            volume,
            evaluator,
            policies,
            clock,
        }
    }

    /// Runs until shutdown, keeping the per-volume evaluator lease fenced.
    pub async fn run(
        self: Arc<Self>,
        cancel: CancellationToken,
    ) -> Result<(), SnapshotPolicyEvaluatorError> {
        let mut lease = loop {
            match await_policy(
                self.policies.claim_evaluator_lease(
                    &self.volume,
                    &self.evaluator,
                    SNAPSHOT_POLICY_EVALUATOR_LEASE_FOR,
                ),
                &cancel,
            )
            .await
            {
                Ok(lease) => break lease,
                Err(SnapshotPolicyEvaluatorError::Port(SnapshotPortError::Permanent {
                    ref code,
                    ..
                })) if code.as_str() == "stale_evaluator_lease" => {
                    match await_policy(
                        self.clock.sleep_until(
                            self.clock.now() + chrono::Duration::seconds(60),
                            cancel.clone(),
                        ),
                        &cancel,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(SnapshotPolicyEvaluatorError::Cancelled) => return Ok(()),
                        Err(error) => return Err(error),
                    }
                }
                Err(SnapshotPolicyEvaluatorError::Cancelled) => return Ok(()),
                Err(error) => return Err(error),
            }
        };

        let result = loop {
            let now = self.clock.now();
            if lease.expires_at - now <= chrono::Duration::seconds(30) {
                lease = await_policy(
                    self.policies
                        .renew_evaluator_lease(&lease, SNAPSHOT_POLICY_EVALUATOR_LEASE_FOR),
                    &cancel,
                )
                .await?;
            }
            let evaluation = await_policy(
                self.policies.evaluate_due_slot(&self.volume, &lease, now),
                &cancel,
            )
            .await?;
            let next_wake_at = match evaluation {
                SnapshotPolicyEvaluation::NoPolicy { next_wake_at }
                | SnapshotPolicyEvaluation::Created { next_wake_at, .. }
                | SnapshotPolicyEvaluation::Followed { next_wake_at, .. }
                | SnapshotPolicyEvaluation::SkippedDstGap { next_wake_at } => next_wake_at,
            };
            let delay = (next_wake_at - self.clock.now())
                .to_std()
                .unwrap_or_default()
                .min(SNAPSHOT_POLICY_EVALUATOR_MAX_POLL);
            match await_policy(
                self.clock
                    .sleep_until(self.clock.now() + delay, cancel.clone()),
                &cancel,
            )
            .await
            {
                Ok(()) => {}
                Err(SnapshotPolicyEvaluatorError::Cancelled) => break Ok(()),
                Err(error) => break Err(error),
            }
        };

        let _ = self.policies.release_evaluator_lease(&lease).await;
        result
    }
}

async fn await_policy<'a, T>(
    future: SnapshotPortFuture<'a, T>,
    cancel: &CancellationToken,
) -> Result<T, SnapshotPolicyEvaluatorError> {
    tokio::select! {
        result = future => result.map_err(|error| match error {
            SnapshotPortError::Cancelled => SnapshotPolicyEvaluatorError::Cancelled,
            error => SnapshotPolicyEvaluatorError::Port(error),
        }),
        _ = cancel.cancelled() => Err(SnapshotPolicyEvaluatorError::Cancelled),
    }
}

/// Reusable Snapshot v2 claim/materialize/upload/publish service.
pub struct SnapshotWorker {
    volume: SnapshotVolumeContext,
    worker: SnapshotWorkerId,
    jobs: Arc<dyn SnapshotJobStore>,
    sources: Arc<dyn SnapshotSourceReader>,
    payloads: Arc<dyn SnapshotPayloadReader>,
    artifacts: Arc<dyn SnapshotArtifactStore>,
    clock: Arc<dyn studiofs_meta::snapshot_ports::SnapshotClock>,
    config: SnapshotWorkerConfig,
}

impl SnapshotWorker {
    /// Creates a worker for one PostgreSQL-verified volume identity.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        volume: SnapshotVolumeContext,
        worker: SnapshotWorkerId,
        jobs: Arc<dyn SnapshotJobStore>,
        sources: Arc<dyn SnapshotSourceReader>,
        payloads: Arc<dyn SnapshotPayloadReader>,
        artifacts: Arc<dyn SnapshotArtifactStore>,
        clock: Arc<dyn studiofs_meta::snapshot_ports::SnapshotClock>,
        config: SnapshotWorkerConfig,
    ) -> Result<Self, SnapshotWorkerError> {
        if config.lease_for.is_zero()
            || config.retry_delay.is_zero()
            || config.idle_delay.is_zero()
            || config.materializer_root.as_os_str().is_empty()
        {
            return Err(SnapshotWorkerError::InvalidConfiguration);
        }
        Ok(Self {
            volume: volume.clone(),
            worker,
            jobs,
            sources,
            payloads,
            artifacts,
            clock,
            config,
        })
    }

    /// Claims queued work until cancellation and publishes only fenced Ready rows.
    pub async fn run(
        self: Arc<Self>,
        cancel: CancellationToken,
    ) -> Result<(), SnapshotWorkerError> {
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let claim = match await_port(
                self.jobs.claim_next(&self.worker, self.config.lease_for),
                &cancel,
            )
            .await
            {
                Ok(claim) => claim,
                Err(SnapshotWorkerError::Cancelled) => return Ok(()),
                Err(SnapshotWorkerError::Port(SnapshotPortError::Retryable { .. })) => {
                    match wait_for_cancel_or_deadline(
                        self.clock.as_ref(),
                        &cancel,
                        self.config.retry_delay,
                    )
                    .await
                    {
                        Ok(()) => continue,
                        Err(SnapshotWorkerError::Cancelled) => return Ok(()),
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            };
            let Some(claim) = claim else {
                match wait_for_cancel_or_deadline(
                    self.clock.as_ref(),
                    &cancel,
                    self.config.idle_delay,
                )
                .await
                {
                    Ok(()) => continue,
                    Err(SnapshotWorkerError::Cancelled) => return Ok(()),
                    Err(error) => return Err(error),
                }
            };

            match self.process_claim(&claim, &cancel).await {
                Ok(()) => {}
                Err(SnapshotWorkerError::Cancelled) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }

    /// Claims a requested ID or follows its durable catalog until it is Ready.
    #[cfg(test)]
    async fn claim_or_follow(
        &self,
        snapshot_id: SnapshotId,
        cancel: CancellationToken,
    ) -> Result<SnapshotCatalogEntry, SnapshotWorkerError> {
        loop {
            let outcome = await_port(
                self.jobs
                    .claim_id_or_follow(&snapshot_id, &self.worker, self.config.lease_for),
                &cancel,
            )
            .await?;
            match outcome {
                SnapshotClaimOrFollow::Claimed(claim) => {
                    match self.process_claim(&claim, &cancel).await {
                        Ok(()) => continue,
                        Err(SnapshotWorkerError::Cancelled) => {
                            return Err(SnapshotWorkerError::Cancelled);
                        }
                        Err(error) => return Err(error),
                    }
                }
                SnapshotClaimOrFollow::Follow(entry) => match entry.lifecycle {
                    studiofs_meta::snapshot::SnapshotLifecycle::Ready => return Ok(entry),
                    studiofs_meta::snapshot::SnapshotLifecycle::Failed => {
                        return Err(SnapshotWorkerError::Port(SnapshotPortError::permanent(
                            "snapshot_failed",
                            "snapshot creation failed",
                        )));
                    }
                    studiofs_meta::snapshot::SnapshotLifecycle::Creating => {
                        wait_for_cancel_or_deadline(
                            self.clock.as_ref(),
                            &cancel,
                            self.config.idle_delay,
                        )
                        .await?;
                    }
                    studiofs_meta::snapshot::SnapshotLifecycle::Deleting
                    | studiofs_meta::snapshot::SnapshotLifecycle::Deleted => {
                        return Err(SnapshotWorkerError::Port(SnapshotPortError::permanent(
                            "snapshot_unavailable",
                            "snapshot is unavailable",
                        )));
                    }
                },
            }
        }
    }

    async fn process_claim(
        &self,
        claim: &SnapshotJobClaim,
        cancel: &CancellationToken,
    ) -> Result<(), SnapshotWorkerError> {
        let active_claim = match self.renew_claim(claim, cancel).await {
            Ok(claim) => claim,
            Err(SnapshotWorkerError::Cancelled) => {
                self.release_claim(claim).await;
                return Err(SnapshotWorkerError::Cancelled);
            }
            Err(error) if is_stale_lease_error(&error) => {
                self.release_claim(claim).await;
                return Ok(());
            }
            Err(error) => {
                tracing::warn!(
                    snapshot_id = %claim.job.snapshot_id,
                    worker = self.worker.as_str(),
                    "snapshot lease renewal was uncertain; stopping before publication"
                );
                return Err(error);
            }
        };
        if cancel.is_cancelled() {
            self.release_claim(&active_claim).await;
            return Err(SnapshotWorkerError::Cancelled);
        }
        let work_cancel = cancel.child_token();
        let renewal_stop = cancel.child_token();
        let materialization = self.materialize_claim(&active_claim, &work_cancel);
        let renewal =
            self.renew_lease_until(&active_claim, renewal_stop.clone(), work_cancel.clone());
        tokio::pin!(materialization);
        tokio::pin!(renewal);
        let materialization = tokio::select! {
            result = &mut materialization => {
                renewal_stop.cancel();
                match renewal.await {
                    Ok(()) => result,
                    Err(error) => Err(error),
                }
            }
            renewal_result = &mut renewal => {
                match renewal_result {
                    Ok(()) => materialization.await,
                    Err(error) => {
                        work_cancel.cancel();
                        let _ = materialization.await;
                        Err(error)
                    }
                }
            }
        };
        match materialization {
            Ok(()) => Ok(()),
            Err(SnapshotWorkerError::Cancelled) => {
                self.release_claim(&active_claim).await;
                Err(SnapshotWorkerError::Cancelled)
            }
            Err(SnapshotWorkerError::Port(SnapshotPortError::Retryable { .. }))
            | Err(SnapshotWorkerError::Io { .. }) => {
                if let Err(error) = self.record_retry(&active_claim, cancel).await {
                    if is_stale_lease_error(&error) {
                        return Ok(());
                    }
                    return Err(error);
                }
                tracing::warn!(
                    snapshot_id = %claim.job.snapshot_id,
                    worker = self.worker.as_str(),
                    "snapshot materialization deferred for retry"
                );
                Ok(())
            }
            Err(SnapshotWorkerError::Port(SnapshotPortError::Permanent { .. }))
            | Err(SnapshotWorkerError::Integrity(_)) => {
                if let Err(error) = self.record_failure(&active_claim, cancel).await {
                    if is_stale_lease_error(&error) {
                        return Ok(());
                    }
                    return Err(error);
                }
                tracing::warn!(
                    snapshot_id = %claim.job.snapshot_id,
                    worker = self.worker.as_str(),
                    "snapshot materialization marked failed"
                );
                Ok(())
            }
            Err(error) if is_stale_lease_error(&error) => {
                self.release_claim(&active_claim).await;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn renew_claim(
        &self,
        claim: &SnapshotJobClaim,
        cancel: &CancellationToken,
    ) -> Result<SnapshotJobClaim, SnapshotWorkerError> {
        let lease = await_port(self.jobs.renew_lease(claim, self.config.lease_for), cancel).await?;
        if lease.owner != claim.lease.owner || lease.fencing_token != claim.lease.fencing_token {
            return Err(SnapshotWorkerError::Port(SnapshotPortError::permanent(
                "stale_lease",
                "snapshot lease generation changed",
            )));
        }
        let mut active = claim.clone();
        active.lease = lease.clone();
        active.job.owner = Some(claim.lease.owner.clone());
        active.job.lease_expires_at = Some(lease.expires_at);
        active.job.fencing_token = claim.lease.fencing_token;
        Ok(active)
    }

    async fn renew_lease_until(
        &self,
        claim: &SnapshotJobClaim,
        stop: CancellationToken,
        work_cancel: CancellationToken,
    ) -> Result<(), SnapshotWorkerError> {
        let interval = (self.config.lease_for / 3).max(Duration::from_millis(1));
        let mut retry_delay = interval;
        let mut lease_expires_at = claim.lease.expires_at;
        loop {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(retry_delay) => {}
            }
            let lease = tokio::select! {
                result = self.jobs.renew_lease(claim, self.config.lease_for) => result,
                _ = stop.cancelled() => return Ok(()),
            };
            match lease {
                Ok(lease) => {
                    if lease.owner != claim.lease.owner
                        || lease.fencing_token != claim.lease.fencing_token
                    {
                        work_cancel.cancel();
                        return Err(SnapshotWorkerError::Port(SnapshotPortError::permanent(
                            "stale_lease",
                            "snapshot lease generation changed",
                        )));
                    }
                    lease_expires_at = lease.expires_at;
                    retry_delay = interval;
                }
                Err(SnapshotPortError::Cancelled) => {
                    return Err(SnapshotWorkerError::Cancelled);
                }
                Err(error @ SnapshotPortError::Retryable { .. }) => {
                    let remaining = (lease_expires_at - self.clock.now())
                        .to_std()
                        .unwrap_or_default();
                    if remaining <= retry_delay {
                        work_cancel.cancel();
                        return Err(SnapshotWorkerError::Port(error));
                    }
                    retry_delay = (interval / 4).max(Duration::from_millis(1));
                    tracing::warn!(
                        snapshot_id = %claim.job.snapshot_id,
                        worker = self.worker.as_str(),
                        "transient lease renewal failure; will retry before expiration"
                    );
                }
                Err(error) => {
                    work_cancel.cancel();
                    return Err(SnapshotWorkerError::Port(error));
                }
            }
        }
    }

    async fn materialize_claim(
        &self,
        claim: &SnapshotJobClaim,
        cancel: &CancellationToken,
    ) -> Result<(), SnapshotWorkerError> {
        let attempt = u64::from(claim.job.retry_count) + 1;
        let attempt_locator = SnapshotArtifactLocator {
            volume_id: self.volume.volume_id,
            volume_uuid: self.volume.volume_uuid.clone(),
            snapshot_id: claim.job.snapshot_id.clone(),
            attempt: Some(attempt),
            kind: SnapshotArtifactKind::Attempt,
        };
        let final_locator = SnapshotArtifactLocator {
            attempt: None,
            kind: SnapshotArtifactKind::Metadata,
            ..attempt_locator.clone()
        };
        let attempt_root = materializer_attempt_root(
            &self.config.materializer_root,
            &claim.job.snapshot_id,
            attempt,
        )?;
        let mut attempt_uploaded = false;
        let mut final_published = false;
        let mut materializer_created = false;
        let mut ready_publish_started = false;
        let result = async {
            let catalog =
                await_port(self.jobs.load_catalog(&claim.job.snapshot_id), cancel).await?;
            if catalog.volume != self.volume || catalog.cut.volume_id() != self.volume.volume_id {
                return Err(SnapshotWorkerError::Integrity(
                    SnapshotIntegrityError::CatalogIdentityMismatch,
                ));
            }

            let source_descriptor =
                await_port(self.sources.load_descriptor(&claim.job.snapshot_id), cancel).await?;
            if source_descriptor.checkpoint_id != claim.job.snapshot_id.as_str() {
                return Err(SnapshotWorkerError::Integrity(
                    SnapshotIntegrityError::SourceIdentityMismatch,
                ));
            }
            tokio::fs::create_dir_all(&attempt_root)
                .await
                .map_err(|_| SnapshotWorkerError::Io {
                    operation: "create snapshot materializer directory",
                })?;
            materializer_created = true;
            let destination = attempt_root.join("metadata.redb");
            let reconstruction = await_reconstruction(
                self.sources
                    .reconstruct(&catalog.cut, &source_descriptor, &destination),
                cancel,
            )
            .await?;
            let payloads = collect_published_payloads(reconstruction.path.clone()).await?;
            let proofs = self.verify_payloads(&payloads, cancel).await?;
            attempt_uploaded = true;
            let stored_attempt = await_port(
                self.artifacts
                    .put_attempt(&attempt_locator, &reconstruction.path),
                cancel,
            )
            .await?;
            if stored_attempt.locator != attempt_locator || stored_attempt.object_key.is_empty() {
                return Err(SnapshotWorkerError::Integrity(
                    SnapshotIntegrityError::ArtifactMetadataMismatch,
                ));
            }
            final_published = true;
            let stored_final = await_port(
                self.artifacts.publish_final(
                    &attempt_locator,
                    &final_locator,
                    &reconstruction.path,
                    &stored_attempt.sha256,
                ),
                cancel,
            )
            .await?;
            if stored_final.locator != final_locator
                || stored_final.object_key.is_empty()
                || stored_final.sha256 != stored_attempt.sha256
                || stored_final.byte_length != stored_attempt.byte_length
            {
                return Err(SnapshotWorkerError::Integrity(
                    SnapshotIntegrityError::ArtifactMetadataMismatch,
                ));
            }
            let descriptor = SnapshotArtifactDescriptor::new(
                stored_final.object_key,
                SNAPSHOT_ARTIFACT_FORMAT_VERSION,
                SNAPSHOT_ARTIFACT_SCHEMA_VERSION,
                stored_final.sha256,
                stored_final.byte_length,
                catalog.cut,
                self.clock.now(),
            )
            .map_err(|_| {
                SnapshotWorkerError::Integrity(SnapshotIntegrityError::ArtifactMetadataMismatch)
            })?;
            ready_publish_started = true;
            await_port(self.jobs.publish_ready(claim, &descriptor, &proofs), cancel)
                .await
                .map(|_| ())
        }
        .await;

        if result.is_ok() {
            if attempt_uploaded {
                self.cleanup_artifact(&attempt_locator).await;
            }
            if materializer_created {
                self.cleanup_materializer(&attempt_root, &claim.job.snapshot_id)
                    .await;
            }
        } else {
            let preserve_final = ready_publish_started
                && match &result {
                    Err(SnapshotWorkerError::Port(SnapshotPortError::Retryable { .. }))
                    | Err(SnapshotWorkerError::Cancelled) => true,
                    Err(SnapshotWorkerError::Port(SnapshotPortError::Permanent {
                        code, ..
                    })) => code.as_str() == "stale_lease",
                    _ => false,
                };
            if final_published && !preserve_final {
                self.cleanup_artifact(&final_locator).await;
            }
            if attempt_uploaded {
                self.cleanup_artifact(&attempt_locator).await;
            }
            if materializer_created {
                self.cleanup_materializer(&attempt_root, &claim.job.snapshot_id)
                    .await;
            }
        }
        result
    }

    async fn cleanup_artifact(&self, locator: &SnapshotArtifactLocator) {
        if let Err(error) = self.artifacts.delete(locator).await {
            tracing::warn!(
                snapshot_id = %locator.snapshot_id,
                kind = ?locator.kind,
                error = %error,
                "snapshot artifact cleanup failed"
            );
        }
    }

    async fn cleanup_materializer(&self, path: &Path, snapshot_id: &SnapshotId) {
        match tokio::fs::remove_dir_all(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(
                    snapshot_id = %snapshot_id,
                    error = %error,
                    "snapshot materializer cleanup failed"
                );
                return;
            }
        }
        let mut parent = path.parent();
        for _ in 0..2 {
            let Some(candidate) = parent else {
                break;
            };
            if candidate == self.config.materializer_root
                || !candidate.starts_with(&self.config.materializer_root)
            {
                break;
            }
            match tokio::fs::remove_dir(candidate).await {
                Ok(()) => parent = candidate.parent(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    parent = candidate.parent();
                }
                Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
                Err(error) => {
                    tracing::warn!(
                        snapshot_id = %snapshot_id,
                        error = %error,
                        "snapshot materializer parent cleanup failed"
                    );
                    break;
                }
            }
        }
    }

    async fn release_claim(&self, claim: &SnapshotJobClaim) {
        if let Err(error) = self.jobs.release_lease(claim).await {
            tracing::warn!(
                snapshot_id = %claim.job.snapshot_id,
                worker = self.worker.as_str(),
                error = %error,
                "snapshot lease release failed after cancellation"
            );
        }
    }

    async fn verify_payloads(
        &self,
        payloads: &[PayloadRef],
        cancel: &CancellationToken,
    ) -> Result<Vec<SnapshotPayloadProof>, SnapshotWorkerError> {
        let mut by_object = BTreeMap::<u64, Vec<PayloadRef>>::new();
        for payload in payloads.iter().copied() {
            by_object
                .entry(payload.object_id)
                .or_default()
                .push(payload);
        }
        let proofs = stream::iter(
            by_object
                .into_values()
                .map(|payloads| self.verify_payload_batch(payloads, cancel)),
        )
        .buffered(SNAPSHOT_PAYLOAD_VERIFY_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
        Ok(proofs.into_iter().flatten().collect())
    }

    async fn verify_payload_batch(
        &self,
        payloads: Vec<PayloadRef>,
        cancel: &CancellationToken,
    ) -> Result<Vec<SnapshotPayloadProof>, SnapshotWorkerError> {
        if payloads.iter().any(|payload| payload.length == 0) {
            return Err(SnapshotWorkerError::Integrity(
                SnapshotIntegrityError::InvalidPayloadRange,
            ));
        }
        let bytes = await_port(self.payloads.stat_and_read_batch(&payloads), cancel).await?;
        if bytes.len() != payloads.len() {
            return Err(SnapshotWorkerError::Integrity(
                SnapshotIntegrityError::PayloadLengthMismatch,
            ));
        }
        payloads
            .into_iter()
            .zip(bytes)
            .map(|(payload, bytes)| Self::verify_payload_bytes(payload, bytes))
            .collect()
    }

    fn verify_payload_bytes(
        payload: PayloadRef,
        bytes: SnapshotPayloadBytes,
    ) -> Result<SnapshotPayloadProof, SnapshotWorkerError> {
        let expected_len = usize::try_from(payload.length).map_err(|_| {
            SnapshotWorkerError::Integrity(SnapshotIntegrityError::PayloadLengthMismatch)
        })?;
        if bytes.bytes.len() != expected_len {
            return Err(SnapshotWorkerError::Integrity(
                SnapshotIntegrityError::PayloadLengthMismatch,
            ));
        }
        let sha256 = digest(&bytes.bytes);
        if bytes.sha256 != sha256 {
            return Err(SnapshotWorkerError::Integrity(
                SnapshotIntegrityError::PayloadChecksumMismatch,
            ));
        }
        Ok(SnapshotPayloadProof {
            object_id: payload.object_id,
            offset: u64::from(payload.offset),
            length: payload.length,
            sha256,
        })
    }

    async fn record_retry(
        &self,
        claim: &SnapshotJobClaim,
        cancel: &CancellationToken,
    ) -> Result<(), SnapshotWorkerError> {
        let next_retry_at = self
            .clock
            .now()
            .checked_add_signed(
                ChronoDuration::from_std(self.config.retry_delay)
                    .map_err(|_| SnapshotWorkerError::InvalidConfiguration)?,
            )
            .ok_or(SnapshotWorkerError::InvalidConfiguration)?;
        await_port(
            self.jobs.mark_retry_wait(
                claim,
                SnapshotRetry {
                    retry_count: claim.job.retry_count.saturating_add(1),
                    next_retry_at,
                    message: studiofs_meta::snapshot_ports::RedactedSnapshotMessage::new(
                        "snapshot materialization retry",
                    ),
                },
            ),
            cancel,
        )
        .await
        .map(|_| ())
    }

    async fn record_failure(
        &self,
        claim: &SnapshotJobClaim,
        cancel: &CancellationToken,
    ) -> Result<(), SnapshotWorkerError> {
        await_port(
            self.jobs.mark_failed(
                claim,
                SnapshotFailure {
                    code: "snapshot_materialization".to_string(),
                    message: studiofs_meta::snapshot_ports::RedactedSnapshotMessage::new(
                        "snapshot materialization failed",
                    ),
                },
            ),
            cancel,
        )
        .await
        .map(|_| ())
    }
}

/// Deletes Snapshot v2 artifacts and finalizes Deleting rows after all
/// active creation and mount leases have drained.
pub struct SnapshotCleanupWorker {
    volume: SnapshotVolumeContext,
    worker: SnapshotWorkerId,
    cleanup: Arc<dyn SnapshotCleanupStore>,
    artifacts: Arc<dyn SnapshotArtifactStore>,
    clock: Arc<dyn SnapshotClock>,
}

impl SnapshotCleanupWorker {
    /// Creates a cleanup worker scoped to one verified volume identity.
    pub fn new(
        volume: SnapshotVolumeContext,
        worker: SnapshotWorkerId,
        cleanup: Arc<dyn SnapshotCleanupStore>,
        artifacts: Arc<dyn SnapshotArtifactStore>,
        clock: Arc<dyn SnapshotClock>,
    ) -> Self {
        Self {
            volume,
            worker,
            cleanup,
            artifacts,
            clock,
        }
    }

    /// Claims cleanup work until cancellation.
    pub async fn run(
        self: Arc<Self>,
        cancel: CancellationToken,
    ) -> Result<(), SnapshotWorkerError> {
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let claim = match await_port(
                self.cleanup
                    .claim_next(&self.worker, SNAPSHOT_CLEANUP_LEASE_FOR),
                &cancel,
            )
            .await
            {
                Ok(claim) => claim,
                Err(SnapshotWorkerError::Cancelled) => return Ok(()),
                Err(SnapshotWorkerError::Port(SnapshotPortError::Retryable { .. })) => {
                    match wait_for_cancel_or_deadline(
                        self.clock.as_ref(),
                        &cancel,
                        SNAPSHOT_CLEANUP_RETRY_DELAY,
                    )
                    .await
                    {
                        Ok(()) => continue,
                        Err(SnapshotWorkerError::Cancelled) => return Ok(()),
                        Err(error) => return Err(error),
                    }
                }
                Err(error) if is_stale_lease_error(&error) => continue,
                Err(error) => return Err(error),
            };
            let Some(claim) = claim else {
                match wait_for_cancel_or_deadline(
                    self.clock.as_ref(),
                    &cancel,
                    SNAPSHOT_CLEANUP_IDLE_DELAY,
                )
                .await
                {
                    Ok(()) => {}
                    Err(SnapshotWorkerError::Cancelled) => return Ok(()),
                    Err(error) => return Err(error),
                }
                continue;
            };

            match self.process_claim(&claim, &cancel).await {
                Ok(()) => {}
                Err(SnapshotWorkerError::Cancelled) => {
                    return Ok(());
                }
                Err(SnapshotWorkerError::Port(SnapshotPortError::Retryable { .. })) => {
                    match wait_for_cancel_or_deadline(
                        self.clock.as_ref(),
                        &cancel,
                        SNAPSHOT_CLEANUP_RETRY_DELAY,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(SnapshotWorkerError::Cancelled) => return Ok(()),
                        Err(error) => return Err(error),
                    }
                }
                Err(error) if is_stale_lease_error(&error) => continue,
                Err(error) => {
                    tracing::error!(
                        snapshot_id = %claim.catalog.snapshot_id,
                        error = %error,
                        "snapshot cleanup claim failed; retaining lease until expiry"
                    );
                    match wait_for_cancel_or_deadline(
                        self.clock.as_ref(),
                        &cancel,
                        SNAPSHOT_CLEANUP_RETRY_DELAY,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(SnapshotWorkerError::Cancelled) => return Ok(()),
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    async fn process_claim(
        &self,
        claim: &SnapshotCleanupClaim,
        cancel: &CancellationToken,
    ) -> Result<(), SnapshotWorkerError> {
        if claim.catalog.volume != self.volume
            || claim.catalog.cut.volume_id() != self.volume.volume_id
            || claim.catalog.lifecycle != SnapshotLifecycle::Deleting
        {
            return Err(SnapshotWorkerError::Port(SnapshotPortError::permanent(
                "invalid_cleanup_claim",
                "snapshot cleanup claim identity or lifecycle is invalid",
            )));
        }

        let expected_snapshot_id = claim.catalog.snapshot_id.clone();
        let expected_owner = claim.lease.owner.clone();
        let expected_fencing_token = claim.lease.fencing_token;
        let claim = match await_port(
            self.cleanup.renew_lease(claim, SNAPSHOT_CLEANUP_LEASE_FOR),
            cancel,
        )
        .await
        {
            Ok(claim) => claim,
            Err(SnapshotWorkerError::Cancelled) => {
                self.release_claim(claim).await;
                return Err(SnapshotWorkerError::Cancelled);
            }
            Err(error) => return Err(error),
        };
        if claim.catalog.volume != self.volume
            || claim.catalog.cut.volume_id() != self.volume.volume_id
            || claim.catalog.snapshot_id != expected_snapshot_id
            || claim.catalog.lifecycle != SnapshotLifecycle::Deleting
            || claim.lease.owner != expected_owner
            || claim.lease.fencing_token != expected_fencing_token
        {
            return Err(SnapshotWorkerError::Port(SnapshotPortError::permanent(
                "invalid_cleanup_claim",
                "renewed snapshot cleanup claim identity or lifecycle is invalid",
            )));
        }
        let work_cancel = cancel.child_token();
        let renewal_stop = cancel.child_token();
        let cleanup = self.cleanup_artifacts(&claim, &work_cancel);
        let renewal =
            self.renew_cleanup_lease_until(&claim, renewal_stop.clone(), work_cancel.clone());
        tokio::pin!(cleanup);
        tokio::pin!(renewal);
        let result = tokio::select! {
            result = &mut cleanup => {
                renewal_stop.cancel();
                match renewal.await {
                    Ok(()) => result,
                    Err(error) => Err(error),
                }
            }
            renewal_result = &mut renewal => {
                match renewal_result {
                    Ok(()) => cleanup.await,
                    Err(error) => {
                        work_cancel.cancel();
                        let _ = cleanup.await;
                        Err(error)
                    }
                }
            }
        };
        match result {
            Ok(()) => Ok(()),
            Err(error) if is_stale_lease_error(&error) => {
                self.release_claim(&claim).await;
                Ok(())
            }
            Err(SnapshotWorkerError::Cancelled) => {
                self.release_claim(&claim).await;
                Err(SnapshotWorkerError::Cancelled)
            }
            Err(error @ SnapshotWorkerError::Port(SnapshotPortError::Retryable { .. })) => {
                self.release_claim(&claim).await;
                Err(error)
            }
            // Keep non-retryable failures fenced until this lease expires. The
            // claim query can then skip this row and advance to later cleanup
            // work instead of repeatedly reclaiming a poisoned snapshot.
            Err(error) => Err(error),
        }
    }

    async fn cleanup_artifacts(
        &self,
        claim: &SnapshotCleanupClaim,
        cancel: &CancellationToken,
    ) -> Result<(), SnapshotWorkerError> {
        let scope = SnapshotArtifactScope {
            volume_id: self.volume.volume_id,
            volume_uuid: self.volume.volume_uuid.clone(),
            snapshot_id: claim.catalog.snapshot_id.clone(),
        };
        let mut active_claim = claim.clone();
        let mut empty_list_confirmations = 0;
        loop {
            let artifacts = await_port(self.artifacts.list_locators(&scope), cancel).await?;
            if artifacts.is_empty() {
                empty_list_confirmations += 1;
                if empty_list_confirmations >= SNAPSHOT_CLEANUP_EMPTY_LIST_CONFIRMATIONS {
                    break;
                }
                // A provider may briefly hide a PUT that was already in
                // flight. Require a second empty observation before the
                // catalog can be committed as Deleted.
                wait_for_cancel_or_deadline(
                    self.clock.as_ref(),
                    cancel,
                    SNAPSHOT_CLEANUP_RETRY_DELAY,
                )
                .await?;
                continue;
            }

            empty_list_confirmations = 0;
            for locator in artifacts {
                if locator.volume_id != self.volume.volume_id
                    || locator.volume_uuid != self.volume.volume_uuid
                    || locator.snapshot_id != claim.catalog.snapshot_id
                {
                    return Err(SnapshotWorkerError::Port(SnapshotPortError::permanent(
                        "invalid_artifact_scope",
                        "listed artifact does not match cleanup scope",
                    )));
                }
                active_claim = await_port(
                    self.cleanup
                        .renew_lease(&active_claim, SNAPSHOT_CLEANUP_LEASE_FOR),
                    cancel,
                )
                .await?;
                if active_claim.catalog.volume != self.volume
                    || active_claim.catalog.cut.volume_id() != self.volume.volume_id
                    || active_claim.catalog.snapshot_id != claim.catalog.snapshot_id
                    || active_claim.catalog.lifecycle != SnapshotLifecycle::Deleting
                    || active_claim.lease.owner != claim.lease.owner
                    || active_claim.lease.fencing_token != claim.lease.fencing_token
                {
                    return Err(SnapshotWorkerError::Port(SnapshotPortError::permanent(
                        "invalid_cleanup_claim",
                        "renewed snapshot cleanup claim identity or lifecycle is invalid",
                    )));
                }
                await_port(self.artifacts.delete(&locator), cancel).await?;
            }
        }

        let finalized = await_port(self.cleanup.finalize_deleted(&active_claim), cancel).await?;
        if finalized.snapshot_id != claim.catalog.snapshot_id
            || finalized.volume != self.volume
            || finalized.lifecycle != SnapshotLifecycle::Deleted
        {
            return Err(SnapshotWorkerError::Port(SnapshotPortError::permanent(
                "invalid_cleanup_result",
                "snapshot cleanup finalization identity or lifecycle is invalid",
            )));
        }
        Ok(())
    }

    async fn renew_cleanup_lease_until(
        &self,
        claim: &SnapshotCleanupClaim,
        stop: CancellationToken,
        work_cancel: CancellationToken,
    ) -> Result<(), SnapshotWorkerError> {
        let interval = (SNAPSHOT_CLEANUP_LEASE_FOR / 3).max(Duration::from_millis(1));
        let mut retry_delay = interval;
        let mut lease_expires_at = claim.lease.expires_at;
        loop {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(retry_delay) => {}
            }
            let lease = tokio::select! {
                result = self.cleanup.renew_lease(claim, SNAPSHOT_CLEANUP_LEASE_FOR) => result,
                _ = stop.cancelled() => return Ok(()),
            };
            match lease {
                Ok(renewed) => {
                    if renewed.lease.owner != claim.lease.owner
                        || renewed.lease.fencing_token != claim.lease.fencing_token
                    {
                        work_cancel.cancel();
                        return Err(SnapshotWorkerError::Port(SnapshotPortError::permanent(
                            "stale_lease",
                            "snapshot cleanup lease generation changed",
                        )));
                    }
                    lease_expires_at = renewed.lease.expires_at;
                    retry_delay = interval;
                }
                Err(SnapshotPortError::Cancelled) => return Err(SnapshotWorkerError::Cancelled),
                Err(error @ SnapshotPortError::Retryable { .. }) => {
                    let remaining = (lease_expires_at - self.clock.now())
                        .to_std()
                        .unwrap_or_default();
                    if remaining <= retry_delay {
                        work_cancel.cancel();
                        return Err(SnapshotWorkerError::Port(error));
                    }
                    retry_delay = (interval / 4).max(Duration::from_millis(1));
                }
                Err(error) => {
                    work_cancel.cancel();
                    return Err(SnapshotWorkerError::Port(error));
                }
            }
        }
    }

    async fn release_claim(&self, claim: &SnapshotCleanupClaim) {
        let _ = self.cleanup.release_lease(claim).await;
    }
}

async fn wait_for_cancel_or_deadline(
    clock: &dyn SnapshotClock,
    cancel: &CancellationToken,
    delay: Duration,
) -> Result<(), SnapshotWorkerError> {
    let deadline = clock
        .now()
        .checked_add_signed(
            ChronoDuration::from_std(delay)
                .map_err(|_| SnapshotWorkerError::InvalidConfiguration)?,
        )
        .ok_or(SnapshotWorkerError::InvalidConfiguration)?;
    await_port(clock.sleep_until(deadline, cancel.clone()), cancel).await
}

fn is_stale_lease_error(error: &SnapshotWorkerError) -> bool {
    matches!(
        error,
        SnapshotWorkerError::Port(SnapshotPortError::Permanent { code, .. })
            if code.as_str() == "stale_lease"
    )
}

async fn await_port<'a, T>(
    future: SnapshotPortFuture<'a, T>,
    cancel: &CancellationToken,
) -> Result<T, SnapshotWorkerError> {
    tokio::select! {
        result = future => result.map_err(|error| match error {
            SnapshotPortError::Cancelled => SnapshotWorkerError::Cancelled,
            error => SnapshotWorkerError::Port(error),
        }),
        _ = cancel.cancelled() => Err(SnapshotWorkerError::Cancelled),
    }
}

async fn await_reconstruction<'a>(
    future: SnapshotPortFuture<'a, SnapshotReconstruction>,
    cancel: &CancellationToken,
) -> Result<SnapshotReconstruction, SnapshotWorkerError> {
    tokio::pin!(future);
    tokio::select! {
        result = &mut future => result.map_err(|error| match error {
            SnapshotPortError::Cancelled => SnapshotWorkerError::Cancelled,
            error => SnapshotWorkerError::Port(error),
        }),
        _ = cancel.cancelled() => {
            let _ = future.await;
            Err(SnapshotWorkerError::Cancelled)
        }
    }
}

async fn collect_published_payloads(path: PathBuf) -> Result<Vec<PayloadRef>, SnapshotWorkerError> {
    tokio::task::spawn_blocking(move || {
        let engine = studiofs_meta::RedbMetaEngine::open(&path).map_err(|_| {
            SnapshotWorkerError::Integrity(SnapshotIntegrityError::InvalidReconstructedMetadata)
        })?;
        let mut payloads = BTreeMap::<(u64, u32, u32), PayloadRef>::new();
        engine
            .visit_regular_file_base_chunks(|_, chunk| {
                for slice in chunk.slices.values() {
                    if slice.payload_state == PayloadRefState::Published
                        && let Some(payload) = slice.payload
                    {
                        let key = (payload.object_id, payload.offset, payload.length);
                        payloads.insert(key, payload);
                    }
                }
            })
            .map_err(|_| {
                SnapshotWorkerError::Integrity(SnapshotIntegrityError::InvalidReconstructedMetadata)
            })?;
        Ok(payloads.into_values().collect())
    })
    .await
    .map_err(|_| SnapshotWorkerError::Io {
        operation: "read reconstructed snapshot metadata",
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use opendal::services::Memory;
    use std::path::{Path, PathBuf};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use studiofs_meta::EncryptionConfig;
    use studiofs_meta::schema::PayloadRef;
    use studiofs_meta::snapshot::{
        SnapshotArtifactDescriptor, SnapshotCatalogEntry, SnapshotCleanupClaim,
        SnapshotCleanupLease, SnapshotCreationJob, SnapshotCreationLease, SnapshotCreationState,
        SnapshotCut, SnapshotDailyPolicy, SnapshotId, SnapshotLifecycle, SnapshotOrigin,
        SnapshotPolicyEvaluation, SnapshotPolicyEvaluatorLease, SnapshotPolicyRecord,
        SnapshotRetention, SnapshotSha256, SnapshotVolumeContext,
    };
    use studiofs_meta::snapshot_ports::{
        SnapshotArtifactBytes, SnapshotArtifactKind, SnapshotArtifactLocator,
        SnapshotArtifactScope, SnapshotArtifactStore, SnapshotClaimOrFollow, SnapshotCleanupStore,
        SnapshotClock, SnapshotFailure, SnapshotJobClaim, SnapshotJobStore, SnapshotPayloadBytes,
        SnapshotPayloadProof, SnapshotPayloadReader, SnapshotPolicyEvaluatorId,
        SnapshotPolicyStore, SnapshotPortError, SnapshotPortFuture, SnapshotRetry,
        SnapshotSourceDescriptor, SnapshotSourceReader, SnapshotStoredArtifact, SnapshotWorkerId,
    };
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    const VOLUME_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[tokio::test]
    async fn evaluator_cancellation_releases_or_expires_lease() {
        let volume = SnapshotVolumeContext::new(7, "projectfs", VOLUME_UUID).expect("volume");
        let store = Arc::new(PolicyEvaluatorTestStore::new());
        let evaluator = Arc::new(SnapshotPolicyEvaluator::new(
            volume,
            SnapshotPolicyEvaluatorId::new("evaluator-a").expect("evaluator id"),
            store.clone(),
            Arc::new(TestClock {
                now: Utc
                    .with_ymd_and_hms(2026, 8, 14, 9, 0, 0)
                    .single()
                    .expect("timestamp"),
            }),
        ));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(evaluator.run(cancel.clone()));

        store.claimed.notified().await;
        cancel.cancel();

        assert!(task.await.expect("evaluator join").is_ok());
        assert!(store.released.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn evaluator_waits_when_peer_owns_lease() {
        let volume = SnapshotVolumeContext::new(7, "projectfs", VOLUME_UUID).expect("volume");
        let store = Arc::new(PolicyEvaluatorTestStore::new());
        store.stale_claim.store(true, Ordering::SeqCst);
        let evaluator = Arc::new(SnapshotPolicyEvaluator::new(
            volume,
            SnapshotPolicyEvaluatorId::new("evaluator-b").expect("evaluator id"),
            store.clone(),
            Arc::new(TestClock {
                now: Utc
                    .with_ymd_and_hms(2026, 8, 14, 9, 0, 0)
                    .single()
                    .expect("timestamp"),
            }),
        ));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(evaluator.run(cancel.clone()));

        store.claimed.notified().await;
        cancel.cancel();

        assert!(task.await.expect("evaluator join").is_ok());
        assert!(!store.released.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn worker_publishes_reconstructed_artifact_after_fenced_upload() {
        let captured_at = Utc
            .with_ymd_and_hms(2026, 8, 13, 12, 0, 0)
            .single()
            .expect("valid test timestamp");
        let snapshot_id = SnapshotId::from_cut("projectfs", captured_at).expect("snapshot id");
        let cut = SnapshotCut::new(7, 4, captured_at).expect("snapshot cut");
        let volume = SnapshotVolumeContext::new(7, "projectfs", VOLUME_UUID).expect("volume");
        let catalog = SnapshotCatalogEntry {
            snapshot_id: snapshot_id.clone(),
            volume: volume.clone(),
            origin: SnapshotOrigin::Manual,
            cut,
            lifecycle: SnapshotLifecycle::Creating,
            creation_state: SnapshotCreationState::Running,
            retention: SnapshotRetention::manual(None).expect("retention"),
            ready_at: None,
            expires_at: None,
            artifact: None,
            failure: None,
        };
        let claim = SnapshotJobClaim {
            job: SnapshotCreationJob {
                snapshot_id: snapshot_id.clone(),
                state: SnapshotCreationState::Running,
                retry_count: 0,
                owner: Some("worker-a".to_string()),
                lease_expires_at: Some(captured_at + chrono::Duration::minutes(1)),
                fencing_token: 11,
                next_retry_at: None,
                failure: None,
            },
            lease: SnapshotCreationLease {
                owner: "worker-a".to_string(),
                expires_at: captured_at + chrono::Duration::minutes(1),
                fencing_token: 11,
            },
        };
        let source_descriptor = SnapshotSourceDescriptor {
            checkpoint_id: snapshot_id.to_string(),
            checkpoint_version: 4,
            checkpoint_format_version: 1,
            checkpoint_schema_version: 1,
            event_start_seq: None,
            event_end_seq: None,
            event_start_version: None,
            event_end_version: None,
            event_row_count: 0,
            replay_proof: SnapshotSha256::from_bytes([1; 32]),
        };
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = source_dir.path().join("source.redb");
        studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");

        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        let artifacts = Arc::new(TestArtifactStore::new());
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                }),
                Arc::new(TestPayloadReader::empty()),
                artifacts.clone(),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new(source_dir.path().join("materializer")),
            )
            .expect("worker"),
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(worker.run(cancel.clone()));

        jobs.published.notified().await;
        cancel.cancel();
        assert!(task.await.expect("worker join").is_ok());

        let published = jobs.published_artifact.lock().expect("published lock");
        let (descriptor, proofs) = published.as_ref().expect("ready publication");
        assert_eq!(
            descriptor.object_key,
            "metadata/snapshots/550e8400-e29b-41d4-a716-446655440000/projectfs.2026-08-13T12-00-00.000Z/metadata.redb"
        );
        assert_eq!(descriptor.cut, cut);
        assert!(proofs.is_empty());
        assert_eq!(artifacts.deletes.load(Ordering::SeqCst), 1);
        assert!(
            !source_dir
                .path()
                .join("materializer")
                .join(snapshot_id.as_str())
                .join("attempts")
                .join("1")
                .exists()
        );
    }

    #[tokio::test]
    async fn worker_retries_transient_claim_errors() {
        let (volume, catalog, claim, source_descriptor, _captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = source_dir.path().join("source.redb");
        studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        *jobs.claim_error.lock().expect("claim error lock") = Some(SnapshotPortError::retryable(
            "claim snapshot job",
            "backend unavailable",
        ));
        let mut config = SnapshotWorkerConfig::new(source_dir.path().join("materializer"));
        config.retry_delay = Duration::from_millis(1);
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                }),
                Arc::new(TestPayloadReader::empty()),
                Arc::new(TestArtifactStore::new()),
                Arc::new(SystemSnapshotClock),
                config,
            )
            .expect("worker"),
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(worker.run(cancel.clone()));

        jobs.published.notified().await;
        cancel.cancel();
        assert!(task.await.expect("worker join").is_ok());
        assert!(jobs.claim_count.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn worker_marks_short_published_payload_failed_before_final_publish() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = published_payload_source(source_dir.path());

        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        let artifacts = Arc::new(TestArtifactStore::new());
        let worker = Arc::new(
            SnapshotWorker::new(
                volume.clone(),
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                }),
                Arc::new(TestPayloadReader::new(SnapshotPayloadBytes {
                    bytes: vec![1, 2],
                    sha256: SnapshotSha256::from_bytes([2; 32]),
                })),
                artifacts.clone(),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new(source_dir.path().join("materializer")),
            )
            .expect("worker"),
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(worker.run(cancel.clone()));

        jobs.failed.notified().await;
        cancel.cancel();
        assert!(task.await.expect("worker join").is_ok());
        assert_eq!(artifacts.final_publishes.load(Ordering::SeqCst), 0);
        assert!(jobs.failed_catalog.lock().expect("failed lock").is_some());
    }

    #[tokio::test]
    async fn worker_rejects_source_descriptor_for_another_snapshot() {
        let (volume, catalog, claim, mut source_descriptor, captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = source_dir.path().join("source.redb");
        studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");
        source_descriptor.checkpoint_id = "otherfs.2026-08-13T12-00-00.000Z".to_string();
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        let artifacts = Arc::new(TestArtifactStore::new());
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                }),
                Arc::new(TestPayloadReader::empty()),
                artifacts.clone(),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new(source_dir.path().join("materializer")),
            )
            .expect("worker"),
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(worker.run(cancel.clone()));

        jobs.failed.notified().await;
        cancel.cancel();
        assert!(task.await.expect("worker join").is_ok());
        assert_eq!(artifacts.final_publishes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn worker_records_retryable_payload_read_without_ready_publication() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = published_payload_source(source_dir.path());
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        let artifacts = Arc::new(TestArtifactStore::new());
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                }),
                Arc::new(TestPayloadReader::retryable()),
                artifacts.clone(),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new(source_dir.path().join("materializer")),
            )
            .expect("worker"),
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(worker.run(cancel.clone()));

        jobs.retry.notified().await;
        cancel.cancel();
        assert!(task.await.expect("worker join").is_ok());
        assert_eq!(artifacts.final_publishes.load(Ordering::SeqCst), 0);
        assert_eq!(
            jobs.retry_job
                .lock()
                .expect("retry lock")
                .as_ref()
                .expect("retry job")
                .state,
            SnapshotCreationState::RetryWait
        );
    }

    #[tokio::test]
    async fn claim_or_follow_does_not_wait_forever_after_owned_retry() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = published_payload_source(source_dir.path());
        let snapshot_id = claim.job.snapshot_id.clone();
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        jobs.ready_after_retry.store(true, Ordering::SeqCst);
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                }),
                Arc::new(TestPayloadReader::retryable()),
                Arc::new(TestArtifactStore::new()),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new(source_dir.path().join("materializer")),
            )
            .expect("worker"),
        );

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            worker.claim_or_follow(snapshot_id, CancellationToken::new()),
        )
        .await
        .expect("claim_or_follow should finish after the retry is observed")
        .expect("ready catalog");
        assert_eq!(result.lifecycle, SnapshotLifecycle::Ready);
    }

    #[tokio::test]
    async fn claim_or_follow_survives_claimant_cancellation() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = source_dir.path().join("source.redb");
        studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");
        let snapshot_id = claim.job.snapshot_id.clone();
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        jobs.renew_blocked.store(true, Ordering::SeqCst);
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                }),
                Arc::new(TestPayloadReader::empty()),
                Arc::new(TestArtifactStore::new()),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new(source_dir.path().join("materializer")),
            )
            .expect("worker"),
        );

        let cancel = CancellationToken::new();
        let renew_started = jobs.renew_started.notified();
        let task_worker = worker.clone();
        let task_cancel = cancel.clone();
        let task =
            tokio::spawn(
                async move { task_worker.claim_or_follow(snapshot_id, task_cancel).await },
            );
        renew_started.await;
        cancel.cancel();
        jobs.renew_release.notify_one();
        let result = task.await.expect("claim or follow join");

        assert!(matches!(result, Err(SnapshotWorkerError::Cancelled)));
        assert_eq!(jobs.release_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn collect_published_payloads_keeps_distinct_lengths_at_shared_offset() {
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = published_payload_source_with_payloads(
            source_dir.path(),
            &[PayloadRef::new_full(77, 4), PayloadRef::new_partial(77, 2)],
        );

        let payloads = collect_published_payloads(source_path)
            .await
            .expect("published payloads");
        assert_eq!(
            payloads
                .iter()
                .map(|payload| payload.length)
                .collect::<Vec<_>>(),
            vec![2, 4]
        );
    }

    #[tokio::test]
    async fn worker_tolerates_one_transient_lease_renewal_failure() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = source_dir.path().join("source.redb");
        studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");
        let reconstruct_started = Arc::new(Notify::new());
        let reconstruct_release = Arc::new(Notify::new());
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        *jobs.renewal_error.lock().expect("renewal error lock") = Some(
            SnapshotPortError::retryable("renew snapshot lease", "lease backend unavailable"),
        );
        jobs.renewal_error_once_after_initial
            .store(true, Ordering::SeqCst);
        let mut config = SnapshotWorkerConfig::new(source_dir.path().join("materializer"));
        config.lease_for = Duration::from_millis(30);
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(BlockingSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                    started: reconstruct_started.clone(),
                    release: reconstruct_release.clone(),
                }),
                Arc::new(TestPayloadReader::empty()),
                Arc::new(TestArtifactStore::new()),
                Arc::new(TestClock { now: captured_at }),
                config,
            )
            .expect("worker"),
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(worker.run(cancel.clone()));

        reconstruct_started.notified().await;
        jobs.renewed.notified().await;
        reconstruct_release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), jobs.published.notified())
            .await
            .expect("transient renewal failure must not abort materialization");
        cancel.cancel();
        assert!(task.await.expect("worker join").is_ok());
        assert!(jobs.renew_count.load(Ordering::SeqCst) >= 3);
    }

    #[tokio::test]
    async fn worker_renews_lease_during_long_materialization() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = source_dir.path().join("source.redb");
        studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");
        let reconstruct_started = Arc::new(Notify::new());
        let reconstruct_release = Arc::new(Notify::new());
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        let mut config = SnapshotWorkerConfig::new(source_dir.path().join("materializer"));
        config.lease_for = Duration::from_millis(30);
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(BlockingSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                    started: reconstruct_started.clone(),
                    release: reconstruct_release.clone(),
                }),
                Arc::new(TestPayloadReader::empty()),
                Arc::new(TestArtifactStore::new()),
                Arc::new(TestClock { now: captured_at }),
                config,
            )
            .expect("worker"),
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(worker.run(cancel.clone()));

        reconstruct_started.notified().await;
        jobs.renewed.notified().await;
        assert!(jobs.renew_count.load(Ordering::SeqCst) >= 2);
        reconstruct_release.notify_one();
        jobs.published.notified().await;
        cancel.cancel();
        assert!(task.await.expect("worker join").is_ok());
    }

    #[tokio::test]
    async fn worker_verifies_payloads_concurrently_with_a_bound() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let payload_reader = Arc::new(ConcurrentPayloadReader {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            started: started.clone(),
            release: release.clone(),
        });
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                Arc::new(TestJobStore::new(claim, catalog)),
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: PathBuf::new(),
                }),
                payload_reader.clone(),
                Arc::new(TestArtifactStore::new()),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new("target/snapshot-v2/materializer"),
            )
            .expect("worker"),
        );
        let payloads = vec![
            PayloadRef::new_full(1, 1),
            PayloadRef::new_full(2, 1),
            PayloadRef::new_full(3, 1),
        ];
        let cancel = CancellationToken::new();
        let task_worker = worker.clone();
        let task_cancel = cancel.clone();
        let task =
            tokio::spawn(async move { task_worker.verify_payloads(&payloads, &task_cancel).await });

        started.notified().await;
        assert!(payload_reader.max_active.load(Ordering::SeqCst) >= 2);
        release.notify_waiters();
        assert!(task.await.expect("verification join").is_ok());
    }

    #[tokio::test]
    async fn worker_batches_payload_ranges_by_object_id() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let payload_reader = Arc::new(BatchCountingPayloadReader {
            batch_count: AtomicUsize::new(0),
        });
        let worker = SnapshotWorker::new(
            volume,
            SnapshotWorkerId::new("worker-a").expect("worker id"),
            Arc::new(TestJobStore::new(claim, catalog)),
            Arc::new(TestSourceReader {
                descriptor: source_descriptor,
                path: PathBuf::new(),
            }),
            payload_reader.clone(),
            Arc::new(TestArtifactStore::new()),
            Arc::new(TestClock { now: captured_at }),
            SnapshotWorkerConfig::new("target/snapshot-v2/materializer"),
        )
        .expect("worker");
        let payloads = vec![
            PayloadRef::new_full(1, 1),
            PayloadRef {
                object_id: 1,
                offset: 1,
                length: 1,
                flags: studiofs_meta::schema::BlockFlags::empty(),
            },
            PayloadRef::new_full(2, 1),
        ];

        let proofs = worker
            .verify_payloads(&payloads, &CancellationToken::new())
            .await
            .expect("payload proofs");
        assert_eq!(proofs.len(), 3);
        assert_eq!(payload_reader.batch_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn datastore_artifact_adapter_uses_typed_custom_prefix_keys() {
        let config = studiofs_data::DataStoreConfig {
            endpoint: "memory://".to_owned(),
            bucket: "snapshot-tests".to_owned(),
            access_key: "access".to_owned(),
            secret_key: "secret".to_owned(),
            region: "us-east-1".to_owned(),
            volume_name: "projectfs".to_owned(),
            data_prefix: "tenant/demo/".to_owned(),
            volume_uuid: VOLUME_UUID.to_owned(),
            crypto: studiofs_data::DataStoreCryptoConfig::from(EncryptionConfig::new([0x42; 32])),
        };
        let data_store = Arc::new(
            studiofs_data::DataStore::with_operator_for_test(
                &config,
                opendal::Operator::new(Memory::default())
                    .expect("memory operator")
                    .finish(),
            )
            .expect("data store"),
        );
        let volume =
            SnapshotVolumeContext::new(7, "projectfs", VOLUME_UUID).expect("volume context");
        let adapter =
            DataStoreSnapshotArtifactStore::new(data_store, volume).expect("artifact adapter");
        let captured_at = Utc
            .with_ymd_and_hms(2026, 8, 13, 12, 0, 0)
            .single()
            .expect("timestamp");
        let snapshot_id = SnapshotId::from_cut("projectfs", captured_at).expect("snapshot id");
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("metadata.redb");
        tokio::fs::write(&source, b"snapshot metadata")
            .await
            .expect("write source");
        let attempt = SnapshotArtifactLocator {
            volume_id: 7,
            volume_uuid: VOLUME_UUID.to_owned(),
            snapshot_id: snapshot_id.clone(),
            attempt: Some(1),
            kind: SnapshotArtifactKind::Attempt,
        };
        let final_artifact = SnapshotArtifactLocator {
            attempt: None,
            kind: SnapshotArtifactKind::Metadata,
            ..attempt.clone()
        };

        let stored_attempt = adapter
            .put_attempt(&attempt, &source)
            .await
            .expect("put attempt");
        let stored_final = adapter
            .publish_final(&attempt, &final_artifact, &source, &stored_attempt.sha256)
            .await
            .expect("publish final");
        assert!(
            stored_attempt
                .object_key
                .starts_with("tenant/demo/metadata/snapshots/")
        );
        assert_eq!(
            stored_final.object_key,
            format!("tenant/demo/metadata/snapshots/{VOLUME_UUID}/{snapshot_id}/metadata.redb")
        );
        let deleted_attempt = SnapshotArtifactLocator {
            attempt: Some(2),
            ..attempt.clone()
        };
        adapter
            .put_attempt(&deleted_attempt, &source)
            .await
            .expect("put second attempt");
        adapter
            .delete(&deleted_attempt)
            .await
            .expect("delete second attempt");
        adapter
            .publish_final(
                &deleted_attempt,
                &final_artifact,
                &source,
                &stored_attempt.sha256,
            )
            .await
            .expect("final publication should use the local source");
        let checksum_error = adapter
            .publish_final(
                &attempt,
                &final_artifact,
                &source,
                &SnapshotSha256::from_bytes([0; 32]),
            )
            .await
            .expect_err("wrong artifact checksum must be rejected");
        assert!(matches!(
            checksum_error,
            SnapshotPortError::Permanent { .. }
        ));
        let listed = adapter
            .list(&SnapshotArtifactScope {
                volume_id: 7,
                volume_uuid: VOLUME_UUID.to_owned(),
                snapshot_id,
            })
            .await
            .expect("list artifacts");
        assert_eq!(listed.len(), 2);
        assert!(
            listed
                .iter()
                .any(|artifact| artifact.locator == final_artifact)
        );
        let listed_locators = adapter
            .list_locators(&SnapshotArtifactScope {
                volume_id: 7,
                volume_uuid: VOLUME_UUID.to_owned(),
                snapshot_id: listed[0].locator.snapshot_id.clone(),
            })
            .await
            .expect("list artifact locators");
        assert_eq!(listed_locators.len(), 2);
        assert!(
            listed_locators
                .iter()
                .any(|locator| locator == &final_artifact)
        );
    }

    #[tokio::test]
    async fn datastore_payload_reader_verifies_object_and_returns_exact_range() {
        let config = studiofs_data::DataStoreConfig {
            endpoint: "memory://".to_owned(),
            bucket: "payload-tests".to_owned(),
            access_key: "access".to_owned(),
            secret_key: "secret".to_owned(),
            region: "us-east-1".to_owned(),
            volume_name: "projectfs".to_owned(),
            data_prefix: "tenant/demo/objects/".to_owned(),
            volume_uuid: VOLUME_UUID.to_owned(),
            crypto: studiofs_data::DataStoreCryptoConfig::from(EncryptionConfig::new([0x42; 32])),
        };
        let data_store = Arc::new(
            studiofs_data::DataStore::with_operator_for_test(
                &config,
                opendal::Operator::new(Memory::default())
                    .expect("memory operator")
                    .finish(),
            )
            .expect("data store"),
        );
        let volume =
            SnapshotVolumeContext::new(7, "projectfs", VOLUME_UUID).expect("volume context");
        let reader =
            DataStoreSnapshotPayloadReader::new(data_store.clone(), volume).expect("reader");
        let object = studiofs_data::object::create_object_buffer(Bytes::from_static(b"0123456789"))
            .expect("object");
        let key = studiofs_data::object::format_object_fragment_keys(data_store.data_prefix(), 77)
            [0]
        .clone();
        data_store
            .put_by_key(&key, object)
            .await
            .expect("put payload");

        let payload = reader
            .stat_and_read(&PayloadRef {
                object_id: 77,
                offset: 2,
                length: 4,
                flags: studiofs_meta::schema::BlockFlags::empty(),
            })
            .await
            .expect("read payload");
        assert_eq!(payload.bytes, b"2345");
        assert_eq!(payload.sha256, digest(b"2345"));
        let error = reader
            .stat_and_read(&PayloadRef::new_full(77, 100))
            .await
            .expect_err("out-of-range payload must fail");
        assert!(matches!(error, SnapshotPortError::Permanent { .. }));
    }

    #[tokio::test]
    async fn tracked_shutdown_fences_stale_ready_publish() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = source_dir.path().join("source.redb");
        studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        *jobs.renewal_error.lock().expect("renewal error lock") = Some(
            SnapshotPortError::retryable("renew snapshot lease", "lease backend unavailable"),
        );
        let artifacts = Arc::new(TestArtifactStore::new());
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs,
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                }),
                Arc::new(TestPayloadReader::empty()),
                artifacts.clone(),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new(source_dir.path().join("materializer")),
            )
            .expect("worker"),
        );

        let result = worker.run(CancellationToken::new()).await;
        assert!(matches!(
            result,
            Err(SnapshotWorkerError::Port(
                SnapshotPortError::Retryable { .. }
            ))
        ));
        assert_eq!(artifacts.final_publishes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn interrupted_ready_publication_preserves_final_artifact() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let snapshot_id = claim.job.snapshot_id.clone();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = source_dir.path().join("source.redb");
        studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        jobs.publish_ready_blocked.store(true, Ordering::SeqCst);
        let artifacts = Arc::new(TestArtifactStore::new());
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                }),
                Arc::new(TestPayloadReader::empty()),
                artifacts.clone(),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new(source_dir.path().join("materializer")),
            )
            .expect("worker"),
        );
        let cancel = CancellationToken::new();
        let started = jobs.publish_ready_started.notified();
        let task = tokio::spawn(worker.clone().run(cancel.clone()));

        started.await;
        cancel.cancel();
        jobs.publish_ready_cancelled.store(true, Ordering::SeqCst);
        jobs.publish_ready_release.notify_one();
        assert!(task.await.expect("worker join").is_ok());
        assert!(
            jobs.published_artifact
                .lock()
                .expect("published lock")
                .is_none()
        );
        assert_eq!(artifacts.final_publishes.load(Ordering::SeqCst), 1);
        assert_eq!(artifacts.deletes.load(Ordering::SeqCst), 1);
        assert_eq!(jobs.release_count.load(Ordering::SeqCst), 1);
        assert!(
            !source_dir
                .path()
                .join("materializer")
                .join(snapshot_id.as_str())
                .join("attempts")
                .join("1")
                .exists()
        );
        assert!(
            !source_dir
                .path()
                .join("materializer")
                .join(snapshot_id.as_str())
                .exists()
        );
        let materializer_root = source_dir.path().join("materializer");
        worker
            .cleanup_materializer(&materializer_root.join("attempts").join("1"), &snapshot_id)
            .await;
        assert!(materializer_root.exists());
    }

    #[tokio::test]
    async fn stale_ready_publication_preserves_final_artifact() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = source_dir.path().join("source.redb");
        studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        *jobs.publish_ready_error.lock().expect("publish error lock") = Some(
            SnapshotPortError::permanent("stale_lease", "snapshot lease generation changed"),
        );
        let artifacts = Arc::new(TestArtifactStore::new());
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs.clone(),
                Arc::new(TestSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                }),
                Arc::new(TestPayloadReader::empty()),
                artifacts.clone(),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new(source_dir.path().join("materializer")),
            )
            .expect("worker"),
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(worker.run(cancel.clone()));

        jobs.failed.notified().await;
        cancel.cancel();
        assert!(task.await.expect("worker join").is_ok());
        assert_eq!(artifacts.final_publishes.load(Ordering::SeqCst), 1);
        assert_eq!(artifacts.deletes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn materializer_path_rejects_snapshot_id_path_separators() {
        let captured_at = Utc
            .with_ymd_and_hms(2026, 8, 13, 12, 0, 0)
            .single()
            .expect("timestamp");
        for volume_name in ["../escape", r"..\escape", "/absolute"] {
            let snapshot_id = SnapshotId::from_cut(volume_name, captured_at)
                .expect("snapshot ID accepts the existing volume-name contract");
            let error = materializer_attempt_root(Path::new("materializer"), &snapshot_id, 1)
                .expect_err("snapshot ID must not become a path");
            assert!(matches!(
                error,
                SnapshotWorkerError::Integrity(SnapshotIntegrityError::InvalidMaterializerPath)
            ));
        }
    }

    #[tokio::test]
    async fn cancellation_waits_for_spawn_blocking_reconstruction() {
        let (volume, catalog, claim, source_descriptor, captured_at) = test_snapshot_state();
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let source_path = source_dir.path().join("source.redb");
        studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let reconstruct_started = Arc::new(Notify::new());
        let worker_completed = Arc::new(Notify::new());
        let jobs = Arc::new(TestJobStore::new(claim, catalog));
        let worker = Arc::new(
            SnapshotWorker::new(
                volume,
                SnapshotWorkerId::new("worker-a").expect("worker id"),
                jobs,
                Arc::new(SpawnBlockingSourceReader {
                    descriptor: source_descriptor,
                    path: source_path,
                    started: reconstruct_started.clone(),
                    release: Mutex::new(Some(release_rx)),
                }),
                Arc::new(TestPayloadReader::empty()),
                Arc::new(TestArtifactStore::new()),
                Arc::new(TestClock { now: captured_at }),
                SnapshotWorkerConfig::new(source_dir.path().join("materializer")),
            )
            .expect("worker"),
        );
        let cancel = CancellationToken::new();
        let completed = worker_completed.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let result = worker.run(task_cancel).await;
            completed.notify_one();
            result
        });

        reconstruct_started.notified().await;
        cancel.cancel();
        let completed_before_release =
            tokio::time::timeout(Duration::from_millis(100), worker_completed.notified())
                .await
                .is_ok();
        release_tx.send(()).expect("release reconstruction");
        assert!(task.await.expect("worker join").is_ok());
        assert!(!completed_before_release);
    }

    #[tokio::test]
    async fn cleanup_retries_artifact_failure_before_finalizing_tombstone() {
        let (volume, mut catalog, _, _, captured_at) = test_snapshot_state();
        catalog.lifecycle = SnapshotLifecycle::Deleting;
        catalog.creation_state = SnapshotCreationState::Completed;
        let claim = SnapshotCleanupClaim {
            catalog: catalog.clone(),
            lease: SnapshotCleanupLease {
                owner: "cleanup-a".to_string(),
                expires_at: captured_at + chrono::Duration::minutes(1),
                fencing_token: 3,
            },
        };
        let attempt = SnapshotArtifactLocator {
            volume_id: volume.volume_id,
            volume_uuid: volume.volume_uuid.clone(),
            snapshot_id: catalog.snapshot_id.clone(),
            attempt: Some(1),
            kind: SnapshotArtifactKind::Attempt,
        };
        let final_artifact = SnapshotArtifactLocator {
            attempt: None,
            kind: SnapshotArtifactKind::Metadata,
            ..attempt.clone()
        };
        let artifacts = Arc::new(CleanupArtifactStore::new(vec![
            stored_cleanup_artifact(attempt),
            stored_cleanup_artifact(final_artifact),
        ]));
        artifacts.fail_next.store(true, Ordering::SeqCst);
        let cleanup = Arc::new(CleanupTestStore::new(claim.clone()));
        let worker = SnapshotCleanupWorker::new(
            volume.clone(),
            SnapshotWorkerId::new("cleanup-a").expect("worker id"),
            cleanup.clone(),
            artifacts.clone(),
            Arc::new(ImmediateSnapshotClock { now: captured_at }),
        );
        let cancel = CancellationToken::new();

        let first = worker
            .process_claim(&claim, &cancel)
            .await
            .expect_err("first artifact delete should retry");
        assert!(matches!(
            first,
            SnapshotWorkerError::Port(SnapshotPortError::Retryable { .. })
        ));
        assert_eq!(cleanup.released.load(Ordering::SeqCst), 1);
        assert_eq!(cleanup.finalized.load(Ordering::SeqCst), 0);

        worker
            .process_claim(&claim, &cancel)
            .await
            .expect("second cleanup attempt");
        assert_eq!(artifacts.deleted.lock().expect("deleted lock").len(), 2);
        assert_eq!(cleanup.finalized.load(Ordering::SeqCst), 1);
        assert_eq!(cleanup.renewed.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn cleanup_rechecks_empty_artifact_prefix_before_finalizing_tombstone() {
        let (volume, mut catalog, _, _, captured_at) = test_snapshot_state();
        catalog.lifecycle = SnapshotLifecycle::Deleting;
        catalog.creation_state = SnapshotCreationState::Completed;
        let claim = SnapshotCleanupClaim {
            catalog: catalog.clone(),
            lease: SnapshotCleanupLease {
                owner: "cleanup-a".to_string(),
                expires_at: captured_at + chrono::Duration::minutes(1),
                fencing_token: 3,
            },
        };
        let initial_artifact = SnapshotArtifactLocator {
            volume_id: volume.volume_id,
            volume_uuid: volume.volume_uuid.clone(),
            snapshot_id: catalog.snapshot_id.clone(),
            attempt: Some(1),
            kind: SnapshotArtifactKind::Attempt,
        };
        let late_artifact = SnapshotArtifactLocator {
            attempt: Some(2),
            kind: SnapshotArtifactKind::Attempt,
            ..initial_artifact.clone()
        };
        let artifacts = Arc::new(CleanupArtifactStore::with_late_artifact(
            vec![stored_cleanup_artifact(initial_artifact)],
            stored_cleanup_artifact(late_artifact),
        ));
        let cleanup = Arc::new(CleanupTestStore::new(claim.clone()));
        let worker = SnapshotCleanupWorker::new(
            volume.clone(),
            SnapshotWorkerId::new("cleanup-a").expect("worker id"),
            cleanup.clone(),
            artifacts.clone(),
            Arc::new(ImmediateSnapshotClock { now: captured_at }),
        );

        worker
            .process_claim(&claim, &CancellationToken::new())
            .await
            .expect("late artifact is drained before finalization");

        assert_eq!(artifacts.list_calls.load(Ordering::SeqCst), 4);
        assert_eq!(artifacts.deleted.lock().expect("deleted lock").len(), 2);
        assert_eq!(cleanup.finalized.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cleanup_retains_claim_after_permanent_artifact_scope_error() {
        let (volume, mut catalog, _, _, captured_at) = test_snapshot_state();
        catalog.lifecycle = SnapshotLifecycle::Deleting;
        catalog.creation_state = SnapshotCreationState::Completed;
        let claim = SnapshotCleanupClaim {
            catalog: catalog.clone(),
            lease: SnapshotCleanupLease {
                owner: "cleanup-a".to_string(),
                expires_at: captured_at + chrono::Duration::minutes(1),
                fencing_token: 3,
            },
        };
        let invalid_artifact = SnapshotArtifactLocator {
            volume_id: volume.volume_id + 1,
            volume_uuid: volume.volume_uuid.clone(),
            snapshot_id: catalog.snapshot_id.clone(),
            attempt: None,
            kind: SnapshotArtifactKind::Metadata,
        };
        let cleanup = Arc::new(CleanupTestStore::new(claim.clone()));
        let worker = SnapshotCleanupWorker::new(
            volume.clone(),
            SnapshotWorkerId::new("cleanup-a").expect("worker id"),
            cleanup.clone(),
            Arc::new(CleanupArtifactStore::new(vec![stored_cleanup_artifact(
                invalid_artifact,
            )])),
            Arc::new(TestClock { now: captured_at }),
        );

        let error = worker
            .process_claim(&claim, &CancellationToken::new())
            .await
            .expect_err("invalid listed artifact must be permanent");

        assert!(matches!(
            error,
            SnapshotWorkerError::Port(SnapshotPortError::Permanent { ref code, .. })
                if code.as_str() == "invalid_artifact_scope"
        ));
        assert_eq!(cleanup.released.load(Ordering::SeqCst), 0);
        assert_eq!(cleanup.finalized.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cleanup_cancellation_releases_claim_once() {
        let (volume, mut catalog, _, _, captured_at) = test_snapshot_state();
        catalog.lifecycle = SnapshotLifecycle::Deleting;
        let claim = SnapshotCleanupClaim {
            catalog,
            lease: SnapshotCleanupLease {
                owner: "cleanup-a".to_string(),
                expires_at: captured_at + chrono::Duration::minutes(1),
                fencing_token: 3,
            },
        };
        let cleanup = Arc::new(CleanupTestStore::new(claim));
        cleanup.claim_next_once.store(true, Ordering::SeqCst);
        *cleanup.renew_error.lock().expect("renew error lock") = Some(SnapshotPortError::Cancelled);
        let worker = Arc::new(SnapshotCleanupWorker::new(
            volume.clone(),
            SnapshotWorkerId::new("cleanup-a").expect("worker id"),
            cleanup.clone(),
            Arc::new(CleanupArtifactStore::new(Vec::new())),
            Arc::new(TestClock { now: captured_at }),
        ));

        worker
            .run(CancellationToken::new())
            .await
            .expect("cancellation is a clean worker shutdown");
        assert_eq!(cleanup.released.load(Ordering::SeqCst), 1);
    }

    fn published_payload_source(root: &Path) -> PathBuf {
        published_payload_source_with_payloads(root, &[PayloadRef::new_full(77, 4)])
    }

    fn published_payload_source_with_payloads(root: &Path, payloads: &[PayloadRef]) -> PathBuf {
        let source_path = root.join("source.redb");
        let source = studiofs_meta::RedbMetaEngine::open(&source_path).expect("source redb");
        let mut chunk = studiofs_meta::schema::RegularFileChunkState::default();
        for (index, payload) in payloads.iter().copied().enumerate() {
            let slice_id = studiofs_meta::schema::SliceId(index as u64);
            chunk.slices.insert(
                slice_id,
                studiofs_meta::schema::SliceRecord {
                    slice_id,
                    order: studiofs_meta::schema::SliceOrder(index as u64),
                    range: studiofs_meta::schema::SliceRange::new(0, payload.length)
                        .expect("slice range"),
                    payload: Some(payload),
                    payload_state: studiofs_meta::schema::PayloadRefState::Published,
                },
            );
        }
        let mut regular = studiofs_meta::schema::RegularFileBaseState::default();
        regular
            .chunks
            .insert(studiofs_meta::schema::ChunkId(0), chunk);
        let snapshot = studiofs_meta::redb_engine::RedbSnapshot {
            regular_file_base: vec![(1, postcard::to_stdvec(&regular).expect("encode graph"))],
            ..Default::default()
        };
        source
            .import_snapshot(&snapshot)
            .expect("import source graph");
        drop(source);
        source_path
    }

    fn test_snapshot_state() -> (
        SnapshotVolumeContext,
        SnapshotCatalogEntry,
        SnapshotJobClaim,
        SnapshotSourceDescriptor,
        DateTime<Utc>,
    ) {
        let captured_at = Utc
            .with_ymd_and_hms(2026, 8, 13, 12, 0, 0)
            .single()
            .expect("valid test timestamp");
        let snapshot_id = SnapshotId::from_cut("projectfs", captured_at).expect("snapshot id");
        let cut = SnapshotCut::new(7, 4, captured_at).expect("snapshot cut");
        let volume = SnapshotVolumeContext::new(7, "projectfs", VOLUME_UUID).expect("volume");
        let catalog = SnapshotCatalogEntry {
            snapshot_id: snapshot_id.clone(),
            volume: volume.clone(),
            origin: SnapshotOrigin::Manual,
            cut,
            lifecycle: SnapshotLifecycle::Creating,
            creation_state: SnapshotCreationState::Running,
            retention: SnapshotRetention::manual(None).expect("retention"),
            ready_at: None,
            expires_at: None,
            artifact: None,
            failure: None,
        };
        let claim = SnapshotJobClaim {
            job: SnapshotCreationJob {
                snapshot_id: snapshot_id.clone(),
                state: SnapshotCreationState::Running,
                retry_count: 0,
                owner: Some("worker-a".to_string()),
                lease_expires_at: Some(captured_at + chrono::Duration::minutes(1)),
                fencing_token: 11,
                next_retry_at: None,
                failure: None,
            },
            lease: SnapshotCreationLease {
                owner: "worker-a".to_string(),
                expires_at: captured_at + chrono::Duration::minutes(1),
                fencing_token: 11,
            },
        };
        let source_descriptor = SnapshotSourceDescriptor {
            checkpoint_id: snapshot_id.to_string(),
            checkpoint_version: 4,
            checkpoint_format_version: 1,
            checkpoint_schema_version: 1,
            event_start_seq: None,
            event_end_seq: None,
            event_start_version: None,
            event_end_version: None,
            event_row_count: 0,
            replay_proof: SnapshotSha256::from_bytes([1; 32]),
        };
        (volume, catalog, claim, source_descriptor, captured_at)
    }

    struct TestJobStore {
        claim: Mutex<Option<SnapshotJobClaim>>,
        claim_error: Mutex<Option<SnapshotPortError>>,
        claim_count: AtomicUsize,
        catalog: SnapshotCatalogEntry,
        follow_lifecycle: Mutex<SnapshotLifecycle>,
        published_artifact: Mutex<Option<(SnapshotArtifactDescriptor, Vec<SnapshotPayloadProof>)>>,
        publish_ready_error: Mutex<Option<SnapshotPortError>>,
        failed_catalog: Mutex<Option<SnapshotCatalogEntry>>,
        retry_job: Mutex<Option<SnapshotCreationJob>>,
        ready_after_retry: AtomicBool,
        renewal_error: Mutex<Option<SnapshotPortError>>,
        renewal_error_once_after_initial: AtomicBool,
        renew_blocked: AtomicBool,
        renew_count: AtomicUsize,
        renew_started: Notify,
        renew_release: Notify,
        renewed: Notify,
        release_count: AtomicUsize,
        publish_ready_blocked: AtomicBool,
        publish_ready_cancelled: AtomicBool,
        publish_ready_started: Notify,
        publish_ready_release: Notify,
        published: Notify,
        failed: Notify,
        retry: Notify,
    }

    impl TestJobStore {
        fn new(claim: SnapshotJobClaim, catalog: SnapshotCatalogEntry) -> Self {
            Self {
                claim: Mutex::new(Some(claim)),
                claim_error: Mutex::new(None),
                claim_count: AtomicUsize::new(0),
                follow_lifecycle: Mutex::new(catalog.lifecycle),
                catalog,
                published_artifact: Mutex::new(None),
                publish_ready_error: Mutex::new(None),
                failed_catalog: Mutex::new(None),
                retry_job: Mutex::new(None),
                ready_after_retry: AtomicBool::new(false),
                renewal_error: Mutex::new(None),
                renewal_error_once_after_initial: AtomicBool::new(false),
                renew_blocked: AtomicBool::new(false),
                renew_count: AtomicUsize::new(0),
                renew_started: Notify::new(),
                renew_release: Notify::new(),
                renewed: Notify::new(),
                release_count: AtomicUsize::new(0),
                publish_ready_blocked: AtomicBool::new(false),
                publish_ready_cancelled: AtomicBool::new(false),
                publish_ready_started: Notify::new(),
                publish_ready_release: Notify::new(),
                published: Notify::new(),
                failed: Notify::new(),
                retry: Notify::new(),
            }
        }
    }

    impl SnapshotJobStore for TestJobStore {
        fn claim_next<'a>(
            &'a self,
            _worker: &'a SnapshotWorkerId,
            _lease_for: Duration,
        ) -> SnapshotPortFuture<'a, Option<SnapshotJobClaim>> {
            Box::pin(async move {
                self.claim_count.fetch_add(1, Ordering::SeqCst);
                if let Some(error) = self.claim_error.lock().expect("claim error lock").take() {
                    return Err(error);
                }
                Ok(self.claim.lock().expect("claim lock").take())
            })
        }

        fn claim_id_or_follow<'a>(
            &'a self,
            _snapshot_id: &'a SnapshotId,
            _worker: &'a SnapshotWorkerId,
            _lease_for: Duration,
        ) -> SnapshotPortFuture<'a, SnapshotClaimOrFollow> {
            Box::pin(async move {
                if let Some(claim) = self.claim.lock().expect("claim lock").take() {
                    return Ok(SnapshotClaimOrFollow::Claimed(claim));
                }
                let mut catalog = self.catalog.clone();
                catalog.lifecycle = *self.follow_lifecycle.lock().expect("follow lifecycle lock");
                Ok(SnapshotClaimOrFollow::Follow(catalog))
            })
        }

        fn renew_lease<'a>(
            &'a self,
            claim: &'a SnapshotJobClaim,
            _lease_for: Duration,
        ) -> SnapshotPortFuture<'a, SnapshotCreationLease> {
            Box::pin(async move {
                if self.renew_blocked.load(Ordering::SeqCst) {
                    self.renew_started.notify_one();
                    self.renew_release.notified().await;
                }
                let renew_number = self.renew_count.fetch_add(1, Ordering::SeqCst) + 1;
                if renew_number >= 2 {
                    self.renewed.notify_one();
                }
                if let Some(error) = self
                    .renewal_error
                    .lock()
                    .expect("renewal error lock")
                    .clone()
                    && (!self.renewal_error_once_after_initial.load(Ordering::SeqCst)
                        || renew_number == 2)
                {
                    return Err(error);
                }
                Ok(claim.lease.clone())
            })
        }

        fn release_lease<'a>(&'a self, _claim: &'a SnapshotJobClaim) -> SnapshotPortFuture<'a, ()> {
            Box::pin(async move {
                self.release_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }

        fn mark_retry_wait<'a>(
            &'a self,
            claim: &'a SnapshotJobClaim,
            retry: SnapshotRetry,
        ) -> SnapshotPortFuture<'a, SnapshotCreationJob> {
            Box::pin(async move {
                let mut job = claim.job.clone();
                job.state = SnapshotCreationState::RetryWait;
                job.retry_count = retry.retry_count;
                job.next_retry_at = Some(retry.next_retry_at);
                *self.retry_job.lock().expect("retry lock") = Some(job.clone());
                if self.ready_after_retry.load(Ordering::SeqCst) {
                    *self.follow_lifecycle.lock().expect("follow lifecycle lock") =
                        SnapshotLifecycle::Ready;
                }
                self.retry.notify_one();
                Ok(job)
            })
        }

        fn mark_failed<'a>(
            &'a self,
            _claim: &'a SnapshotJobClaim,
            _failure: SnapshotFailure,
        ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
            Box::pin(async move {
                let mut failed = self.catalog.clone();
                failed.lifecycle = SnapshotLifecycle::Failed;
                failed.creation_state = SnapshotCreationState::Failed;
                *self.failed_catalog.lock().expect("failed lock") = Some(failed.clone());
                self.failed.notify_one();
                Ok(failed)
            })
        }

        fn publish_ready<'a>(
            &'a self,
            _claim: &'a SnapshotJobClaim,
            descriptor: &'a SnapshotArtifactDescriptor,
            payload_proofs: &'a [SnapshotPayloadProof],
        ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
            let descriptor = descriptor.clone();
            let payload_proofs = payload_proofs.to_vec();
            Box::pin(async move {
                if self.publish_ready_blocked.load(Ordering::SeqCst) {
                    self.publish_ready_started.notify_one();
                    self.publish_ready_release.notified().await;
                }
                if self.publish_ready_cancelled.load(Ordering::SeqCst) {
                    return Err(SnapshotPortError::Cancelled);
                }
                if let Some(error) = self
                    .publish_ready_error
                    .lock()
                    .expect("publish error lock")
                    .take()
                {
                    return Err(error);
                }
                *self.published_artifact.lock().expect("published lock") =
                    Some((descriptor, payload_proofs));
                self.published.notify_one();
                Ok(self.catalog.clone())
            })
        }

        fn load_catalog<'a>(
            &'a self,
            _snapshot_id: &'a SnapshotId,
        ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
            Box::pin(async move { Ok(self.catalog.clone()) })
        }
    }

    struct TestSourceReader {
        descriptor: SnapshotSourceDescriptor,
        path: PathBuf,
    }

    struct BlockingSourceReader {
        descriptor: SnapshotSourceDescriptor,
        path: PathBuf,
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct SpawnBlockingSourceReader {
        descriptor: SnapshotSourceDescriptor,
        path: PathBuf,
        started: Arc<Notify>,
        release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    }

    impl SnapshotSourceReader for BlockingSourceReader {
        fn load_descriptor<'a>(
            &'a self,
            snapshot_id: &'a SnapshotId,
        ) -> SnapshotPortFuture<'a, SnapshotSourceDescriptor> {
            Box::pin(async move {
                if snapshot_id.as_str() != self.descriptor.checkpoint_id {
                    return Err(SnapshotPortError::permanent(
                        "invalid_source",
                        "snapshot source ID mismatch",
                    ));
                }
                Ok(self.descriptor.clone())
            })
        }

        fn reconstruct<'a>(
            &'a self,
            _cut: &'a SnapshotCut,
            _descriptor: &'a SnapshotSourceDescriptor,
            _destination: &'a Path,
        ) -> SnapshotPortFuture<'a, SnapshotReconstruction> {
            let path = self.path.clone();
            let started = self.started.clone();
            let release = self.release.clone();
            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                Ok(SnapshotReconstruction { path })
            })
        }
    }

    impl SnapshotSourceReader for SpawnBlockingSourceReader {
        fn load_descriptor<'a>(
            &'a self,
            snapshot_id: &'a SnapshotId,
        ) -> SnapshotPortFuture<'a, SnapshotSourceDescriptor> {
            Box::pin(async move {
                if snapshot_id.as_str() != self.descriptor.checkpoint_id {
                    return Err(SnapshotPortError::permanent(
                        "invalid_source",
                        "snapshot source ID mismatch",
                    ));
                }
                Ok(self.descriptor.clone())
            })
        }

        fn reconstruct<'a>(
            &'a self,
            _cut: &'a SnapshotCut,
            _descriptor: &'a SnapshotSourceDescriptor,
            _destination: &'a Path,
        ) -> SnapshotPortFuture<'a, SnapshotReconstruction> {
            let path = self.path.clone();
            let started = self.started.clone();
            let release = self
                .release
                .lock()
                .expect("reconstruction release lock")
                .take()
                .expect("one reconstruction");
            Box::pin(async move {
                tokio::task::spawn_blocking(move || {
                    started.notify_one();
                    release.recv().map_err(|_| {
                        SnapshotPortError::retryable(
                            "snapshot source reconstruct",
                            "reconstruction release unavailable",
                        )
                    })?;
                    Ok(SnapshotReconstruction { path })
                })
                .await
                .map_err(|_| {
                    SnapshotPortError::retryable(
                        "snapshot source reconstruct",
                        "snapshot reconstruction task failed",
                    )
                })?
            })
        }
    }

    impl SnapshotSourceReader for TestSourceReader {
        fn load_descriptor<'a>(
            &'a self,
            _snapshot_id: &'a SnapshotId,
        ) -> SnapshotPortFuture<'a, SnapshotSourceDescriptor> {
            Box::pin(async move { Ok(self.descriptor.clone()) })
        }

        fn reconstruct<'a>(
            &'a self,
            _cut: &'a SnapshotCut,
            _descriptor: &'a SnapshotSourceDescriptor,
            _destination: &'a Path,
        ) -> SnapshotPortFuture<'a, SnapshotReconstruction> {
            Box::pin(async move {
                Ok(SnapshotReconstruction {
                    path: self.path.clone(),
                })
            })
        }
    }

    struct TestPayloadReader {
        response: Mutex<Result<SnapshotPayloadBytes, SnapshotPortError>>,
    }

    struct ConcurrentPayloadReader {
        active: AtomicUsize,
        max_active: AtomicUsize,
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct BatchCountingPayloadReader {
        batch_count: AtomicUsize,
    }

    impl SnapshotPayloadReader for BatchCountingPayloadReader {
        fn stat_and_read<'a>(
            &'a self,
            _payload: &'a PayloadRef,
        ) -> SnapshotPortFuture<'a, SnapshotPayloadBytes> {
            Box::pin(async {
                Err(SnapshotPortError::permanent(
                    "unexpected_payload_read",
                    "test requires batch reads",
                ))
            })
        }

        fn stat_and_read_batch<'a>(
            &'a self,
            payloads: &'a [PayloadRef],
        ) -> SnapshotPortFuture<'a, Vec<SnapshotPayloadBytes>> {
            let payloads = payloads.to_vec();
            Box::pin(async move {
                self.batch_count.fetch_add(1, Ordering::SeqCst);
                Ok(payloads
                    .into_iter()
                    .map(|payload| {
                        let bytes = vec![0; payload.length as usize];
                        SnapshotPayloadBytes {
                            sha256: digest(&bytes),
                            bytes,
                        }
                    })
                    .collect())
            })
        }
    }

    impl SnapshotPayloadReader for ConcurrentPayloadReader {
        fn stat_and_read<'a>(
            &'a self,
            payload: &'a PayloadRef,
        ) -> SnapshotPortFuture<'a, SnapshotPayloadBytes> {
            let length = payload.length as usize;
            Box::pin(async move {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active.fetch_max(active, Ordering::SeqCst);
                if active >= 2 {
                    self.started.notify_one();
                }
                self.release.notified().await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                let bytes = vec![0; length];
                Ok(SnapshotPayloadBytes {
                    sha256: digest(&bytes),
                    bytes,
                })
            })
        }
    }

    impl TestPayloadReader {
        fn empty() -> Self {
            Self::new(SnapshotPayloadBytes {
                bytes: Vec::new(),
                sha256: SnapshotSha256::from_bytes([0; 32]),
            })
        }

        fn new(response: SnapshotPayloadBytes) -> Self {
            Self {
                response: Mutex::new(Ok(response)),
            }
        }

        fn retryable() -> Self {
            Self {
                response: Mutex::new(Err(SnapshotPortError::retryable(
                    "read payload",
                    "payload backend unavailable",
                ))),
            }
        }
    }

    impl SnapshotPayloadReader for TestPayloadReader {
        fn stat_and_read<'a>(
            &'a self,
            _payload: &'a PayloadRef,
        ) -> SnapshotPortFuture<'a, SnapshotPayloadBytes> {
            Box::pin(async move { self.response.lock().expect("payload lock").clone() })
        }
    }

    struct TestArtifactStore {
        final_publishes: AtomicUsize,
        deletes: AtomicUsize,
    }

    impl TestArtifactStore {
        fn new() -> Self {
            Self {
                final_publishes: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
            }
        }
    }

    impl SnapshotArtifactStore for TestArtifactStore {
        fn put_attempt<'a>(
            &'a self,
            locator: &'a SnapshotArtifactLocator,
            _source: &'a Path,
        ) -> SnapshotPortFuture<'a, SnapshotStoredArtifact> {
            Box::pin(async move {
                Ok(SnapshotStoredArtifact {
                    object_key: format!("attempt/{}", locator.snapshot_id),
                    locator: locator.clone(),
                    byte_length: 42,
                    sha256: SnapshotSha256::from_bytes([7; 32]),
                })
            })
        }

        fn publish_final<'a>(
            &'a self,
            _attempt: &'a SnapshotArtifactLocator,
            final_artifact: &'a SnapshotArtifactLocator,
            _source: &'a Path,
            _expected_sha256: &'a SnapshotSha256,
        ) -> SnapshotPortFuture<'a, SnapshotStoredArtifact> {
            Box::pin(async move {
                self.final_publishes.fetch_add(1, Ordering::SeqCst);
                Ok(SnapshotStoredArtifact {
                    object_key: format!(
                        "metadata/snapshots/{}/{}/metadata.redb",
                        final_artifact.volume_uuid, final_artifact.snapshot_id
                    ),
                    locator: final_artifact.clone(),
                    byte_length: 42,
                    sha256: SnapshotSha256::from_bytes([7; 32]),
                })
            })
        }

        fn get<'a>(
            &'a self,
            _locator: &'a SnapshotArtifactLocator,
        ) -> SnapshotPortFuture<'a, SnapshotArtifactBytes> {
            Box::pin(async { Err(SnapshotPortError::permanent("unused", "unused test port")) })
        }

        fn delete<'a>(
            &'a self,
            _locator: &'a SnapshotArtifactLocator,
        ) -> SnapshotPortFuture<'a, ()> {
            Box::pin(async move {
                self.deletes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }

        fn list<'a>(
            &'a self,
            _scope: &'a SnapshotArtifactScope,
        ) -> SnapshotPortFuture<'a, Vec<SnapshotStoredArtifact>> {
            Box::pin(async { Err(SnapshotPortError::permanent("unused", "unused test port")) })
        }
    }

    struct CleanupTestStore {
        claim: SnapshotCleanupClaim,
        claim_next_once: AtomicBool,
        released: AtomicUsize,
        renewed: AtomicUsize,
        finalized: AtomicUsize,
        renew_error: Mutex<Option<SnapshotPortError>>,
    }

    impl CleanupTestStore {
        fn new(claim: SnapshotCleanupClaim) -> Self {
            Self {
                claim,
                claim_next_once: AtomicBool::new(false),
                released: AtomicUsize::new(0),
                renewed: AtomicUsize::new(0),
                finalized: AtomicUsize::new(0),
                renew_error: Mutex::new(None),
            }
        }
    }

    impl SnapshotCleanupStore for CleanupTestStore {
        fn request_delete<'a>(
            &'a self,
            _snapshot_id: &'a SnapshotId,
        ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
            Box::pin(async { Err(SnapshotPortError::permanent("unused", "unused test port")) })
        }

        fn claim_next<'a>(
            &'a self,
            _worker: &'a SnapshotWorkerId,
            _lease_for: Duration,
        ) -> SnapshotPortFuture<'a, Option<SnapshotCleanupClaim>> {
            let claim = self
                .claim_next_once
                .swap(false, Ordering::SeqCst)
                .then(|| self.claim.clone());
            Box::pin(async move { Ok(claim) })
        }

        fn renew_lease<'a>(
            &'a self,
            _claim: &'a SnapshotCleanupClaim,
            _lease_for: Duration,
        ) -> SnapshotPortFuture<'a, SnapshotCleanupClaim> {
            let claim = self.claim.clone();
            Box::pin(async move {
                self.renewed.fetch_add(1, Ordering::SeqCst);
                if let Some(error) = self.renew_error.lock().expect("renew error lock").take() {
                    return Err(error);
                }
                Ok(claim)
            })
        }

        fn release_lease<'a>(
            &'a self,
            _claim: &'a SnapshotCleanupClaim,
        ) -> SnapshotPortFuture<'a, ()> {
            Box::pin(async move {
                self.released.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }

        fn finalize_deleted<'a>(
            &'a self,
            _claim: &'a SnapshotCleanupClaim,
        ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
            let mut catalog = self.claim.catalog.clone();
            catalog.lifecycle = SnapshotLifecycle::Deleted;
            Box::pin(async move {
                self.finalized.fetch_add(1, Ordering::SeqCst);
                Ok(catalog)
            })
        }
    }

    struct CleanupArtifactStore {
        artifacts: Mutex<Vec<SnapshotStoredArtifact>>,
        deleted: Mutex<Vec<SnapshotArtifactLocator>>,
        fail_next: AtomicBool,
        list_calls: AtomicUsize,
        late_artifact: Mutex<Option<SnapshotStoredArtifact>>,
    }

    impl CleanupArtifactStore {
        fn new(artifacts: Vec<SnapshotStoredArtifact>) -> Self {
            Self {
                artifacts: Mutex::new(artifacts),
                deleted: Mutex::new(Vec::new()),
                fail_next: AtomicBool::new(false),
                list_calls: AtomicUsize::new(0),
                late_artifact: Mutex::new(None),
            }
        }

        fn with_late_artifact(
            artifacts: Vec<SnapshotStoredArtifact>,
            late_artifact: SnapshotStoredArtifact,
        ) -> Self {
            let store = Self::new(artifacts);
            *store.late_artifact.lock().expect("late artifact lock") = Some(late_artifact);
            store
        }
    }

    fn stored_cleanup_artifact(locator: SnapshotArtifactLocator) -> SnapshotStoredArtifact {
        SnapshotStoredArtifact {
            object_key: format!("cleanup/{}", locator.snapshot_id),
            locator,
            byte_length: 1,
            sha256: SnapshotSha256::from_bytes([1; 32]),
        }
    }

    impl SnapshotArtifactStore for CleanupArtifactStore {
        fn put_attempt<'a>(
            &'a self,
            _locator: &'a SnapshotArtifactLocator,
            _source: &'a Path,
        ) -> SnapshotPortFuture<'a, SnapshotStoredArtifact> {
            Box::pin(async { Err(SnapshotPortError::permanent("unused", "unused test port")) })
        }

        fn publish_final<'a>(
            &'a self,
            _attempt: &'a SnapshotArtifactLocator,
            _final_artifact: &'a SnapshotArtifactLocator,
            _source: &'a Path,
            _expected_sha256: &'a SnapshotSha256,
        ) -> SnapshotPortFuture<'a, SnapshotStoredArtifact> {
            Box::pin(async { Err(SnapshotPortError::permanent("unused", "unused test port")) })
        }

        fn get<'a>(
            &'a self,
            _locator: &'a SnapshotArtifactLocator,
        ) -> SnapshotPortFuture<'a, SnapshotArtifactBytes> {
            Box::pin(async { Err(SnapshotPortError::permanent("unused", "unused test port")) })
        }

        fn delete<'a>(
            &'a self,
            locator: &'a SnapshotArtifactLocator,
        ) -> SnapshotPortFuture<'a, ()> {
            let locator = locator.clone();
            Box::pin(async move {
                if self.fail_next.swap(false, Ordering::SeqCst) {
                    return Err(SnapshotPortError::retryable(
                        "delete snapshot artifact",
                        "artifact backend unavailable",
                    ));
                }
                self.artifacts
                    .lock()
                    .expect("artifact lock")
                    .retain(|artifact| artifact.locator != locator);
                self.deleted.lock().expect("deleted lock").push(locator);
                Ok(())
            })
        }

        fn list<'a>(
            &'a self,
            _scope: &'a SnapshotArtifactScope,
        ) -> SnapshotPortFuture<'a, Vec<SnapshotStoredArtifact>> {
            let list_call = self.list_calls.fetch_add(1, Ordering::SeqCst);
            if list_call == 1
                && let Some(late_artifact) = self
                    .late_artifact
                    .lock()
                    .expect("late artifact lock")
                    .take()
            {
                self.artifacts
                    .lock()
                    .expect("artifact lock")
                    .push(late_artifact);
            }
            let artifacts = self.artifacts.lock().expect("artifact lock").clone();
            Box::pin(async move { Ok(artifacts) })
        }
    }

    struct PolicyEvaluatorTestStore {
        claimed: Notify,
        released: AtomicBool,
        stale_claim: AtomicBool,
    }

    impl PolicyEvaluatorTestStore {
        fn new() -> Self {
            Self {
                claimed: Notify::new(),
                released: AtomicBool::new(false),
                stale_claim: AtomicBool::new(false),
            }
        }
    }

    impl SnapshotPolicyStore for PolicyEvaluatorTestStore {
        fn set_policy<'a>(
            &'a self,
            _policy: &'a SnapshotDailyPolicy,
            _expected_generation: Option<i64>,
        ) -> SnapshotPortFuture<'a, SnapshotPolicyRecord> {
            Box::pin(async { Err(SnapshotPortError::permanent("unused", "unused test port")) })
        }

        fn load_policy<'a>(&'a self) -> SnapshotPortFuture<'a, Option<SnapshotPolicyRecord>> {
            Box::pin(async { Err(SnapshotPortError::permanent("unused", "unused test port")) })
        }

        fn cancel_policy<'a>(&'a self, _expected_generation: i64) -> SnapshotPortFuture<'a, ()> {
            Box::pin(async { Err(SnapshotPortError::permanent("unused", "unused test port")) })
        }

        fn claim_evaluator_lease<'a>(
            &'a self,
            _context: &'a SnapshotVolumeContext,
            owner: &'a SnapshotPolicyEvaluatorId,
            _lease_for: Duration,
        ) -> SnapshotPortFuture<'a, SnapshotPolicyEvaluatorLease> {
            let owner = owner.as_str().to_string();
            Box::pin(async move {
                self.claimed.notify_one();
                if self.stale_claim.swap(false, Ordering::SeqCst) {
                    return Err(SnapshotPortError::permanent(
                        "stale_evaluator_lease",
                        "snapshot policy evaluator lease is held by a peer",
                    ));
                }
                Ok(SnapshotPolicyEvaluatorLease {
                    owner,
                    expires_at: Utc::now() + chrono::Duration::minutes(1),
                    fencing_token: 1,
                })
            })
        }

        fn renew_evaluator_lease<'a>(
            &'a self,
            lease: &'a SnapshotPolicyEvaluatorLease,
            _lease_for: Duration,
        ) -> SnapshotPortFuture<'a, SnapshotPolicyEvaluatorLease> {
            Box::pin(async move { Ok(lease.clone()) })
        }

        fn evaluate_due_slot<'a>(
            &'a self,
            _context: &'a SnapshotVolumeContext,
            _lease: &'a SnapshotPolicyEvaluatorLease,
            now: DateTime<Utc>,
        ) -> SnapshotPortFuture<'a, SnapshotPolicyEvaluation> {
            Box::pin(async move {
                Ok(SnapshotPolicyEvaluation::NoPolicy {
                    next_wake_at: now + chrono::Duration::minutes(1),
                })
            })
        }

        fn release_evaluator_lease<'a>(
            &'a self,
            _lease: &'a SnapshotPolicyEvaluatorLease,
        ) -> SnapshotPortFuture<'a, ()> {
            Box::pin(async move {
                self.released.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    struct ImmediateSnapshotClock {
        now: DateTime<Utc>,
    }

    impl SnapshotClock for ImmediateSnapshotClock {
        fn now(&self) -> DateTime<Utc> {
            self.now
        }

        fn sleep_until<'a>(
            &'a self,
            _deadline: DateTime<Utc>,
            cancel: CancellationToken,
        ) -> SnapshotPortFuture<'a, ()> {
            Box::pin(async move {
                tokio::task::yield_now().await;
                if cancel.is_cancelled() {
                    Err(SnapshotPortError::Cancelled)
                } else {
                    Ok(())
                }
            })
        }
    }

    struct TestClock {
        now: DateTime<Utc>,
    }

    impl SnapshotClock for TestClock {
        fn now(&self) -> DateTime<Utc> {
            self.now
        }

        fn sleep_until<'a>(
            &'a self,
            _deadline: DateTime<Utc>,
            cancel: CancellationToken,
        ) -> SnapshotPortFuture<'a, ()> {
            Box::pin(async move {
                cancel.cancelled().await;
                Err(SnapshotPortError::Cancelled)
            })
        }
    }
}
