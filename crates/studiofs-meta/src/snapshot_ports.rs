//! Backend-agnostic Snapshot v2 ports.

use crate::schema::PayloadRef;
use crate::snapshot::{
    SnapshotArtifactDescriptor, SnapshotCatalogEntry, SnapshotCreationJob, SnapshotCreationLease,
    SnapshotCut, SnapshotId, SnapshotSha256, VolumeId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// A validated message safe to expose beyond a storage adapter.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RedactedSnapshotMessage(String);

impl RedactedSnapshotMessage {
    /// Creates a safe message from a fixed, caller-controlled description.
    pub fn new(message: impl AsRef<str>) -> Self {
        let message = message.as_ref();
        let safe = message
            .chars()
            .filter(|character| {
                character.is_ascii_alphanumeric()
                    || matches!(character, ' ' | '_' | '-' | '.' | '/' | ':')
            })
            .take(160)
            .collect::<String>();
        if safe.is_empty() {
            Self("snapshot operation failed".to_string())
        } else {
            Self(safe)
        }
    }

    /// Returns the sanitized message text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for RedactedSnapshotMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RedactedSnapshotMessage")
            .field(&self.0)
            .finish()
    }
}

impl std::fmt::Display for RedactedSnapshotMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Redacted failure categories crossing a Snapshot v2 port seam.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Error)]
pub enum SnapshotPortError {
    /// A bounded retry may succeed without changing the durable contract.
    #[error("snapshot operation retryable: {operation}: {message}")]
    Retryable {
        /// Fixed operation name, never a backend error string.
        operation: String,
        /// Sanitized diagnostic message.
        message: RedactedSnapshotMessage,
    },
    /// The operation cannot succeed until its caller or data is corrected.
    #[error("snapshot operation rejected: {code}: {message}")]
    Permanent {
        /// Stable domain error code.
        code: String,
        /// Sanitized diagnostic message.
        message: RedactedSnapshotMessage,
    },
    /// The caller cancelled before the port could complete.
    #[error("snapshot operation cancelled")]
    Cancelled,
}

impl SnapshotPortError {
    /// Converts an arbitrary backend failure into a safe retryable error.
    pub fn from_backend(operation: &str, _source: &str) -> Self {
        let operation = safe_token(operation);
        Self::Retryable {
            operation,
            message: RedactedSnapshotMessage::new("backend unavailable"),
        }
    }
}

fn safe_token(value: &str) -> String {
    let token = value
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, ' ' | '_' | '-' | '.')
        })
        .take(80)
        .collect::<String>();
    if token.is_empty() {
        "snapshot operation".to_string()
    } else {
        token
    }
}

/// Future returned by an asynchronous Snapshot v2 port.
pub type SnapshotPortFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SnapshotPortError>> + Send + 'a>>;

/// Stable identity of one materializer worker.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SnapshotWorkerId(String);

impl SnapshotWorkerId {
    /// Creates a non-empty worker identity.
    pub fn new(value: impl Into<String>) -> Result<Self, SnapshotPortError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(SnapshotPortError::Permanent {
                code: "invalid_worker".to_string(),
                message: RedactedSnapshotMessage::new("worker identity is empty"),
            });
        }
        Ok(Self(value))
    }

    /// Returns the worker identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A claimed durable creation job and its fencing lease.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotJobClaim {
    /// Durable job projection.
    pub job: SnapshotCreationJob,
    /// Lease that fences updates to the job.
    pub lease: SnapshotCreationLease,
}

/// Result of claiming a requested ID or following another worker's job.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotClaimOrFollow {
    /// This caller owns the durable job.
    Claimed(SnapshotJobClaim),
    /// The caller must observe the existing catalog entry.
    Follow(SnapshotCatalogEntry),
}

/// Redacted retry metadata for a creation job.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRetry {
    /// Retry ordinal.
    pub retry_count: u32,
    /// Earliest next retry time.
    pub next_retry_at: DateTime<Utc>,
    /// Safe failure description.
    pub message: RedactedSnapshotMessage,
}

/// Redacted terminal creation failure.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotFailure {
    /// Stable failure code.
    pub code: String,
    /// Safe failure description.
    pub message: RedactedSnapshotMessage,
}

/// Exact proof that a payload range was present and verified remotely.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPayloadProof {
    /// Immutable payload object identity.
    pub object_id: u64,
    /// Byte offset within the object.
    pub offset: u64,
    /// Verified byte length.
    pub length: u32,
    /// Verified payload checksum.
    pub sha256: SnapshotSha256,
}

/// Reference to the immutable captured source used by a materializer.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotSourceDescriptor {
    /// Internal immutable source checkpoint row identifier.
    pub checkpoint_id: i64,
    /// Source checkpoint version.
    pub checkpoint_version: i64,
    /// First event version available to replay.
    pub event_start_version: i64,
    /// Last event version included by the cut.
    pub event_end_version: i64,
}

/// Result of reconstructing one immutable source artifact.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotReconstruction {
    /// Local path containing the reconstructed read model.
    pub path: std::path::PathBuf,
}

/// Payload bytes and their verified identity.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPayloadBytes {
    /// Payload contents.
    pub bytes: Vec<u8>,
    /// Exact checksum of `bytes`.
    pub sha256: SnapshotSha256,
}

/// Stored artifact metadata returned by an artifact adapter.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotStoredArtifact {
    /// Artifact locator.
    pub locator: SnapshotArtifactLocator,
    /// Byte length.
    pub byte_length: u64,
    /// Remote checksum.
    pub sha256: SnapshotSha256,
}

/// Downloaded artifact bytes.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotArtifactBytes {
    /// Artifact contents.
    pub bytes: Vec<u8>,
    /// Exact checksum of `bytes`.
    pub sha256: SnapshotSha256,
}

/// Artifact kind stored in object storage.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotArtifactKind {
    /// Retryable materializer attempt object.
    Attempt,
    /// Immutable final metadata artifact.
    Metadata,
}

/// Validated artifact locator without a storage prefix or raw object key.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotArtifactLocator {
    /// Verified numeric volume identity.
    pub volume_id: VolumeId,
    /// Verified immutable volume UUID.
    pub volume_uuid: String,
    /// Public snapshot identity.
    pub snapshot_id: SnapshotId,
    /// Attempt ordinal, when this is an attempt artifact.
    pub attempt: Option<u64>,
    /// Artifact kind.
    pub kind: SnapshotArtifactKind,
}

/// Scope used for listing artifacts belonging to one verified snapshot.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotArtifactScope {
    /// Verified numeric volume identity.
    pub volume_id: VolumeId,
    /// Verified immutable volume UUID.
    pub volume_uuid: String,
    /// Public snapshot identity.
    pub snapshot_id: SnapshotId,
}

/// Catalog and creation-job operations implemented by PostgreSQL.
pub trait SnapshotJobStore: Send + Sync {
    /// Claims the next queued or retryable job through a fenced CAS.
    fn claim_next<'a>(
        &'a self,
        worker: &'a SnapshotWorkerId,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, Option<SnapshotJobClaim>>;
    /// Claims a requested ID or follows the durable job already recorded for it.
    fn claim_id_or_follow<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
        worker: &'a SnapshotWorkerId,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotClaimOrFollow>;
    /// Renews a lease only when its owner and fence still match.
    fn renew_lease<'a>(
        &'a self,
        claim: &'a SnapshotJobClaim,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotCreationLease>;
    /// Records a retry deadline for the current fenced claim.
    fn mark_retry_wait<'a>(
        &'a self,
        claim: &'a SnapshotJobClaim,
        retry: SnapshotRetry,
    ) -> SnapshotPortFuture<'a, SnapshotCreationJob>;
    /// Records a redacted terminal failure for the current fenced claim.
    fn mark_failed<'a>(
        &'a self,
        claim: &'a SnapshotJobClaim,
        failure: SnapshotFailure,
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry>;
    /// Publishes the descriptor and Ready state only with the current fence.
    fn publish_ready<'a>(
        &'a self,
        claim: &'a SnapshotJobClaim,
        descriptor: &'a SnapshotArtifactDescriptor,
        payload_proofs: &'a [SnapshotPayloadProof],
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry>;
    /// Loads the authoritative catalog projection.
    fn load_catalog<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry>;
}

/// Reconstructs the immutable read model for a captured cut.
pub trait SnapshotSourceReader: Send + Sync {
    /// Writes a reconstruction below the caller-owned temporary root.
    fn reconstruct<'a>(
        &'a self,
        cut: &'a SnapshotCut,
        descriptor: &'a SnapshotSourceDescriptor,
        destination: &'a Path,
    ) -> SnapshotPortFuture<'a, SnapshotReconstruction>;
}

/// Reads and verifies immutable payload ranges.
pub trait SnapshotPayloadReader: Send + Sync {
    /// Reads the exact remote payload range represented by a metadata reference.
    fn stat_and_read<'a>(
        &'a self,
        payload: &'a PayloadRef,
    ) -> SnapshotPortFuture<'a, SnapshotPayloadBytes>;
}

/// Supplies durable time and cancellation-aware waits.
pub trait SnapshotClock: Send + Sync {
    /// Returns the current UTC time used by domain decisions.
    fn now(&self) -> DateTime<Utc>;
    /// Waits until a deadline or cancellation, whichever comes first.
    fn sleep_until<'a>(
        &'a self,
        deadline: DateTime<Utc>,
        cancel: CancellationToken,
    ) -> SnapshotPortFuture<'a, ()>;
}

/// Stores immutable attempt and final artifact objects.
pub trait SnapshotArtifactStore: Send + Sync {
    /// Uploads an attempt object under a validated locator.
    fn put_attempt<'a>(
        &'a self,
        locator: &'a SnapshotArtifactLocator,
        source: &'a Path,
    ) -> SnapshotPortFuture<'a, SnapshotStoredArtifact>;
    /// Verifies and publishes an immutable final artifact.
    fn publish_final<'a>(
        &'a self,
        attempt: &'a SnapshotArtifactLocator,
        final_artifact: &'a SnapshotArtifactLocator,
        expected_sha256: &'a SnapshotSha256,
    ) -> SnapshotPortFuture<'a, SnapshotStoredArtifact>;
    /// Downloads an artifact by validated locator.
    fn get<'a>(
        &'a self,
        locator: &'a SnapshotArtifactLocator,
    ) -> SnapshotPortFuture<'a, SnapshotArtifactBytes>;
    /// Deletes an attempt or cleanup-eligible artifact idempotently.
    fn delete<'a>(&'a self, locator: &'a SnapshotArtifactLocator) -> SnapshotPortFuture<'a, ()>;
    /// Lists artifacts within one validated snapshot scope.
    fn list<'a>(
        &'a self,
        scope: &'a SnapshotArtifactScope,
    ) -> SnapshotPortFuture<'a, Vec<SnapshotStoredArtifact>>;
}
