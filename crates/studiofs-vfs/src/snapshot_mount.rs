//! Production-backed Snapshot v2 mount admission and cache maintenance.
//!
//! The session owns only local cache bookkeeping and the ordering of mount
//! admission. Catalog state, immutable artifact bytes, and active mount leases
//! come from the Snapshot v2 ports supplied by the metadata/runtime layer.

use crate::{StudioVfs, VfsConfigValidated, VfsResult};
use crate::{VfsMeta, into_vfs_meta};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use studiofs_data::DataStore;
use studiofs_meta::snapshot::{
    SnapshotArtifactDescriptor, SnapshotCatalogEntry, SnapshotId, SnapshotLifecycle,
    SnapshotMountLease, SnapshotSha256, SnapshotVolumeContext,
};
use studiofs_meta::snapshot_ports::{
    SnapshotArtifactBytes, SnapshotArtifactKind, SnapshotArtifactLocator, SnapshotArtifactStore,
    SnapshotClock, SnapshotJobStore, SnapshotMountStore, SnapshotPortError,
};
#[cfg(test)]
use studiofs_meta::snapshot_ports::{
    SnapshotArtifactScope, SnapshotFailure, SnapshotJobClaim, SnapshotPayloadProof,
    SnapshotPortFuture, SnapshotRetry, SnapshotStoredArtifact, SnapshotWorkerId,
};
use studiofs_meta::{AccessLevel, MetaEngine, MetaError, MetaFormat};
use thiserror::Error;
use tokio::fs;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const CACHE_IDLE_TTL: ChronoDuration = ChronoDuration::days(7);
const DEFAULT_MOUNT_LEASE_TTL: Duration = Duration::from_secs(60);
const CACHE_LAST_USED_SUFFIX: &str = ".last-used";

/// Configuration for one production snapshot mount session.
#[derive(Debug, Clone)]
pub struct SnapshotMountConfig {
    /// Root below which immutable snapshot cache files are stored.
    pub cache_root: PathBuf,
    /// Duration requested for each durable mount lease.
    pub lease_for: Duration,
}

impl SnapshotMountConfig {
    /// Creates a configuration with the bounded default lease duration.
    #[must_use]
    pub fn new(cache_root: impl Into<PathBuf>) -> Self {
        Self {
            cache_root: cache_root.into(),
            lease_for: DEFAULT_MOUNT_LEASE_TTL,
        }
    }

    /// Replaces the default lease duration.
    #[must_use]
    pub fn with_lease_for(mut self, lease_for: Duration) -> Self {
        self.lease_for = lease_for;
        self
    }
}

/// Immutable identity of a locally cached snapshot artifact.
///
/// The checksum is part of the identity so a republished artifact can never
/// accidentally reuse bytes belonging to an older checksum.
#[derive(Clone, Debug, Eq)]
pub struct SnapshotArtifactCacheKey {
    volume: SnapshotVolumeContext,
    snapshot_id: SnapshotId,
    checksum: SnapshotSha256,
}

impl SnapshotArtifactCacheKey {
    /// Builds a cache identity from the PostgreSQL-verified volume context.
    pub fn new(
        volume: SnapshotVolumeContext,
        snapshot_id: SnapshotId,
        checksum: SnapshotSha256,
    ) -> Self {
        Self {
            volume,
            snapshot_id,
            checksum,
        }
    }

    /// Returns the deterministic local path for this immutable artifact.
    pub fn cache_path(&self, root: &Path) -> PathBuf {
        root.join("metadata")
            .join("snapshots")
            .join(format!("volume-{}", self.volume.volume_id))
            .join(format!(
                "name-{}",
                encode_path_component(&self.volume.volume_name)
            ))
            .join(format!(
                "uuid-{}",
                encode_path_component(&self.volume.volume_uuid)
            ))
            .join(format!(
                "snapshot-{}-sha256-{}.redb",
                encode_path_component(self.snapshot_id.as_str()),
                self.checksum.to_hex()
            ))
    }

    /// Returns the volume identity included in this cache key.
    pub fn volume(&self) -> &SnapshotVolumeContext {
        &self.volume
    }

    /// Returns the snapshot identity included in this cache key.
    pub fn snapshot_id(&self) -> &SnapshotId {
        &self.snapshot_id
    }

    /// Returns the artifact checksum included in this cache key.
    pub const fn checksum(&self) -> SnapshotSha256 {
        self.checksum
    }
}

impl PartialEq for SnapshotArtifactCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.volume == other.volume
            && self.snapshot_id == other.snapshot_id
            && self.checksum == other.checksum
    }
}

impl Hash for SnapshotArtifactCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.volume.volume_id.hash(state);
        self.volume.volume_name.hash(state);
        self.volume.volume_uuid.hash(state);
        self.snapshot_id.hash(state);
        self.checksum.hash(state);
    }
}

/// Errors raised while admitting or maintaining a bounded snapshot mount.
#[derive(Debug, Error)]
pub enum SnapshotMountError {
    /// The catalog entry is not in the only lifecycle state that is mountable.
    #[error("snapshot is not ready")]
    NotReady,
    /// No catalog entry was found for the requested snapshot.
    #[error("snapshot was not found")]
    SnapshotNotFound,
    /// The catalog entry is past its durable retention expiry.
    #[error("snapshot has expired")]
    SnapshotExpired,
    /// The catalog volume does not match the session's verified volume.
    #[error("snapshot volume identity does not match the mount")]
    VolumeMismatch,
    /// A Ready catalog entry did not publish an artifact descriptor.
    #[error("snapshot artifact is missing")]
    ArtifactMissing,
    /// The downloaded or cached bytes have a different length than declared.
    #[error("snapshot artifact length mismatch")]
    ArtifactLengthMismatch,
    /// The downloaded or cached bytes have a different digest than declared.
    #[error("snapshot artifact checksum mismatch")]
    ArtifactChecksumMismatch,
    /// A previously cached artifact failed re-verification.
    #[error("snapshot cache integrity check failed")]
    CacheIntegrity,
    /// The verified artifact could not be opened as snapshot metadata.
    #[error("snapshot metadata could not be opened")]
    MetadataOpen,
    /// The local cache could not be read or updated.
    #[error("snapshot cache I/O failed")]
    CacheIo,
    /// The mount lease has expired.
    #[error("snapshot mount lease expired")]
    LeaseExpired,
    /// The mount lease was already released.
    #[error("snapshot mount lease released")]
    LeaseReleased,
    /// The caller supplied an unusable session configuration.
    #[error("invalid snapshot mount configuration")]
    InvalidConfig,
    /// A backend port rejected or could not complete the operation.
    #[error("snapshot backend operation failed: {0}")]
    Port(#[source] SnapshotPortError),
}

fn map_port_error(error: SnapshotPortError) -> SnapshotMountError {
    match &error {
        SnapshotPortError::Permanent { code, .. } if code.as_str() == "snapshot_not_found" => {
            SnapshotMountError::SnapshotNotFound
        }
        SnapshotPortError::Permanent { code, .. } if code.as_str() == "snapshot_not_ready" => {
            SnapshotMountError::NotReady
        }
        SnapshotPortError::Permanent { code, .. } if code.as_str() == "stale_lease" => {
            SnapshotMountError::LeaseExpired
        }
        _ => SnapshotMountError::Port(error),
    }
}

/// Production-backed session for one verified volume.
pub struct SnapshotMountSession {
    volume: SnapshotVolumeContext,
    jobs: Arc<dyn SnapshotJobStore>,
    artifacts: Arc<dyn SnapshotArtifactStore>,
    leases: Arc<dyn SnapshotMountStore>,
    clock: Arc<dyn SnapshotClock>,
    config: SnapshotMountConfig,
    cache_last_used: Arc<Mutex<HashMap<SnapshotArtifactCacheKey, DateTime<Utc>>>>,
    metadata_handles: Arc<Mutex<HashMap<SnapshotArtifactCacheKey, Arc<dyn VfsMeta>>>>,
    #[cfg(test)]
    test_clock: Arc<TestClock>,
    #[cfg(test)]
    test_jobs: Arc<TestJobStore>,
    #[cfg(test)]
    test_artifacts: Arc<TestArtifactStore>,
    #[cfg(test)]
    test_downloads: Arc<AtomicUsize>,
}

impl SnapshotMountSession {
    /// Creates a mount session over the production Snapshot v2 ports.
    pub fn new(
        volume: SnapshotVolumeContext,
        jobs: Arc<dyn SnapshotJobStore>,
        artifacts: Arc<dyn SnapshotArtifactStore>,
        leases: Arc<dyn SnapshotMountStore>,
        clock: Arc<dyn SnapshotClock>,
        config: SnapshotMountConfig,
    ) -> Result<Self, SnapshotMountError> {
        if config.cache_root.as_os_str().is_empty() || config.lease_for.is_zero() {
            return Err(SnapshotMountError::InvalidConfig);
        }
        Ok(Self {
            volume,
            jobs,
            artifacts,
            leases,
            clock,
            config,
            cache_last_used: Arc::new(Mutex::new(HashMap::new())),
            metadata_handles: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            test_clock: Arc::new(TestClock::new(Utc::now())),
            #[cfg(test)]
            test_jobs: Arc::new(TestJobStore::default()),
            #[cfg(test)]
            test_artifacts: Arc::new(TestArtifactStore::default()),
            #[cfg(test)]
            test_downloads: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Builds a production session from the current PostgreSQL-backed volume
    /// metadata and its configured object-store access.
    pub fn from_meta_engine(
        meta: Arc<dyn MetaEngine>,
        data_store: Arc<DataStore>,
        clock: Arc<dyn SnapshotClock>,
        config: SnapshotMountConfig,
    ) -> Result<Self, SnapshotMountError> {
        let format_info = meta.get_format_info().map_err(map_metadata_open_error)?;
        let postgres = format_info
            .metadata
            .postgres
            .ok_or(SnapshotMountError::InvalidConfig)?;
        let volume =
            SnapshotVolumeContext::new(postgres.volume_id, format_info.name, format_info.uuid)
                .map_err(|_| SnapshotMountError::InvalidConfig)?;
        let jobs = meta
            .snapshot_job_store()
            .ok_or(SnapshotMountError::InvalidConfig)?;
        let leases = meta
            .snapshot_mount_store()
            .ok_or(SnapshotMountError::InvalidConfig)?;
        let artifacts = Arc::new(
            crate::snapshot_worker::DataStoreSnapshotArtifactStore::new(data_store, volume.clone())
                .map_err(|_| SnapshotMountError::InvalidConfig)?,
        );
        Self::new(volume, jobs, artifacts, leases, clock, config)
    }

    /// Mounts a Ready, unexpired snapshot after verifying its immutable bytes.
    pub async fn mount(
        &self,
        snapshot_id: &SnapshotId,
        owner: impl Into<String>,
    ) -> Result<SnapshotMount, SnapshotMountError> {
        let owner = owner.into();
        if owner.trim().is_empty() {
            return Err(SnapshotMountError::Port(SnapshotPortError::permanent(
                "invalid_mount_lease",
                "mount owner identity is empty",
            )));
        }
        let catalog = self
            .jobs
            .load_catalog(snapshot_id)
            .await
            .map_err(map_port_error)?;
        self.validate_catalog(snapshot_id, &catalog)?;
        let descriptor = catalog
            .artifact
            .as_ref()
            .ok_or(SnapshotMountError::ArtifactMissing)?
            .clone();
        if descriptor.cut != catalog.cut {
            return Err(SnapshotMountError::Port(SnapshotPortError::permanent(
                "snapshot_artifact_cut_mismatch",
                "snapshot artifact cut does not match its catalog",
            )));
        }
        let cache_key = SnapshotArtifactCacheKey::new(
            self.volume.clone(),
            catalog.snapshot_id.clone(),
            descriptor.sha256,
        );
        let locator = SnapshotArtifactLocator {
            volume_id: self.volume.volume_id,
            volume_uuid: self.volume.volume_uuid.clone(),
            snapshot_id: catalog.snapshot_id.clone(),
            attempt: None,
            kind: SnapshotArtifactKind::Metadata,
        };
        let lease = self
            .leases
            .acquire(
                snapshot_id,
                &owner,
                &self.volume.volume_uuid,
                &descriptor.sha256,
                self.config.lease_for,
            )
            .await
            .map_err(map_port_error)?;
        if lease.snapshot_id != *snapshot_id
            || lease.owner != owner
            || lease.cache_volume_uuid != self.volume.volume_uuid
            || lease.cache_sha256 != descriptor.sha256
            || lease.released
            || lease.expires_at <= self.clock.now()
        {
            let _ = self.leases.release(&lease).await;
            return Err(SnapshotMountError::Port(SnapshotPortError::permanent(
                "invalid_mount_lease",
                "mount lease identity does not match",
            )));
        }
        let lease_state = Arc::new(SnapshotMountLeaseState {
            leases: Arc::clone(&self.leases),
            clock: Arc::clone(&self.clock),
            lease: Mutex::new(lease),
            lease_for: self.config.lease_for,
        });
        let mounted = SnapshotMount {
            lease_state: Arc::clone(&lease_state),
            metadata: Mutex::new(None),
            metadata_handles: Arc::clone(&self.metadata_handles),
            renewal: Mutex::new(Some(SnapshotMountLeaseRenewal::spawn(lease_state))),
            cache_key: cache_key.clone(),
        };
        let cache_path = match self
            .load_or_download(&cache_key, &locator, &descriptor)
            .await
        {
            Ok(path) => path,
            Err(error) => {
                let _ = mounted.release().await;
                return Err(error);
            }
        };
        let runtime_meta = match self.open_runtime_meta(&cache_key, &cache_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                let _ = mounted.release().await;
                return Err(error);
            }
        };
        *mounted.metadata.lock() = Some(runtime_meta);
        self.cache_last_used
            .lock()
            .insert(cache_key.clone(), self.clock.now());
        if let Err(error) = self.persist_last_used(&cache_key, self.clock.now()).await {
            let _ = mounted.release().await;
            self.metadata_handles.lock().remove(&cache_key);
            self.cache_last_used.lock().remove(&cache_key);
            return Err(error);
        }
        if mounted.is_closed() {
            let _ = mounted.release().await;
            return Err(SnapshotMountError::LeaseExpired);
        }
        Ok(mounted)
    }

    /// Removes locally tracked cache entries idle for seven days only when no
    /// matching durable mount lease remains active.
    pub async fn evict_idle_cache(&self) -> Result<usize, SnapshotMountError> {
        self.load_persisted_last_used().await?;
        let now = self.clock.now();
        let candidates = self
            .cache_last_used
            .lock()
            .iter()
            .filter_map(|(cache_key, last_used)| {
                (now.signed_duration_since(*last_used) >= CACHE_IDLE_TTL)
                    .then_some(cache_key.clone())
            })
            .collect::<Vec<_>>();

        let mut evicted = 0;
        for cache_key in candidates {
            let active = self
                .leases
                .has_active_lease(
                    cache_key.snapshot_id(),
                    &cache_key.volume().volume_uuid,
                    &cache_key.checksum(),
                )
                .await
                .map_err(map_port_error)?;
            if active {
                continue;
            }
            let path = cache_key.cache_path(&self.config.cache_root);
            self.metadata_handles.lock().remove(&cache_key);
            match fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(SnapshotMountError::CacheIo),
            }
            let _ = fs::remove_file(last_used_path(&path)).await;
            self.cache_last_used.lock().remove(&cache_key);
            evicted += 1;
        }
        Ok(evicted)
    }

    fn validate_catalog(
        &self,
        snapshot_id: &SnapshotId,
        catalog: &SnapshotCatalogEntry,
    ) -> Result<(), SnapshotMountError> {
        if catalog.snapshot_id != *snapshot_id {
            return Err(SnapshotMountError::Port(SnapshotPortError::permanent(
                "snapshot_identity_mismatch",
                "snapshot catalog identity does not match the request",
            )));
        }
        if catalog.volume != self.volume {
            return Err(SnapshotMountError::VolumeMismatch);
        }
        if catalog.cut.volume_id() != self.volume.volume_id {
            return Err(SnapshotMountError::VolumeMismatch);
        }
        if catalog.lifecycle != SnapshotLifecycle::Ready {
            return Err(SnapshotMountError::NotReady);
        }
        if catalog
            .expires_at
            .is_some_and(|expires_at| expires_at <= self.clock.now())
        {
            return Err(SnapshotMountError::SnapshotExpired);
        }
        Ok(())
    }

    fn open_runtime_meta(
        &self,
        cache_key: &SnapshotArtifactCacheKey,
        cache_path: &Path,
    ) -> Result<Arc<dyn VfsMeta>, SnapshotMountError> {
        let cached = self.metadata_handles.lock().get(cache_key).cloned();
        if let Some(metadata) = cached {
            return Ok(metadata);
        }
        let snapshot_meta = studiofs_meta::open_read_only_snapshot_meta(cache_path)
            .map_err(map_metadata_open_error)?;
        let format_info = snapshot_meta
            .get_format_info()
            .map_err(map_metadata_open_error)?;
        if format_info.name != self.volume.volume_name
            || format_info.uuid != self.volume.volume_uuid
        {
            return Err(SnapshotMountError::VolumeMismatch);
        }
        let runtime_meta: Arc<dyn VfsMeta> = into_vfs_meta(snapshot_meta);
        let mut metadata_handles = self.metadata_handles.lock();
        Ok(metadata_handles
            .entry(cache_key.clone())
            .or_insert(runtime_meta)
            .clone())
    }

    async fn load_or_download(
        &self,
        cache_key: &SnapshotArtifactCacheKey,
        locator: &SnapshotArtifactLocator,
        descriptor: &SnapshotArtifactDescriptor,
    ) -> Result<PathBuf, SnapshotMountError> {
        let cache_path = cache_key.cache_path(&self.config.cache_root);
        match fs::read(&cache_path).await {
            Ok(bytes) => {
                verify_artifact(&bytes, descriptor, true)?;
                Ok(cache_path)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let downloaded = self.artifacts.get(locator).await.map_err(map_port_error)?;
                verify_downloaded_artifact(&downloaded, descriptor)?;
                match write_cache_if_absent(&cache_path, &downloaded.bytes).await? {
                    CacheWriteOutcome::Stored => {
                        #[cfg(test)]
                        self.test_downloads.fetch_add(1, Ordering::Relaxed);
                        Ok(cache_path)
                    }
                    CacheWriteOutcome::AlreadyExists => {
                        let bytes = fs::read(&cache_path)
                            .await
                            .map_err(|_| SnapshotMountError::CacheIo)?;
                        verify_artifact(&bytes, descriptor, true)?;
                        Ok(cache_path)
                    }
                }
            }
            Err(_) => Err(SnapshotMountError::CacheIo),
        }
    }

    async fn persist_last_used(
        &self,
        cache_key: &SnapshotArtifactCacheKey,
        at: DateTime<Utc>,
    ) -> Result<(), SnapshotMountError> {
        let path = cache_key.cache_path(&self.config.cache_root);
        let sidecar = last_used_path(&path);
        let parent = sidecar.parent().ok_or(SnapshotMountError::CacheIo)?;
        fs::create_dir_all(parent)
            .await
            .map_err(|_| SnapshotMountError::CacheIo)?;
        let temporary = parent.join(format!(".snapshot-last-used-{}.tmp", uuid::Uuid::new_v4()));
        if fs::write(&temporary, at.to_rfc3339().as_bytes())
            .await
            .is_err()
        {
            let _ = fs::remove_file(&temporary).await;
            return Err(SnapshotMountError::CacheIo);
        }
        if let Err(error) = fs::remove_file(&sidecar).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            let _ = fs::remove_file(&temporary).await;
            return Err(SnapshotMountError::CacheIo);
        }
        if fs::rename(&temporary, &sidecar).await.is_err() {
            let _ = fs::remove_file(&temporary).await;
            return Err(SnapshotMountError::CacheIo);
        }
        Ok(())
    }

    async fn load_persisted_last_used(&self) -> Result<(), SnapshotMountError> {
        let root = self.config.cache_root.join("metadata").join("snapshots");
        let mut volumes = match fs::read_dir(&root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(SnapshotMountError::CacheIo),
        };
        while let Some(volume_entry) = volumes
            .next_entry()
            .await
            .map_err(|_| SnapshotMountError::CacheIo)?
        {
            let Some(volume_id) = volume_entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix("volume-"))
                .and_then(|value| value.parse::<i64>().ok())
            else {
                continue;
            };
            let mut names = match fs::read_dir(volume_entry.path()).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            while let Some(name_entry) = names
                .next_entry()
                .await
                .map_err(|_| SnapshotMountError::CacheIo)?
            {
                let Some(volume_name) = name_entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix("name-"))
                    .and_then(decode_path_component)
                else {
                    continue;
                };
                let mut uuids = match fs::read_dir(name_entry.path()).await {
                    Ok(entries) => entries,
                    Err(_) => continue,
                };
                while let Some(uuid_entry) = uuids
                    .next_entry()
                    .await
                    .map_err(|_| SnapshotMountError::CacheIo)?
                {
                    let Some(volume_uuid) = uuid_entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.strip_prefix("uuid-"))
                        .and_then(decode_path_component)
                    else {
                        continue;
                    };
                    let mut artifacts = match fs::read_dir(uuid_entry.path()).await {
                        Ok(entries) => entries,
                        Err(_) => continue,
                    };
                    while let Some(artifact_entry) = artifacts
                        .next_entry()
                        .await
                        .map_err(|_| SnapshotMountError::CacheIo)?
                    {
                        let path = artifact_entry.path();
                        if path.extension().and_then(|value| value.to_str()) != Some("redb") {
                            continue;
                        }
                        let Some((snapshot_id, checksum)) = parse_cache_file_name(
                            artifact_entry.file_name().to_str().unwrap_or_default(),
                        ) else {
                            continue;
                        };
                        let sidecar = last_used_path(&path);
                        let last_used = match fs::read_to_string(sidecar).await {
                            Ok(value) => value.trim().parse::<DateTime<Utc>>().ok(),
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                            Err(_) => continue,
                        };
                        let last_used = match last_used {
                            Some(last_used) => last_used,
                            None => {
                                // A crash can leave the immutable artifact without its
                                // last-used sidecar. Use the artifact mtime so the entry
                                // remains eligible for bounded cache cleanup.
                                let Some(modified) = fs::metadata(&path)
                                    .await
                                    .ok()
                                    .and_then(|metadata| metadata.modified().ok())
                                else {
                                    continue;
                                };
                                DateTime::<Utc>::from(modified)
                            }
                        };
                        let Ok(volume) = SnapshotVolumeContext::new(
                            volume_id,
                            volume_name.clone(),
                            volume_uuid.clone(),
                        ) else {
                            continue;
                        };
                        if volume != self.volume {
                            continue;
                        }
                        self.cache_last_used.lock().insert(
                            SnapshotArtifactCacheKey::new(volume, snapshot_id, checksum),
                            last_used,
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn new_for_tests(root: &Path, now: DateTime<Utc>, volume: SnapshotVolumeContext) -> Self {
        Self::new_for_tests_with_lease_for(root, now, volume, DEFAULT_MOUNT_LEASE_TTL)
    }

    #[cfg(test)]
    fn new_for_tests_with_lease_for(
        root: &Path,
        now: DateTime<Utc>,
        volume: SnapshotVolumeContext,
        lease_for: Duration,
    ) -> Self {
        let clock = Arc::new(TestClock::new(now));
        let jobs = Arc::new(TestJobStore::default());
        let artifacts = Arc::new(TestArtifactStore::default());
        let leases = Arc::new(TestMountStore::new(Arc::clone(&clock)));
        let mut session = Self::new(
            volume,
            Arc::clone(&jobs) as Arc<dyn SnapshotJobStore>,
            Arc::clone(&artifacts) as Arc<dyn SnapshotArtifactStore>,
            leases,
            Arc::clone(&clock) as Arc<dyn SnapshotClock>,
            SnapshotMountConfig::new(root).with_lease_for(lease_for),
        )
        .unwrap_or_else(|_| unreachable!("test mount configuration is valid"));
        session.test_clock = clock;
        session.test_jobs = jobs;
        session.test_artifacts = artifacts;
        session
    }

    #[cfg(test)]
    fn set_catalog(&self, catalog: SnapshotCatalogEntry) {
        self.test_jobs.set_catalog(catalog);
    }

    #[cfg(test)]
    fn put_artifact(&self, bytes: Vec<u8>) {
        self.test_artifacts
            .put_for_snapshot(self.test_jobs.snapshot_id(), bytes);
    }

    #[cfg(test)]
    fn download_count(&self) -> usize {
        self.test_downloads.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn advance_clock(&self, duration: Duration) {
        self.test_clock.advance(duration);
    }

    #[cfg(test)]
    async fn corrupt_cached_artifact(&self, cache_key: &SnapshotArtifactCacheKey) {
        let path = cache_key.cache_path(&self.config.cache_root);
        let _ = fs::write(path, b"corrupt snapshot cache").await;
    }
}

struct SnapshotMountLeaseState {
    leases: Arc<dyn SnapshotMountStore>,
    clock: Arc<dyn SnapshotClock>,
    lease: Mutex<SnapshotMountLease>,
    lease_for: Duration,
}

struct SnapshotMountLeaseRenewal {
    cancel: CancellationToken,
    task: JoinHandle<Result<(), SnapshotMountError>>,
}

impl SnapshotMountLeaseRenewal {
    fn spawn(lease_state: Arc<SnapshotMountLeaseState>) -> Self {
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let task =
            tokio::spawn(
                async move { renew_until_cancelled_state(lease_state, task_cancel).await },
            );
        Self { cancel, task }
    }
}

impl SnapshotMountLeaseState {
    fn is_closed(&self) -> bool {
        let lease = self.lease.lock();
        lease.released || lease.expires_at <= self.clock.now()
    }

    fn mark_released(&self) {
        self.lease.lock().released = true;
    }
}

async fn renew_lease_state(
    lease_state: &SnapshotMountLeaseState,
) -> Result<(), SnapshotMountError> {
    let current = {
        let mut lease = lease_state.lease.lock();
        if lease.released {
            return Err(SnapshotMountError::LeaseReleased);
        }
        if lease.expires_at <= lease_state.clock.now() {
            lease.released = true;
            return Err(SnapshotMountError::LeaseExpired);
        }
        lease.clone()
    };
    let renewed = lease_state
        .leases
        .renew(&current, lease_state.lease_for)
        .await
        .map_err(map_port_error);
    match renewed {
        Ok(renewed) => {
            if renewed.snapshot_id != current.snapshot_id
                || renewed.owner != current.owner
                || renewed.cache_volume_uuid != current.cache_volume_uuid
                || renewed.cache_sha256 != current.cache_sha256
                || renewed.fencing_token != current.fencing_token
                || renewed.released
                || renewed.expires_at <= lease_state.clock.now()
            {
                lease_state.mark_released();
                return Err(SnapshotMountError::Port(SnapshotPortError::permanent(
                    "invalid_mount_lease",
                    "renewed mount lease identity does not match",
                )));
            }
            *lease_state.lease.lock() = renewed;
            Ok(())
        }
        Err(error @ (SnapshotMountError::LeaseExpired | SnapshotMountError::LeaseReleased)) => {
            lease_state.mark_released();
            Err(error)
        }
        Err(error @ SnapshotMountError::Port(SnapshotPortError::Retryable { .. })) => Err(error),
        Err(error) => {
            lease_state.mark_released();
            Err(error)
        }
    }
}

async fn renew_until_cancelled_state(
    lease_state: Arc<SnapshotMountLeaseState>,
    cancel: CancellationToken,
) -> Result<(), SnapshotMountError> {
    let interval = (lease_state.lease_for / 3).max(Duration::from_millis(1));
    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let deadline = {
            let lease = lease_state.lease.lock();
            if lease.released {
                return Err(SnapshotMountError::LeaseReleased);
            }
            let now = lease_state.clock.now();
            if lease.expires_at <= now {
                drop(lease);
                lease_state.mark_released();
                return Err(SnapshotMountError::LeaseExpired);
            }
            let interval =
                ChronoDuration::from_std(interval).unwrap_or_else(|_| ChronoDuration::zero());
            now.checked_add_signed(interval)
                .unwrap_or(lease.expires_at)
                .min(lease.expires_at)
        };
        match lease_state
            .clock
            .sleep_until(deadline, cancel.clone())
            .await
        {
            Ok(()) => {}
            Err(SnapshotPortError::Cancelled) if cancel.is_cancelled() => return Ok(()),
            Err(error) => return Err(map_port_error(error)),
        }
        if cancel.is_cancelled() {
            return Ok(());
        }
        match renew_lease_state(&lease_state).await {
            Ok(()) => {}
            Err(SnapshotMountError::Port(SnapshotPortError::Retryable { .. })) => {
                if lease_state.is_closed() {
                    lease_state.mark_released();
                    return Err(SnapshotMountError::LeaseExpired);
                }
            }
            Err(error) => {
                lease_state.mark_released();
                return Err(error);
            }
        }
    }
}

/// A verified local snapshot session protected by a renewable durable lease.
pub struct SnapshotMount {
    lease_state: Arc<SnapshotMountLeaseState>,
    metadata: Mutex<Option<Arc<dyn VfsMeta>>>,
    metadata_handles: Arc<Mutex<HashMap<SnapshotArtifactCacheKey, Arc<dyn VfsMeta>>>>,
    renewal: Mutex<Option<SnapshotMountLeaseRenewal>>,
    cache_key: SnapshotArtifactCacheKey,
}

impl SnapshotMount {
    /// Returns the read-only metadata facade for this immutable snapshot.
    pub fn metadata(&self) -> Arc<dyn VfsMeta> {
        self.metadata
            .lock()
            .as_ref()
            .cloned()
            .expect("snapshot metadata is available while the mount is admitted")
    }

    /// Opens the normal VFS read path over this snapshot without enabling any
    /// write-side workers or metadata mutations.
    pub fn open_vfs(
        &self,
        data_config: &studiofs_data::DataStoreConfig,
        cache_dir: &str,
        cache_size: u64,
        memory_cache_budget_bytes: u64,
        config: VfsConfigValidated,
    ) -> VfsResult<Arc<StudioVfs>> {
        let lease_state = Arc::clone(&self.lease_state);
        let snapshot_mount_fence: crate::vfs::SnapshotMountFence = Arc::new(move |ino| {
            if lease_state.is_closed() {
                return Err(crate::VfsError::Meta(MetaError::AccessDenied {
                    ino,
                    principal: "snapshot-mount-lease".to_string(),
                    required: AccessLevel::ReadOnly,
                    actual: AccessLevel::NoAccess,
                }));
            }
            Ok(())
        });
        StudioVfs::new_read_only_with_snapshot_mount_fence(
            self.metadata(),
            data_config,
            cache_dir,
            cache_size,
            memory_cache_budget_bytes,
            config,
            snapshot_mount_fence,
        )
    }

    /// Returns the immutable identity of the mounted artifact.
    pub fn cache_key(&self) -> SnapshotArtifactCacheKey {
        self.cache_key.clone()
    }

    /// Renews this mount's durable lease while its local fence is still valid.
    pub async fn renew_lease(&self) -> Result<(), SnapshotMountError> {
        renew_lease_state(&self.lease_state).await
    }

    async fn stop_lease_renewal(&self) -> Result<(), SnapshotMountError> {
        let Some(renewal) = self.renewal.lock().take() else {
            return Ok(());
        };
        renewal.cancel.cancel();
        renewal.task.await.map_err(|_error| {
            SnapshotMountError::Port(SnapshotPortError::permanent(
                "lease_renewal_task",
                "snapshot lease renewal task failed",
            ))
        })?
    }

    /// Releases this mount's durable lease after stopping renewal and closing
    /// the local metadata handle. The caller should drop the VFS first so the
    /// last redb handle is gone before another process can evict the cache.
    pub async fn release(&self) -> Result<(), SnapshotMountError> {
        let renewal_result = self.stop_lease_renewal().await;
        self.metadata.lock().take();
        self.metadata_handles.lock().remove(&self.cache_key);
        let to_release = {
            let mut lease = self.lease_state.lease.lock();
            if lease.released {
                None
            } else if lease.expires_at <= self.lease_state.clock.now() {
                lease.released = true;
                None
            } else {
                Some(lease.clone())
            }
        };
        let release_result = if let Some(current) = to_release {
            self.lease_state
                .leases
                .release(&current)
                .await
                .map_err(map_port_error)
        } else {
            Ok(())
        };
        self.lease_state.mark_released();
        match (renewal_result, release_result) {
            (Ok(()), result) => result,
            (Err(error), Ok(())) => Err(error),
            (Err(_), Err(error)) => Err(error),
        }
    }

    /// Reports whether this handle has been released or its lease has expired.
    pub fn is_closed(&self) -> bool {
        self.lease_state.is_closed()
    }
}

fn map_metadata_open_error(_error: MetaError) -> SnapshotMountError {
    SnapshotMountError::MetadataOpen
}

impl Drop for SnapshotMount {
    fn drop(&mut self) {
        if let Some(renewal) = self.renewal.get_mut().take() {
            renewal.cancel.cancel();
        }
        // Drop cannot await the durable port. Marking the local handle closed
        // prevents reuse while PostgreSQL retains the lease until expiry.
        self.lease_state.mark_released();
    }
}

fn verify_downloaded_artifact(
    artifact: &SnapshotArtifactBytes,
    descriptor: &SnapshotArtifactDescriptor,
) -> Result<(), SnapshotMountError> {
    let computed = SnapshotSha256::from_bytes(Sha256::digest(&artifact.bytes).into());
    if artifact.sha256 != computed {
        return Err(SnapshotMountError::ArtifactChecksumMismatch);
    }
    verify_artifact(&artifact.bytes, descriptor, false)
}

fn verify_artifact(
    bytes: &[u8],
    descriptor: &SnapshotArtifactDescriptor,
    cached: bool,
) -> Result<(), SnapshotMountError> {
    let integrity_error = |error| {
        if cached {
            SnapshotMountError::CacheIntegrity
        } else {
            error
        }
    };
    if bytes.len() as u64 != descriptor.byte_length {
        return Err(integrity_error(SnapshotMountError::ArtifactLengthMismatch));
    }
    let digest = SnapshotSha256::from_bytes(Sha256::digest(bytes).into());
    if digest != descriptor.sha256 {
        return Err(integrity_error(
            SnapshotMountError::ArtifactChecksumMismatch,
        ));
    }
    Ok(())
}

enum CacheWriteOutcome {
    Stored,
    AlreadyExists,
}

async fn write_cache_if_absent(
    path: &Path,
    bytes: &[u8],
) -> Result<CacheWriteOutcome, SnapshotMountError> {
    let parent = path.parent().ok_or(SnapshotMountError::CacheIo)?;
    fs::create_dir_all(parent)
        .await
        .map_err(|_| SnapshotMountError::CacheIo)?;
    let temporary = parent.join(format!(".snapshot-cache-{}.tmp", uuid::Uuid::new_v4()));
    if fs::write(&temporary, bytes).await.is_err() {
        let _ = fs::remove_file(&temporary).await;
        return Err(SnapshotMountError::CacheIo);
    }
    let outcome = match fs::hard_link(&temporary, path).await {
        Ok(()) => CacheWriteOutcome::Stored,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            CacheWriteOutcome::AlreadyExists
        }
        Err(error) if hard_link_unsupported(&error) => match fs::rename(&temporary, path).await {
            Ok(()) => CacheWriteOutcome::Stored,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                CacheWriteOutcome::AlreadyExists
            }
            Err(_) => {
                let _ = fs::remove_file(&temporary).await;
                return Err(SnapshotMountError::CacheIo);
            }
        },
        Err(_) => {
            let _ = fs::remove_file(&temporary).await;
            return Err(SnapshotMountError::CacheIo);
        }
    };
    let _ = fs::remove_file(&temporary).await;
    Ok(outcome)
}

fn hard_link_unsupported(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::Unsupported
        || (cfg!(windows) && error.raw_os_error() == Some(50))
}

fn encode_path_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        use std::fmt::Write;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn last_used_path(cache_path: &Path) -> PathBuf {
    PathBuf::from(format!(
        "{}{}",
        cache_path.display(),
        CACHE_LAST_USED_SUFFIX
    ))
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn decode_hex_bytes(value: &str) -> Option<Vec<u8>> {
    if !value.is_ascii() || !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|chunk| Some((decode_hex_nibble(chunk[0])? << 4) | decode_hex_nibble(chunk[1])?))
        .collect::<Option<Vec<_>>>()
}

fn decode_path_component(value: &str) -> Option<String> {
    String::from_utf8(decode_hex_bytes(value)?).ok()
}

fn parse_cache_file_name(value: &str) -> Option<(SnapshotId, SnapshotSha256)> {
    let value = value.strip_suffix(".redb")?;
    let value = value.strip_prefix("snapshot-")?;
    let (snapshot_hex, checksum_hex) = value.split_once("-sha256-")?;
    if checksum_hex.len() != 64 {
        return None;
    }
    let checksum = decode_hex_bytes(checksum_hex)?;
    let checksum = SnapshotSha256::from_bytes(checksum.as_slice().try_into().ok()?);
    let snapshot = SnapshotId::parse(&decode_path_component(snapshot_hex)?).ok()?;
    Some((snapshot, checksum))
}

#[cfg(test)]
fn unsupported<'a, T>() -> SnapshotPortFuture<'a, T> {
    Box::pin(async {
        Err(SnapshotPortError::permanent(
            "unsupported_test_port",
            "operation is not used by this test",
        ))
    })
}

#[cfg(test)]
#[derive(Clone)]
struct TestClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

#[cfg(test)]
impl TestClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    fn advance(&self, duration: Duration) {
        let Ok(delta) = ChronoDuration::from_std(duration) else {
            return;
        };
        let mut now = self.now.lock();
        *now = now
            .checked_add_signed(delta)
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
    }
}

#[cfg(test)]
impl SnapshotClock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock()
    }

    fn sleep_until<'a>(
        &'a self,
        deadline: DateTime<Utc>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> SnapshotPortFuture<'a, ()> {
        Box::pin(async move {
            loop {
                if cancel.is_cancelled() {
                    return Err(SnapshotPortError::Cancelled);
                }
                if self.now() >= deadline {
                    return Ok(());
                }
                tokio::task::yield_now().await;
            }
        })
    }
}

#[cfg(test)]
#[derive(Default)]
struct TestJobStore {
    state: Mutex<TestJobState>,
}

#[cfg(test)]
#[derive(Default)]
struct TestJobState {
    catalogs: HashMap<SnapshotId, SnapshotCatalogEntry>,
    last_snapshot_id: Option<SnapshotId>,
}

#[cfg(test)]
impl TestJobStore {
    fn set_catalog(&self, catalog: SnapshotCatalogEntry) {
        let mut state = self.state.lock();
        state.last_snapshot_id = Some(catalog.snapshot_id.clone());
        state.catalogs.insert(catalog.snapshot_id.clone(), catalog);
    }

    fn snapshot_id(&self) -> Option<SnapshotId> {
        self.state.lock().last_snapshot_id.clone()
    }
}

#[cfg(test)]
impl SnapshotJobStore for TestJobStore {
    fn claim_next<'a>(
        &'a self,
        _worker: &'a SnapshotWorkerId,
        _lease_for: Duration,
    ) -> SnapshotPortFuture<'a, Option<SnapshotJobClaim>> {
        unsupported()
    }

    fn claim_id_or_follow<'a>(
        &'a self,
        _snapshot_id: &'a SnapshotId,
        _worker: &'a SnapshotWorkerId,
        _lease_for: Duration,
    ) -> SnapshotPortFuture<'a, studiofs_meta::snapshot_ports::SnapshotClaimOrFollow> {
        unsupported()
    }

    fn renew_lease<'a>(
        &'a self,
        _claim: &'a SnapshotJobClaim,
        _lease_for: Duration,
    ) -> SnapshotPortFuture<'a, studiofs_meta::snapshot::SnapshotCreationLease> {
        unsupported()
    }

    fn release_lease<'a>(&'a self, _claim: &'a SnapshotJobClaim) -> SnapshotPortFuture<'a, ()> {
        unsupported()
    }

    fn mark_retry_wait<'a>(
        &'a self,
        _claim: &'a SnapshotJobClaim,
        _retry: SnapshotRetry,
    ) -> SnapshotPortFuture<'a, studiofs_meta::snapshot::SnapshotCreationJob> {
        unsupported()
    }

    fn mark_failed<'a>(
        &'a self,
        _claim: &'a SnapshotJobClaim,
        _failure: SnapshotFailure,
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
        unsupported()
    }

    fn publish_ready<'a>(
        &'a self,
        _claim: &'a SnapshotJobClaim,
        _descriptor: &'a SnapshotArtifactDescriptor,
        _payload_proofs: &'a [SnapshotPayloadProof],
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
        unsupported()
    }

    fn load_catalog<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
        let catalog = self.state.lock().catalogs.get(snapshot_id).cloned();
        Box::pin(async move {
            catalog.ok_or_else(|| {
                SnapshotPortError::permanent("snapshot_not_found", "snapshot does not exist")
            })
        })
    }
}

#[cfg(test)]
#[derive(Default)]
struct TestArtifactState {
    artifacts: HashMap<SnapshotId, Vec<u8>>,
    get_delay: Option<Duration>,
}

#[cfg(test)]
#[derive(Default)]
struct TestArtifactStore {
    state: Mutex<TestArtifactState>,
}

#[cfg(test)]
impl TestArtifactStore {
    fn put_for_snapshot(&self, snapshot_id: Option<SnapshotId>, bytes: Vec<u8>) {
        if let Some(snapshot_id) = snapshot_id {
            self.state.lock().artifacts.insert(snapshot_id, bytes);
        }
    }

    fn set_get_delay(&self, delay: Duration) {
        self.state.lock().get_delay = Some(delay);
    }
}

#[cfg(test)]
impl SnapshotArtifactStore for TestArtifactStore {
    fn put_attempt<'a>(
        &'a self,
        _locator: &'a SnapshotArtifactLocator,
        _source: &'a Path,
    ) -> SnapshotPortFuture<'a, SnapshotStoredArtifact> {
        unsupported()
    }

    fn publish_final<'a>(
        &'a self,
        _attempt: &'a SnapshotArtifactLocator,
        _final_artifact: &'a SnapshotArtifactLocator,
        _source: &'a Path,
        _expected_sha256: &'a SnapshotSha256,
    ) -> SnapshotPortFuture<'a, SnapshotStoredArtifact> {
        unsupported()
    }

    fn get<'a>(
        &'a self,
        locator: &'a SnapshotArtifactLocator,
    ) -> SnapshotPortFuture<'a, SnapshotArtifactBytes> {
        let (bytes, get_delay) = {
            let state = self.state.lock();
            (
                state.artifacts.get(&locator.snapshot_id).cloned(),
                state.get_delay,
            )
        };
        Box::pin(async move {
            if let Some(delay) = get_delay {
                tokio::time::sleep(delay).await;
            }
            let bytes = bytes.ok_or_else(|| {
                SnapshotPortError::permanent(
                    "artifact_not_found",
                    "snapshot artifact is unavailable",
                )
            })?;
            let sha256 = SnapshotSha256::from_bytes(Sha256::digest(&bytes).into());
            Ok(SnapshotArtifactBytes { bytes, sha256 })
        })
    }

    fn delete<'a>(&'a self, _locator: &'a SnapshotArtifactLocator) -> SnapshotPortFuture<'a, ()> {
        unsupported()
    }

    fn list<'a>(
        &'a self,
        _scope: &'a SnapshotArtifactScope,
    ) -> SnapshotPortFuture<'a, Vec<SnapshotStoredArtifact>> {
        unsupported()
    }
}

#[cfg(test)]
struct TestMountStore {
    clock: Arc<TestClock>,
    state: Mutex<TestMountState>,
}

#[cfg(test)]
#[derive(Default)]
struct TestMountState {
    leases: HashMap<i64, SnapshotMountLease>,
    next_lease_id: i64,
}

#[cfg(test)]
impl TestMountStore {
    fn new(clock: Arc<TestClock>) -> Self {
        Self {
            clock,
            state: Mutex::new(TestMountState {
                next_lease_id: 1,
                ..TestMountState::default()
            }),
        }
    }

    fn expiry(&self, lease_for: Duration) -> DateTime<Utc> {
        let delta = ChronoDuration::from_std(lease_for).unwrap_or_else(|_| ChronoDuration::zero());
        self.clock
            .now()
            .checked_add_signed(delta)
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
    }

    fn matches(lease: &SnapshotMountLease, expected: &SnapshotMountLease) -> bool {
        lease.lease_id == expected.lease_id
            && lease.snapshot_id == expected.snapshot_id
            && lease.owner == expected.owner
            && lease.cache_volume_uuid == expected.cache_volume_uuid
            && lease.cache_sha256 == expected.cache_sha256
            && lease.fencing_token == expected.fencing_token
    }
}

#[cfg(test)]
impl SnapshotMountStore for TestMountStore {
    fn acquire<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
        owner: &'a str,
        cache_volume_uuid: &'a str,
        cache_sha256: &'a SnapshotSha256,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotMountLease> {
        let mut state = self.state.lock();
        let lease_id = state.next_lease_id;
        state.next_lease_id += 1;
        let now = self.clock.now();
        let lease = SnapshotMountLease {
            lease_id,
            snapshot_id: snapshot_id.clone(),
            owner: owner.to_string(),
            cache_volume_uuid: cache_volume_uuid.to_string(),
            cache_sha256: *cache_sha256,
            expires_at: self.expiry(lease_for),
            heartbeat_at: now,
            fencing_token: lease_id,
            released: false,
        };
        state.leases.insert(lease_id, lease.clone());
        Box::pin(async move { Ok(lease) })
    }

    fn renew<'a>(
        &'a self,
        lease: &'a SnapshotMountLease,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotMountLease> {
        let mut state = self.state.lock();
        let Some(current) = state.leases.get_mut(&lease.lease_id) else {
            return Box::pin(async {
                Err(SnapshotPortError::permanent(
                    "stale_lease",
                    "snapshot mount lease is no longer owned",
                ))
            });
        };
        if current.released
            || current.expires_at <= self.clock.now()
            || !Self::matches(current, lease)
        {
            return Box::pin(async {
                Err(SnapshotPortError::permanent(
                    "stale_lease",
                    "snapshot mount lease is no longer owned",
                ))
            });
        }
        current.expires_at = self.expiry(lease_for);
        current.heartbeat_at = self.clock.now();
        let renewed = current.clone();
        Box::pin(async move { Ok(renewed) })
    }

    fn release<'a>(&'a self, lease: &'a SnapshotMountLease) -> SnapshotPortFuture<'a, ()> {
        let mut state = self.state.lock();
        let Some(current) = state.leases.get_mut(&lease.lease_id) else {
            return Box::pin(async {
                Err(SnapshotPortError::permanent(
                    "stale_lease",
                    "snapshot mount lease is no longer owned",
                ))
            });
        };
        if !Self::matches(current, lease) {
            return Box::pin(async {
                Err(SnapshotPortError::permanent(
                    "stale_lease",
                    "snapshot mount lease is no longer owned",
                ))
            });
        }
        current.released = true;
        current.expires_at = self.clock.now();
        Box::pin(async { Ok(()) })
    }

    fn has_active_lease<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
        cache_volume_uuid: &'a str,
        cache_sha256: &'a SnapshotSha256,
    ) -> SnapshotPortFuture<'a, bool> {
        let now = self.clock.now();
        let active = self.state.lock().leases.values().any(|lease| {
            !lease.released
                && lease.expires_at > now
                && lease.snapshot_id == *snapshot_id
                && lease.cache_volume_uuid == cache_volume_uuid
                && lease.cache_sha256 == *cache_sha256
        });
        Box::pin(async move { Ok(active) })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;
    use studiofs_data::DataStoreConfig;
    use studiofs_meta::MetaError;
    use studiofs_meta::snapshot::{
        SnapshotArtifactDescriptor, SnapshotCatalogEntry, SnapshotCreationState, SnapshotCut,
        SnapshotId, SnapshotLifecycle, SnapshotOrigin, SnapshotRetention, SnapshotSha256,
        SnapshotVolumeContext,
    };

    fn valid_snapshot_bytes(root: &Path, stem: &str, volume: &SnapshotVolumeContext) -> Vec<u8> {
        use studiofs_meta::MetaFormat;
        use studiofs_meta::RedbMetaEngine;

        let path = root.join(format!("{stem}.redb"));
        let meta = RedbMetaEngine::open(&path).expect("open snapshot fixture database");
        let mut format_info = studiofs_meta::contract_harness::contract_format_info(stem);
        format_info.name = volume.volume_name.clone();
        format_info.uuid = volume.volume_uuid.clone();
        meta.set_format_info(&format_info)
            .expect("format snapshot fixture database");
        meta.init_root(0, 0)
            .expect("initialize snapshot fixture root");
        drop(meta);
        fs::read(path).expect("read snapshot fixture database")
    }

    use super::{SnapshotArtifactCacheKey, SnapshotMountSession};

    fn volume(id: i64, uuid: &str) -> SnapshotVolumeContext {
        SnapshotVolumeContext::new(id, format!("snapshot-test-{id}"), uuid)
            .expect("test volume identity")
    }

    fn snapshot_id(volume_name: &str, captured_at: DateTime<Utc>) -> SnapshotId {
        SnapshotId::from_cut(volume_name, captured_at).expect("test snapshot id")
    }

    fn cut(volume_id: i64, captured_at: DateTime<Utc>) -> SnapshotCut {
        SnapshotCut::new(volume_id, 7, captured_at).expect("test snapshot cut")
    }

    fn artifact(
        object_key: &str,
        snapshot_cut: SnapshotCut,
        bytes: &[u8],
        checksum: SnapshotSha256,
    ) -> SnapshotArtifactDescriptor {
        SnapshotArtifactDescriptor::new(
            object_key,
            1,
            1,
            checksum,
            bytes.len() as u64,
            snapshot_cut,
            snapshot_cut.captured_at(),
        )
        .expect("test artifact descriptor")
    }

    fn catalog(
        lifecycle: SnapshotLifecycle,
        volume: SnapshotVolumeContext,
        snapshot_id: SnapshotId,
        snapshot_cut: SnapshotCut,
        descriptor: Option<SnapshotArtifactDescriptor>,
    ) -> SnapshotCatalogEntry {
        SnapshotCatalogEntry {
            snapshot_id,
            volume,
            origin: SnapshotOrigin::Manual,
            cut: snapshot_cut,
            lifecycle,
            creation_state: SnapshotCreationState::Completed,
            retention: SnapshotRetention::manual(None).expect("manual retention"),
            ready_at: (lifecycle == SnapshotLifecycle::Ready).then_some(snapshot_cut.captured_at()),
            expires_at: None,
            artifact: descriptor,
            failure: None,
        }
    }

    fn digest(bytes: &[u8]) -> SnapshotSha256 {
        SnapshotSha256::from_bytes(Sha256::digest(bytes).into())
    }

    #[test]
    fn cache_identity_is_volume_snapshot_and_checksum() {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-v2-mount-cache-");
        let captured_at = Utc.with_ymd_and_hms(2026, 8, 14, 9, 0, 0).unwrap();
        let first_volume = volume(11, "00000000-0000-0000-0000-000000000011");
        let second_volume = volume(12, "00000000-0000-0000-0000-000000000012");
        let first_snapshot = snapshot_id(&first_volume.volume_name, captured_at);
        let second_snapshot = snapshot_id(
            &first_volume.volume_name,
            captured_at + chrono::Duration::seconds(1),
        );
        let first_checksum = digest(b"first artifact");
        let second_checksum = digest(b"second artifact");

        let key = SnapshotArtifactCacheKey::new(
            first_volume.clone(),
            first_snapshot.clone(),
            first_checksum,
        );

        assert_ne!(
            key,
            SnapshotArtifactCacheKey::new(second_volume, first_snapshot.clone(), first_checksum),
            "different immutable volume identities must never alias"
        );
        assert_ne!(
            key,
            SnapshotArtifactCacheKey::new(first_volume.clone(), second_snapshot, first_checksum),
            "different snapshot IDs must never alias"
        );
        assert_ne!(
            key,
            SnapshotArtifactCacheKey::new(first_volume, first_snapshot, second_checksum),
            "different checksums must never alias"
        );
        assert_ne!(
            key.cache_path(root.path()),
            SnapshotArtifactCacheKey::new(
                volume(12, "00000000-0000-0000-0000-000000000012"),
                snapshot_id("snapshot-test-12", captured_at),
                first_checksum,
            )
            .cache_path(root.path()),
            "path derivation must retain the complete composite identity"
        );
    }

    #[test]
    fn cache_name_decoding_rejects_non_ascii_without_panicking() {
        assert_eq!(super::decode_path_component("€a"), None);

        let checksum = format!("€{}", "0".repeat(61));
        let cache_name = format!("snapshot-placeholder-sha256-{checksum}.redb");
        assert_eq!(super::parse_cache_file_name(&cache_name), None);
    }

    #[tokio::test]
    async fn lease_and_checksum_cache_are_fenced() {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-v2-mount-cache-");
        let now = Utc.with_ymd_and_hms(2026, 8, 14, 9, 0, 0).unwrap();
        let snapshot_volume = volume(21, "00000000-0000-0000-0000-000000000021");
        let snapshot_cut = cut(snapshot_volume.volume_id, now);
        let snapshot = snapshot_id(&snapshot_volume.volume_name, now);
        let bytes = valid_snapshot_bytes(root.path(), "verified", &snapshot_volume);
        let checksum = digest(&bytes);
        let descriptor = artifact("snapshots/verified.redb", snapshot_cut, &bytes, checksum);
        let corrupt_checksum = digest(b"corrupt artifact");

        let harness =
            SnapshotMountSession::new_for_tests(root.path(), now, snapshot_volume.clone());
        harness.set_catalog(catalog(
            SnapshotLifecycle::Creating,
            snapshot_volume.clone(),
            snapshot.clone(),
            snapshot_cut,
            None,
        ));
        assert!(
            harness
                .mount(&snapshot, "mount-before-ready")
                .await
                .is_err(),
            "Creating snapshots must not acquire a mount lease"
        );

        harness.set_catalog(catalog(
            SnapshotLifecycle::Ready,
            snapshot_volume.clone(),
            snapshot.clone(),
            snapshot_cut,
            Some(SnapshotArtifactDescriptor {
                sha256: corrupt_checksum,
                ..descriptor.clone()
            }),
        ));
        harness.put_artifact(bytes.clone());
        assert!(
            harness
                .mount(&snapshot, "mount-corrupt-proof")
                .await
                .is_err(),
            "checksum and byte length must be verified before opening the artifact"
        );

        let foreign_volume = volume(22, "00000000-0000-0000-0000-000000000022");
        let foreign_bytes = valid_snapshot_bytes(root.path(), "foreign", &foreign_volume);
        let foreign_descriptor = artifact(
            "snapshots/foreign.redb",
            snapshot_cut,
            &foreign_bytes,
            digest(&foreign_bytes),
        );
        harness.set_catalog(catalog(
            SnapshotLifecycle::Ready,
            snapshot_volume.clone(),
            snapshot.clone(),
            snapshot_cut,
            Some(foreign_descriptor),
        ));
        harness.put_artifact(foreign_bytes);
        assert!(matches!(
            harness.mount(&snapshot, "mount-foreign-volume").await,
            Err(super::SnapshotMountError::VolumeMismatch)
        ));

        harness.set_catalog(catalog(
            SnapshotLifecycle::Ready,
            snapshot_volume,
            snapshot.clone(),
            snapshot_cut,
            Some(descriptor),
        ));
        harness.put_artifact(bytes);
        let first = harness
            .mount(&snapshot, "mount-a")
            .await
            .expect("verified Ready artifact should mount");
        let second = harness
            .mount(&snapshot, "mount-b")
            .await
            .expect("equal composite identity should reuse the verified cache entry");
        assert_eq!(first.cache_key(), second.cache_key());
        assert_eq!(harness.download_count(), 2);
        let metadata = first.metadata();
        assert!(!metadata.metadata_authority_status().allows_new_mutations());
        assert!(matches!(
            metadata.set_counter("snapshot-test", 1),
            Err(MetaError::AccessDenied { .. })
        ));
        let format_info = metadata
            .get_format_info()
            .expect("read-only snapshot metadata should expose format info");
        let data_config = DataStoreConfig::from(&format_info);
        let vfs_cache_dir = root.path().join("snapshot-vfs-cache");
        let snapshot_vfs = first
            .open_vfs(
                &data_config,
                vfs_cache_dir.to_str().expect("snapshot VFS cache path"),
                64 * 1024 * 1024,
                32 * 1024 * 1024,
                crate::VfsConfig::default()
                    .try_into()
                    .expect("valid VFS config"),
            )
            .expect("verified snapshot metadata should build a read-only VFS");
        snapshot_vfs
            .start()
            .expect("read-only VFS start is a no-op");
        assert!(
            snapshot_vfs
                .create(studiofs_meta::ROOT_INO, b"must-not-create", 0, 0, 0o644)
                .is_err()
        );

        harness.corrupt_cached_artifact(&first.cache_key()).await;
        assert!(
            harness
                .mount(&snapshot, "mount-after-cache-corruption")
                .await
                .is_err(),
            "a corrupt cache entry must be rejected rather than opened"
        );

        harness.advance_clock(Duration::from_secs(10));
        first
            .renew_lease()
            .await
            .expect("active mount lease should renew before expiry");
        harness.advance_clock(Duration::from_secs(120));
        assert!(
            first.is_closed(),
            "an expired lease must close the snapshot session"
        );
        assert!(
            snapshot_vfs.readdir(studiofs_meta::ROOT_INO).is_err(),
            "an expired lease must fence an already-open snapshot VFS"
        );
    }

    #[tokio::test]
    async fn admission_renews_lease_during_slow_artifact_load() {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-v2-mount-cache-");
        let now = Utc.with_ymd_and_hms(2026, 8, 14, 9, 0, 0).unwrap();
        let snapshot_volume = volume(25, "00000000-0000-0000-0000-000000000025");
        let snapshot_cut = cut(snapshot_volume.volume_id, now);
        let snapshot = snapshot_id(&snapshot_volume.volume_name, now);
        let bytes = valid_snapshot_bytes(root.path(), "slow-admission", &snapshot_volume);
        let descriptor = artifact(
            "snapshots/slow-admission.redb",
            snapshot_cut,
            &bytes,
            digest(&bytes),
        );
        let harness = SnapshotMountSession::new_for_tests_with_lease_for(
            root.path(),
            now,
            snapshot_volume,
            Duration::from_secs(2),
        );
        harness.set_catalog(catalog(
            SnapshotLifecycle::Ready,
            volume(25, "00000000-0000-0000-0000-000000000025"),
            snapshot.clone(),
            snapshot_cut,
            Some(descriptor),
        ));
        harness.put_artifact(bytes);
        harness
            .test_artifacts
            .set_get_delay(Duration::from_millis(40));

        let clock = Arc::clone(&harness.test_clock);
        let advance = tokio::spawn(async move {
            for _ in 0..8 {
                clock.advance(Duration::from_millis(500));
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
            }
        });
        let mount = harness
            .mount(&snapshot, "slow-admission-mount")
            .await
            .expect("admission renewal should keep a slow mount lease alive");
        advance.await.expect("clock advancer should finish");

        assert!(
            !mount.is_closed(),
            "slow admission must not return an expired mount"
        );
        mount.release().await.expect("release slow admission mount");
    }

    #[tokio::test]
    async fn idle_cache_expires_after_seven_days_without_active_mount_lease() {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-v2-mount-cache-");
        let now = Utc.with_ymd_and_hms(2026, 8, 14, 9, 0, 0).unwrap();
        let snapshot_volume = volume(31, "00000000-0000-0000-0000-000000000031");
        let snapshot_cut = cut(snapshot_volume.volume_id, now);
        let snapshot = snapshot_id(&snapshot_volume.volume_name, now);
        let bytes = valid_snapshot_bytes(root.path(), "idle", &snapshot_volume);
        let descriptor = artifact("snapshots/idle.redb", snapshot_cut, &bytes, digest(&bytes));

        let harness = SnapshotMountSession::new_for_tests_with_lease_for(
            root.path(),
            now,
            snapshot_volume.clone(),
            Duration::from_secs(8 * 24 * 60 * 60),
        );
        harness.set_catalog(catalog(
            SnapshotLifecycle::Ready,
            snapshot_volume,
            snapshot.clone(),
            snapshot_cut,
            Some(descriptor),
        ));
        harness.put_artifact(bytes);
        let mount = harness
            .mount(&snapshot, "idle-cache-mount")
            .await
            .expect("ready artifact should mount");

        harness.advance_clock(Duration::from_secs(7 * 24 * 60 * 60 - 1));
        assert_eq!(harness.evict_idle_cache().await.expect("idle eviction"), 0);
        harness.advance_clock(Duration::from_secs(1));
        assert_eq!(
            harness
                .evict_idle_cache()
                .await
                .expect("active lease protects cache"),
            0
        );
        mount.release().await.expect("release mount lease");
        assert!(
            harness.metadata_handles.lock().is_empty(),
            "releasing a snapshot mount must drop the session metadata owner before eviction"
        );
        assert_eq!(harness.evict_idle_cache().await.expect("idle eviction"), 1);
    }

    #[tokio::test]
    async fn persisted_last_used_state_survives_session_restart() {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-v2-mount-cache-");
        let now = Utc.with_ymd_and_hms(2026, 8, 14, 9, 0, 0).unwrap();
        let snapshot_volume = volume(41, "00000000-0000-0000-0000-000000000041");
        let snapshot_cut = cut(snapshot_volume.volume_id, now);
        let snapshot = snapshot_id(&snapshot_volume.volume_name, now);
        let bytes = valid_snapshot_bytes(root.path(), "restart", &snapshot_volume);
        let descriptor = artifact(
            "snapshots/restart.redb",
            snapshot_cut,
            &bytes,
            digest(&bytes),
        );

        let cache_path = {
            let harness =
                SnapshotMountSession::new_for_tests(root.path(), now, snapshot_volume.clone());
            harness.set_catalog(catalog(
                SnapshotLifecycle::Ready,
                snapshot_volume.clone(),
                snapshot.clone(),
                snapshot_cut,
                Some(descriptor),
            ));
            harness.put_artifact(bytes);
            let mount = harness
                .mount(&snapshot, "restart-cache-mount")
                .await
                .expect("ready artifact should mount");
            let cache_path = mount.cache_key().cache_path(root.path());
            mount.release().await.expect("release mount lease");
            assert!(super::last_used_path(&cache_path).exists());
            cache_path
        };

        let restarted = SnapshotMountSession::new_for_tests(
            root.path(),
            now + chrono::Duration::days(7),
            snapshot_volume,
        );
        assert_eq!(
            restarted
                .evict_idle_cache()
                .await
                .expect("restart eviction should load persisted last-used state"),
            1
        );
        assert!(!cache_path.exists());
        assert!(!super::last_used_path(&cache_path).exists());
    }

    #[tokio::test]
    async fn missing_last_used_sidecar_uses_cache_mtime_for_indexing() {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-v2-mount-cache-");
        let now = Utc::now();
        let snapshot_volume = volume(42, "00000000-0000-0000-0000-000000000042");
        let snapshot_cut = cut(snapshot_volume.volume_id, now);
        let snapshot = snapshot_id(&snapshot_volume.volume_name, now);
        let bytes = valid_snapshot_bytes(root.path(), "missing-sidecar", &snapshot_volume);
        let descriptor = artifact(
            "snapshots/missing-sidecar.redb",
            snapshot_cut,
            &bytes,
            digest(&bytes),
        );

        let harness =
            SnapshotMountSession::new_for_tests(root.path(), now, snapshot_volume.clone());
        harness.set_catalog(catalog(
            SnapshotLifecycle::Ready,
            snapshot_volume.clone(),
            snapshot.clone(),
            snapshot_cut,
            Some(descriptor),
        ));
        harness.put_artifact(bytes);
        let mount = harness
            .mount(&snapshot, "missing-sidecar-mount")
            .await
            .expect("ready artifact should mount");
        let cache_path = mount.cache_key().cache_path(root.path());
        mount.release().await.expect("release mount lease");
        fs::remove_file(super::last_used_path(&cache_path)).expect("remove last-used sidecar");

        let restarted = SnapshotMountSession::new_for_tests(root.path(), now, snapshot_volume);
        assert_eq!(restarted.evict_idle_cache().await.expect("index cache"), 0);
        assert_eq!(restarted.cache_last_used.lock().len(), 1);
    }
}
