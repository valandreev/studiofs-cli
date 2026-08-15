//! `studiofs mount` — mount a filesystem volume.

mod backend;
mod session;

use clap::Args;
use std::path::PathBuf;

use super::runtime_tuning::PublicRuntimeTuningArgs;
use studiofs_meta::snapshot::SnapshotId;

pub(crate) use session::MountSession;

#[derive(Debug, Clone)]
pub(crate) struct MountCommand {
    pub(crate) name: String,
    pub(crate) meta_dir: PathBuf,
    pub(crate) mount_point: Option<String>,
    pub(crate) cache_dir: Option<PathBuf>,
    pub(crate) cache_size: Option<String>,
    pub(crate) principal: Option<String>,
    pub(crate) foreground: bool,
    pub(crate) detach: bool,
    pub(crate) profile: Option<super::mount_profile_types::MountProfileMode>,
    pub(crate) read_only: bool,
    pub(crate) endpoint: Option<String>,
    pub(crate) runtime_tuning: PublicRuntimeTuningArgs,
    pub(crate) snapshot_id: Option<SnapshotId>,
}

impl MountCommand {
    fn from_args(args: MountArgs) -> anyhow::Result<Self> {
        let meta_dir = super::utils::get_default_meta_dir(&args.name)?;
        Ok(Self {
            name: args.name,
            meta_dir,
            mount_point: args.mount_point,
            cache_dir: args.cache_dir,
            cache_size: args.cache_size,
            principal: args.principal,
            foreground: args.foreground,
            detach: args.detach,
            profile: args.profile,
            read_only: args.read_only,
            endpoint: args.endpoint,
            runtime_tuning: args.runtime_tuning,
            snapshot_id: None,
        })
    }

    pub(crate) fn from_snapshot_args(
        name: String,
        snapshot_id: SnapshotId,
        mount_point: String,
        cache_dir: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let meta_dir = super::utils::get_default_meta_dir(&name)?;
        Ok(Self {
            name,
            meta_dir,
            mount_point: Some(mount_point),
            cache_dir,
            cache_size: None,
            principal: None,
            foreground: false,
            detach: false,
            profile: None,
            read_only: true,
            endpoint: None,
            runtime_tuning: PublicRuntimeTuningArgs::default(),
            snapshot_id: Some(snapshot_id),
        })
    }
}

#[derive(Debug, Clone, Args)]
#[command(about = "Mount a filesystem volume as a local drive")]
#[command(long_about = "\
Mount a previously initialized volume. On Windows, the mount point is a drive \
letter (e.g. Z:). On macOS, it is an existing directory path. Linux mount is \
currently unsupported.

The local client uses its cached metadata database while synchronizing shared \
state through PostgreSQL. Same-mount reads observe accepted writes \
immediately through the Local-First view. Other mounts converge after \
background upload and metadata publish; close does not imply a global \
close-to-open barrier.

On Windows and macOS, StudioFS automatically attempts to show a tray / menu-bar \
status indicator when GUI services are available. If tray initialization fails, \
the mount continues normally in CLI mode.

The process blocks while the filesystem is mounted. Press Ctrl+C to unmount.")]
#[command(after_help = "\
EXAMPLES:
  Basic mount (Windows):
    studiofs mount myvolume Z:

  Basic mount (macOS):
    studiofs mount myvolume /mnt/studio

  Local data cache mount (location + size):
    studiofs mount myvolume Z: --cache-dir D:\\cache\\studiofs --cache-size 500GB

  Local-First IO tuned mount:
    studiofs mount myvolume /mnt/studio --read-buffer 4GB --readahead 256 --prefetch 192

  Debug logging:
    studiofs --log-level debug mount myvolume /mnt/studio

PYROSCOPE PROFILING:
  studiofs mount myvolume /mnt/studio --profile
  studiofs mount myvolume /mnt/studio --profile=off
  STUDIOFS_PROFILE=pyroscope|off
  STUDIOFS_PYROSCOPE_URL=http://localhost:4040
  STUDIOFS_PYROSCOPE_APP_NAME=studiofs.mount
  STUDIOFS_PYROSCOPE_SAMPLE_RATE=100
  STUDIOFS_PYROSCOPE_TAGS=team=vfx,cache_contract=fast-local-authority

SIZE FORMATS:
  --cache-size accepts: KB, MB, GB, TB (e.g. 512MB, 4GB, 1TB)
  --read-buffer accepts the same size formats.

RUNTIME IO:
  Tune only the main Local-First IO controls here:
    --read-buffer
    --readahead
    --prefetch
  Metadata-cache internals stay on strong built-in defaults.

CONSISTENCY MODEL:
  Same-mount reads see accepted writes immediately.
  Other mounts converge after background upload + publish.
  `flush()` / `fsync()` and the last-handle close/release barrier confirm local
  durability when dirty state remains, not remote close-to-open visibility.

ACL PRINCIPAL:
  By default, the mount uses STUDIOFS_PRINCIPAL (or '*' when unset).
  You can override it per mount with:
    --principal alice
")]
pub(crate) struct MountArgs {
    /// Volume name (must be initialized first via `studiofs init`)
    name: String,

    /// Mount point: drive letter on Windows (Z:), directory path on Unix (/mnt/studio)
    mount_point: Option<String>,

    /// Disk cache directory (defaults to `<meta-dir>/cache`)
    #[arg(long)]
    cache_dir: Option<PathBuf>,

    /// Maximum disk cache size (e.g. 100GB, 50GB, 1TB)
    #[arg(long)]
    cache_size: Option<String>,

    #[command(flatten, next_help_heading = "Runtime IO")]
    runtime_tuning: PublicRuntimeTuningArgs,

    /// Principal identity used for ACL checks in this mount instance.
    #[arg(long)]
    principal: Option<String>,

    /// Run in foreground (keep terminal attached, default behavior)
    #[arg(long, conflicts_with = "detach")]
    foreground: bool,

    /// Detach and run mount in the background (daemon mode).
    /// Logs are written to `<client-root>/logs/`, defaulting to `~/.studiofs/logs/`.
    /// Use `studiofs umount <mount_point>` to stop.
    #[arg(short = 'd', long, conflicts_with = "foreground")]
    detach: bool,

    /// Enable or disable Pyroscope profiling for the mount runtime.
    #[arg(long, num_args = 0..=1, default_missing_value = "pyroscope", value_enum)]
    profile: Option<super::mount_profile_types::MountProfileMode>,

    /// Mount the live volume read-only.
    #[arg(long)]
    read_only: bool,

    /// Override the S3 endpoint URL for this mount session.
    ///
    /// Use https:// for TLS or http:// to explicitly select plain HTTP
    /// transport.
    /// This override only applies to the current process and is NOT saved.
    #[arg(long)]
    endpoint: Option<String>,
}

impl MountArgs {
    #[cfg(any(windows, target_os = "macos", target_os = "linux"))]
    pub(crate) fn should_detach(&self) -> bool {
        self.detach
    }

    #[cfg(any(windows, target_os = "macos", target_os = "linux"))]
    pub(crate) fn volume_name(&self) -> &str {
        &self.name
    }

    pub(crate) fn dispatch_with_auto_tray(self) -> anyhow::Result<()> {
        crate::tray::dispatch_mount_with_auto_tray(self)
    }

    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        #[cfg(windows)]
        super::winfsp_bootstrap::ensure_mountmgr_bootstrap()?;

        MountSession::prepare(MountCommand::from_args(self)?)?.launch()
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use clap::error::ErrorKind;
    use sha2::{Digest, Sha256};
    use std::path::Path;
    use std::sync::Arc;
    use studiofs_meta::snapshot::{
        SnapshotArtifactDescriptor, SnapshotCatalogEntry, SnapshotCreationJob,
        SnapshotCreationLease, SnapshotCreationState, SnapshotCut, SnapshotId, SnapshotLifecycle,
        SnapshotMountLease, SnapshotOrigin, SnapshotRetention, SnapshotSha256,
        SnapshotVolumeContext,
    };
    use studiofs_meta::snapshot_ports::{
        SnapshotArtifactBytes, SnapshotArtifactLocator, SnapshotArtifactScope,
        SnapshotArtifactStore, SnapshotClaimOrFollow, SnapshotClock, SnapshotFailure,
        SnapshotJobClaim, SnapshotJobStore, SnapshotMountStore, SnapshotPayloadProof,
        SnapshotPortError, SnapshotPortFuture, SnapshotRetry, SnapshotStoredArtifact,
        SnapshotWorkerId,
    };

    fn unsupported<'a, T>() -> SnapshotPortFuture<'a, T> {
        Box::pin(async {
            Err(SnapshotPortError::permanent(
                "unsupported_test_port",
                "operation is not used by this test",
            ))
        })
    }

    #[derive(Default)]
    struct CliJobStore {
        catalog: parking_lot::Mutex<Option<SnapshotCatalogEntry>>,
    }

    impl CliJobStore {
        fn set_catalog(&self, catalog: SnapshotCatalogEntry) {
            *self.catalog.lock() = Some(catalog);
        }
    }

    impl SnapshotJobStore for CliJobStore {
        fn claim_next<'a>(
            &'a self,
            _worker: &'a SnapshotWorkerId,
            _lease_for: std::time::Duration,
        ) -> SnapshotPortFuture<'a, Option<SnapshotJobClaim>> {
            unsupported()
        }

        fn claim_id_or_follow<'a>(
            &'a self,
            _snapshot_id: &'a SnapshotId,
            _worker: &'a SnapshotWorkerId,
            _lease_for: std::time::Duration,
        ) -> SnapshotPortFuture<'a, SnapshotClaimOrFollow> {
            unsupported()
        }

        fn renew_lease<'a>(
            &'a self,
            _claim: &'a SnapshotJobClaim,
            _lease_for: std::time::Duration,
        ) -> SnapshotPortFuture<'a, SnapshotCreationLease> {
            unsupported()
        }

        fn release_lease<'a>(&'a self, _claim: &'a SnapshotJobClaim) -> SnapshotPortFuture<'a, ()> {
            unsupported()
        }

        fn mark_retry_wait<'a>(
            &'a self,
            _claim: &'a SnapshotJobClaim,
            _retry: SnapshotRetry,
        ) -> SnapshotPortFuture<'a, SnapshotCreationJob> {
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
            _snapshot_id: &'a SnapshotId,
        ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
            let catalog = self.catalog.lock().clone();
            Box::pin(async move {
                catalog.ok_or_else(|| {
                    SnapshotPortError::permanent("snapshot_not_found", "snapshot does not exist")
                })
            })
        }
    }

    #[derive(Default)]
    struct CliArtifactStore {
        bytes: parking_lot::Mutex<Option<Vec<u8>>>,
    }

    impl CliArtifactStore {
        fn put(&self, bytes: Vec<u8>) {
            *self.bytes.lock() = Some(bytes);
        }
    }

    impl SnapshotArtifactStore for CliArtifactStore {
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
            _locator: &'a SnapshotArtifactLocator,
        ) -> SnapshotPortFuture<'a, SnapshotArtifactBytes> {
            let bytes = self.bytes.lock().clone();
            Box::pin(async move {
                let bytes = bytes.ok_or_else(|| {
                    SnapshotPortError::permanent(
                        "artifact_not_found",
                        "snapshot artifact is unavailable",
                    )
                })?;
                Ok(SnapshotArtifactBytes {
                    sha256: SnapshotSha256::from_bytes(Sha256::digest(&bytes).into()),
                    bytes,
                })
            })
        }

        fn delete<'a>(
            &'a self,
            _locator: &'a SnapshotArtifactLocator,
        ) -> SnapshotPortFuture<'a, ()> {
            unsupported()
        }

        fn list<'a>(
            &'a self,
            _scope: &'a SnapshotArtifactScope,
        ) -> SnapshotPortFuture<'a, Vec<SnapshotStoredArtifact>> {
            unsupported()
        }
    }

    struct CliClock {
        now: chrono::DateTime<Utc>,
    }

    impl SnapshotClock for CliClock {
        fn now(&self) -> chrono::DateTime<Utc> {
            self.now
        }

        fn sleep_until<'a>(
            &'a self,
            deadline: chrono::DateTime<Utc>,
            cancel: tokio_util::sync::CancellationToken,
        ) -> SnapshotPortFuture<'a, ()> {
            let delay = (deadline - self.now).to_std().unwrap_or_default();
            Box::pin(async move {
                tokio::select! {
                    _ = cancel.cancelled() => Err(SnapshotPortError::Cancelled),
                    _ = tokio::time::sleep(delay) => Ok(()),
                }
            })
        }
    }

    struct CliMountStore;

    impl SnapshotMountStore for CliMountStore {
        fn acquire<'a>(
            &'a self,
            snapshot_id: &'a SnapshotId,
            owner: &'a str,
            cache_volume_uuid: &'a str,
            cache_sha256: &'a SnapshotSha256,
            lease_for: std::time::Duration,
        ) -> SnapshotPortFuture<'a, SnapshotMountLease> {
            let lease = SnapshotMountLease {
                lease_id: 1,
                snapshot_id: snapshot_id.clone(),
                owner: owner.to_string(),
                cache_volume_uuid: cache_volume_uuid.to_string(),
                cache_sha256: *cache_sha256,
                expires_at: Utc::now() + chrono::Duration::from_std(lease_for).unwrap_or_default(),
                heartbeat_at: Utc::now(),
                fencing_token: 1,
                released: false,
            };
            Box::pin(async move { Ok(lease) })
        }

        fn renew<'a>(
            &'a self,
            lease: &'a SnapshotMountLease,
            _lease_for: std::time::Duration,
        ) -> SnapshotPortFuture<'a, SnapshotMountLease> {
            Box::pin(async move { Ok(lease.clone()) })
        }

        fn release<'a>(&'a self, _lease: &'a SnapshotMountLease) -> SnapshotPortFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn has_active_lease<'a>(
            &'a self,
            _snapshot_id: &'a SnapshotId,
            _cache_volume_uuid: &'a str,
            _cache_sha256: &'a SnapshotSha256,
        ) -> SnapshotPortFuture<'a, bool> {
            Box::pin(async { Ok(false) })
        }
    }

    fn snapshot_mount_fixture(
        lifecycle: SnapshotLifecycle,
        descriptor: Option<SnapshotArtifactDescriptor>,
    ) -> SnapshotCatalogEntry {
        let captured_at = Utc.with_ymd_and_hms(2026, 8, 14, 9, 0, 0).unwrap();
        let volume = SnapshotVolumeContext::new(
            41,
            "cli-snapshot-test",
            "00000000-0000-0000-0000-000000000041",
        )
        .expect("test volume identity");
        let cut = SnapshotCut::new(volume.volume_id, 3, captured_at).expect("test cut");
        SnapshotCatalogEntry {
            snapshot_id: SnapshotId::from_cut(&volume.volume_name, captured_at)
                .expect("test snapshot id"),
            volume,
            origin: SnapshotOrigin::Manual,
            cut,
            lifecycle,
            creation_state: SnapshotCreationState::Completed,
            retention: SnapshotRetention::manual(None).expect("manual retention"),
            ready_at: (lifecycle == SnapshotLifecycle::Ready).then_some(captured_at),
            expires_at: None,
            artifact: descriptor,
            failure: None,
        }
    }

    #[test]
    fn mount_accepts_public_runtime_tuning_flags() {
        let mount_point = if cfg!(windows) { "Z:" } else { "/mnt/studio" };
        crate::cli::parse_for_test(&[
            "mount",
            "demo",
            mount_point,
            "--read-buffer",
            "4GB",
            "--readahead",
            "256",
            "--prefetch",
            "192",
        ])
        .expect("new public runtime tuning flags should parse");
    }

    #[test]
    fn mount_rejects_writeback_delay_flag() {
        let mount_point = if cfg!(windows) { "Z:" } else { "/mnt/studio" };
        let err = match crate::cli::parse_for_test(&[
            "mount",
            "demo",
            mount_point,
            "--writeback-delay",
            "1500",
        ]) {
            Ok(_) => panic!("expected unknown argument for removed writeback-delay flag"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
        assert!(err.to_string().contains("--writeback-delay"));
    }

    #[test]
    fn mount_rejects_zero_readahead() {
        let mount_point = if cfg!(windows) { "Z:" } else { "/mnt/studio" };
        let err =
            match crate::cli::parse_for_test(&["mount", "demo", mount_point, "--readahead", "0"]) {
                Ok(_) => panic!("zero readahead should be rejected"),
                Err(err) => err,
            };
        assert_eq!(err.kind(), ErrorKind::ValueValidation);
        assert!(err.to_string().contains("--readahead"));
    }

    #[test]
    fn mount_rejects_zero_prefetch() {
        let mount_point = if cfg!(windows) { "Z:" } else { "/mnt/studio" };
        let err =
            match crate::cli::parse_for_test(&["mount", "demo", mount_point, "--prefetch", "0"]) {
                Ok(_) => panic!("zero prefetch should be rejected"),
                Err(err) => err,
            };
        assert_eq!(err.kind(), ErrorKind::ValueValidation);
        assert!(err.to_string().contains("--prefetch"));
    }

    #[tokio::test]
    async fn snapshot_mount_rejects_non_ready_and_corrupt_artifact() {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-v2-cli-mount-");
        let bytes = b"cli snapshot artifact";
        let checksum = SnapshotSha256::from_bytes(Sha256::digest(bytes).into());
        let mut ready = snapshot_mount_fixture(SnapshotLifecycle::Ready, None);
        let descriptor = SnapshotArtifactDescriptor::new(
            "snapshots/cli.redb",
            1,
            1,
            checksum,
            bytes.len() as u64,
            ready.cut,
            ready.cut.captured_at(),
        )
        .expect("test artifact descriptor");
        ready.artifact = Some(descriptor.clone());

        let snapshot_id = ready.snapshot_id.clone();
        let jobs = Arc::new(CliJobStore::default());
        let artifacts = Arc::new(CliArtifactStore::default());
        let harness = studiofs_vfs::SnapshotMountSession::new(
            ready.volume.clone(),
            jobs.clone(),
            artifacts.clone(),
            Arc::new(CliMountStore),
            Arc::new(CliClock { now: Utc::now() }),
            studiofs_vfs::SnapshotMountConfig::new(root.path()),
        )
        .expect("test ports should produce a valid production session");
        let creating = snapshot_mount_fixture(SnapshotLifecycle::Creating, None);
        jobs.set_catalog(creating);
        assert!(
            harness
                .mount(&ready.snapshot_id, "cli-before-ready")
                .await
                .is_err(),
            "the CLI mount path must reject a snapshot before Ready"
        );

        let corrupt = SnapshotArtifactDescriptor {
            sha256: SnapshotSha256::from_bytes([0xCC; 32]),
            ..descriptor
        };
        ready.artifact = Some(corrupt);
        jobs.set_catalog(ready);
        artifacts.put(bytes.to_vec());
        assert!(
            harness
                .mount(&snapshot_id, "cli-corrupt-artifact")
                .await
                .is_err(),
            "the CLI mount path must verify the artifact before opening it"
        );
    }
}
