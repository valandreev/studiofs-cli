use parking_lot::{Condvar, Mutex};
use std::time::Duration;

use super::{PendingUpload, PrefetchWorkItem, VfsError, VfsResult};

#[derive(Debug)]
pub(super) struct StartupChannels {
    pub(super) prefetch_rx: tokio::sync::mpsc::UnboundedReceiver<PrefetchWorkItem>,
    pub(super) upload_rx: tokio::sync::mpsc::UnboundedReceiver<PendingUpload>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartupStatus {
    Uninitialized,
    Starting,
    Running,
    Failed,
}

pub(super) struct StartupLifecycle {
    pub(super) status: StartupStatus,
    pub(super) channels: Option<StartupChannels>,
}

pub(super) struct StartupState {
    pub(super) lifecycle: Mutex<StartupLifecycle>,
    lifecycle_notify: Condvar,
    pub(super) metadata_invalidation_sync_interval: Duration,
}

impl StartupState {
    pub(super) fn new(
        prefetch_rx: tokio::sync::mpsc::UnboundedReceiver<PrefetchWorkItem>,
        upload_rx: tokio::sync::mpsc::UnboundedReceiver<PendingUpload>,
        metadata_invalidation_sync_interval: Duration,
    ) -> Self {
        Self {
            lifecycle: Mutex::new(StartupLifecycle {
                status: StartupStatus::Uninitialized,
                channels: Some(StartupChannels {
                    prefetch_rx,
                    upload_rx,
                }),
            }),
            lifecycle_notify: Condvar::new(),
            metadata_invalidation_sync_interval,
        }
    }

    pub(super) fn claim_channels_for_start(&self) -> VfsResult<Option<StartupChannels>> {
        let mut lifecycle = self.lifecycle.lock();
        loop {
            match lifecycle.status {
                StartupStatus::Running => return Ok(None),
                StartupStatus::Failed => {
                    return Err(VfsError::Other(
                        "StudioVfs background worker startup previously failed".to_string(),
                    ));
                }
                StartupStatus::Starting => self.lifecycle_notify.wait(&mut lifecycle),
                StartupStatus::Uninitialized => {
                    lifecycle.status = StartupStatus::Starting;
                    match lifecycle.channels.take() {
                        Some(channels) => return Ok(Some(channels)),
                        None => {
                            lifecycle.status = StartupStatus::Failed;
                            self.lifecycle_notify.notify_all();
                            return Err(VfsError::Other(
                                "StudioVfs startup channels are unavailable".to_string(),
                            ));
                        }
                    }
                }
            }
        }
    }

    pub(super) fn finish_start(&self, start_result: &VfsResult<()>) {
        let mut lifecycle = self.lifecycle.lock();
        lifecycle.status = if start_result.is_ok() {
            StartupStatus::Running
        } else {
            StartupStatus::Failed
        };
        self.lifecycle_notify.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::StudioVfs;
    use crate::vfs::VfsConfig;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use studiofs_data::store::DataStoreConfig;
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{MetaFormat, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("startup-tests-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("startup-tests-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn make_unstarted_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let rt = Arc::new(Runtime::new().expect("runtime"));
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        (rt, vfs, meta)
    }

    #[test]
    fn startup_state_claim_channels_transitions_to_starting_and_consumes_channels() {
        let (_prefetch_tx, prefetch_rx) =
            tokio::sync::mpsc::unbounded_channel::<PrefetchWorkItem>();
        let (_upload_tx, upload_rx) = tokio::sync::mpsc::unbounded_channel::<PendingUpload>();
        let startup = StartupState::new(prefetch_rx, upload_rx, Duration::from_secs(5));

        let claimed = startup
            .claim_channels_for_start()
            .expect("claim channels")
            .expect("channels should be present");
        drop(claimed);

        let lifecycle = startup.lifecycle.lock();
        assert!(
            matches!(lifecycle.status, StartupStatus::Starting),
            "claiming startup channels should transition lifecycle to Starting"
        );
        assert!(
            lifecycle.channels.is_none(),
            "claiming startup channels should consume the one-shot channels"
        );
    }

    #[test]
    fn startup_state_claim_channels_marks_failed_when_channels_are_missing() {
        let (_prefetch_tx, prefetch_rx) =
            tokio::sync::mpsc::unbounded_channel::<PrefetchWorkItem>();
        let (_upload_tx, upload_rx) = tokio::sync::mpsc::unbounded_channel::<PendingUpload>();
        let startup = StartupState::new(prefetch_rx, upload_rx, Duration::from_secs(5));
        startup.lifecycle.lock().channels = None;

        let error = startup
            .claim_channels_for_start()
            .expect_err("missing channels should fail startup");

        assert!(
            error
                .to_string()
                .contains("startup channels are unavailable"),
            "missing startup channels should map to a clear lifecycle error"
        );
        assert!(
            matches!(startup.lifecycle.lock().status, StartupStatus::Failed),
            "missing startup channels should move lifecycle into terminal Failed state"
        );
    }

    #[test]
    fn studio_vfs_start_requires_active_tokio_runtime() {
        let tmp = TempDir::new().expect("tempdir");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "start-requires-runtime");

        let error = vfs
            .start()
            .expect_err("starting without an entered Tokio runtime should fail");

        assert!(
            error
                .to_string()
                .contains("requires an active Tokio runtime"),
            "start should reject being called outside of a Tokio runtime"
        );
    }

    #[test]
    fn studio_vfs_start_rejects_when_shutdown_is_already_requested() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "start-after-shutdown");
        vfs.state
            .shutdown
            .shutdown_requested
            .store(true, Ordering::Release);

        let _guard = rt.enter();
        let error = vfs
            .start()
            .expect_err("start should reject after shutdown begins");

        assert!(
            error.to_string().contains("after shutdown has begun"),
            "start should fail fast once shutdown has been requested"
        );
    }

    #[test]
    fn studio_vfs_start_failure_marks_failed_and_rejects_retry() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "start-missing-channels");
        vfs.startup.lifecycle.lock().channels = None;

        let _guard = rt.enter();
        let first_error = vfs
            .start()
            .expect_err("missing startup channels should fail the first start attempt");
        assert!(
            first_error
                .to_string()
                .contains("startup channels are unavailable"),
            "missing startup channels should surface a concrete startup failure"
        );
        assert!(
            matches!(vfs.startup.lifecycle.lock().status, StartupStatus::Failed),
            "failed startup should leave lifecycle in terminal Failed state"
        );

        let retry_error = vfs
            .start()
            .expect_err("failed startup should reject retry attempts");
        assert!(
            retry_error
                .to_string()
                .contains("startup previously failed"),
            "retry after failed startup should surface the terminal lifecycle error"
        );
    }

    #[test]
    fn studio_vfs_start_is_idempotent_once_running() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "start-idempotent");

        let _guard = rt.enter();
        vfs.start().expect("first start");
        let initial_background_task_count = vfs.state.shutdown.background_tasks.lock().len();
        assert!(
            initial_background_task_count > 0,
            "first start should register background tasks"
        );

        vfs.start().expect("second start should be a no-op");
        assert_eq!(
            vfs.state.shutdown.background_tasks.lock().len(),
            initial_background_task_count,
            "second start should not register duplicate background tasks"
        );
        assert!(
            matches!(vfs.startup.lifecycle.lock().status, StartupStatus::Running),
            "idempotent second start should preserve Running lifecycle state"
        );
    }
}
