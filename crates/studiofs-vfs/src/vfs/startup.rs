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
    #[allow(clippy::too_many_arguments)]
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
