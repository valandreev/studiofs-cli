use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Deref;
use std::sync::{Arc, Weak};
#[cfg(test)]
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use studiofs_meta::{FileType, Ino};
use studiofs_vfs::{KernelInvalidator, NamespacePathStyle, StudioVfs, VfsFuseInvalidationSnapshot};
use tracing::{debug, trace, warn};
use winfsp::filesystem::WideNameInfo;
use winfsp::notify::{Notifier, NotifyInfo, NotifyingFileSystemContext};

use super::StudioFsContext;
use crate::ensure_winfsp_runtime_loaded;

pub(super) const FILE_NOTIFY_CHANGE_FILE_NAME: u32 = 0x0000_0001;
pub(super) const FILE_NOTIFY_CHANGE_DIR_NAME: u32 = 0x0000_0002;
pub(super) const FILE_NOTIFY_CHANGE_ATTRIBUTES: u32 = 0x0000_0004;
pub(super) const FILE_NOTIFY_CHANGE_SIZE: u32 = 0x0000_0008;
pub(super) const FILE_NOTIFY_CHANGE_LAST_WRITE: u32 = 0x0000_0010;
pub(super) const FILE_ACTION_MODIFIED: u32 = 0x0000_0003;
pub(super) const FILE_ACTION_REMOVED: u32 = 0x0000_0002;
/// Maximum number of distinct inode invalidations retained by WinFSP.
pub const WINFSP_INVALIDATION_QUEUE_CAPACITY: usize = 8192;

pub(super) fn notify_filter_for_kind(kind: FileType) -> u32 {
    if kind == FileType::Directory {
        FILE_NOTIFY_CHANGE_DIR_NAME | FILE_NOTIFY_CHANGE_ATTRIBUTES
    } else {
        FILE_NOTIFY_CHANGE_FILE_NAME
            | FILE_NOTIFY_CHANGE_SIZE
            | FILE_NOTIFY_CHANGE_ATTRIBUTES
            | FILE_NOTIFY_CHANGE_LAST_WRITE
    }
}

#[cfg(test)]
pub(super) fn build_notify_info(path: &str, kind: FileType) -> Option<NotifyInfo<512>> {
    build_notify_info_with_action(path, kind, FILE_ACTION_MODIFIED)
}

fn build_notify_info_with_action(
    path: &str,
    kind: FileType,
    action: u32,
) -> Option<NotifyInfo<512>> {
    if let Err(error) = ensure_winfsp_runtime_loaded() {
        warn!(
            error = %error,
            "WinFSP runtime unavailable while building notify info; skipping notification"
        );
        return None;
    }
    let mut info = NotifyInfo::<512>::new();
    info.filter = notify_filter_for_kind(kind);
    info.action = action;
    info.set_name(path).ok()?;
    Some(info)
}

#[derive(Debug)]
pub(crate) struct PreparedWinfspNotify {
    pub(super) ino: Ino,
    pub(super) path: String,
    kind: FileType,
    action: u32,
    pub(super) info: NotifyInfo<512>,
}

pub(crate) struct PreparedWinfspNotifications {
    notifications: Vec<PreparedWinfspNotify>,
    completion_queue: Option<Arc<WinfspInvalidationQueue>>,
    published: bool,
}

impl PreparedWinfspNotifications {
    fn new(
        notifications: Vec<PreparedWinfspNotify>,
        completion_queue: Arc<WinfspInvalidationQueue>,
    ) -> Self {
        Self {
            notifications,
            completion_queue: Some(completion_queue),
            published: false,
        }
    }
}

impl Deref for PreparedWinfspNotifications {
    type Target = [PreparedWinfspNotify];

    fn deref(&self) -> &Self::Target {
        &self.notifications
    }
}

impl Drop for PreparedWinfspNotifications {
    fn drop(&mut self) {
        if let Some(queue) = self.completion_queue.take() {
            // WinFSP's timer drops this value when NotifyBegin returns
            // STATUS_CANT_WAIT, for example while a rename is in progress.
            // Requeue the batch so a transient begin failure cannot leave
            // the OS cache permanently stale.
            if !self.published {
                for notification in &self.notifications {
                    queue.push_path(
                        notification.ino,
                        notification.path.clone(),
                        notification.kind,
                        notification.action,
                    );
                }
            }
            queue.finish_notification_batch();
        }
    }
}

#[derive(Debug)]
struct WinfspInvalidationOverride {
    path: String,
    kind: FileType,
    action: u32,
}

#[derive(Debug, Default)]
struct WinfspInvalidationQueueState {
    closed: bool,
    order: VecDeque<Ino>,
    pending: HashSet<Ino>,
    path_overrides: HashMap<Ino, WinfspInvalidationOverride>,
    queue_peak: usize,
    coalesced: u64,
    snapshot_generation: u64,
    notification_batches_in_flight: usize,
    #[cfg(test)]
    waiting_producers: usize,
}

/// Bounded, coalescing queue shared by WinFSP invalidation producers and the
/// notification timer.
#[derive(Debug)]
pub struct WinfspInvalidationQueue {
    state: Mutex<WinfspInvalidationQueueState>,
    not_full: Condvar,
    quiescent: Condvar,
    metrics_vfs: Weak<StudioVfs>,
}

impl WinfspInvalidationQueue {
    /// Creates an empty bounded invalidation queue.
    pub fn new(vfs: &Arc<StudioVfs>) -> Arc<Self> {
        let queue = Arc::new(Self {
            state: Mutex::new(WinfspInvalidationQueueState::default()),
            not_full: Condvar::new(),
            quiescent: Condvar::new(),
            metrics_vfs: Arc::downgrade(vfs),
        });
        let snapshot = {
            let mut state = queue.state.lock();
            Self::next_metrics_snapshot(&mut state)
        };
        queue.record_metrics(snapshot);
        queue
    }

    pub(crate) fn push(&self, ino: Ino) {
        let snapshot = {
            let mut state = self.state.lock();
            if self.push_locked(&mut state, ino) {
                Some(Self::next_metrics_snapshot(&mut state))
            } else {
                None
            }
        };
        if let Some(snapshot) = snapshot {
            self.record_metrics(snapshot);
        }
    }

    fn push_path(&self, ino: Ino, path: String, kind: FileType, action: u32) {
        let snapshot = {
            let mut state = self.state.lock();
            if state.closed {
                None
            } else {
                state
                    .path_overrides
                    .insert(ino, WinfspInvalidationOverride { path, kind, action });
                self.push_locked(&mut state, ino)
                    .then(|| Self::next_metrics_snapshot(&mut state))
            }
        };
        if let Some(snapshot) = snapshot {
            self.record_metrics(snapshot);
        }
    }

    pub(crate) fn push_deleted_path(&self, ino: Ino, path: String, kind: FileType) {
        self.push_path(ino, path, kind, FILE_ACTION_REMOVED);
    }

    fn push_locked(
        &self,
        state: &mut parking_lot::MutexGuard<'_, WinfspInvalidationQueueState>,
        ino: Ino,
    ) -> bool {
        loop {
            if state.closed {
                return false;
            }
            if state.pending.contains(&ino) {
                state.coalesced = state.coalesced.saturating_add(1);
                return true;
            }
            if state.pending.len() < WINFSP_INVALIDATION_QUEUE_CAPACITY {
                state.pending.insert(ino);
                state.order.push_back(ino);
                state.queue_peak = state.queue_peak.max(state.pending.len());
                return true;
            }
            #[cfg(test)]
            {
                state.waiting_producers = state.waiting_producers.saturating_add(1);
                self.not_full.notify_all();
            }
            self.not_full.wait(state);
            #[cfg(test)]
            {
                state.waiting_producers = state.waiting_producers.saturating_sub(1);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn drain(&self) -> Vec<Ino> {
        self.drain_targets(false)
            .into_iter()
            .map(|(ino, _)| ino)
            .collect()
    }

    fn drain_for_notify(&self) -> Vec<(Ino, Option<WinfspInvalidationOverride>)> {
        self.drain_targets(true)
    }

    fn drain_targets(
        &self,
        mark_notification_in_flight: bool,
    ) -> Vec<(Ino, Option<WinfspInvalidationOverride>)> {
        let mut state = self.state.lock();
        let mut inodes: Vec<_> = state.order.drain(..).collect();
        state.pending.clear();
        let mut targets = inodes
            .drain(..)
            .map(|ino| (ino, state.path_overrides.remove(&ino)))
            .collect::<Vec<_>>();
        if mark_notification_in_flight && !targets.is_empty() {
            state.notification_batches_in_flight =
                state.notification_batches_in_flight.saturating_add(1);
        }
        self.not_full.notify_all();
        self.quiescent.notify_all();
        let snapshot = (!targets.is_empty()).then(|| Self::next_metrics_snapshot(&mut state));
        drop(state);
        targets.sort_unstable_by_key(|(ino, _)| *ino);
        if let Some(snapshot) = snapshot {
            self.record_metrics(snapshot);
        }
        targets
    }

    #[cfg(test)]
    pub(crate) fn extend(&self, inodes: impl IntoIterator<Item = Ino>) {
        let mut state = self.state.lock();
        let mut changed = false;
        for ino in inodes {
            changed |= self.push_locked(&mut state, ino);
        }
        let snapshot = changed.then(|| Self::next_metrics_snapshot(&mut state));
        drop(state);
        if let Some(snapshot) = snapshot {
            self.record_metrics(snapshot);
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.state.lock().pending.is_empty()
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        self.not_full.notify_all();
        self.quiescent.notify_all();
        let snapshot = Self::next_metrics_snapshot(&mut state);
        drop(state);
        self.record_metrics(snapshot);
    }

    pub(crate) fn reset_metrics(&self) {
        let mut state = self.state.lock();
        state.queue_peak = 0;
        state.coalesced = 0;
    }

    #[cfg(test)]
    pub(crate) fn wait_for_quiescence(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock();
        while (!state.pending.is_empty() || state.notification_batches_in_flight != 0)
            && !state.closed
        {
            if self.quiescent.wait_until(&mut state, deadline).timed_out() {
                break;
            }
        }
        state.pending.is_empty() && state.notification_batches_in_flight == 0
    }

    fn finish_notification_batch(&self) {
        let mut state = self.state.lock();
        state.notification_batches_in_flight =
            state.notification_batches_in_flight.saturating_sub(1);
        self.quiescent.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn wait_until_producer_blocked(&self, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut state = self.state.lock();
        while state.waiting_producers == 0 {
            if self.not_full.wait_until(&mut state, deadline).timed_out() {
                return state.waiting_producers > 0;
            }
        }
        true
    }

    fn next_metrics_snapshot(
        state: &mut WinfspInvalidationQueueState,
    ) -> VfsFuseInvalidationSnapshot {
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        VfsFuseInvalidationSnapshot {
            snapshot_generation: state.snapshot_generation,
            queue_depth: state.pending.len(),
            queue_peak: state.queue_peak,
            coalesced: state.coalesced,
            ..VfsFuseInvalidationSnapshot::default()
        }
    }

    fn record_metrics(&self, snapshot: VfsFuseInvalidationSnapshot) {
        if let Some(vfs) = self.metrics_vfs.upgrade() {
            vfs.record_fuse_invalidation_snapshot(snapshot);
        }
    }
}

/// Resolve an inode number to a Windows-style path (`\dir\file`) by walking
/// the parent chain. Returns `None` if the inode or any ancestor no longer
/// exists (e.g. already deleted).
pub(super) fn ino_to_windows_path(vfs: &StudioVfs, ino: Ino) -> Option<String> {
    vfs.namespace_resolver(NamespacePathStyle::Windows)
        .path_for_inode(ino)
}

/// Bridges VFS metadata invalidation to WinFSP's
/// `FspFileSystemNotify` mechanism.
///
/// Metadata refreshes push inode numbers into a shared queue.
/// The WinFSP timer drains the queue and sends Windows change notifications
/// so that Explorer and other applications see remote changes promptly.
pub struct WinfspKernelInvalidator {
    queue: Arc<WinfspInvalidationQueue>,
}

impl WinfspKernelInvalidator {
    /// Creates a new invalidator backed by the shared inode queue.
    pub fn new(queue: Arc<WinfspInvalidationQueue>) -> Self {
        Self { queue }
    }
}

impl KernelInvalidator for WinfspKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.queue.push(ino);
    }

    fn reset_diagnostics(&self) {
        self.queue.reset_metrics();
    }
}

impl NotifyingFileSystemContext<PreparedWinfspNotifications> for StudioFsContext {
    fn should_notify(&self) -> Option<PreparedWinfspNotifications> {
        let targets = self.invalidation_queue.drain_for_notify();
        if targets.is_empty() {
            return None;
        }

        let meta = self.vfs.meta();
        let mut notifications = Vec::with_capacity(targets.len());
        for (ino, path_override) in targets {
            let (path, kind, action) = if let Some(path_override) = path_override {
                (path_override.path, path_override.kind, path_override.action)
            } else {
                let Some(path) = ino_to_windows_path(&self.vfs, ino) else {
                    trace!(ino, "skipping notify - inode no longer resolvable");
                    continue;
                };
                let kind = meta
                    .get_attr(ino)
                    .map(|a| a.kind)
                    .unwrap_or(FileType::RegularFile);
                (path, kind, FILE_ACTION_MODIFIED)
            };

            let Some(info) = build_notify_info_with_action(&path, kind, action) else {
                warn!(ino, path, "notify path too long, skipping");
                continue;
            };
            notifications.push(PreparedWinfspNotify {
                ino,
                path,
                kind,
                action,
                info,
            });
        }
        if notifications.is_empty() {
            self.invalidation_queue.finish_notification_batch();
            None
        } else {
            Some(PreparedWinfspNotifications::new(
                notifications,
                self.invalidation_queue.clone(),
            ))
        }
    }

    fn notify(&self, mut notifications: PreparedWinfspNotifications, notifier: &Notifier) {
        for notification in notifications.iter() {
            notifier.notify(&notification.info);
            debug!(
                ino = notification.ino,
                path = %notification.path,
                "WinFSP notify sent"
            );
        }
        notifications.published = true;
    }
}
