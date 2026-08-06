use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

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

pub(super) fn build_notify_info(path: &str, kind: FileType) -> Option<NotifyInfo<512>> {
    if let Err(error) = ensure_winfsp_runtime_loaded() {
        warn!(
            error = %error,
            "WinFSP runtime unavailable while building notify info; skipping notification"
        );
        return None;
    }
    let mut info = NotifyInfo::<512>::new();
    info.filter = notify_filter_for_kind(kind);
    info.action = FILE_ACTION_MODIFIED;
    info.set_name(path).ok()?;
    Some(info)
}

#[derive(Debug)]
pub(crate) struct PreparedWinfspNotify {
    pub(super) ino: Ino,
    pub(super) path: String,
    pub(super) info: NotifyInfo<512>,
}

pub(crate) type PreparedWinfspNotifications = Vec<PreparedWinfspNotify>;

#[derive(Debug, Default)]
struct WinfspInvalidationQueueState {
    order: VecDeque<Ino>,
    pending: HashSet<Ino>,
    queue_peak: usize,
    coalesced: u64,
}

/// Bounded, coalescing queue shared by WinFSP invalidation producers and the
/// notification timer.
#[derive(Debug)]
pub struct WinfspInvalidationQueue {
    state: Mutex<WinfspInvalidationQueueState>,
    not_full: Condvar,
}

#[derive(Debug, Clone, Copy)]
struct WinfspInvalidationQueueSnapshot {
    queue_depth: usize,
    queue_peak: usize,
    coalesced: u64,
}

impl WinfspInvalidationQueue {
    /// Creates an empty bounded invalidation queue.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(WinfspInvalidationQueueState::default()),
            not_full: Condvar::new(),
        })
    }

    pub(crate) fn push(&self, ino: Ino) {
        let mut state = self.state.lock();
        loop {
            if state.pending.contains(&ino) {
                state.coalesced = state.coalesced.saturating_add(1);
                return;
            }
            if state.pending.len() < WINFSP_INVALIDATION_QUEUE_CAPACITY {
                state.pending.insert(ino);
                state.order.push_back(ino);
                state.queue_peak = state.queue_peak.max(state.pending.len());
                return;
            }
            self.not_full.wait(&mut state);
        }
    }

    pub(crate) fn drain(&self) -> Vec<Ino> {
        let mut state = self.state.lock();
        let mut inodes: Vec<_> = state.order.drain(..).collect();
        state.pending.clear();
        self.not_full.notify_all();
        inodes.sort_unstable();
        inodes
    }

    pub(crate) fn extend(&self, inodes: impl IntoIterator<Item = Ino>) {
        for ino in inodes {
            self.push(ino);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.state.lock().pending.is_empty()
    }

    fn snapshot(&self) -> WinfspInvalidationQueueSnapshot {
        let state = self.state.lock();
        WinfspInvalidationQueueSnapshot {
            queue_depth: state.pending.len(),
            queue_peak: state.queue_peak,
            coalesced: state.coalesced,
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
}

impl NotifyingFileSystemContext<PreparedWinfspNotifications> for StudioFsContext {
    fn should_notify(&self) -> Option<PreparedWinfspNotifications> {
        let inodes = self.invalidation_queue.drain();
        if inodes.is_empty() {
            return None;
        }
        let queue_snapshot = self.invalidation_queue.snapshot();
        let mut diagnostics = self
            .vfs
            .write_path_observability_snapshot()
            .fuse_invalidation;
        diagnostics.snapshot_generation = diagnostics.snapshot_generation.saturating_add(1);
        diagnostics.queue_depth = queue_snapshot.queue_depth;
        diagnostics.queue_peak = queue_snapshot.queue_peak;
        diagnostics.coalesced = queue_snapshot.coalesced;
        self.vfs.record_fuse_invalidation_snapshot(diagnostics);

        let meta = self.vfs.meta();
        let mut notifications = Vec::with_capacity(inodes.len());
        for ino in &inodes {
            let Some(path) = ino_to_windows_path(&self.vfs, *ino) else {
                trace!(ino, "skipping notify - inode no longer resolvable");
                continue;
            };

            let kind = meta
                .get_attr(*ino)
                .map(|a| a.kind)
                .unwrap_or(FileType::RegularFile);

            let Some(info) = build_notify_info(&path, kind) else {
                warn!(ino, path, "notify path too long, skipping");
                continue;
            };
            notifications.push(PreparedWinfspNotify {
                ino: *ino,
                path,
                info,
            });
        }
        (!notifications.is_empty()).then_some(notifications)
    }

    fn notify(&self, notifications: PreparedWinfspNotifications, notifier: &Notifier) {
        for notification in &notifications {
            notifier.notify(&notification.info);
            debug!(
                ino = notification.ino,
                path = %notification.path,
                "WinFSP notify sent"
            );
        }
    }
}
