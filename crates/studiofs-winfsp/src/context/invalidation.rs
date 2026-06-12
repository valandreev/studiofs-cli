use std::sync::Arc;

use parking_lot::Mutex;
use studiofs_meta::{FileType, Ino};
use studiofs_vfs::{KernelInvalidator, NamespacePathStyle, StudioVfs};
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
    queue: Arc<Mutex<Vec<Ino>>>,
}

impl WinfspKernelInvalidator {
    /// Creates a new invalidator backed by the shared inode queue.
    pub fn new(queue: Arc<Mutex<Vec<Ino>>>) -> Self {
        Self { queue }
    }
}

impl KernelInvalidator for WinfspKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.queue.lock().push(ino);
    }
}

impl NotifyingFileSystemContext<PreparedWinfspNotifications> for StudioFsContext {
    fn should_notify(&self) -> Option<PreparedWinfspNotifications> {
        let mut queue = self.invalidation_queue.lock();
        if queue.is_empty() {
            return None;
        }
        // Drain and deduplicate while preserving the queue allocation for the
        // next invalidation burst.
        let replacement = Vec::with_capacity(queue.capacity());
        let mut inodes = std::mem::replace(&mut *queue, replacement);
        inodes.sort_unstable();
        inodes.dedup();
        drop(queue);

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
