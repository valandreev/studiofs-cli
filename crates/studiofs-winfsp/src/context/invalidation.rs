use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Deref;
use std::sync::{Arc, Weak};
#[cfg(test)]
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use studiofs_meta::coherence::{InvalidationBatch, InvalidationIntent};
use studiofs_meta::{FileType, Ino};
use studiofs_vfs::coherence::InvalidationAcceptance;
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
/// Maximum number of distinct notification targets retained by WinFSP.
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
                {
                    let mut state = queue.state.lock();
                    state.delivery.retried_batches =
                        state.delivery.retried_batches.saturating_add(1);
                }
                for notification in &self.notifications {
                    queue.push_path(
                        notification.ino,
                        notification.path.clone(),
                        notification.kind,
                        notification.action,
                        notification.info.filter,
                    );
                }
            }
            queue.finish_notification_batch();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum NotificationKey {
    Inode(Ino),
    Path {
        ino: Ino,
        path: String,
        action: u32,
        filter: u32,
    },
}

#[derive(Debug, Default, Clone, Copy)]
/// Queue acceptance and native submission diagnostics; submission is not delivery.
pub struct WinfspDeliverySnapshot {
    /// Highest generation fully accepted into the bounded queue.
    pub queued_generation: Option<u64>,
    /// Native delivery is unobservable through winfsp::Notifier and remains unknown.
    pub delivered_generation: Option<u64>,
    /// Batches rejected due to capacity, closure, or unresolved targets.
    pub failed_batches: u64,
    /// Batches containing unsupported reset requests.
    pub unsupported_batches: u64,
    /// Notifications submitted to a native API that discards its status.
    pub submitted_notifications: u64,
    /// Prepared batches retried because NotifyBegin did not accept them.
    pub retried_batches: u64,
    /// Targets that could not be prepared or retained for retry.
    pub failed_notifications: u64,
    /// Native purges suppressed to preserve an active local writer.
    pub deferred_notifications: u64,
}

#[derive(Debug)]
struct WinfspInvalidationOverride {
    path: String,
    kind: FileType,
    action: u32,
    filter: u32,
}

#[derive(Debug, Default)]
struct WinfspInvalidationQueueState {
    closed: bool,
    order: VecDeque<NotificationKey>,
    pending: HashSet<NotificationKey>,
    path_overrides: HashMap<NotificationKey, WinfspInvalidationOverride>,
    delivery: WinfspDeliverySnapshot,
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
            if self.push_locked(&mut state, NotificationKey::Inode(ino)) {
                Some(Self::next_metrics_snapshot(&mut state))
            } else {
                None
            }
        };
        if let Some(snapshot) = snapshot {
            self.record_metrics(snapshot);
        }
    }

    fn push_path(&self, ino: Ino, path: String, kind: FileType, action: u32, filter: u32) {
        let key = NotificationKey::Path {
            ino,
            path: path.clone(),
            action,
            filter,
        };
        let mut state = self.state.lock();
        // Retry runs on the sole consumer: never wait for this same consumer to drain.
        if state.closed
            || (!state.pending.contains(&key)
                && state.pending.len() == WINFSP_INVALIDATION_QUEUE_CAPACITY)
        {
            state.delivery.failed_notifications =
                state.delivery.failed_notifications.saturating_add(1);
            return;
        }
        state.path_overrides.insert(
            key.clone(),
            WinfspInvalidationOverride {
                path,
                kind,
                action,
                filter,
            },
        );
        self.push_locked(&mut state, key);
        let snapshot = Self::next_metrics_snapshot(&mut state);
        drop(state);
        self.record_metrics(snapshot);
    }

    /// Returns local acceptance and native submission diagnostics.
    pub fn delivery_snapshot(&self) -> WinfspDeliverySnapshot {
        self.state.lock().delivery
    }

    fn record_failed_notification(&self) {
        let mut state = self.state.lock();
        state.delivery.failed_notifications = state.delivery.failed_notifications.saturating_add(1);
    }

    fn defer_for_writer(&self, vfs: &StudioVfs, path: &str) -> bool {
        let deferred = match vfs
            .namespace_resolver(NamespacePathStyle::Windows)
            .resolve_path(path)
        {
            Ok(attr) => vfs.defer_kernel_invalidation_for_active_writer(attr.ino),
            Err(studiofs_vfs::VfsError::Meta(
                studiofs_meta::MetaError::EntryNotFound { .. }
                | studiofs_meta::MetaError::InodeNotFound(_),
            )) => false,
            Err(_) => true,
        };
        if deferred {
            let mut state = self.state.lock();
            state.delivery.deferred_notifications =
                state.delivery.deferred_notifications.saturating_add(1);
        }
        deferred
    }

    fn defer_inode_writer(&self, vfs: &StudioVfs, ino: Ino) -> bool {
        if !vfs.defer_kernel_invalidation_for_active_writer(ino) {
            return false;
        }
        let mut state = self.state.lock();
        state.delivery.deferred_notifications =
            state.delivery.deferred_notifications.saturating_add(1);
        true
    }

    fn enqueue_batch(
        &self,
        batch: &InvalidationBatch,
        generation_complete: bool,
    ) -> InvalidationAcceptance {
        let (targets, acceptance) = self.resolve_batch(batch);
        let mut state = self.state.lock();
        if state.closed {
            state.delivery.failed_batches = state.delivery.failed_batches.saturating_add(1);
            return InvalidationAcceptance::Failed;
        }
        let additional = targets
            .iter()
            .filter(|(key, _)| !state.pending.contains(key))
            .count();
        if state.pending.len().saturating_add(additional) > WINFSP_INVALIDATION_QUEUE_CAPACITY {
            state.delivery.failed_batches = state.delivery.failed_batches.saturating_add(1);
            return InvalidationAcceptance::Failed;
        }
        match acceptance {
            InvalidationAcceptance::Failed => {
                state.delivery.failed_batches = state.delivery.failed_batches.saturating_add(1)
            }
            InvalidationAcceptance::Unsupported => {
                state.delivery.unsupported_batches =
                    state.delivery.unsupported_batches.saturating_add(1)
            }
            InvalidationAcceptance::Queued => (),
        }
        for (key, target) in targets {
            state.path_overrides.insert(key.clone(), target);
            self.push_locked(&mut state, key);
        }
        if generation_complete && acceptance == InvalidationAcceptance::Queued {
            state.delivery.queued_generation = Some(
                state
                    .delivery
                    .queued_generation
                    .unwrap_or(0)
                    .max(batch.generation),
            );
        }
        let snapshot = Self::next_metrics_snapshot(&mut state);
        drop(state);
        self.record_metrics(snapshot);
        acceptance
    }

    fn resolve_batch(
        &self,
        batch: &InvalidationBatch,
    ) -> (
        Vec<(NotificationKey, WinfspInvalidationOverride)>,
        InvalidationAcceptance,
    ) {
        let Some(vfs) = self.metrics_vfs.upgrade() else {
            return (Vec::new(), InvalidationAcceptance::Failed);
        };
        let mut acceptance = InvalidationAcceptance::Queued;
        let mut targets = Vec::new();
        let mut keys = HashSet::new();
        let mut writer_deferred = false;
        for intent in &batch.intents {
            let mut add = |ino, path: String, kind, action, filter| {
                if self.defer_inode_writer(&vfs, ino) || self.defer_for_writer(&vfs, &path) {
                    writer_deferred = true;
                }
                if path.encode_utf16().count() >= 512 || path.contains('\0') {
                    return Err(InvalidationAcceptance::Failed);
                }
                let key = NotificationKey::Path {
                    ino,
                    path: path.clone(),
                    action,
                    filter,
                };
                if !keys.contains(&key) && keys.len() == WINFSP_INVALIDATION_QUEUE_CAPACITY {
                    return Err(InvalidationAcceptance::Failed);
                }
                if keys.insert(key.clone()) {
                    targets.push((
                        key,
                        WinfspInvalidationOverride {
                            path,
                            kind,
                            action,
                            filter,
                        },
                    ));
                }
                Ok(())
            };
            let result = (|| {
                match intent {
                    InvalidationIntent::Reset => return Err(InvalidationAcceptance::Unsupported),
                    InvalidationIntent::Entry {
                        parent,
                        name,
                        ancestors,
                        ino: entry_ino,
                    } => {
                        let current_parent = ino_to_windows_path(&vfs, *parent);
                        let parent_path = if let Some(components) = ancestors {
                            let mut path = "\\".to_string();
                            for component in components {
                                path = append_entry_path(&path, component)
                                    .ok_or(InvalidationAcceptance::Failed)?;
                            }
                            path
                        } else {
                            current_parent
                                .clone()
                                .ok_or(InvalidationAcceptance::Failed)?
                        };
                        let path = append_entry_path(&parent_path, name)
                            .ok_or(InvalidationAcceptance::Failed)?;
                        let current_entry = match vfs.meta().lookup(*parent, name) {
                            Ok(entry) => Some(entry),
                            Err(
                                studiofs_meta::MetaError::EntryNotFound { .. }
                                | studiofs_meta::MetaError::InodeNotFound(_),
                            ) => None,
                            Err(_) => return Err(InvalidationAcceptance::Failed),
                        };
                        let historical = current_parent.as_ref() != Some(&parent_path);
                        let (ino, kind, action) = match &current_entry {
                            Some(entry) if !historical => (
                                entry_ino.unwrap_or(entry.ino),
                                entry.kind,
                                FILE_ACTION_MODIFIED,
                            ),
                            Some(entry) => (
                                entry_ino.unwrap_or(entry.ino),
                                entry.kind,
                                FILE_ACTION_REMOVED,
                            ),
                            _ => (
                                entry_ino.unwrap_or(0),
                                FileType::RegularFile,
                                FILE_ACTION_REMOVED,
                            ),
                        };
                        // Native WinFSP purges content regardless of filter; filters
                        // select change reports, not the strength of invalidation.
                        let filter = FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_DIR_NAME;
                        add(ino, path, kind, action, filter)?;
                        if historical
                            && let (Some(parent_path), Some(entry)) =
                                (current_parent, current_entry)
                        {
                            let path = append_entry_path(&parent_path, name)
                                .ok_or(InvalidationAcceptance::Failed)?;
                            add(entry.ino, path, entry.kind, FILE_ACTION_MODIFIED, filter)?;
                        }
                    }
                    InvalidationIntent::Attributes { ino }
                    | InvalidationIntent::Content { ino }
                    | InvalidationIntent::Directory { ino } => {
                        let attr = vfs
                            .meta()
                            .get_attr(*ino)
                            .map_err(|_| InvalidationAcceptance::Failed)?;
                        let filter = match intent {
                            InvalidationIntent::Content { .. } => {
                                FILE_NOTIFY_CHANGE_SIZE | FILE_NOTIFY_CHANGE_LAST_WRITE
                            }
                            InvalidationIntent::Directory { .. } => {
                                FILE_NOTIFY_CHANGE_DIR_NAME | FILE_NOTIFY_CHANGE_FILE_NAME
                            }
                            _ => FILE_NOTIFY_CHANGE_ATTRIBUTES,
                        };
                        for path in inode_notification_paths(&vfs, *ino, attr.kind, attr.nlink)? {
                            add(*ino, path, attr.kind, FILE_ACTION_MODIFIED, filter)?;
                        }
                    }
                }
                Ok(())
            })();
            if let Err(failure) = result
                && acceptance != InvalidationAcceptance::Failed
            {
                acceptance = failure;
            }
        }
        if writer_deferred {
            acceptance = InvalidationAcceptance::Failed;
        }
        (targets, acceptance)
    }

    pub(crate) fn push_deleted_path(&self, ino: Ino, path: String, kind: FileType) {
        self.push_path(
            ino,
            path,
            kind,
            FILE_ACTION_REMOVED,
            notify_filter_for_kind(kind),
        );
    }

    fn push_locked(
        &self,
        state: &mut parking_lot::MutexGuard<'_, WinfspInvalidationQueueState>,
        key: NotificationKey,
    ) -> bool {
        loop {
            if state.closed {
                return false;
            }
            if state.pending.contains(&key) {
                state.coalesced = state.coalesced.saturating_add(1);
                return true;
            }
            if state.pending.len() < WINFSP_INVALIDATION_QUEUE_CAPACITY {
                state.pending.insert(key.clone());
                state.order.push_back(key);
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
        let mut inodes: Vec<_> = self
            .drain_targets(false)
            .into_iter()
            .map(|(ino, _)| ino)
            .collect();
        inodes.sort_unstable();
        inodes
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
        let targets = inodes
            .drain(..)
            .map(|key| {
                let ino = match &key {
                    NotificationKey::Inode(ino) | NotificationKey::Path { ino, .. } => *ino,
                };
                (ino, state.path_overrides.remove(&key))
            })
            .collect::<Vec<_>>();
        if mark_notification_in_flight && !targets.is_empty() {
            state.notification_batches_in_flight =
                state.notification_batches_in_flight.saturating_add(1);
        }
        self.not_full.notify_all();
        self.quiescent.notify_all();
        let snapshot = (!targets.is_empty()).then(|| Self::next_metrics_snapshot(&mut state));
        drop(state);
        // Keep insertion order for old/new namespace names. Legacy test drains sort separately.
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
            changed |= self.push_locked(&mut state, NotificationKey::Inode(ino));
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

fn append_entry_path(parent: &str, name: &[u8]) -> Option<String> {
    let name = std::str::from_utf8(name).ok()?;
    if name.is_empty() || name.contains(['\\', '/', '\0']) || name == "." || name == ".." {
        return None;
    }
    Some(format!("{}\\{}", parent.trim_end_matches('\\'), name))
}

fn inode_notification_paths(
    vfs: &StudioVfs,
    ino: Ino,
    kind: FileType,
    nlink: u32,
) -> Result<Vec<String>, InvalidationAcceptance> {
    if kind == FileType::Directory || nlink <= 1 {
        return ino_to_windows_path(vfs, ino)
            .map(|path| vec![path])
            .ok_or(InvalidationAcceptance::Failed);
    }
    // The metadata port has no reverse alias index. Enumerate the local serving
    // namespace with a fixed work bound; incomplete coverage rejects the batch.
    let mut directories = VecDeque::from([(studiofs_meta::ROOT_INO, "\\".to_string())]);
    let mut seen = HashSet::new();
    let mut inspected = 0usize;
    let mut paths = Vec::new();
    while let Some((parent, parent_path)) = directories.pop_front() {
        if !seen.insert(parent) {
            continue;
        }
        for entry in vfs
            .readdir(parent)
            .map_err(|_| InvalidationAcceptance::Failed)?
        {
            inspected += 1;
            if inspected > WINFSP_INVALIDATION_QUEUE_CAPACITY {
                return Err(InvalidationAcceptance::Failed);
            }
            let path = append_entry_path(&parent_path, &entry.name)
                .ok_or(InvalidationAcceptance::Failed)?;
            if entry.ino == ino {
                paths.push(path.clone());
                if paths.len() == nlink as usize {
                    return Ok(paths);
                }
            }
            if entry.kind == FileType::Directory {
                directories.push_back((entry.ino, path));
            }
        }
    }
    Err(InvalidationAcceptance::Failed)
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
    fn invalidate_batch(
        &self,
        batch: &InvalidationBatch,
        generation_complete: bool,
    ) -> InvalidationAcceptance {
        self.queue.enqueue_batch(batch, generation_complete)
    }
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
            let (path, kind, action, filter) = if let Some(path_override) = path_override {
                (
                    path_override.path,
                    path_override.kind,
                    path_override.action,
                    path_override.filter,
                )
            } else {
                let Some(path) = ino_to_windows_path(&self.vfs, ino) else {
                    trace!(ino, "skipping notify - inode no longer resolvable");
                    self.invalidation_queue.record_failed_notification();
                    continue;
                };
                let kind = meta
                    .get_attr(ino)
                    .map(|a| a.kind)
                    .unwrap_or(FileType::RegularFile);
                (
                    path,
                    kind,
                    FILE_ACTION_MODIFIED,
                    notify_filter_for_kind(kind),
                )
            };

            if self.invalidation_queue.defer_inode_writer(&self.vfs, ino)
                || self.invalidation_queue.defer_for_writer(&self.vfs, &path)
            {
                self.invalidation_queue
                    .push_path(ino, path, kind, action, filter);
                continue;
            }
            let Some(mut info) = build_notify_info_with_action(&path, kind, action) else {
                warn!(ino, path, "notify path too long, skipping");
                self.invalidation_queue.record_failed_notification();
                continue;
            };
            info.filter = filter;
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
        let mut submitted = 0u64;
        for notification in notifications.iter() {
            if self
                .invalidation_queue
                .defer_inode_writer(&self.vfs, notification.ino)
                || self
                    .invalidation_queue
                    .defer_for_writer(&self.vfs, &notification.path)
            {
                self.invalidation_queue.push_path(
                    notification.ino,
                    notification.path.clone(),
                    notification.kind,
                    notification.action,
                    notification.info.filter,
                );
                continue;
            }
            notifier.notify(&notification.info);
            submitted = submitted.saturating_add(1);
            debug!(
                ino = notification.ino,
                path = %notification.path,
                "WinFSP notify sent"
            );
        }
        let mut state = self.invalidation_queue.state.lock();
        state.delivery.submitted_notifications = state
            .delivery
            .submitted_notifications
            .saturating_add(submitted);
        notifications.published = true;
    }
}
