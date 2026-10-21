use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Deref;
use std::sync::{Arc, Weak};
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

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

#[cfg(test)]
mod overflow_mount_test;

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
    // Native notifications use a counted name; set_name includes a rejected NUL.
    let name: Vec<_> = path.encode_utf16().collect();
    info.set_name_raw(name.as_slice()).ok()?;
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

    pub(super) fn mark_published(&mut self) {
        self.published = true;
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
                    queue.push_path_preserving_deferred_age(
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
    overflow_since: Option<Instant>,
    reconciliation_after: Option<Ino>,
    overflow_epoch: u64,
    order: VecDeque<NotificationKey>,
    pending: HashSet<NotificationKey>,
    path_overrides: HashMap<NotificationKey, WinfspInvalidationOverride>,
    delivery: WinfspDeliverySnapshot,
    queue_peak: usize,
    coalesced: u64,
    snapshot_generation: u64,
    notification_batches_in_flight: usize,
    pending_since: Option<Instant>,
    /// Oldest timestamp of drained targets that may be requeued after writer
    /// deferral or an unsubmitted notification batch.
    deferred_since: Option<Instant>,
}

/// Bounded, coalescing queue shared by WinFSP invalidation producers and the
/// notification timer.
#[derive(Debug)]
pub struct WinfspInvalidationQueue {
    state: Mutex<WinfspInvalidationQueueState>,
    quiescent: Condvar,
    metrics_vfs: Weak<StudioVfs>,
}

impl WinfspInvalidationQueue {
    /// Creates an empty bounded invalidation queue.
    pub fn new(vfs: &Arc<StudioVfs>) -> Arc<Self> {
        let queue = Arc::new(Self {
            state: Mutex::new(WinfspInvalidationQueueState::default()),
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
            if self.push_locked(&mut state, NotificationKey::Inode(ino), None) {
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
        self.push_path_with_age(ino, path, kind, action, filter, false);
    }

    fn push_path_preserving_deferred_age(
        &self,
        ino: Ino,
        path: String,
        kind: FileType,
        action: u32,
        filter: u32,
    ) {
        self.push_path_with_age(ino, path, kind, action, filter, true);
    }

    fn push_path_with_age(
        &self,
        ino: Ino,
        path: String,
        kind: FileType,
        action: u32,
        filter: u32,
        preserve_deferred_age: bool,
    ) {
        let key = NotificationKey::Path {
            ino,
            path: path.clone(),
            action,
            filter,
        };
        let mut state = self.state.lock();
        if state.closed {
            state.delivery.failed_notifications =
                state.delivery.failed_notifications.saturating_add(1);
            return;
        }
        if !state.pending.contains(&key)
            && state.pending.len() == WINFSP_INVALIDATION_QUEUE_CAPACITY
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
        let preserved_since = preserve_deferred_age
            .then_some(state.deferred_since)
            .flatten();
        self.push_locked(&mut state, key, preserved_since);
        let snapshot = Self::next_metrics_snapshot(&mut state);
        drop(state);
        self.record_metrics(snapshot);
    }

    /// Returns local acceptance and native submission diagnostics.
    pub fn delivery_snapshot(&self) -> WinfspDeliverySnapshot {
        self.state.lock().delivery
    }

    /// Return the age of the oldest pending or in-flight invalidation target.
    pub fn pending_delivery_age_ms(&self) -> Option<u64> {
        let state = self.state.lock();
        [
            state.pending_since,
            state.deferred_since,
            state.overflow_since,
        ]
        .into_iter()
        .flatten()
        .min()
        .map(|started| started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64)
    }

    #[cfg(test)]
    pub(super) fn pending_since_for_test(&self) -> Option<Instant> {
        self.state.lock().pending_since
    }

    #[cfg(test)]
    pub(super) fn set_pending_since_for_test(&self, since: Instant) {
        self.state.lock().pending_since = Some(since);
    }

    fn record_failed_notification(&self) {
        let mut state = self.state.lock();
        state.delivery.failed_notifications = state.delivery.failed_notifications.saturating_add(1);
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
        let (targets, acceptance, writer_deferred) = self.resolve_batch(batch);
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
        // Writer deferral is retryable and leaves the generation uncovered,
        // but it is not a delivery failure when the target was retained.
        let acceptance = if writer_deferred {
            InvalidationAcceptance::Failed
        } else {
            acceptance
        };
        let mut changed = false;
        for (key, target) in targets {
            state.path_overrides.insert(key.clone(), target);
            changed |= self.push_locked(&mut state, key, None);
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
        let snapshot = changed.then(|| Self::next_metrics_snapshot(&mut state));
        drop(state);
        if let Some(snapshot) = snapshot {
            self.record_metrics(snapshot);
        }
        acceptance
    }

    fn resolve_batch(
        &self,
        batch: &InvalidationBatch,
    ) -> (
        Vec<(NotificationKey, WinfspInvalidationOverride)>,
        InvalidationAcceptance,
        bool,
    ) {
        let Some(vfs) = self.metrics_vfs.upgrade() else {
            return (Vec::new(), InvalidationAcceptance::Failed, false);
        };
        let mut acceptance = InvalidationAcceptance::Queued;
        let mut targets = Vec::new();
        let mut keys = HashSet::new();
        let mut writer_deferred = false;
        let mut entry_targets_by_index = HashMap::new();
        let mut entry_paths_by_ino: HashMap<Ino, Vec<(String, FileType)>> = HashMap::new();
        for (index, intent) in batch.intents.iter().enumerate() {
            let InvalidationIntent::Entry {
                parent,
                name,
                ancestors,
                ino: entry_ino,
            } = intent
            else {
                continue;
            };
            match resolve_entry_notification_targets(
                &vfs,
                *parent,
                name,
                ancestors.as_ref(),
                *entry_ino,
            ) {
                Ok(entry_targets) => {
                    for (ino, path, kind, _, _) in &entry_targets {
                        entry_paths_by_ino
                            .entry(*ino)
                            .or_default()
                            .push((path.clone(), *kind));
                    }
                    entry_targets_by_index.insert(index, entry_targets);
                }
                Err(failure) if acceptance != InvalidationAcceptance::Failed => {
                    acceptance = failure;
                }
                Err(_) => {}
            }
        }
        for (index, intent) in batch.intents.iter().enumerate() {
            let mut add = |ino, path: String, kind, action, filter| {
                if self.defer_inode_writer(&vfs, ino) {
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
                    InvalidationIntent::Entry { .. } => {
                        let entry_targets = entry_targets_by_index
                            .get(&index)
                            .ok_or(InvalidationAcceptance::Failed)?;
                        for (ino, path, kind, action, filter) in entry_targets {
                            add(*ino, path.clone(), *kind, *action, *filter)?;
                        }
                    }
                    InvalidationIntent::Attributes { ino }
                    | InvalidationIntent::Content { ino }
                    | InvalidationIntent::Directory { ino } => {
                        let filter = match intent {
                            InvalidationIntent::Content { .. } => {
                                FILE_NOTIFY_CHANGE_SIZE | FILE_NOTIFY_CHANGE_LAST_WRITE
                            }
                            InvalidationIntent::Directory { .. } => {
                                FILE_NOTIFY_CHANGE_DIR_NAME | FILE_NOTIFY_CHANGE_FILE_NAME
                            }
                            _ => FILE_NOTIFY_CHANGE_ATTRIBUTES,
                        };
                        let paths = if let Some(paths) = entry_paths_by_ino.get(ino) {
                            paths.clone()
                        } else {
                            match vfs.meta().get_attr(*ino) {
                                Ok(attr) if attr.kind == FileType::Directory || attr.nlink <= 1 => {
                                    vec![(
                                        ino_to_windows_path(&vfs, *ino)
                                            .ok_or(InvalidationAcceptance::Failed)?,
                                        attr.kind,
                                    )]
                                }
                                Ok(_) => return Err(InvalidationAcceptance::Failed),
                                // WinFSP caches by path. A removed inode has no
                                // current path left to purge; its old names are
                                // covered by entry intents with historical paths.
                                Err(studiofs_meta::MetaError::InodeNotFound(_)) => {
                                    return Ok(());
                                }
                                Err(_) => return Err(InvalidationAcceptance::Failed),
                            }
                        };
                        for (path, kind) in paths {
                            add(*ino, path, kind, FILE_ACTION_MODIFIED, filter)?;
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
        (targets, acceptance, writer_deferred)
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
        preserved_since: Option<Instant>,
    ) -> bool {
        if state.closed {
            state.delivery.failed_notifications =
                state.delivery.failed_notifications.saturating_add(1);
            return false;
        }
        if let (Some(current), Some(since)) = (state.pending_since.as_mut(), preserved_since) {
            *current = (*current).min(since);
        }
        if state.pending.contains(&key) {
            state.coalesced = state.coalesced.saturating_add(1);
            return true;
        }
        if state.pending.len() < WINFSP_INVALIDATION_QUEUE_CAPACITY {
            if state.pending.is_empty() {
                state.pending_since = Some(preserved_since.unwrap_or_else(Instant::now));
            }
            state.pending.insert(key.clone());
            state.order.push_back(key);
            state.queue_peak = state.queue_peak.max(state.pending.len());
            return true;
        }
        state.delivery.failed_notifications = state.delivery.failed_notifications.saturating_add(1);
        if matches!(key, NotificationKey::Inode(_)) {
            // A later overflow may be behind the sweep cursor; restart without
            // forgetting how long the oldest lost request has been pending.
            state.overflow_since.get_or_insert_with(Instant::now);
            state.reconciliation_after = None;
            state.overflow_epoch = state.overflow_epoch.wrapping_add(1);
        }
        false
    }

    fn refill_overflow(&self) {
        let (epoch, after, limit) = {
            let state = self.state.lock();
            if state.closed || state.overflow_since.is_none() {
                return;
            }
            (
                state.overflow_epoch,
                state.reconciliation_after,
                WINFSP_INVALIDATION_QUEUE_CAPACITY - state.pending.len(),
            )
        };
        if limit == 0 {
            return;
        }
        let Some(vfs) = self.metrics_vfs.upgrade() else {
            return;
        };
        // Read bounded local metadata pages outside the producer's queue lock.
        let inodes = match vfs.meta().list_inode_ids_after(after, limit) {
            Ok(inodes) => inodes,
            Err(_) => {
                self.record_failed_notification();
                return;
            }
        };
        let mut state = self.state.lock();
        if state.closed || state.overflow_epoch != epoch || state.reconciliation_after != after {
            return;
        }
        let since = state.overflow_since;
        let mut accepted = 0;
        for &ino in &inodes {
            let key = NotificationKey::Inode(ino);
            if state.pending.len() == WINFSP_INVALIDATION_QUEUE_CAPACITY
                && !state.pending.contains(&key)
            {
                break;
            }
            self.push_locked(&mut state, key, since);
            state.reconciliation_after = Some(ino);
            accepted += 1;
        }
        if accepted == inodes.len() && inodes.len() < limit {
            state.overflow_since = None;
            state.reconciliation_after = None;
        }
        let snapshot = Self::next_metrics_snapshot(&mut state);
        drop(state);
        self.record_metrics(snapshot);
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
        // Refill before draining so deferred writers can reuse the freed slots.
        self.refill_overflow();
        let mut state = self.state.lock();
        let mut targets = Vec::with_capacity(state.order.len());
        state.pending.clear();
        let drained_since = state.pending_since.take();
        while let Some(key) = state.order.pop_front() {
            let ino = match &key {
                NotificationKey::Inode(ino) | NotificationKey::Path { ino, .. } => *ino,
            };
            let path_override = state.path_overrides.remove(&key);
            targets.push((ino, path_override));
        }
        if mark_notification_in_flight && !targets.is_empty() {
            state.deferred_since = match (state.deferred_since, drained_since) {
                (Some(existing), Some(drained)) => Some(existing.min(drained)),
                (existing, drained) => existing.or(drained),
            };
            state.notification_batches_in_flight =
                state.notification_batches_in_flight.saturating_add(1);
        } else if !mark_notification_in_flight {
            state.deferred_since = None;
        }
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
            changed |= self.push_locked(&mut state, NotificationKey::Inode(ino), None);
        }
        let snapshot = changed.then(|| Self::next_metrics_snapshot(&mut state));
        drop(state);
        if let Some(snapshot) = snapshot {
            self.record_metrics(snapshot);
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        let state = self.state.lock();
        state.pending.is_empty() && state.overflow_since.is_none()
    }

    #[cfg(test)]
    pub(crate) fn order_capacity(&self) -> usize {
        self.state.lock().order.capacity()
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        state.overflow_since = None;
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
        while (!state.pending.is_empty()
            || state.overflow_since.is_some()
            || state.notification_batches_in_flight != 0)
            && !state.closed
        {
            if self.quiescent.wait_until(&mut state, deadline).timed_out() {
                break;
            }
        }
        state.pending.is_empty()
            && state.overflow_since.is_none()
            && state.notification_batches_in_flight == 0
    }

    fn finish_notification_batch(&self) {
        let mut state = self.state.lock();
        state.notification_batches_in_flight =
            state.notification_batches_in_flight.saturating_sub(1);
        if state.notification_batches_in_flight == 0 {
            state.deferred_since = None;
            if state.pending.is_empty() {
                state.pending_since = None;
            }
        }
        self.quiescent.notify_all();
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

type EntryNotificationTarget = (Ino, String, FileType, u32, u32);

fn resolve_entry_notification_targets(
    vfs: &StudioVfs,
    parent: Ino,
    name: &[u8],
    ancestors: Option<&Vec<Vec<u8>>>,
    entry_ino: Option<Ino>,
) -> Result<Vec<EntryNotificationTarget>, InvalidationAcceptance> {
    let current_parent = ino_to_windows_path(vfs, parent);
    let historical_parent = if let Some(components) = ancestors {
        let mut path = "\\".to_string();
        for component in components {
            path = append_entry_path(&path, component).ok_or(InvalidationAcceptance::Failed)?;
        }
        path
    } else {
        current_parent
            .clone()
            .ok_or(InvalidationAcceptance::Failed)?
    };
    let historical_path =
        append_entry_path(&historical_parent, name).ok_or(InvalidationAcceptance::Failed)?;
    let current_entry = match vfs.meta().lookup(parent, name) {
        Ok(entry) => Some(entry),
        Err(
            studiofs_meta::MetaError::EntryNotFound { .. }
            | studiofs_meta::MetaError::InodeNotFound(_),
        ) => None,
        Err(_) => return Err(InvalidationAcceptance::Failed),
    };
    let historical = current_parent.as_ref() != Some(&historical_parent);
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
    let filter = FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_DIR_NAME;
    let mut targets = vec![(ino, historical_path, kind, action, filter)];
    if historical && let (Some(current_parent), Some(entry)) = (current_parent, current_entry) {
        let current_path =
            append_entry_path(&current_parent, name).ok_or(InvalidationAcceptance::Failed)?;
        targets.push((
            entry.ino,
            current_path,
            entry.kind,
            FILE_ACTION_MODIFIED,
            filter,
        ));
    }
    Ok(targets)
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

    fn invalidation_delivery_age_ms(&self) -> Option<u64> {
        self.queue.pending_delivery_age_ms()
    }

    fn invalidation_delivery_failure_count(&self) -> u64 {
        let delivery = self.queue.delivery_snapshot();
        // NotifyBegin rejection is retried by `PreparedWinfspNotifications::drop`;
        // pending delivery age accounts for work that remains outstanding. A
        // transient retry must not become a permanent coherence failure after
        // the requeued notification is later submitted.
        delivery
            .failed_batches
            .saturating_add(delivery.failed_notifications)
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
                    if matches!(
                        meta.get_attr(ino),
                        Err(studiofs_meta::MetaError::InodeNotFound(_))
                    ) {
                        // Removed inodes have no current path to purge; their
                        // old names are delivered as explicit path notifications.
                        trace!(ino, "skipping notify - inode removed");
                        continue;
                    }
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

            if self.invalidation_queue.defer_inode_writer(&self.vfs, ino) {
                self.invalidation_queue
                    .push_path_preserving_deferred_age(ino, path, kind, action, filter);
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
            {
                self.invalidation_queue.push_path_preserving_deferred_age(
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
        notifications.mark_published();
    }
}

#[cfg(test)]
mod overflow_tests {
    use super::*;
    use crate::context::tests::make_test_context;

    #[test]
    fn overflow_reconciliation_pages_restart_and_preserve_age() {
        let (_tmp, vfs, _ctx, queue) = make_test_context("overflow-page-restart");
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"overflow", 0, 0, 0o644)
            .unwrap();
        queue.extend(10_000..10_000 + WINFSP_INVALIDATION_QUEUE_CAPACITY as u64);
        queue.push(attr.ino);
        let since = Instant::now() - Duration::from_secs(5);
        {
            let mut state = queue.state.lock();
            state.overflow_since = Some(since);
            let key = state.order.pop_back().unwrap();
            state.pending.remove(&key);
        }
        queue.refill_overflow();
        {
            let state = queue.state.lock();
            assert_eq!(state.pending.len(), WINFSP_INVALIDATION_QUEUE_CAPACITY);
            assert_eq!(state.reconciliation_after, Some(studiofs_meta::ROOT_INO));
            assert_eq!(state.overflow_since, Some(since));
        }
        queue.push(attr.ino);
        {
            let state = queue.state.lock();
            assert_eq!(state.reconciliation_after, None);
            assert_eq!(state.overflow_since, Some(since));
        }
        assert_eq!(queue.drain().len(), WINFSP_INVALIDATION_QUEUE_CAPACITY);
        assert_eq!(queue.state.lock().overflow_since, Some(since));
        assert!(queue.pending_delivery_age_ms().unwrap() >= 5_000);
        assert!(queue.drain().contains(&attr.ino));
        assert!(queue.is_empty());
        assert_eq!(queue.pending_delivery_age_ms(), None);
    }

    #[test]
    fn closing_queue_cancels_overflow_reconciliation() {
        let (_tmp, _vfs, _ctx, queue) = make_test_context("overflow-close");
        queue.extend(10_000..10_000 + WINFSP_INVALIDATION_QUEUE_CAPACITY as u64);
        queue.push(studiofs_meta::ROOT_INO);
        queue.close();
        assert_eq!(queue.drain().len(), WINFSP_INVALIDATION_QUEUE_CAPACITY);
        assert!(queue.is_empty());
        assert!(queue.drain().is_empty());
        assert_eq!(queue.pending_delivery_age_ms(), None);
    }
}
