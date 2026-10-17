//! Upload Queue — in-memory scheduler for pending S3 uploads.
//!
//! The local backend persists upload intent in lifecycle flags (`DIRTY` /
//! transient `UPLOADING`). The queue itself is intentionally in-memory:
//! startup replay is reconstructed from the local backend + metadata mapping.

use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashSet, btree_map::Entry};
use std::ops::Bound::{Excluded, Included};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, mpsc};
use tracing::debug;

/// Inode number (re-exported convenience).
pub type Ino = u64;

/// A pending upload operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingUpload {
    /// Inode that owns this payload object.
    pub ino: Ino,
    /// File offset this payload object corresponds to.
    pub offset: u64,
    /// File offset corresponding to byte 0 of the uploaded payload object.
    pub object_file_offset: u64,
    /// Object ID (object ID).
    pub object_id: u64,
    /// Uploaded payload length in bytes.
    pub payload_len: u64,
    /// Monotonic content generation captured when the upload was queued.
    pub content_epoch: u64,
    /// Enqueue timestamp in unix milliseconds.
    pub created_unix_ms: u64,
    /// Earliest unix-millis timestamp when the upload may be retried.
    pub not_before_unix_ms: u64,
    /// In-memory retry attempt counter for cooldown requeue logic.
    pub retry_count: u32,
}

/// Manages in-memory pending upload state and dispatch channel.
pub struct UploadQueue {
    sender: mpsc::Sender<()>,
    pending_state: Mutex<PendingState>,
    idle_notify: Arc<Notify>,
    shutdown_requested: AtomicBool,
    shutdown_notify: Notify,
    #[cfg(test)]
    wait_for_idle_hook: Mutex<Option<Arc<WaitForIdleHook>>>,
    #[cfg(test)]
    closed_dispatch_hook: Mutex<Option<ClosedDispatchHook>>,
}

#[cfg(test)]
struct WaitForIdleHook {
    entered: Notify,
    release: Notify,
}

#[cfg(test)]
impl Default for WaitForIdleHook {
    fn default() -> Self {
        Self {
            entered: Notify::new(),
            release: Notify::new(),
        }
    }
}

#[cfg(test)]
/// One-shot test hook; consumed on first activation.
struct ClosedDispatchHook {
    entered: tokio::sync::mpsc::UnboundedSender<PendingUpload>,
    release: std::sync::mpsc::Receiver<()>,
}

/// Atomically-consistent index of in-flight uploads.
///
/// Both maps are guarded by the same mutex so that `has_pending_for_ino`
/// never observes an inconsistent state where a payload object exists in the
/// object-id index but the per-inode index has not yet been updated.
struct PendingState {
    by_object: BTreeMap<u64, PendingUpload>,
    by_inode: BTreeMap<Ino, BTreeSet<u64>>,
    // Dirty IDs permit cancellation before due; deadline and due indexes partition them.
    dirty_by_inode: BTreeMap<Ino, BTreeSet<u64>>,
    due_by_inode: BTreeMap<Ino, BTreeSet<u64>>,
    // false keys use creation time plus initial delay; true keys use retry deadlines.
    deadlines: BTreeSet<(bool, u64, Ino, u64)>,
    // Freeze each sweep's upper ID so continuous arrivals cannot postpone older retries.
    claim_inode_cursor: Option<(Ino, Ino)>,
    claim_object_cursors: BTreeMap<Ino, (u64, u64)>,
    // Mirror of the caller's insert-only tombstones; `canceled_dirty` is the subset with dirty IDs.
    canceled_inodes: HashSet<Ino>,
    canceled_dirty: BTreeSet<Ino>,
    wakeup_coalesced: u64,
}

/// Runtime ingress state for the upload worker wakeup channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadQueueIngressSnapshot {
    /// Number of wakeups currently buffered for the worker.
    pub depth: usize,
    /// Maximum number of wakeups buffered by the queue.
    pub capacity: usize,
    /// Number of submissions coalesced while the wakeup was already buffered.
    pub coalesced: u64,
}

const UPLOAD_WAKEUP_CAPACITY: usize = 1;

/// Result of submitting an upload to the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadSubmitOutcome {
    /// The submission created a new queue owner; the caller must retain its pin.
    New,
    /// The submission updated an existing queue entry without adding an owner.
    Updated,
    /// The submission was stale or duplicated an existing queue entry.
    Ignored,
    /// The queue wakeup channel was closed and the attempted change was rolled back.
    Closed,
}

enum PendingSubmit {
    New,
    Updated(PendingUpload),
    Ignored,
}

impl UploadQueue {
    /// Create a new upload queue.
    ///
    /// Returns `(queue, receiver)`. The receiver yields coalesced wakeups for
    /// the VFS upload worker; the authoritative operations remain in this
    /// queue's pending state.
    ///
    /// `cache_dir` is accepted for API compatibility with older call sites,
    /// but queue state is no longer persisted on disk.
    pub fn new(_cache_dir: impl AsRef<Path>) -> (Arc<Self>, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::channel(UPLOAD_WAKEUP_CAPACITY);
        let idle_notify = Arc::new(Notify::new());

        let queue = Arc::new(Self {
            sender: tx,
            pending_state: Mutex::new(PendingState {
                by_object: BTreeMap::new(),
                by_inode: BTreeMap::new(),
                dirty_by_inode: BTreeMap::new(),
                due_by_inode: BTreeMap::new(),
                deadlines: BTreeSet::new(),
                claim_inode_cursor: None,
                claim_object_cursors: BTreeMap::new(),
                canceled_inodes: HashSet::new(),
                canceled_dirty: BTreeSet::new(),
                wakeup_coalesced: 0,
            }),
            idle_notify,
            shutdown_requested: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
            #[cfg(test)]
            wait_for_idle_hook: Mutex::new(None),
            #[cfg(test)]
            closed_dispatch_hook: Mutex::new(None),
        });

        (queue, rx)
    }

    /// Submit a pending upload (hot path — non-blocking).
    ///
    /// Callers pinning the payload must acquire a provisional pin before submission.
    /// Retain it only for [`UploadSubmitOutcome::New`]; release it for `Updated`,
    /// `Ignored`, and `Closed`. Existing entry pin
    /// ownership is unchanged by `Updated`/`Ignored` and restored if `Closed`
    /// rolls an update back.
    pub fn submit(
        &self,
        ino: Ino,
        offset: u64,
        object_id: u64,
        content_epoch: u64,
    ) -> UploadSubmitOutcome {
        self.submit_with_payload(ino, offset, offset, object_id, content_epoch, 0)
    }

    /// Submit a pending upload with explicit uploaded-payload geometry.
    ///
    /// Callers pinning the payload must acquire a provisional pin before submission.
    /// Retain it only for [`UploadSubmitOutcome::New`]; release it for `Updated`,
    /// `Ignored`, and `Closed`. Existing entry pin
    /// ownership is unchanged by `Updated`/`Ignored` and restored if `Closed`
    /// rolls an update back.
    pub fn submit_with_payload(
        &self,
        ino: Ino,
        offset: u64,
        object_file_offset: u64,
        object_id: u64,
        content_epoch: u64,
        payload_len: u64,
    ) -> UploadSubmitOutcome {
        self.submit_with_payload_and_pending_count(
            ino,
            offset,
            object_file_offset,
            object_id,
            content_epoch,
            payload_len,
        )
        .0
    }

    /// Returns the outcome and global pending-object count across all inodes.
    /// The count is sampled under the queue lock after submission or `Closed`
    /// rollback, so rejected candidates do not inflate the observed depth.
    pub(crate) fn submit_with_payload_and_pending_count(
        &self,
        ino: Ino,
        offset: u64,
        object_file_offset: u64,
        object_id: u64,
        content_epoch: u64,
        payload_len: u64,
    ) -> (UploadSubmitOutcome, usize) {
        let op = PendingUpload {
            ino,
            offset,
            object_file_offset,
            object_id,
            payload_len,
            content_epoch,
            created_unix_ms: now_unix_ms(),
            not_before_unix_ms: 0,
            retry_count: 0,
        };

        let mut state = self.pending_state.lock();
        let mut became_idle = false;
        let outcome = match Self::record_pending_or_update(&mut state, &op) {
            PendingSubmit::New if self.dispatch_wakeup_locked(&mut state) => {
                UploadSubmitOutcome::New
            }
            PendingSubmit::New => {
                #[cfg(test)]
                self.pause_closed_dispatch_for_test(&op);
                (_, became_idle) = Self::remove_pending_if_matches_locked(&mut state, &op);
                UploadSubmitOutcome::Closed
            }
            PendingSubmit::Updated(_) if self.dispatch_wakeup_locked(&mut state) => {
                UploadSubmitOutcome::Updated
            }
            PendingSubmit::Updated(previous) => {
                #[cfg(test)]
                self.pause_closed_dispatch_for_test(&op);
                Self::restore_pending_if_current_matches_locked(&mut state, &op, previous);
                UploadSubmitOutcome::Closed
            }
            PendingSubmit::Ignored => {
                debug!(
                    object_id,
                    ino, offset, content_epoch, "upload submit ignored: object already pending"
                );
                UploadSubmitOutcome::Ignored
            }
        };
        let pending_count = state.by_object.len();
        drop(state);
        if became_idle {
            self.idle_notify.notify_waiters();
        }
        (outcome, pending_count)
    }

    /// Mark an upload as complete.
    ///
    /// Returns `true` when the pending entry matched and was removed; `false`
    /// means the op was already terminal or stale.
    pub fn complete(&self, op: &PendingUpload) -> bool {
        self.remove_pending_if_matches(op)
    }

    /// Requeue a failed upload for retry (does not increment pending count).
    ///
    /// Returns `true` when the pending entry matched, was updated, and was
    /// sent for retry; `false` means the op was already terminal, stale, or
    /// the worker channel is closed.
    pub fn requeue(&self, op: PendingUpload) -> bool {
        let mut state = self.pending_state.lock();
        let Some(previous) = Self::update_pending_if_matches_locked(&mut state, &op) else {
            return false;
        };
        if !self.dispatch_wakeup_locked(&mut state) {
            #[cfg(test)]
            self.pause_closed_dispatch_for_test(&op);
            Self::restore_pending_if_current_matches_locked(&mut state, &op, previous);
            return false;
        }
        true
    }

    /// Requeue an upload after metadata rebased it onto a different content epoch.
    ///
    /// The `current` op must still match the live pending entry. `rebased` is
    /// then sent to workers and becomes the new pending state.
    pub fn requeue_rebased(&self, current: &PendingUpload, rebased: PendingUpload) -> bool {
        let mut state = self.pending_state.lock();
        let Some(previous) =
            Self::update_pending_if_matches_to_locked(&mut state, current, &rebased)
        else {
            return false;
        };
        if !self.dispatch_wakeup_locked(&mut state) {
            #[cfg(test)]
            self.pause_closed_dispatch_for_test(&rebased);
            Self::restore_pending_if_current_matches_locked(&mut state, &rebased, previous);
            return false;
        }
        true
    }

    /// Cancel a pending upload and decrement pending count.
    ///
    /// Returns `true` when the pending entry matched and was removed; `false`
    /// means the op was already terminal or stale.
    pub fn cancel(&self, op: &PendingUpload) -> bool {
        self.remove_pending_if_matches(op)
    }

    /// Wait until all currently pending uploads complete or `timeout` elapses.
    pub async fn wait_for_idle(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let idle_notified = self.idle_notify.notified();
            tokio::pin!(idle_notified);
            idle_notified.as_mut().enable();
            let shutdown_notified = self.shutdown_notify.notified();
            tokio::pin!(shutdown_notified);
            shutdown_notified.as_mut().enable();

            if self.shutdown_requested.load(Ordering::Acquire) {
                return false;
            }
            // Register interest before checking the state. This prevents a
            // TOCTOU race where completion notifies between the check and wait.
            if self.pending_state.lock().by_object.is_empty() {
                return true;
            }
            #[cfg(test)]
            let wait_for_idle_hook = self.wait_for_idle_hook.lock().clone();
            #[cfg(test)]
            if let Some(hook) = wait_for_idle_hook {
                hook.entered.notify_one();
                hook.release.notified().await;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            tokio::select! {
                _ = &mut shutdown_notified => return false,
                result = tokio::time::timeout(remaining, &mut idle_notified) => {
                    if result.is_err() {
                        return self.pending_state.lock().by_object.is_empty();
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn set_wait_for_idle_hook(&self, hook: Arc<WaitForIdleHook>) {
        *self.wait_for_idle_hook.lock() = Some(hook);
    }

    /// Current number of pending uploads.
    pub fn pending_count(&self) -> usize {
        self.pending_state.lock().by_object.len()
    }

    /// Claims at most `max_items` due uploads, or canceled uploads regardless of deadline.
    /// Unclaimed operations remain authoritative and do not require another wakeup.
    /// `canceled_inodes` must be insert-only; it is re-read only when it grows.
    pub fn claim_pending(
        &self,
        max_items: usize,
        now_unix_ms: u64,
        initial_delay_ms: u64,
        canceled_inodes: &HashSet<Ino>,
    ) -> Vec<PendingUpload> {
        let mut state = self.pending_state.lock();
        let mut claimed = Vec::with_capacity(max_items.min(state.by_object.len()));
        if max_items == 0 {
            return claimed;
        }
        // Sync only new tombstones so steady-state claims cost O(claimed), not O(backlog).
        if canceled_inodes.len() > state.canceled_inodes.len() {
            for &ino in canceled_inodes {
                if state.canceled_inodes.insert(ino) && state.dirty_by_inode.contains_key(&ino) {
                    state.canceled_dirty.insert(ino);
                }
            }
        }
        while claimed.len() < max_items {
            let Some(&ino) = state.canceled_dirty.first() else {
                break;
            };
            let page: Vec<_> = state.dirty_by_inode[&ino]
                .iter()
                .take(max_items - claimed.len())
                .map(|object_id| state.by_object[object_id].clone())
                .collect();
            for op in &page {
                Self::unmark_dirty(&mut state, op);
            }
            claimed.extend(page);
        }
        if claimed.len() == max_items {
            return claimed;
        }
        Self::promote_due(&mut state, now_unix_ms, initial_delay_ms);
        while claimed.len() < max_items {
            let next = state
                .claim_inode_cursor
                .and_then(|(cursor, end)| {
                    state
                        .due_by_inode
                        .range((Excluded(cursor), Included(end)))
                        .next()
                        .map(|(&ino, _)| (ino, end))
                })
                .or_else(|| {
                    Some((
                        *state.due_by_inode.first_key_value()?.0,
                        *state.due_by_inode.last_key_value()?.0,
                    ))
                });
            let Some((ino, inode_end)) = next else { break };
            let objects = &state.due_by_inode[&ino];
            let next = state
                .claim_object_cursors
                .get(&ino)
                .and_then(|&(cursor, end)| {
                    objects
                        .range((Excluded(cursor), Included(end)))
                        .next()
                        .map(|&id| (id, end))
                })
                .or_else(|| Some((*objects.first()?, *objects.last()?)));
            let Some((object_id, object_end)) = next else {
                break;
            };
            state.claim_inode_cursor = Some((ino, inode_end));
            state
                .claim_object_cursors
                .insert(ino, (object_id, object_end));
            let op = state.by_object[&object_id].clone();
            Self::unmark_dirty(&mut state, &op);
            claimed.push(op);
        }
        claimed
    }

    /// Earliest effective deadline among unclaimed uploads, without a backlog scan.
    /// Returns zero when promoted IDs are already eligible for admission.
    pub fn next_pending_upload_deadline(&self, initial_delay_ms: u64) -> Option<u64> {
        let state = self.pending_state.lock();
        if !state.due_by_inode.is_empty() {
            return Some(0);
        }
        let initial = state
            .deadlines
            .range((false, 0, 0, 0)..=(false, u64::MAX, u64::MAX, u64::MAX))
            .next()
            .map(|(_, created, _, _)| created.saturating_add(initial_delay_ms));
        let retry = state
            .deadlines
            .range((true, 0, 0, 0)..=(true, u64::MAX, u64::MAX, u64::MAX))
            .next()
            .map(|(_, deadline, _, _)| *deadline);
        initial.into_iter().chain(retry).min()
    }

    fn promote_due(state: &mut PendingState, now: u64, initial_delay_ms: u64) {
        for retry in [false, true] {
            loop {
                let next = state
                    .deadlines
                    .range((retry, 0, 0, 0)..=(retry, u64::MAX, u64::MAX, u64::MAX))
                    .next()
                    .copied();
                let Some(key @ (_, timestamp, ino, object_id)) = next else {
                    break;
                };
                let deadline = if retry {
                    timestamp
                } else {
                    timestamp.saturating_add(initial_delay_ms)
                };
                if deadline > now {
                    break;
                }
                state.deadlines.remove(&key);
                state.due_by_inode.entry(ino).or_default().insert(object_id);
            }
        }
    }

    fn deadline_key(op: &PendingUpload) -> (bool, u64, Ino, u64) {
        (
            op.not_before_unix_ms != 0,
            if op.not_before_unix_ms == 0 {
                op.created_unix_ms
            } else {
                op.not_before_unix_ms
            },
            op.ino,
            op.object_id,
        )
    }

    fn mark_dirty(state: &mut PendingState, op: &PendingUpload) {
        state
            .dirty_by_inode
            .entry(op.ino)
            .or_default()
            .insert(op.object_id);
        state.deadlines.insert(Self::deadline_key(op));
        if state.canceled_inodes.contains(&op.ino) {
            state.canceled_dirty.insert(op.ino);
        }
    }

    fn unmark_dirty(state: &mut PendingState, op: &PendingUpload) {
        state.deadlines.remove(&Self::deadline_key(op));
        if let Entry::Occupied(mut entry) = state.dirty_by_inode.entry(op.ino) {
            entry.get_mut().remove(&op.object_id);
            if entry.get().is_empty() {
                entry.remove();
                state.canceled_dirty.remove(&op.ino);
            }
        }
        if let Entry::Occupied(mut entry) = state.due_by_inode.entry(op.ino) {
            entry.get_mut().remove(&op.object_id);
            if entry.get().is_empty() {
                entry.remove();
                state.claim_object_cursors.remove(&op.ino);
            }
        }
        if state.due_by_inode.is_empty() {
            state.claim_inode_cursor = None;
            state.claim_object_cursors.clear();
        }
    }

    /// Returns whether `op` still matches the authoritative pending operation.
    pub fn is_current(&self, op: &PendingUpload) -> bool {
        self.pending_state
            .lock()
            .by_object
            .get(&op.object_id)
            .is_some_and(|current| {
                current.ino == op.ino
                    && current.offset == op.offset
                    && current.content_epoch == op.content_epoch
            })
    }

    /// Return current upload wakeup ingress metrics.
    pub fn ingress_snapshot(&self) -> UploadQueueIngressSnapshot {
        let state = self.pending_state.lock();
        UploadQueueIngressSnapshot {
            depth: self
                .sender
                .max_capacity()
                .saturating_sub(self.sender.capacity()),
            capacity: UPLOAD_WAKEUP_CAPACITY,
            coalesced: state.wakeup_coalesced,
        }
    }

    /// Reset cumulative upload wakeup counters while preserving live depth.
    pub(crate) fn reset_ingress_metrics(&self) {
        self.pending_state.lock().wakeup_coalesced = 0;
    }

    /// Snapshot all currently pending upload ops from the live in-memory index.
    pub fn snapshot_pending(&self) -> Vec<PendingUpload> {
        let state = self.pending_state.lock();
        let mut ops: Vec<_> = state.by_object.values().cloned().collect();
        ops.sort_by_key(|op| (op.created_unix_ms, op.object_id));
        ops
    }

    /// Snapshot currently pending upload ops for one inode.
    pub fn snapshot_pending_for_ino(&self, ino: Ino) -> Vec<PendingUpload> {
        let state = self.pending_state.lock();
        let mut ops: Vec<_> = state
            .by_inode
            .get(&ino)
            .map(|object_ids| {
                object_ids
                    .iter()
                    .filter_map(|object_id| state.by_object.get(object_id).cloned())
                    .collect()
            })
            .unwrap_or_default();
        ops.sort_by_key(|op: &PendingUpload| (op.created_unix_ms, op.object_id));
        ops
    }

    /// O(1) check whether any pending uploads reference the given inode.
    pub fn has_pending_for_ino(&self, ino: Ino) -> bool {
        self.pending_state.lock().by_inode.contains_key(&ino)
    }

    /// Check whether any pending upload for `ino` belongs to a newer content epoch.
    pub fn has_pending_for_ino_newer_than(&self, ino: Ino, content_epoch: u64) -> bool {
        let state = self.pending_state.lock();
        state
            .by_inode
            .get(&ino)
            .into_iter()
            .flat_map(|object_ids| object_ids.iter())
            .filter_map(|object_id| state.by_object.get(object_id))
            .any(|op| op.content_epoch > content_epoch)
    }

    /// Request shutdown of this queue and wake all `wait_for_idle` callers.
    pub fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }

    fn record_pending_or_update(state: &mut PendingState, op: &PendingUpload) -> PendingSubmit {
        if let Some(current) = state.by_object.get_mut(&op.object_id) {
            if current.ino == op.ino
                && current.offset == op.offset
                && op.content_epoch > current.content_epoch
            {
                let previous = std::mem::replace(current, op.clone());
                Self::unmark_dirty(state, &previous);
                Self::mark_dirty(state, op);
                return PendingSubmit::Updated(previous);
            }
            return PendingSubmit::Ignored;
        }

        let ino = op.ino;
        state.by_object.insert(op.object_id, op.clone());
        state.by_inode.entry(ino).or_default().insert(op.object_id);
        Self::mark_dirty(state, op);
        PendingSubmit::New
    }

    fn dispatch_wakeup_locked(&self, state: &mut PendingState) -> bool {
        match self.sender.try_send(()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(())) => {
                state.wakeup_coalesced = state.wakeup_coalesced.saturating_add(1);
                true
            }
            Err(mpsc::error::TrySendError::Closed(())) => false,
        }
    }

    #[cfg(test)]
    fn set_closed_dispatch_hook(&self, hook: ClosedDispatchHook) {
        *self.closed_dispatch_hook.lock() = Some(hook);
    }

    #[cfg(test)]
    fn pause_closed_dispatch_for_test(&self, op: &PendingUpload) {
        let hook = self.closed_dispatch_hook.lock().take();
        if let Some(hook) = hook {
            let _ = hook.entered.send(op.clone());
            let _ = hook.release.recv_timeout(Duration::from_secs(5));
        }
    }

    fn remove_pending_if_matches(&self, op: &PendingUpload) -> bool {
        let mut state = self.pending_state.lock();
        let (removed, became_idle) = Self::remove_pending_if_matches_locked(&mut state, op);
        drop(state);
        if became_idle {
            self.idle_notify.notify_waiters();
        }
        removed
    }

    fn remove_pending_if_matches_locked(
        state: &mut PendingState,
        op: &PendingUpload,
    ) -> (bool, bool) {
        let Entry::Occupied(entry) = state.by_object.entry(op.object_id) else {
            return (false, false);
        };
        let current = entry.get();
        if current.ino != op.ino
            || current.offset != op.offset
            || current.content_epoch != op.content_epoch
        {
            return (false, false);
        }
        let removed = entry.remove();
        Self::remove_inode_index_entry(state, removed.ino, op.object_id);
        Self::unmark_dirty(state, &removed);
        let became_idle = state.by_object.is_empty();
        (true, became_idle)
    }

    fn update_pending_if_matches_locked(
        state: &mut PendingState,
        op: &PendingUpload,
    ) -> Option<PendingUpload> {
        let current = state.by_object.get_mut(&op.object_id)?;
        if current.ino != op.ino
            || current.offset != op.offset
            || current.content_epoch != op.content_epoch
        {
            return None;
        }
        let previous = std::mem::replace(current, op.clone());
        Self::unmark_dirty(state, &previous);
        Self::mark_dirty(state, op);
        Some(previous)
    }

    fn update_pending_if_matches_to_locked(
        state: &mut PendingState,
        expected: &PendingUpload,
        replacement: &PendingUpload,
    ) -> Option<PendingUpload> {
        if replacement.object_id != expected.object_id || replacement.ino != expected.ino {
            return None;
        }

        let current = state.by_object.get_mut(&expected.object_id)?;
        if current.ino != expected.ino
            || current.offset != expected.offset
            || current.content_epoch != expected.content_epoch
        {
            return None;
        }
        let previous = std::mem::replace(current, replacement.clone());
        Self::unmark_dirty(state, &previous);
        Self::mark_dirty(state, replacement);
        Some(previous)
    }

    fn restore_pending_if_current_matches_locked(
        state: &mut PendingState,
        failed_op: &PendingUpload,
        previous: PendingUpload,
    ) {
        let Some(current) = state.by_object.get_mut(&failed_op.object_id) else {
            return;
        };
        if current == failed_op {
            *current = previous.clone();
            Self::unmark_dirty(state, failed_op);
            Self::mark_dirty(state, &previous);
        }
    }

    fn remove_inode_index_entry(state: &mut PendingState, ino: Ino, object_id: u64) {
        if let std::collections::btree_map::Entry::Occupied(mut entry) = state.by_inode.entry(ino) {
            entry.get_mut().remove(&object_id);
            if entry.get().is_empty() {
                entry.remove_entry();
            }
        }
    }
}

pub(crate) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn recv_claimed(queue: &UploadQueue, rx: &mut mpsc::Receiver<()>) -> Vec<PendingUpload> {
        rx.recv().await.expect("upload wakeup");
        queue.claim_pending(usize::MAX, u64::MAX, 0, &HashSet::new())
    }

    async fn recv_one(queue: &UploadQueue, rx: &mut mpsc::Receiver<()>) -> PendingUpload {
        rx.recv().await.expect("upload wakeup");
        queue
            .claim_pending(1, u64::MAX, 0, &HashSet::new())
            .into_iter()
            .next()
            .expect("claimed upload")
    }

    #[tokio::test]
    async fn submit_complete_roundtrip() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        assert_eq!(queue.submit(100, 0, 42, 7), UploadSubmitOutcome::New);
        assert_eq!(queue.pending_count(), 1);

        let op = recv_one(&queue, &mut rx).await;
        assert_eq!(op.object_id, 42);
        assert_eq!(op.ino, 100);
        assert_eq!(op.content_epoch, 7);

        queue.complete(&op);
        assert_eq!(queue.pending_count(), 0);
    }

    #[tokio::test]
    async fn startup_does_not_recover_from_filesystem_artifacts() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let upload_dir = dir.path().join("upload");
        std::fs::create_dir_all(&upload_dir).unwrap();
        std::fs::write(upload_dir.join("abc.pending"), b"legacy").unwrap();

        let (queue, _rx) = UploadQueue::new(dir.path());
        assert_eq!(queue.pending_count(), 0);
        assert!(queue.snapshot_pending().is_empty());
    }

    #[tokio::test]
    async fn duplicate_submit_deduplicates_object_id() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        assert_eq!(queue.submit(1, 0, 77, 1), UploadSubmitOutcome::New);
        assert_eq!(queue.submit(1, 0, 77, 1), UploadSubmitOutcome::Ignored);
        assert_eq!(
            queue.submit(2, 0, 77, 2),
            UploadSubmitOutcome::Ignored,
            "a reused object ID with a different owner identity is ignored"
        );

        assert_eq!(queue.pending_count(), 1);
        let op = recv_one(&queue, &mut rx).await;
        assert_eq!(op.object_id, 77);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn duplicate_submit_updates_pending_upload_to_newer_epoch() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        assert_eq!(queue.submit(1, 1_048_569, 77, 2), UploadSubmitOutcome::New);
        let stale = recv_one(&queue, &mut rx).await;
        assert_eq!(
            queue.submit(1, 1_048_569, 77, 3),
            UploadSubmitOutcome::Updated
        );

        assert_eq!(queue.pending_count(), 1);
        let updated = recv_one(&queue, &mut rx).await;
        assert_eq!(updated.content_epoch, 3);
        assert!(!queue.is_current(&stale));
        assert!(queue.is_current(&updated));
        assert!(
            !queue.cancel(&stale),
            "stale worker op must not cancel newer state"
        );
        assert_eq!(queue.pending_count(), 1);

        let pending = queue.snapshot_pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].object_id, 77);
        assert_eq!(pending[0].content_epoch, 3);
        let pending_for_inode = queue.snapshot_pending_for_ino(1);
        assert_eq!(pending_for_inode.len(), 1);
        assert_eq!(pending_for_inode[0].content_epoch, 3);

        assert!(queue.complete(&updated));
        assert_eq!(queue.pending_count(), 0);
    }

    #[test]
    fn sfs752_submission_reports_count_after_commit_or_rollback() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, rx) = UploadQueue::new(dir.path());
        assert_eq!(
            queue.submit_with_payload_and_pending_count(1, 0, 0, 101, 1, 7),
            (UploadSubmitOutcome::New, 1)
        );
        assert_eq!(
            queue.submit_with_payload_and_pending_count(1, 0, 0, 101, 2, 7),
            (UploadSubmitOutcome::Updated, 1)
        );
        assert_eq!(
            queue.submit_with_payload_and_pending_count(1, 0, 0, 101, 1, 7),
            (UploadSubmitOutcome::Ignored, 1)
        );
        assert_eq!(
            queue.submit_with_payload_and_pending_count(1, 8, 8, 102, 1, 5),
            (UploadSubmitOutcome::New, 2)
        );
        drop(rx);
        assert_eq!(
            queue.submit_with_payload_and_pending_count(1, 16, 16, 103, 1, 9),
            (UploadSubmitOutcome::Closed, 2)
        );
        assert_eq!(
            queue.submit_with_payload_and_pending_count(1, 0, 0, 101, 3, 9),
            (UploadSubmitOutcome::Closed, 2)
        );
        assert_eq!(queue.snapshot_pending()[0].content_epoch, 2);
        assert_eq!(queue.snapshot_pending()[0].payload_len, 7);
    }

    #[tokio::test]
    async fn sfs752_closed_update_restores_previous_pending_upload() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());
        assert_eq!(queue.submit(1, 0, 77, 1), UploadSubmitOutcome::New);
        let previous = recv_one(&queue, &mut rx).await;
        drop(rx);

        assert_eq!(queue.submit(1, 0, 77, 2), UploadSubmitOutcome::Closed);
        assert_eq!(queue.snapshot_pending(), vec![previous]);
    }

    #[tokio::test]
    async fn sfs752_closed_submit_holds_state_lock_through_rollback() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, rx) = UploadQueue::new(dir.path());
        drop(rx);

        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        queue.set_closed_dispatch_hook(ClosedDispatchHook {
            entered: entered_tx,
            release: release_rx,
        });

        let first_queue = Arc::clone(&queue);
        let first = tokio::task::spawn_blocking(move || {
            first_queue.submit_with_payload(1, 0, 0, 77, 1, 10)
        });
        let first_paused = tokio::time::timeout(Duration::from_secs(1), entered_rx.recv())
            .await
            .expect("first closed submit reaches rollback")
            .expect("closed submit hook remains connected");
        assert_eq!(first_paused.content_epoch, 1);
        let rollback_holds_state_lock = queue.pending_state.try_lock().is_none();
        release_tx.send(()).expect("release first closed submit");
        assert_eq!(
            first.await.expect("first submit task"),
            UploadSubmitOutcome::Closed
        );
        assert!(rollback_holds_state_lock, "submit lock must cover rollback");
        assert_eq!(queue.pending_count(), 0);
    }

    #[tokio::test]
    async fn sfs752_failed_requeue_holds_state_lock_through_rollback() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());
        queue.submit(1, 0, 77, 1);
        let mut retry = recv_one(&queue, &mut rx).await;
        drop(rx);
        retry.retry_count = 1;

        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        queue.set_closed_dispatch_hook(ClosedDispatchHook {
            entered: entered_tx,
            release: release_rx,
        });

        let requeue_queue = Arc::clone(&queue);
        let requeue = tokio::task::spawn_blocking(move || requeue_queue.requeue(retry));
        let paused = tokio::time::timeout(Duration::from_secs(1), entered_rx.recv())
            .await
            .expect("closed requeue reaches rollback")
            .expect("closed dispatch hook remains connected");
        assert_eq!(paused.retry_count, 1);
        let rollback_holds_state_lock = queue.pending_state.try_lock().is_none();
        release_tx.send(()).expect("release failed requeue");
        assert!(!requeue.await.expect("requeue task"));
        assert!(
            rollback_holds_state_lock,
            "requeue lock must cover rollback"
        );
        assert_eq!(queue.snapshot_pending()[0].retry_count, 0);
    }

    #[tokio::test]
    async fn sfs752_failed_rebased_requeue_holds_state_lock_and_restores_pending() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());
        queue.submit_with_payload(1, 0, 0, 77, 1, 10);
        let current = recv_one(&queue, &mut rx).await;
        drop(rx);
        let mut rebased = current.clone();
        rebased.offset = 10;
        rebased.object_file_offset = 10;
        rebased.content_epoch = 2;

        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        queue.set_closed_dispatch_hook(ClosedDispatchHook {
            entered: entered_tx,
            release: release_rx,
        });

        let requeue_queue = Arc::clone(&queue);
        let expected = current.clone();
        let requeue =
            tokio::task::spawn_blocking(move || requeue_queue.requeue_rebased(&current, rebased));
        let paused = tokio::time::timeout(Duration::from_secs(1), entered_rx.recv())
            .await
            .expect("closed rebased requeue reaches rollback")
            .expect("closed dispatch hook remains connected");
        assert_eq!(paused.offset, 10);
        let rollback_holds_state_lock = queue.pending_state.try_lock().is_none();
        release_tx.send(()).expect("release failed rebased requeue");
        assert!(!requeue.await.expect("rebased requeue task"));
        assert!(
            rollback_holds_state_lock,
            "rebased requeue lock must cover rollback"
        );
        assert_eq!(queue.snapshot_pending(), vec![expected]);
    }

    #[tokio::test]
    async fn current_upload_ignores_worker_scheduling_state() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(1, 0, 77, 1);
        let mut dispatched = recv_one(&queue, &mut rx).await;
        dispatched.not_before_unix_ms = now_unix_ms().saturating_add(1_000);
        dispatched.retry_count = 1;

        assert!(queue.is_current(&dispatched));
    }

    #[tokio::test]
    async fn snapshot_pending_for_ino_tracks_only_that_inode() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(10, 0, 100, 1);
        queue.submit(11, 0, 101, 1);
        queue.submit(10, 4096, 102, 1);

        let pending = queue.snapshot_pending_for_ino(10);
        assert_eq!(
            pending.iter().map(|op| op.object_id).collect::<Vec<_>>(),
            vec![100, 102]
        );

        let mut claimed = recv_claimed(&queue, &mut rx).await;
        let first = claimed.remove(0);
        assert!(queue.complete(&first));
        assert_eq!(
            queue
                .snapshot_pending_for_ino(10)
                .iter()
                .map(|op| op.object_id)
                .collect::<Vec<_>>(),
            vec![102]
        );
        assert!(!queue.snapshot_pending_for_ino(11).is_empty());
    }

    #[tokio::test]
    async fn duplicate_complete_does_not_undercount_other_pending() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(10, 0, 100, 1);
        queue.submit(11, 0, 101, 1);
        assert_eq!(queue.pending_count(), 2);

        let mut claimed = recv_claimed(&queue, &mut rx).await;
        let first = claimed.remove(0);
        let second = claimed.remove(0);

        queue.complete(&first);
        assert_eq!(queue.pending_count(), 1);

        // Duplicate completion for the same object must be a no-op.
        queue.complete(&first);
        assert_eq!(queue.pending_count(), 1);

        queue.complete(&second);
        assert_eq!(queue.pending_count(), 0);
    }

    #[tokio::test]
    async fn stale_cancel_does_not_remove_newer_epoch_for_same_object() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(10, 0, 100, 1);
        let stale = recv_one(&queue, &mut rx).await;
        queue.cancel(&stale);
        assert_eq!(queue.pending_count(), 0);

        queue.submit(10, 0, 100, 2);
        queue.cancel(&stale);

        assert_eq!(queue.pending_count(), 1);
        let pending = queue.snapshot_pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].object_id, 100);
        assert_eq!(pending[0].content_epoch, 2);
    }

    #[tokio::test]
    async fn requeue_updates_pending_snapshot_retry_state() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(10, 0, 100, 1);
        let mut retry = recv_one(&queue, &mut rx).await;
        retry.retry_count = 2;
        retry.not_before_unix_ms = now_unix_ms().saturating_add(1_000);

        assert!(queue.requeue(retry.clone()));

        let pending = queue.snapshot_pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].object_id, 100);
        assert_eq!(pending[0].retry_count, 2);
        assert_eq!(pending[0].not_before_unix_ms, retry.not_before_unix_ms);
    }

    #[tokio::test]
    async fn requeue_rebased_allows_offset_change_for_same_object() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit_with_payload(10, 0, 0, 100, 1, 4096);
        let current = recv_one(&queue, &mut rx).await;
        let mut rebased = current.clone();
        rebased.offset = 4096;
        rebased.object_file_offset = 4096;
        rebased.content_epoch = 2;

        assert!(queue.requeue_rebased(&current, rebased.clone()));
        let queued = recv_one(&queue, &mut rx).await;
        assert_eq!(queued.offset, 4096);
        assert_eq!(queued.object_file_offset, 4096);
        assert_eq!(queued.content_epoch, 2);

        let pending = queue.snapshot_pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].offset, 4096);
        assert_eq!(pending[0].content_epoch, 2);
        assert!(!queue.complete(&current));
        assert!(queue.complete(&rebased));
    }

    #[tokio::test]
    async fn failed_requeue_does_not_mutate_pending_snapshot() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(10, 0, 100, 1);
        let mut retry = recv_one(&queue, &mut rx).await;
        drop(rx);
        retry.retry_count = 2;
        retry.not_before_unix_ms = now_unix_ms().saturating_add(1_000);

        assert!(!queue.requeue(retry));

        let pending = queue.snapshot_pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].object_id, 100);
        assert_eq!(pending[0].retry_count, 0);
        assert_eq!(pending[0].not_before_unix_ms, 0);
    }

    #[tokio::test]
    async fn snapshot_pending() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, _rx) = UploadQueue::new(dir.path());

        queue.submit(1, 0, 100, 1);
        queue.submit(2, 0, 200, 2);

        let snap = queue.snapshot_pending();
        assert_eq!(snap.len(), 2);
    }

    #[tokio::test]
    async fn wait_for_idle() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, _rx) = UploadQueue::new(dir.path());

        // No pending ops — should return immediately.
        assert!(queue.wait_for_idle(Duration::from_millis(10)).await);
    }

    #[tokio::test]
    async fn wait_for_idle_wakes_after_last_completion() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(1, 0, 100, 1);
        let op = recv_one(&queue, &mut rx).await;
        let hook = Arc::new(WaitForIdleHook::default());
        queue.set_wait_for_idle_hook(Arc::clone(&hook));
        let waiter = tokio::spawn({
            let queue = Arc::clone(&queue);
            async move { queue.wait_for_idle(Duration::from_secs(60)).await }
        });

        hook.entered.notified().await;
        assert!(queue.complete(&op));
        hook.release.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), waiter)
                .await
                .is_ok(),
            "completion notification must wake idle waiter promptly"
        );
    }

    #[tokio::test]
    async fn upload_burst_keeps_count_and_inode_index_consistent() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());
        const BURST: u64 = 1024;

        for object_id in 0..BURST {
            queue.submit(7, object_id * 4096, object_id + 1, 1);
        }

        assert_eq!(queue.pending_count(), BURST as usize);
        assert_eq!(queue.snapshot_pending().len(), BURST as usize);
        assert_eq!(queue.snapshot_pending_for_ino(7).len(), BURST as usize);
        assert!(queue.has_pending_for_ino(7));

        for op in recv_claimed(&queue, &mut rx).await {
            assert!(queue.complete(&op));
        }

        assert_eq!(queue.pending_count(), 0);
        assert!(queue.snapshot_pending().is_empty());
        assert!(queue.snapshot_pending_for_ino(7).is_empty());
        assert!(!queue.has_pending_for_ino(7));
    }

    #[tokio::test]
    async fn has_pending_for_ino_tracks_inserts_and_removals() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        assert!(!queue.has_pending_for_ino(1));
        assert!(!queue.has_pending_for_ino(2));

        // Submit two payload objects for ino 1 and one for ino 2.
        queue.submit(1, 0, 100, 1);
        queue.submit(1, 4096, 101, 1);
        queue.submit(2, 0, 200, 1);

        assert!(queue.has_pending_for_ino(1));
        assert!(queue.has_pending_for_ino(2));
        assert!(!queue.has_pending_for_ino(3));

        // Complete one of the two payload objects for ino 1.
        let mut claimed = recv_claimed(&queue, &mut rx).await;
        let op = claimed.remove(0);
        assert_eq!(op.ino, 1);
        queue.complete(&op);

        // ino 1 still has the second payload object pending.
        assert!(queue.has_pending_for_ino(1));

        // Complete the remaining authoritative ops.
        for next in claimed {
            queue.complete(&next);
        }

        assert!(!queue.has_pending_for_ino(1));
        assert!(!queue.has_pending_for_ino(2));
    }

    #[tokio::test]
    async fn wakeup_burst_coalesces_without_losing_authoritative_pending_state() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        for object_id in 1..=4 {
            queue.submit(7, object_id * 4096, object_id, 1);
        }

        let ingress = queue.ingress_snapshot();
        assert_eq!(ingress.capacity, 1);
        assert_eq!(ingress.depth, 1);
        assert_eq!(ingress.coalesced, 3);
        assert_eq!(queue.pending_count(), 4);

        rx.recv().await.expect("coalesced wakeup");
        let claimed = queue.claim_pending(usize::MAX, u64::MAX, 0, &HashSet::new());
        assert_eq!(claimed.len(), 4);
    }

    #[tokio::test]
    async fn due_claim_preserves_initial_delay_retry_deadlines_and_overflow() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, _rx) = UploadQueue::new(dir.path());
        for object_id in 1..=3 {
            queue.submit(7, 0, object_id, 1);
        }
        for mut op in queue.snapshot_pending() {
            op.created_unix_ms = 100;
            op.not_before_unix_ms = match op.object_id {
                1 => 0,
                2 => 150,
                _ => u64::MAX,
            };
            assert!(queue.requeue(op));
        }
        let canceled = HashSet::new();
        assert_eq!(queue.next_pending_upload_deadline(100), Some(150));
        assert!(queue.claim_pending(8, 149, 100, &canceled).is_empty());
        assert_eq!(queue.claim_pending(8, 150, 100, &canceled)[0].object_id, 2);
        assert_eq!(queue.next_pending_upload_deadline(100), Some(200));
        assert!(queue.claim_pending(8, 199, 100, &canceled).is_empty());
        assert_eq!(queue.claim_pending(8, 200, 100, &canceled)[0].object_id, 1);
        assert_eq!(queue.next_pending_upload_deadline(100), Some(u64::MAX));
        assert_eq!(
            queue.pending_count(),
            3,
            "claiming retains authoritative owners"
        );
        assert_eq!(
            queue.claim_pending(8, u64::MAX, 100, &canceled)[0].object_id,
            3
        );
        assert_eq!(queue.next_pending_upload_deadline(100), None);

        queue.submit(8, 0, 4, 1);
        assert!(
            queue
                .claim_pending(1, u64::MAX - 1, u64::MAX, &canceled)
                .is_empty()
        );
        assert_eq!(queue.next_pending_upload_deadline(u64::MAX), Some(u64::MAX));
        assert_eq!(
            queue.claim_pending(1, u64::MAX, u64::MAX, &canceled)[0].object_id,
            4
        );
    }

    #[tokio::test]
    async fn bounded_claim_rotates_inodes_and_preserves_each_object_sweep() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, _rx) = UploadQueue::new(dir.path());
        for object_id in 1..=1005 {
            queue.submit(if object_id <= 1000 { 7 } else { 8 }, 0, object_id, 1);
        }
        let first = queue.claim_pending(4, u64::MAX, 0, &HashSet::new());
        assert_eq!(
            first.iter().map(|op| op.object_id).collect::<Vec<_>>(),
            vec![1, 1001, 2, 1002]
        );
        assert!(queue.requeue(first[0].clone()));
        assert!(queue.requeue(first[1].clone()));
        queue.submit(9, 0, 2001, 1);
        let next = queue.claim_pending(3, u64::MAX, 0, &HashSet::new());
        assert_eq!(
            next.iter().map(|op| op.object_id).collect::<Vec<_>>(),
            vec![3, 1003, 2001]
        );
    }

    #[tokio::test]
    async fn bounded_inode_sweep_wraps_despite_continuously_higher_arrivals() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, _rx) = UploadQueue::new(dir.path());
        queue.submit(7, 0, 1, 1);
        queue.submit(8, 0, 2, 1);
        let first = queue
            .claim_pending(1, u64::MAX, 0, &HashSet::new())
            .pop()
            .unwrap();
        assert!(queue.requeue(first));
        queue.submit(9, 0, 3, 1);
        assert_eq!(
            queue.claim_pending(1, u64::MAX, 0, &HashSet::new())[0].ino,
            8
        );
        queue.submit(10, 0, 4, 1);
        assert_eq!(
            queue.claim_pending(1, u64::MAX, 0, &HashSet::new())[0].ino,
            7
        );
    }

    #[tokio::test]
    async fn future_cancellation_is_bounded_and_empty_claim_resets_cursors() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, _rx) = UploadQueue::new(dir.path());
        for object_id in 1..=40 {
            queue.submit(7, 0, object_id, 1);
        }
        for mut op in queue.snapshot_pending() {
            op.not_before_unix_ms = u64::MAX;
            assert!(queue.requeue(op));
        }
        assert!(queue.claim_pending(8, 100, 0, &HashSet::new()).is_empty());
        let canceled = HashSet::from([7]);
        let page = queue.claim_pending(8, 100, 0, &canceled);
        assert_eq!(page.len(), 8);
        assert_eq!(queue.next_pending_upload_deadline(0), Some(u64::MAX));
        for op in page {
            assert!(queue.cancel(&op));
        }
        let rest = queue.claim_pending(usize::MAX, 100, 0, &canceled);
        assert_eq!(rest.len(), 32);
        for op in rest {
            assert!(queue.cancel(&op));
        }
        assert!(queue.pending_state.lock().claim_inode_cursor.is_none());
        assert!(queue.pending_state.lock().claim_object_cursors.is_empty());
        assert_eq!(queue.next_pending_upload_deadline(0), None);
    }

    #[tokio::test]
    async fn cancellation_index_tracks_only_tombstoned_inodes_with_unclaimed_work() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, _rx) = UploadQueue::new(dir.path());
        for ino in 1..=64 {
            queue.submit(ino, 0, ino, 1);
        }
        // Tombstones for inodes without pending work must not be revisited by later claims.
        let mut canceled: HashSet<Ino> = (1_000..2_000).collect();
        assert_eq!(queue.claim_pending(1, u64::MAX, 0, &canceled)[0].ino, 1);
        assert!(queue.pending_state.lock().canceled_dirty.is_empty());

        // New future work on an already-mirrored tombstone is canceled before it is due.
        queue.submit(1_500, 0, 100, 1);
        let mut op = queue
            .snapshot_pending()
            .into_iter()
            .find(|op| op.ino == 1_500)
            .unwrap();
        op.not_before_unix_ms = u64::MAX;
        assert!(queue.requeue(op));
        assert_eq!(queue.claim_pending(1, 0, 0, &canceled)[0].ino, 1_500);
        assert!(queue.pending_state.lock().canceled_dirty.is_empty());

        // A grown tombstone set is merged and its pending work is claimed first.
        canceled.insert(64);
        assert_eq!(queue.claim_pending(1, 0, 0, &canceled)[0].ino, 64);
        assert!(queue.pending_state.lock().canceled_dirty.is_empty());
    }

    #[tokio::test]
    async fn closed_requeue_restores_deadline_index_for_the_previous_owner() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, rx) = UploadQueue::new(dir.path());
        queue.submit(7, 0, 1, 1);
        let mut op = queue.snapshot_pending().pop().unwrap();
        op.not_before_unix_ms = 1000;
        assert!(queue.requeue(op.clone()));
        drop(rx);
        let mut failed = op.clone();
        failed.not_before_unix_ms = 2000;
        assert!(!queue.requeue(failed));
        assert_eq!(queue.next_pending_upload_deadline(0), Some(1000));
        assert!(queue.claim_pending(1, 999, 0, &HashSet::new()).is_empty());
        assert_eq!(queue.claim_pending(1, 1000, 0, &HashSet::new()), vec![op]);
    }

    #[tokio::test]
    async fn bounded_claim_preserves_the_next_page_and_zero_limit() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        for object_id in 1..=4 {
            queue.submit(7, object_id * 4096, object_id, 1);
        }
        rx.recv().await.expect("coalesced wakeup");

        assert!(
            queue
                .claim_pending(0, u64::MAX, 0, &HashSet::new())
                .is_empty()
        );
        let first_page = queue.claim_pending(2, u64::MAX, 0, &HashSet::new());
        assert_eq!(
            first_page.iter().map(|op| op.object_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let second_page = queue.claim_pending(2, u64::MAX, 0, &HashSet::new());
        assert_eq!(
            second_page
                .iter()
                .map(|op| op.object_id)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert!(
            queue
                .claim_pending(1, u64::MAX, 0, &HashSet::new())
                .is_empty()
        );
        for op in first_page.iter().chain(&second_page) {
            assert!(queue.complete(op));
        }
        assert_eq!(queue.pending_count(), 0);
    }

    #[tokio::test]
    async fn bounded_claim_advances_past_requeued_low_ids() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        for object_id in 1..=4 {
            queue.submit(7, object_id * 4096, object_id, 1);
        }
        rx.recv().await.expect("coalesced wakeup");

        let first_page = queue.claim_pending(2, u64::MAX, 0, &HashSet::new());
        assert_eq!(
            first_page.iter().map(|op| op.object_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        for op in &first_page {
            assert!(queue.requeue(op.clone()));
        }

        let second_page = queue.claim_pending(2, u64::MAX, 0, &HashSet::new());
        assert_eq!(
            second_page
                .iter()
                .map(|op| op.object_id)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        let wrapped_page = queue.claim_pending(2, u64::MAX, 0, &HashSet::new());
        assert_eq!(
            wrapped_page
                .iter()
                .map(|op| op.object_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        for op in second_page.iter().chain(&wrapped_page) {
            assert!(queue.complete(op));
        }
        assert_eq!(queue.pending_count(), 0);
    }

    #[tokio::test]
    async fn bounded_claim_finishes_frozen_sweep_before_newer_ids() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(7, 0, 1, 1);
        queue.submit(7, 4096, 2, 1);
        rx.recv().await.expect("initial wakeup");

        let first = queue
            .claim_pending(1, u64::MAX, 0, &HashSet::new())
            .pop()
            .expect("first item");
        assert_eq!(first.object_id, 1);
        assert!(queue.requeue(first));

        queue.submit(7, 8192, 3, 1);
        let second = queue
            .claim_pending(1, u64::MAX, 0, &HashSet::new())
            .pop()
            .expect("second item");
        assert_eq!(second.object_id, 2);

        queue.submit(7, 12_288, 4, 1);
        let wrapped = queue
            .claim_pending(1, u64::MAX, 0, &HashSet::new())
            .pop()
            .expect("wrapped item");
        assert_eq!(wrapped.object_id, 1);

        for op in queue.snapshot_pending() {
            assert!(queue.complete(&op));
        }
        assert_eq!(queue.pending_count(), 0);
    }

    #[tokio::test]
    async fn bounded_claim_returns_authoritative_updates_and_retries_and_skips_deletions() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(7, 0, 41, 1);
        queue.submit(7, 8, 42, 1);
        queue.submit(7, 16, 43, 1);
        rx.recv().await.expect("initial wakeup");

        let first = queue
            .claim_pending(1, u64::MAX, 0, &HashSet::new())
            .pop()
            .expect("first page item");
        assert_eq!(first.object_id, 41);
        let deleted = queue
            .snapshot_pending()
            .into_iter()
            .find(|op| op.object_id == 43)
            .expect("pending operation to cancel");
        assert!(queue.cancel(&deleted));
        queue.submit(7, 8, 42, 2);

        let updated = queue.claim_pending(2, u64::MAX, 0, &HashSet::new());
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].object_id, 42);
        assert_eq!(updated[0].content_epoch, 2);

        let mut retry = updated[0].clone();
        retry.retry_count = 3;
        let retry_not_before_unix_ms = now_unix_ms().saturating_add(1_000);
        retry.not_before_unix_ms = retry_not_before_unix_ms;
        assert!(queue.requeue(retry));
        rx.recv().await.expect("retry wakeup");
        let retried = queue
            .claim_pending(1, u64::MAX, 0, &HashSet::new())
            .pop()
            .expect("retried upload");
        assert_eq!(retried.object_id, 42);
        assert_eq!(retried.retry_count, 3);
        assert_eq!(retried.not_before_unix_ms, retry_not_before_unix_ms);
        assert!(queue.complete(&first));
        assert!(queue.complete(&retried));
        assert_eq!(queue.pending_count(), 0);
    }

    #[tokio::test]
    async fn wakeup_claim_returns_newest_updated_upload_once() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(7, 0, 41, 1);
        rx.recv().await.expect("initial wakeup");
        let first = queue
            .claim_pending(usize::MAX, u64::MAX, 0, &HashSet::new())
            .pop()
            .expect("initial claim");

        queue.submit(7, 0, 41, 2);
        rx.recv().await.expect("updated wakeup");
        let updated = queue
            .claim_pending(usize::MAX, u64::MAX, 0, &HashSet::new())
            .pop()
            .expect("updated claim");

        assert_eq!(first.content_epoch, 1);
        assert_eq!(updated.content_epoch, 2);
        assert!(
            queue
                .claim_pending(usize::MAX, u64::MAX, 0, &HashSet::new())
                .is_empty()
        );
    }

    #[tokio::test]
    async fn wakeup_claim_only_returns_objects_changed_since_last_claim() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(7, 0, 41, 1);
        queue.submit(7, 8, 42, 1);
        queue.submit(7, 16, 43, 1);
        rx.recv().await.expect("initial wakeup");

        assert_eq!(
            queue
                .claim_pending(usize::MAX, u64::MAX, 0, &HashSet::new())
                .len(),
            3
        );

        queue.submit(7, 8, 42, 2);
        rx.recv().await.expect("updated wakeup");
        let updated = queue.claim_pending(usize::MAX, u64::MAX, 0, &HashSet::new());

        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].object_id, 42);
        assert_eq!(updated[0].content_epoch, 2);
    }
}
