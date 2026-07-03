//! Upload Queue — in-memory scheduler for pending S3 uploads.
//!
//! The local backend persists upload intent in lifecycle flags (`DIRTY` /
//! transient `UPLOADING`). The queue itself is intentionally in-memory:
//! startup replay is reconstructed from the local backend + metadata mapping.

use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, mpsc};
use tracing::debug;

/// Inode number (re-exported convenience).
pub type Ino = u64;

/// A pending upload operation.
#[derive(Debug, Clone)]
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
    sender: mpsc::UnboundedSender<PendingUpload>,
    pending_count: AtomicUsize,
    pending_state: Mutex<PendingState>,
    idle_notify: Arc<Notify>,
    shutdown_requested: AtomicBool,
    shutdown_notify: Notify,
}

/// Atomically-consistent index of in-flight uploads.
///
/// Both maps are guarded by the same mutex so that `has_pending_for_ino`
/// never observes an inconsistent state where a payload object exists in the
/// object-id index but the per-inode index has not yet been updated.
struct PendingState {
    by_object: BTreeMap<u64, PendingUpload>,
    by_inode: BTreeMap<Ino, BTreeSet<u64>>,
}

enum PendingSubmit {
    New,
    Updated(PendingUpload),
    Ignored,
}

impl UploadQueue {
    /// Create a new upload queue.
    ///
    /// Returns `(queue, receiver)`. The receiver yields [`PendingUpload`]
    /// items for the VFS upload worker.
    ///
    /// `cache_dir` is accepted for API compatibility with older call sites,
    /// but queue state is no longer persisted on disk.
    pub fn new(
        _cache_dir: impl AsRef<Path>,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<PendingUpload>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let idle_notify = Arc::new(Notify::new());

        let queue = Arc::new(Self {
            sender: tx,
            pending_count: AtomicUsize::new(0),
            pending_state: Mutex::new(PendingState {
                by_object: BTreeMap::new(),
                by_inode: BTreeMap::new(),
            }),
            idle_notify,
            shutdown_requested: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
        });

        (queue, rx)
    }

    /// Submit a new pending upload (hot path — non-blocking).
    pub fn submit(&self, ino: Ino, offset: u64, object_id: u64, content_epoch: u64) {
        self.submit_with_payload(ino, offset, offset, object_id, content_epoch, 0);
    }

    /// Submit a pending upload with explicit uploaded-payload geometry.
    pub fn submit_with_payload(
        &self,
        ino: Ino,
        offset: u64,
        object_file_offset: u64,
        object_id: u64,
        content_epoch: u64,
        payload_len: u64,
    ) {
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

        match self.record_pending_or_update(op.clone()) {
            PendingSubmit::New => {
                self.pending_count.fetch_add(1, Ordering::Release);
                if self.sender.send(op).is_err() && self.remove_pending(object_id) {
                    self.mark_done();
                }
            }
            PendingSubmit::Updated(previous) => {
                if self.sender.send(op.clone()).is_err() {
                    self.restore_pending_if_current_matches(&op, previous);
                }
            }
            PendingSubmit::Ignored => {
                debug!(
                    object_id,
                    ino, offset, content_epoch, "upload submit ignored: object already pending"
                );
            }
        }
    }

    /// Mark an upload as complete.
    ///
    /// Returns `true` when the pending entry matched and was removed; `false`
    /// means the op was already terminal or stale.
    pub fn complete(&self, op: &PendingUpload) -> bool {
        if self.remove_pending_if_matches(op) {
            self.mark_done();
            true
        } else {
            false
        }
    }

    /// Requeue a failed upload for retry (does not increment pending count).
    ///
    /// Returns `true` when the pending entry matched, was updated, and was
    /// sent for retry; `false` means the op was already terminal, stale, or
    /// the worker channel is closed.
    pub fn requeue(&self, op: PendingUpload) -> bool {
        let Some(previous) = self.update_pending_if_matches(&op) else {
            return false;
        };
        if self.sender.send(op.clone()).is_ok() {
            true
        } else {
            self.restore_pending_if_current_matches(&op, previous);
            false
        }
    }

    /// Cancel a pending upload and decrement pending count.
    ///
    /// Returns `true` when the pending entry matched and was removed; `false`
    /// means the op was already terminal or stale.
    pub fn cancel(&self, op: &PendingUpload) -> bool {
        if self.remove_pending_if_matches(op) {
            self.mark_done();
            true
        } else {
            false
        }
    }

    /// Kept for API compatibility with older journal-backed behavior.
    pub async fn flush_journal(&self) {}

    /// Wait until all currently pending uploads complete or `timeout` elapses.
    pub async fn wait_for_idle(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.shutdown_requested.load(Ordering::Acquire) {
                return false;
            }
            // Register interest BEFORE checking the count. This prevents a
            // TOCTOU race where mark_done() fires notify_waiters() between our
            // load() and .notified().await.
            if self.pending_count.load(Ordering::Acquire) == 0 {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let notified = self.idle_notify.notified();
            tokio::select! {
                _ = self.shutdown_notify.notified() => return false,
                result = tokio::time::timeout(remaining, notified) => {
                    if result.is_err() {
                        return self.pending_count.load(Ordering::Acquire) == 0;
                    }
                }
            }
        }
    }

    /// Current number of pending uploads.
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Relaxed)
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

    /// O(1) check whether a specific object id is currently pending upload.
    pub fn has_pending_object(&self, object_id: u64) -> bool {
        self.pending_state.lock().by_object.contains_key(&object_id)
    }

    /// Request shutdown of this queue and wake all `wait_for_idle` callers.
    pub fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }

    fn mark_done(&self) {
        // Saturating decrement — guards against double-complete races.
        let result = self
            .pending_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                if v > 0 { Some(v - 1) } else { None }
            });
        if result == Ok(1) {
            // Was 1, now 0 — notify idle waiters.
            self.idle_notify.notify_waiters();
        }
    }

    fn record_pending_or_update(&self, op: PendingUpload) -> PendingSubmit {
        let mut state = self.pending_state.lock();
        if let Some(current) = state.by_object.get_mut(&op.object_id) {
            if current.ino == op.ino
                && current.offset == op.offset
                && op.content_epoch > current.content_epoch
            {
                let previous = current.clone();
                *current = op.clone();
                Self::remove_inode_index_entry(&mut state, previous.ino, previous.object_id);
                state
                    .by_inode
                    .entry(op.ino)
                    .or_default()
                    .insert(op.object_id);
                return PendingSubmit::Updated(previous);
            }
            return PendingSubmit::Ignored;
        }

        let ino = op.ino;
        state.by_object.insert(op.object_id, op.clone());
        state.by_inode.entry(ino).or_default().insert(op.object_id);
        PendingSubmit::New
    }

    fn remove_pending(&self, object_id: u64) -> bool {
        let mut state = self.pending_state.lock();
        if let Some(removed) = state.by_object.remove(&object_id) {
            Self::remove_inode_index_entry(&mut state, removed.ino, object_id);
            return true;
        }
        false
    }

    fn remove_pending_if_matches(&self, op: &PendingUpload) -> bool {
        let mut state = self.pending_state.lock();
        let Some(current) = state.by_object.get(&op.object_id) else {
            return false;
        };
        if current.ino != op.ino
            || current.offset != op.offset
            || current.content_epoch != op.content_epoch
        {
            return false;
        }
        if let Some(removed) = state.by_object.remove(&op.object_id) {
            Self::remove_inode_index_entry(&mut state, removed.ino, op.object_id);
            return true;
        }
        false
    }

    fn update_pending_if_matches(&self, op: &PendingUpload) -> Option<PendingUpload> {
        let mut state = self.pending_state.lock();
        let current = state.by_object.get_mut(&op.object_id)?;
        if current.ino != op.ino
            || current.offset != op.offset
            || current.content_epoch != op.content_epoch
        {
            return None;
        }
        let previous = current.clone();
        *current = op.clone();
        Self::remove_inode_index_entry(&mut state, previous.ino, previous.object_id);
        state
            .by_inode
            .entry(op.ino)
            .or_default()
            .insert(op.object_id);
        Some(previous)
    }

    fn restore_pending_if_current_matches(
        &self,
        failed_op: &PendingUpload,
        previous: PendingUpload,
    ) {
        let mut state = self.pending_state.lock();
        let Some(current) = state.by_object.get_mut(&failed_op.object_id) else {
            return;
        };
        if current.ino == failed_op.ino
            && current.offset == failed_op.offset
            && current.content_epoch == failed_op.content_epoch
            && current.retry_count == failed_op.retry_count
            && current.not_before_unix_ms == failed_op.not_before_unix_ms
        {
            *current = previous.clone();
            Self::remove_inode_index_entry(&mut state, failed_op.ino, failed_op.object_id);
            state
                .by_inode
                .entry(previous.ino)
                .or_default()
                .insert(previous.object_id);
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

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn submit_complete_roundtrip() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(100, 0, 42, 7);
        assert_eq!(queue.pending_count(), 1);

        let op = rx.recv().await.unwrap();
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

        queue.submit(1, 0, 77, 1);
        queue.submit(1, 0, 77, 1);

        assert_eq!(queue.pending_count(), 1);
        let op = rx.recv().await.unwrap();
        assert_eq!(op.object_id, 77);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn duplicate_submit_updates_pending_upload_to_newer_epoch() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(1, 1_048_569, 77, 2);
        queue.submit(1, 1_048_569, 77, 3);

        assert_eq!(queue.pending_count(), 1);
        let stale = rx.recv().await.unwrap();
        assert_eq!(stale.content_epoch, 2);
        let updated = rx.recv().await.unwrap();
        assert_eq!(updated.content_epoch, 3);

        assert!(
            !queue.cancel(&stale),
            "stale worker op must not cancel the newer pending upload"
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

        let first = rx.recv().await.unwrap();
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

        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();

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
        let stale = rx.recv().await.unwrap();
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
        let mut retry = rx.recv().await.unwrap();
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
    async fn failed_requeue_does_not_mutate_pending_snapshot() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(10, 0, 100, 1);
        let mut retry = rx.recv().await.unwrap();
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
        let op = rx.recv().await.unwrap();
        assert_eq!(op.ino, 1);
        queue.complete(&op);

        // ino 1 still has the second payload object pending.
        assert!(queue.has_pending_for_ino(1));

        // Drain remaining ops.
        while let Ok(next) = rx.try_recv() {
            queue.complete(&next);
        }

        assert!(!queue.has_pending_for_ino(1));
        assert!(!queue.has_pending_for_ino(2));
    }
}
