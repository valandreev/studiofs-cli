use crate::access::{AccessClass, AccessDecision, AccessTrackerConfig, HandleAccessState};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;
use studiofs_meta::types::{Ino, InodeAttr};
use tracing::debug;

use crate::utils::now_unix_millis;

use super::{OsCacheOpenDecision, OsCacheReason};

// =============================================================================
// Open File Handle Tracking
// =============================================================================

/// Aggregated metrics for file sessions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsFileSessionStats {
    /// Number of active inode sessions currently tracked.
    pub active_sessions: usize,
    /// Total count of currently open file handles.
    pub open_handles: u64,
    /// Number of operations queued for processing.
    pub queued_ops: u64,
    /// Maximum observed queue depth.
    pub max_queue_depth: u64,
    /// Number of queued operations that waited for the session gate.
    pub wait_events: u64,
    /// Total accumulated queue wait time in milliseconds.
    pub wait_ms: u64,
    /// Total number of observed state transitions.
    pub transitions: u64,
    /// Aggregated wait counts by operation type.
    pub wait_reasons: Vec<(String, u64)>,
}

#[derive(Debug, Clone)]
pub(super) struct FileSessionDebugSnapshot {
    pub(super) open_handles: u32,
    pub(super) queued_ops: u64,
    pub(super) active_shared_locks: u32,
    pub(super) active_writer: Option<&'static str>,
    pub(super) last_transition_unix_ms: u64,
    pub(super) last_transition_label: (&'static str, &'static str),
}

#[derive(Clone, Debug)]
struct ReadWindow {
    ino: Ino,
    start: u64,
    data: Bytes,
}

#[derive(Debug, Default)]
struct ReadWindowState {
    last_end: Option<u64>,
    window: Option<ReadWindow>,
}

/// Flags associated with an open file descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenFlags(u32);

impl OpenFlags {
    /// Open in read-only mode.
    pub const RDONLY: Self = Self(0x01);
    /// Open in write-only mode.
    pub const WRONLY: Self = Self(0x02);
    /// Open in read-write mode.
    pub const RDWR: Self = Self(0x04);
    /// Open with append-only semantics.
    pub const APPEND: Self = Self(0x08);
    /// Open with synchronous write-through semantics.
    pub const SYNC: Self = Self(0x10);

    /// Returns an empty set of flags.
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Returns the raw bit value of these flags.
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Returns true when all bits from `other` are present.
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Returns the union of this and another flag set.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOr for OpenFlags {
    type Output = Self;
    /// Combines two flag sets with bitwise OR.
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for OpenFlags {
    /// Adds flags from `rhs` to this flag set.
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

#[derive(Debug)]
pub(super) enum OsCacheHandleLifecycle {
    ForegroundRead,
    Writer { ino: Ino },
}

#[derive(Debug, Default)]
pub(super) enum OsCacheHandleState {
    #[default]
    Unprepared,
    Prepared {
        decision: OsCacheOpenDecision,
        lifecycle: Option<OsCacheHandleLifecycle>,
    },
    Released,
}

/// Information about a single open file descriptor.
#[derive(Clone, Debug)]
pub struct OpenFile {
    /// VFS-level file handle identifier.
    pub fh: u64,
    /// Inode for the opened file.
    pub ino: Ino,
    /// Open flags applied to this handle.
    pub flags: OpenFlags,
    pub(super) open_attr: Arc<RwLock<Option<Arc<InodeAttr>>>>,
    pub(super) active_open_overlay: bool,
    pub(super) os_cache_state: Arc<Mutex<OsCacheHandleState>>,
    access_state: Arc<Mutex<HandleAccessState>>,
    read_window: Arc<Mutex<ReadWindowState>>,
}

impl OpenFile {
    pub(super) fn new(
        fh: u64,
        ino: Ino,
        flags: OpenFlags,
        open_attr: Option<InodeAttr>,
        active_open_overlay: bool,
    ) -> Self {
        Self {
            fh,
            ino,
            flags,
            open_attr: Arc::new(RwLock::new(open_attr.map(Arc::new))),
            active_open_overlay,
            os_cache_state: Arc::new(Mutex::new(OsCacheHandleState::default())),
            access_state: Arc::new(Mutex::new(HandleAccessState::new(fh, flags))),
            read_window: Arc::new(Mutex::new(ReadWindowState::default())),
        }
    }

    pub(super) fn open_attr(&self) -> Option<InodeAttr> {
        self.open_attr.read().as_deref().cloned()
    }

    pub(super) fn prepare_os_cache(
        &self,
        prepare: impl FnOnce() -> (OsCacheOpenDecision, Option<OsCacheHandleLifecycle>),
    ) -> OsCacheOpenDecision {
        let mut state = self.os_cache_state.lock();
        match &*state {
            OsCacheHandleState::Prepared { decision, .. } => return *decision,
            OsCacheHandleState::Released => {
                return OsCacheOpenDecision::replace(OsCacheReason::UnknownHandle);
            }
            OsCacheHandleState::Unprepared => {}
        }

        let (decision, lifecycle) = prepare();
        *state = OsCacheHandleState::Prepared {
            decision,
            lifecycle,
        };
        decision
    }

    pub(super) fn take_os_cache_lifecycle_for_release(&self) -> Option<OsCacheHandleLifecycle> {
        match std::mem::replace(
            &mut *self.os_cache_state.lock(),
            OsCacheHandleState::Released,
        ) {
            OsCacheHandleState::Prepared { lifecycle, .. } => lifecycle,
            OsCacheHandleState::Unprepared | OsCacheHandleState::Released => None,
        }
    }

    pub(super) fn set_open_attr(&self, attr: InodeAttr) {
        self.set_shared_open_attr(Arc::new(attr));
    }

    pub(super) fn set_shared_open_attr(&self, attr: Arc<InodeAttr>) {
        *self.open_attr.write() = Some(attr);
    }

    pub(super) fn stream_id(&self) -> u64 {
        self.access_state.lock().stream_id()
    }

    pub(super) fn current_access_class(&self) -> AccessClass {
        self.access_state.lock().current_class()
    }

    /// Returns the current file-access class string label for this handle.
    pub fn access_class_name(&self) -> &'static str {
        self.current_access_class().as_str()
    }

    pub(super) fn observe_read(
        &self,
        offset: u64,
        len: usize,
        cfg: AccessTrackerConfig,
    ) -> AccessDecision {
        self.access_state.lock().observe_read(offset, len, cfg)
    }

    pub(super) fn observe_write(
        &self,
        offset: u64,
        len: usize,
        cfg: AccessTrackerConfig,
    ) -> AccessDecision {
        self.access_state.lock().observe_write(offset, len, cfg)
    }

    pub(super) fn read_window_hit(&self, ino: Ino, offset: u64, buffer: &mut [u8]) -> Option<u32> {
        let mut state = self.read_window.lock();
        let window = state.window.as_ref()?;
        if window.ino != ino || offset < window.start {
            return None;
        }
        let rel = usize::try_from(offset - window.start).ok()?;
        let end = rel.checked_add(buffer.len())?;
        if end > window.data.len() {
            return None;
        }
        buffer.copy_from_slice(&window.data[rel..end]);
        state.last_end = Some(offset.saturating_add(buffer.len() as u64));
        Some(buffer.len() as u32)
    }

    pub(super) fn should_fill_read_window(
        &self,
        offset: u64,
        requested_len: usize,
        window_len: usize,
    ) -> bool {
        requested_len < window_len && self.read_window.lock().last_end == Some(offset)
    }

    pub(super) fn record_read_progress(&self, offset: u64, read_len: usize) {
        self.read_window.lock().last_end = Some(offset.saturating_add(read_len as u64));
    }

    pub(super) fn replace_read_window(&self, ino: Ino, start: u64, data: Bytes) {
        let mut state = self.read_window.lock();
        state.last_end = Some(start.saturating_add(data.len() as u64));
        state.window = Some(ReadWindow { ino, start, data });
    }

    pub(super) fn clear_read_window(&self) {
        let mut state = self.read_window.lock();
        state.last_end = None;
        state.window = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileSessionOperation {
    /// Read operation.
    Read,
    /// Write operation.
    Write,
    /// Flush operation.
    Flush,
    /// Fsync operation.
    Fsync,
    /// Release operation.
    Release,
    /// Materialize operation.
    Materialize,
    /// Upload operation.
    Upload,
    /// Finalize operation.
    Finalize,
    /// Delete operation.
    Delete,
}

impl FileSessionOperation {
    const COUNT: usize = 9;

    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Flush => "flush",
            Self::Fsync => "fsync",
            Self::Release => "release",
            Self::Materialize => "materialize",
            Self::Upload => "upload",
            Self::Finalize => "finalize",
            Self::Delete => "delete",
        }
    }

    fn shared_lock(self) -> bool {
        matches!(self, Self::Read | Self::Materialize | Self::Finalize)
    }

    fn blocks_materialize(self) -> bool {
        matches!(
            self,
            Self::Flush | Self::Fsync | Self::Release | Self::Upload | Self::Delete
        )
    }

    fn index(self) -> usize {
        match self {
            Self::Read => 0,
            Self::Write => 1,
            Self::Flush => 2,
            Self::Fsync => 3,
            Self::Release => 4,
            Self::Materialize => 5,
            Self::Upload => 6,
            Self::Finalize => 7,
            Self::Delete => 8,
        }
    }

    fn from_index(idx: usize) -> Option<Self> {
        match idx {
            0 => Some(Self::Read),
            1 => Some(Self::Write),
            2 => Some(Self::Flush),
            3 => Some(Self::Fsync),
            4 => Some(Self::Release),
            5 => Some(Self::Materialize),
            6 => Some(Self::Upload),
            7 => Some(Self::Finalize),
            8 => Some(Self::Delete),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub(super) struct FileSessionEntry {
    /// Gate that serializes conflicting operations for this inode.
    gate: Arc<tokio::sync::RwLock<()>>,
    /// Gate that keeps background materialization ordered with durability and lifecycle barriers.
    materialize_gate: Arc<tokio::sync::RwLock<()>>,
    /// Gate that serializes background finalization while allowing readers.
    finalize_gate: Arc<tokio::sync::Mutex<()>>,
    open_handles: AtomicU32,
    queued_ops: AtomicU64,
    max_queue_depth: AtomicU64,
    wait_events: AtomicU64,
    wait_ms: AtomicU64,
    transitions: AtomicU64,
    queued_delete_ops: AtomicU64,
    active_delete_ops: AtomicU64,
    active_shared_locks: AtomicU32,
    active_writer: Mutex<Option<&'static str>>,
    last_transition_unix_ms: AtomicU64,
    last_transition_label: Mutex<(&'static str, &'static str)>,
}

impl FileSessionEntry {
    /// Constructs a new file-session state entry.
    fn new() -> Self {
        Self {
            gate: Arc::new(tokio::sync::RwLock::new(())),
            materialize_gate: Arc::new(tokio::sync::RwLock::new(())),
            finalize_gate: Arc::new(tokio::sync::Mutex::new(())),
            open_handles: AtomicU32::new(0),
            queued_ops: AtomicU64::new(0),
            max_queue_depth: AtomicU64::new(0),
            wait_events: AtomicU64::new(0),
            wait_ms: AtomicU64::new(0),
            transitions: AtomicU64::new(0),
            queued_delete_ops: AtomicU64::new(0),
            active_delete_ops: AtomicU64::new(0),
            active_shared_locks: AtomicU32::new(0),
            active_writer: Mutex::new(None),
            last_transition_unix_ms: AtomicU64::new(0),
            last_transition_label: Mutex::new(("", "")),
        }
    }

    fn record_transition(&self, op: FileSessionOperation, phase: &'static str) {
        self.transitions.fetch_add(1, Ordering::Relaxed);
        self.last_transition_unix_ms
            .store(now_unix_millis(), Ordering::Relaxed);
        *self.last_transition_label.lock() = (op.as_str(), phase);
    }

    fn update_max_queue_depth(&self, candidate: u64) {
        let _ =
            self.max_queue_depth
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    (candidate > current).then_some(candidate)
                });
    }

    fn note_active_start(&self, op: FileSessionOperation) {
        if op == FileSessionOperation::Delete {
            self.active_delete_ops.fetch_add(1, Ordering::Relaxed);
        }
        if op.shared_lock() {
            self.active_shared_locks.fetch_add(1, Ordering::Relaxed);
        } else {
            *self.active_writer.lock() = Some(op.as_str());
        }
    }

    fn note_active_finish(&self, op: FileSessionOperation) {
        if op == FileSessionOperation::Delete {
            self.active_delete_ops
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                })
                .ok();
        }
        if op.shared_lock() {
            self.active_shared_locks
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                })
                .ok();
        } else {
            *self.active_writer.lock() = None;
        }
    }
}

#[derive(Debug)]
pub(super) struct FileSessionPermit {
    /// Finalize-only guard held before the shared read gate.
    _finalize_gate: Option<tokio::sync::OwnedMutexGuard<()>>,
    /// Read side lock guard held while the operation is active.
    _read_gate: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    /// Write side lock guard held while the operation is active.
    _write_gate: Option<tokio::sync::OwnedRwLockWriteGuard<()>>,
    /// Background-materialization guard held while an opportunistic drain is active.
    _materialize_read_gate: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    /// Barrier guard that waits for active background materialization.
    _materialize_write_gate: Option<tokio::sync::OwnedRwLockWriteGuard<()>>,
    /// Shared counters used by the permit.
    counters: Arc<FileSessionCounters>,
    /// Session that owns the active permit.
    session: Arc<FileSessionEntry>,
    /// Operation that created this permit.
    op: FileSessionOperation,
}

impl Drop for FileSessionPermit {
    /// Releases lock guards and records the finish transition.
    fn drop(&mut self) {
        // Release session lock(s) before publishing "finish" so waiters can
        // observe lower tail latency on contended inodes.
        drop(self._read_gate.take());
        drop(self._write_gate.take());
        drop(self._materialize_read_gate.take());
        drop(self._materialize_write_gate.take());
        drop(self._finalize_gate.take());
        self.session.note_active_finish(self.op);
        self.counters.transitions.fetch_add(1, Ordering::Relaxed);
        self.session.record_transition(self.op, "finish");
    }
}

#[derive(Debug)]
struct FileSessionCounters {
    active_sessions: AtomicU64,
    open_handles: AtomicU64,
    queued_ops: AtomicU64,
    max_queue_depth: AtomicU64,
    wait_events: AtomicU64,
    wait_ms: AtomicU64,
    transitions: AtomicU64,
    wait_reasons: [AtomicU64; FileSessionOperation::COUNT],
}

impl FileSessionCounters {
    fn new() -> Self {
        Self {
            active_sessions: AtomicU64::new(0),
            open_handles: AtomicU64::new(0),
            queued_ops: AtomicU64::new(0),
            max_queue_depth: AtomicU64::new(0),
            wait_events: AtomicU64::new(0),
            wait_ms: AtomicU64::new(0),
            transitions: AtomicU64::new(0),
            wait_reasons: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn note_wait_reason(&self, op: FileSessionOperation) {
        self.wait_reasons[op.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn note_transition(&self) {
        self.transitions.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(super) struct FileSessionState {
    /// Active sessions tracked by inode.
    sessions: Mutex<HashMap<Ino, Arc<FileSessionEntry>>>,
    counters: Arc<FileSessionCounters>,
}

impl FileSessionState {
    /// Creates a new file-session state map.
    pub(super) fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            counters: Arc::new(FileSessionCounters::new()),
        }
    }

    /// Gets (or creates) the session entry for `ino`.
    pub(super) fn session(&self, ino: Ino) -> Arc<FileSessionEntry> {
        let mut sessions = self.sessions.lock();
        if let Some(existing) = sessions.get(&ino) {
            return Arc::clone(existing);
        }

        let session = Arc::new(FileSessionEntry::new());
        sessions.insert(ino, Arc::clone(&session));
        self.counters
            .active_sessions
            .fetch_add(1, Ordering::Relaxed);
        session
    }

    fn live_session(&self, ino: Ino) -> Option<Arc<FileSessionEntry>> {
        self.sessions.lock().get(&ino).cloned()
    }

    /// Returns whether a delete operation is queued or active for an inode.
    pub(super) fn has_delete_inflight(&self, ino: Ino) -> bool {
        self.live_session(ino).is_some_and(|session| {
            session.queued_delete_ops.load(Ordering::Relaxed) > 0
                || session.active_delete_ops.load(Ordering::Relaxed) > 0
        })
    }

    fn session_is_idle(session: &Arc<FileSessionEntry>) -> bool {
        session.open_handles.load(Ordering::Relaxed) == 0
            && session.queued_ops.load(Ordering::Relaxed) == 0
            && session.active_shared_locks.load(Ordering::Relaxed) == 0
            && session.active_writer.lock().is_none()
            // The map must hold the only strong ref. `gc_if_idle` runs while
            // holding the session map lock, so no new `session()` caller can
            // clone this entry between the count check and removal.
            && Arc::strong_count(session) == 1
    }

    /// Removes an idle session entry when no handles remain.
    pub(super) fn gc_if_idle(&self, ino: Ino) -> bool {
        let mut sessions = self.sessions.lock();
        let Some(session) = sessions.get(&ino) else {
            return false;
        };
        if !Self::session_is_idle(session) {
            return false;
        }
        sessions.remove(&ino);
        self.counters
            .active_sessions
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
        debug!(ino, "file session gc removed idle session");
        true
    }

    /// Enters the operation gate for `ino` and returns a session permit.
    pub(super) async fn enter_op(&self, ino: Ino, op: FileSessionOperation) -> FileSessionPermit {
        let session = self.session(ino);
        if op == FileSessionOperation::Delete {
            session.queued_delete_ops.fetch_add(1, Ordering::Relaxed);
        }
        let counters = self.counters.clone();
        let queue_depth = session.queued_ops.fetch_add(1, Ordering::Relaxed) + 1;
        session.update_max_queue_depth(queue_depth);
        let global_queue_depth = counters.queued_ops.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = counters.max_queue_depth.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| (global_queue_depth > current).then_some(global_queue_depth),
        );

        let wait_started = Instant::now();
        let finalize_guard = if op == FileSessionOperation::Finalize {
            Some(session.finalize_gate.clone().lock_owned().await)
        } else {
            None
        };
        let materialize_write_guard = if op.blocks_materialize() {
            Some(session.materialize_gate.clone().write_owned().await)
        } else {
            None
        };
        let materialize_read_guard = if op == FileSessionOperation::Materialize {
            Some(session.materialize_gate.clone().read_owned().await)
        } else {
            None
        };
        let (read_guard, write_guard) = if op == FileSessionOperation::Materialize {
            (None, None)
        } else if op.shared_lock() {
            (Some(session.gate.clone().read_owned().await), None)
        } else {
            (None, Some(session.gate.clone().write_owned().await))
        };
        session.queued_ops.fetch_sub(1, Ordering::Relaxed);
        counters
            .queued_ops
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();

        let waited_ms = wait_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        if waited_ms > 0 {
            session.wait_events.fetch_add(1, Ordering::Relaxed);
            session.wait_ms.fetch_add(waited_ms, Ordering::Relaxed);
            counters.wait_events.fetch_add(1, Ordering::Relaxed);
            counters.wait_ms.fetch_add(waited_ms, Ordering::Relaxed);
            counters.note_wait_reason(op);
        }

        counters.note_transition();
        session.note_active_start(op);
        session.record_transition(op, "start");
        if op == FileSessionOperation::Delete {
            session
                .queued_delete_ops
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                })
                .ok();
        }
        FileSessionPermit {
            _finalize_gate: finalize_guard,
            _read_gate: read_guard,
            _write_gate: write_guard,
            _materialize_read_gate: materialize_read_guard,
            _materialize_write_gate: materialize_write_guard,
            counters,
            session,
            op,
        }
    }

    /// Increments handle counters for `ino` and returns new open handle count.
    pub(super) fn note_open_handle(&self, ino: Ino) -> u32 {
        let session = self.session(ino);
        let next = session.open_handles.fetch_add(1, Ordering::Relaxed) + 1;
        self.counters.open_handles.fetch_add(1, Ordering::Relaxed);
        self.counters.note_transition();
        session.record_transition(FileSessionOperation::Read, "open");
        debug!(ino, open_handles = next, "file session open handle");
        next
    }

    /// Updates close counters for `ino` and triggers GC when empty.
    pub(super) fn note_close_handle(&self, ino: Ino, remaining: u32) {
        let session = self.session(ino);
        session.open_handles.store(remaining, Ordering::Relaxed);
        self.counters
            .open_handles
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
        self.counters.note_transition();
        session.record_transition(FileSessionOperation::Release, "handle_close");
        debug!(ino, open_handles = remaining, "file session close handle");
        if remaining == 0 {
            self.gc_if_idle(ino);
        }
    }

    /// Records background transition metadata for the given operation and phase.
    pub(super) fn note_background_transition(
        &self,
        ino: Ino,
        op: FileSessionOperation,
        phase: &'static str,
    ) {
        self.counters.note_transition();
        {
            let Some(session) = self.live_session(ino) else {
                return;
            };
            session.record_transition(op, phase);
        }
        if matches!(phase, "complete" | "published" | "recovered" | "skipped") {
            self.gc_if_idle(ino);
        }
    }

    /// Returns a snapshot of file-session statistics.
    pub(super) fn stats(&self) -> VfsFileSessionStats {
        let counters = self.counters.clone();
        let mut stats = VfsFileSessionStats {
            active_sessions: counters.active_sessions.load(Ordering::Relaxed) as usize,
            open_handles: counters.open_handles.load(Ordering::Relaxed),
            queued_ops: counters.queued_ops.load(Ordering::Relaxed),
            max_queue_depth: counters.max_queue_depth.load(Ordering::Relaxed),
            wait_events: counters.wait_events.load(Ordering::Relaxed),
            wait_ms: counters.wait_ms.load(Ordering::Relaxed),
            transitions: counters.transitions.load(Ordering::Relaxed),
            wait_reasons: Vec::new(),
        };
        for idx in 0..FileSessionOperation::COUNT {
            let Some(op) = FileSessionOperation::from_index(idx) else {
                continue;
            };
            let count = counters.wait_reasons[idx].load(Ordering::Relaxed);
            stats.wait_reasons.push((op.as_str().to_string(), count));
        }
        stats
    }

    pub(super) fn debug_snapshot(&self, ino: Ino) -> Option<FileSessionDebugSnapshot> {
        let session = self.sessions.lock().get(&ino).cloned()?;
        Some(FileSessionDebugSnapshot {
            open_handles: session.open_handles.load(Ordering::Relaxed),
            queued_ops: session.queued_ops.load(Ordering::Relaxed),
            active_shared_locks: session.active_shared_locks.load(Ordering::Relaxed),
            active_writer: *session.active_writer.lock(),
            last_transition_unix_ms: session.last_transition_unix_ms.load(Ordering::Relaxed),
            last_transition_label: *session.last_transition_label.lock(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_flags_bitwise() {
        let f = OpenFlags::RDWR | OpenFlags::APPEND;
        assert!(f.contains(OpenFlags::RDWR));
        assert!(f.contains(OpenFlags::APPEND));
        assert!(!f.contains(OpenFlags::WRONLY));

        let f2 = f.union(OpenFlags::WRONLY);
        assert!(f2.contains(OpenFlags::WRONLY));

        let mut f3 = OpenFlags::RDONLY;
        f3 |= OpenFlags::APPEND;
        assert!(f3.contains(OpenFlags::RDONLY));
        assert!(f3.contains(OpenFlags::APPEND));
    }

    #[test]
    fn read_window_hit_requires_full_range_coverage() {
        let open_file = OpenFile::new(7, 42, OpenFlags::RDONLY, None, false);
        open_file.replace_read_window(42, 1024, Bytes::from_static(b"abcdef"));

        let mut hit = [0u8; 3];
        assert_eq!(open_file.read_window_hit(42, 1026, &mut hit), Some(3));
        assert_eq!(&hit, b"cde");

        let mut miss = [0u8; 4];
        assert_eq!(open_file.read_window_hit(42, 1027, &mut miss), None);
    }

    #[test]
    fn read_window_fill_requires_sequential_read() {
        let open_file = OpenFile::new(7, 42, OpenFlags::RDONLY, None, false);

        assert!(!open_file.should_fill_read_window(128, 128, 4096));
        open_file.record_read_progress(0, 128);
        assert!(open_file.should_fill_read_window(128, 128, 4096));
        assert!(!open_file.should_fill_read_window(512, 128, 4096));
        assert!(!open_file.should_fill_read_window(128, 4096, 4096));
    }

    #[test]
    fn file_session_gc_keeps_session_with_external_reference() {
        let sessions = FileSessionState::new();
        let session = sessions.session(42);

        assert!(!sessions.gc_if_idle(42));
        assert!(Arc::ptr_eq(&sessions.session(42), &session));

        drop(session);
        assert!(sessions.gc_if_idle(42));
    }
}
