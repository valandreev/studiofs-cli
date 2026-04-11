use super::*;

// =============================================================================
// Open File Handle Tracking
// =============================================================================

/// Flags associated with an open file descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenFlags(u32);

impl OpenFlags {
    pub const RDONLY: Self = Self(0x01);
    pub const WRONLY: Self = Self(0x02);
    pub const RDWR: Self = Self(0x04);
    pub const APPEND: Self = Self(0x08);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOr for OpenFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// Information about a single open file descriptor.
#[derive(Clone, Debug)]
pub struct OpenFile {
    pub fh: u64,
    pub ino: Ino,
    pub flags: OpenFlags,
    access_state: Arc<Mutex<HandleAccessState>>,
}

impl OpenFile {
    pub(super) fn new(fh: u64, ino: Ino, flags: OpenFlags) -> Self {
        Self {
            fh,
            ino,
            flags,
            access_state: Arc::new(Mutex::new(HandleAccessState::new(fh, flags))),
        }
    }

    pub(super) fn stream_id(&self) -> u64 {
        self.access_state.lock().stream_id()
    }

    pub(super) fn current_access_class(&self) -> AccessClass {
        self.access_state.lock().current_class()
    }

    pub fn access_class_name(&self) -> &'static str {
        self.current_access_class().as_str()
    }

    pub(super) fn observe_read(
        &self,
        offset: u64,
        len: usize,
        cfg: AccessClassifierConfig,
    ) -> AccessDecision {
        self.access_state.lock().observe_read(offset, len, cfg)
    }

    pub(super) fn observe_write(
        &self,
        offset: u64,
        len: usize,
        cfg: AccessClassifierConfig,
    ) -> AccessDecision {
        self.access_state.lock().observe_write(offset, len, cfg)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileSessionOperation {
    Read,
    Write,
    Flush,
    Fsync,
    Release,
    Materialize,
    Upload,
    Finalize,
}

impl FileSessionOperation {
    const COUNT: usize = 8;

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
        }
    }

    fn read_lock(self) -> bool {
        matches!(self, Self::Read)
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
            _ => None,
        }
    }
}

#[derive(Debug)]
pub(super) struct FileSessionEntry {
    gate: Arc<tokio::sync::RwLock<()>>,
    open_handles: AtomicU32,
    queued_ops: AtomicU64,
    max_queue_depth: AtomicU64,
    wait_events: AtomicU64,
    wait_ms: AtomicU64,
    transitions: AtomicU64,
    last_transition_unix_ms: AtomicU64,
    last_transition_label: Mutex<String>,
}

impl FileSessionEntry {
    fn new() -> Self {
        Self {
            gate: Arc::new(tokio::sync::RwLock::new(())),
            open_handles: AtomicU32::new(0),
            queued_ops: AtomicU64::new(0),
            max_queue_depth: AtomicU64::new(0),
            wait_events: AtomicU64::new(0),
            wait_ms: AtomicU64::new(0),
            transitions: AtomicU64::new(0),
            last_transition_unix_ms: AtomicU64::new(0),
            last_transition_label: Mutex::new(String::new()),
        }
    }

    fn record_transition(&self, op: FileSessionOperation, phase: &'static str) {
        self.transitions.fetch_add(1, Ordering::Relaxed);
        self.last_transition_unix_ms
            .store(StudioVfs::now_unix_millis(), Ordering::Relaxed);
        *self.last_transition_label.lock() = format!("{}:{phase}", op.as_str());
    }

    fn update_max_queue_depth(&self, candidate: u64) {
        let _ =
            self.max_queue_depth
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    (candidate > current).then_some(candidate)
                });
    }
}

#[derive(Debug)]
pub(super) struct FileSessionPermit {
    _read_gate: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    _write_gate: Option<tokio::sync::OwnedRwLockWriteGuard<()>>,
    counters: Arc<FileSessionCounters>,
    session: Arc<FileSessionEntry>,
    op: FileSessionOperation,
}

impl Drop for FileSessionPermit {
    fn drop(&mut self) {
        // Release session lock(s) before publishing "finish" so waiters can
        // observe lower tail latency on contended inodes.
        drop(self._read_gate.take());
        drop(self._write_gate.take());
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
    sessions: Mutex<HashMap<Ino, Weak<FileSessionEntry>>>,
    counters: Arc<FileSessionCounters>,
}

impl FileSessionState {
    pub(super) fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            counters: Arc::new(FileSessionCounters::new()),
        }
    }

    pub(super) fn session(&self, ino: Ino) -> Arc<FileSessionEntry> {
        let mut sessions = self.sessions.lock();
        if let Some(existing) = sessions.get(&ino).and_then(Weak::upgrade) {
            return existing;
        }
        if sessions.get(&ino).is_some() {
            sessions.remove(&ino);
            self.counters
                .active_sessions
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                })
                .ok();
        }

        let session = Arc::new(FileSessionEntry::new());
        sessions.insert(ino, Arc::downgrade(&session));
        self.counters
            .active_sessions
            .fetch_add(1, Ordering::Relaxed);
        session
    }

    pub(super) fn gc_if_idle(&self, ino: Ino) {
        let mut sessions = self.sessions.lock();
        if sessions.get(&ino).and_then(Weak::upgrade).is_none() {
            sessions.remove(&ino);
            self.counters
                .active_sessions
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                })
                .ok();
            debug!(ino, "file session gc removed idle session");
        }
    }

    pub(super) async fn enter_op(&self, ino: Ino, op: FileSessionOperation) -> FileSessionPermit {
        let session = self.session(ino);
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
        let (read_guard, write_guard) = if op.read_lock() {
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
        session.record_transition(op, "start");
        FileSessionPermit {
            _read_gate: read_guard,
            _write_gate: write_guard,
            counters,
            session,
            op,
        }
    }

    pub(super) fn note_open_handle(&self, ino: Ino) -> u32 {
        let session = self.session(ino);
        let next = session.open_handles.fetch_add(1, Ordering::Relaxed) + 1;
        self.counters.open_handles.fetch_add(1, Ordering::Relaxed);
        self.counters.note_transition();
        session.record_transition(FileSessionOperation::Read, "open");
        debug!(ino, open_handles = next, "file session open handle");
        next
    }

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

    pub(super) fn note_background_transition(
        &self,
        ino: Ino,
        op: FileSessionOperation,
        phase: &'static str,
    ) {
        self.counters.note_transition();
        self.session(ino).record_transition(op, phase);
        if matches!(phase, "complete" | "published" | "recovered" | "skipped") {
            self.gc_if_idle(ino);
        }
    }

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
    }
}
