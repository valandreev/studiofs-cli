use super::{ExplicitFlushOutcome, ReleaseFhOutcome, VfsError, VfsResult};
use studiofs_meta::types::Ino;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Struct $name.
pub struct HandleDrainRequest {
    /// Field $name.
    pub ino: Ino,
    /// Field $name.
    pub fh: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct HandleDrainDeleteRequest {
    /// Field $name.
    pub ino: Ino,
    /// Field $name.
    pub parent_ino: Ino,
    /// Field $name.
    pub name: Vec<u8>,
    /// The bridge passes the callback's delete intent explicitly so the delete
    /// adapter can choose `unlink` vs `rmdir` without reloading inode state
    /// after the callback has already classified the path.
    pub is_dir: bool,
}

/// Trait $name.
pub trait LocalSyncBarrierPort {
    /// Function $name.
    fn inode_has_local_dirty_state(&self, ino: Ino) -> bool;

    /// Function $name.
    fn explicit_flush(&self, request: HandleDrainRequest) -> VfsResult<ExplicitFlushOutcome>;
}

/// Trait $name.
pub trait HandleReleasePort {
    /// Function $name.
    fn release_handle(&self, request: HandleDrainRequest) -> VfsResult<ReleaseFhOutcome>;
}

/// Trait $name.
pub trait DeferredCleanupHintPort {
    /// Function $name.
    fn set_deferred_cleanup_hint(&self, ino: Ino);

    /// Function $name.
    fn clear_deferred_cleanup_hint(&self, ino: Ino);

    /// Function $name.
    fn take_deferred_cleanup_hint(&self, ino: Ino) -> bool;

    /// Function $name.
    fn has_deferred_cleanup_hint(&self, ino: Ino) -> bool;
}

/// Trait $name.
pub trait FlushGatePort {
    /// Function $name.
    fn with_flush_gate<T, F>(&self, ino: Ino, op: F) -> VfsResult<T>
    where
        F: FnOnce() -> VfsResult<T>;
}

/// Trait $name.
pub trait DeleteCleanupPort {
    /// Function $name.
    fn cleanup_delete(&self, request: HandleDrainDeleteRequest);
}

/// Trait $name.
pub trait BarrierObservabilityPort {
    /// Function $name.
    fn note_cleanup_callback(&self);

    /// Function $name.
    fn note_flush_callback(&self);

    /// Function $name.
    fn note_cleanup_barrier_outcome(&self, flushed_local_dirty_state: bool);

    /// Function $name.
    fn note_flush_barrier_outcome(&self, flushed_local_dirty_state: bool);

    /// Function $name.
    fn note_close_callback(&self);

    /// Function $name.
    fn note_release_success(
        &self,
        request: HandleDrainRequest,
        hint_was_taken: bool,
        outcome: ReleaseFhOutcome,
    );

    /// Function $name.
    fn note_release_error(
        &self,
        request: HandleDrainRequest,
        cleanup_deferred_hint: bool,
        error: &VfsError,
    );
}

/// Struct $name.
pub struct HandleDrainBarrier<
    LocalSync,
    HandleRelease,
    DeferredCleanupHints,
    FlushGate,
    DeleteCleanup,
    Observability,
> {
    local_sync: LocalSync,
    handle_release: HandleRelease,
    deferred_cleanup_hints: DeferredCleanupHints,
    flush_gate: FlushGate,
    delete_cleanup: DeleteCleanup,
    observability: Observability,
}

impl<LocalSync, HandleRelease, DeferredCleanupHints, FlushGate, DeleteCleanup, Observability>
    HandleDrainBarrier<
        LocalSync,
        HandleRelease,
        DeferredCleanupHints,
        FlushGate,
        DeleteCleanup,
        Observability,
    >
where
    LocalSync: LocalSyncBarrierPort,
    HandleRelease: HandleReleasePort,
    DeferredCleanupHints: DeferredCleanupHintPort,
    FlushGate: FlushGatePort,
    DeleteCleanup: DeleteCleanupPort,
    Observability: BarrierObservabilityPort,
{
    /// Function $name.
    pub fn new(
        local_sync: LocalSync,
        handle_release: HandleRelease,
        deferred_cleanup_hints: DeferredCleanupHints,
        flush_gate: FlushGate,
        delete_cleanup: DeleteCleanup,
        observability: Observability,
    ) -> Self {
        Self {
            local_sync,
            handle_release,
            deferred_cleanup_hints,
            flush_gate,
            delete_cleanup,
            observability,
        }
    }

    /// Function $name.
    pub fn on_flush(&self, request: HandleDrainRequest) -> VfsResult<()> {
        self.observability.note_flush_callback();
        let flushed = self.flush_gate.with_flush_gate(request.ino, || {
            if !self.local_sync.inode_has_local_dirty_state(request.ino) {
                return Ok(false);
            }
            let outcome = self.local_sync.explicit_flush(request)?;
            Ok(outcome.flushed_local_dirty_state)
        })?;
        self.observability.note_flush_barrier_outcome(flushed);
        self.deferred_cleanup_hints
            .clear_deferred_cleanup_hint(request.ino);
        Ok(())
    }

    /// Function $name.
    pub fn on_cleanup_non_delete(&self, request: HandleDrainRequest) {
        self.observability.note_cleanup_callback();
        if request.fh == 0 {
            self.deferred_cleanup_hints
                .clear_deferred_cleanup_hint(request.ino);
            return;
        }
        let flushed = self
            .flush_gate
            .with_flush_gate(request.ino, || {
                if !self.local_sync.inode_has_local_dirty_state(request.ino) {
                    return Ok(false);
                }
                let outcome = self.local_sync.explicit_flush(request)?;
                Ok(outcome.flushed_local_dirty_state)
            })
            .unwrap_or(false);
        self.observability.note_cleanup_barrier_outcome(flushed);
        self.deferred_cleanup_hints
            .clear_deferred_cleanup_hint(request.ino);
    }

    /// Function $name.
    pub fn on_close(&self, request: HandleDrainRequest) {
        self.observability.note_close_callback();
        if request.fh == 0 {
            self.deferred_cleanup_hints
                .clear_deferred_cleanup_hint(request.ino);
            return;
        }

        let _ = self.try_release(request);
    }

    /// Function $name.
    pub fn on_release(&self, request: HandleDrainRequest) -> VfsResult<()> {
        self.try_release(request)
    }

    /// Function $name.
    pub fn on_cleanup_delete(&self, request: HandleDrainDeleteRequest) {
        self.observability.note_cleanup_callback();
        self.delete_cleanup.cleanup_delete(request);
    }

    fn complete_release(&self, request: HandleDrainRequest, outcome: ReleaseFhOutcome) {
        if !outcome.closed_last_handle {
            return;
        }
        let hint_was_taken = self
            .deferred_cleanup_hints
            .take_deferred_cleanup_hint(request.ino);
        self.observability
            .note_release_success(request, hint_was_taken, outcome);
    }

    fn try_release(&self, request: HandleDrainRequest) -> VfsResult<()> {
        let had_cleanup_deferred_hint = self
            .deferred_cleanup_hints
            .has_deferred_cleanup_hint(request.ino);
        match self.handle_release.release_handle(request) {
            Ok(outcome) => {
                self.complete_release(request, outcome);
                Ok(())
            }
            Err(error) => {
                self.note_release_error(request, had_cleanup_deferred_hint, &error);
                Err(error)
            }
        }
    }

    fn note_release_error(
        &self,
        request: HandleDrainRequest,
        had_cleanup_deferred_hint: bool,
        error: &VfsError,
    ) {
        // `release_handle` only reaches this error path once the underlying
        // implementation has engaged the last-handle release flow. Non-final
        // handle closes return `Ok(default())`, so clearing the hint here avoids
        // leaving stale deferred-cleanup state behind after a terminal release
        // failure on the inode lifecycle boundary.
        let hint_was_present_for_log = self
            .deferred_cleanup_hints
            .take_deferred_cleanup_hint(request.ino)
            || had_cleanup_deferred_hint;
        self.observability
            .note_release_error(request, hint_was_present_for_log, error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    type FakeBarrier = HandleDrainBarrier<
        Arc<FakePorts>,
        Arc<FakePorts>,
        Arc<FakePorts>,
        Arc<FakePorts>,
        Arc<FakePorts>,
        Arc<FakePorts>,
    >;

    #[derive(Default)]
    struct FakePorts {
        dirty: Mutex<HashSet<Ino>>,
        release_results: Mutex<VecDeque<VfsResult<ReleaseFhOutcome>>>,
        hints: Mutex<HashSet<Ino>>,
        delete_requests: Mutex<Vec<HandleDrainDeleteRequest>>,
        flush_calls: Mutex<Vec<HandleDrainRequest>>,
        release_calls: Mutex<Vec<HandleDrainRequest>>,
        cleanup_callbacks: AtomicUsize,
        flush_callbacks: AtomicUsize,
        close_callbacks: AtomicUsize,
        cleanup_barrier_outcomes: Mutex<Vec<bool>>,
        flush_barrier_outcomes: Mutex<Vec<bool>>,
        release_successes: Mutex<Vec<(HandleDrainRequest, bool, ReleaseFhOutcome)>>,
        release_errors: Mutex<Vec<(HandleDrainRequest, bool, String)>>,
        gates: Mutex<HashMap<Ino, Arc<Mutex<()>>>>,
    }

    impl FakePorts {
        fn mark_dirty(&self, ino: Ino) {
            self.dirty.lock().insert(ino);
        }

        fn queue_release_result(&self, result: VfsResult<ReleaseFhOutcome>) {
            self.release_results.lock().push_back(result);
        }
    }

    impl LocalSyncBarrierPort for Arc<FakePorts> {
        fn inode_has_local_dirty_state(&self, ino: Ino) -> bool {
            self.dirty.lock().contains(&ino)
        }

        fn explicit_flush(&self, request: HandleDrainRequest) -> VfsResult<ExplicitFlushOutcome> {
            self.flush_calls.lock().push(request);
            let flushed = self.dirty.lock().remove(&request.ino);
            Ok(ExplicitFlushOutcome {
                flushed_local_dirty_state: flushed,
            })
        }
    }

    impl HandleReleasePort for Arc<FakePorts> {
        fn release_handle(&self, request: HandleDrainRequest) -> VfsResult<ReleaseFhOutcome> {
            self.release_calls.lock().push(request);
            if let Some(result) = self.release_results.lock().pop_front() {
                return result;
            }
            let flushed = self.dirty.lock().remove(&request.ino);
            Ok(ReleaseFhOutcome {
                closed_last_handle: true,
                flushed_local_dirty_state: flushed,
            })
        }
    }

    impl DeferredCleanupHintPort for Arc<FakePorts> {
        fn set_deferred_cleanup_hint(&self, ino: Ino) {
            self.hints.lock().insert(ino);
        }

        fn clear_deferred_cleanup_hint(&self, ino: Ino) {
            self.hints.lock().remove(&ino);
        }

        fn take_deferred_cleanup_hint(&self, ino: Ino) -> bool {
            self.hints.lock().remove(&ino)
        }

        fn has_deferred_cleanup_hint(&self, ino: Ino) -> bool {
            self.hints.lock().contains(&ino)
        }
    }

    impl FlushGatePort for Arc<FakePorts> {
        fn with_flush_gate<T, F>(&self, ino: Ino, op: F) -> VfsResult<T>
        where
            F: FnOnce() -> VfsResult<T>,
        {
            let gate = {
                let mut gates = self.gates.lock();
                gates
                    .entry(ino)
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            };
            let _guard = gate.lock();
            let result = op();
            let mut gates = self.gates.lock();
            let should_remove = gates.get(&ino).is_some_and(|current| {
                Arc::ptr_eq(current, &gate) && Arc::strong_count(current) == 2
            });
            if should_remove {
                gates.remove(&ino);
            }
            result
        }
    }

    impl DeleteCleanupPort for Arc<FakePorts> {
        fn cleanup_delete(&self, request: HandleDrainDeleteRequest) {
            self.delete_requests.lock().push(request);
        }
    }

    impl BarrierObservabilityPort for Arc<FakePorts> {
        fn note_cleanup_callback(&self) {
            self.cleanup_callbacks.fetch_add(1, Ordering::Relaxed);
        }

        fn note_flush_callback(&self) {
            self.flush_callbacks.fetch_add(1, Ordering::Relaxed);
        }

        fn note_cleanup_barrier_outcome(&self, flushed_local_dirty_state: bool) {
            self.cleanup_barrier_outcomes
                .lock()
                .push(flushed_local_dirty_state);
        }

        fn note_flush_barrier_outcome(&self, flushed_local_dirty_state: bool) {
            self.flush_barrier_outcomes
                .lock()
                .push(flushed_local_dirty_state);
        }

        fn note_close_callback(&self) {
            self.close_callbacks.fetch_add(1, Ordering::Relaxed);
        }

        fn note_release_success(
            &self,
            request: HandleDrainRequest,
            hint_was_taken: bool,
            outcome: ReleaseFhOutcome,
        ) {
            self.release_successes
                .lock()
                .push((request, hint_was_taken, outcome));
        }

        fn note_release_error(
            &self,
            request: HandleDrainRequest,
            cleanup_deferred_hint: bool,
            error: &VfsError,
        ) {
            self.release_errors
                .lock()
                .push((request, cleanup_deferred_hint, error.to_string()));
        }
    }

    fn barrier(ports: Arc<FakePorts>) -> FakeBarrier {
        HandleDrainBarrier::new(
            ports.clone(),
            ports.clone(),
            ports.clone(),
            ports.clone(),
            ports.clone(),
            ports,
        )
    }

    #[test]
    fn explicit_flush_drains_dirty_state_and_clears_hint() {
        let ports = Arc::new(FakePorts::default());
        ports.mark_dirty(11);
        ports.set_deferred_cleanup_hint(11);
        let barrier = barrier(ports.clone());

        barrier
            .on_flush(HandleDrainRequest { ino: 11, fh: 7 })
            .expect("flush should succeed");

        assert!(!ports.inode_has_local_dirty_state(11));
        assert!(!ports.has_deferred_cleanup_hint(11));
        assert_eq!(ports.flush_calls.lock().len(), 1);
        assert_eq!(ports.flush_barrier_outcomes.lock().as_slice(), &[true]);
    }

    #[test]
    fn clean_flush_short_circuits_without_flush_call() {
        let ports = Arc::new(FakePorts::default());
        let barrier = barrier(ports.clone());

        barrier
            .on_flush(HandleDrainRequest { ino: 12, fh: 9 })
            .expect("clean flush should succeed");

        assert!(ports.flush_calls.lock().is_empty());
        assert_eq!(ports.flush_barrier_outcomes.lock().as_slice(), &[false]);
    }

    #[test]
    fn non_delete_cleanup_flushes_dirty_handles() {
        let ports = Arc::new(FakePorts::default());
        ports.mark_dirty(13);
        let barrier = barrier(ports.clone());

        barrier.on_cleanup_non_delete(HandleDrainRequest { ino: 13, fh: 3 });
        barrier.on_cleanup_non_delete(HandleDrainRequest { ino: 14, fh: 0 });

        assert!(!ports.inode_has_local_dirty_state(13));
        assert!(!ports.has_deferred_cleanup_hint(13));
        assert!(!ports.has_deferred_cleanup_hint(14));
        assert_eq!(ports.cleanup_callbacks.load(Ordering::Relaxed), 2);
        assert_eq!(
            ports.flush_calls.lock().as_slice(),
            &[HandleDrainRequest { ino: 13, fh: 3 }]
        );
        assert_eq!(ports.cleanup_barrier_outcomes.lock().as_slice(), &[true]);
        assert!(ports.flush_barrier_outcomes.lock().is_empty());
    }

    #[test]
    fn close_owns_deferred_fallback_barrier_on_last_handle_release() {
        let ports = Arc::new(FakePorts::default());
        ports.mark_dirty(15);
        ports.set_deferred_cleanup_hint(15);
        let barrier = barrier(ports.clone());

        barrier.on_close(HandleDrainRequest { ino: 15, fh: 5 });

        assert_eq!(ports.close_callbacks.load(Ordering::Relaxed), 1);
        assert_eq!(ports.release_calls.lock().len(), 1);
        assert!(!ports.has_deferred_cleanup_hint(15));
        assert_eq!(ports.release_successes.lock().len(), 1);
        let (_, hint_was_taken, outcome) = ports.release_successes.lock()[0];
        assert!(hint_was_taken);
        assert!(outcome.flushed_local_dirty_state);
    }

    #[test]
    fn close_after_explicit_flush_becomes_clean_release() {
        let ports = Arc::new(FakePorts::default());
        ports.mark_dirty(16);
        ports.set_deferred_cleanup_hint(16);
        let barrier = barrier(ports.clone());

        barrier
            .on_flush(HandleDrainRequest { ino: 16, fh: 8 })
            .expect("flush should succeed");
        barrier.on_close(HandleDrainRequest { ino: 16, fh: 8 });

        let successes = ports.release_successes.lock();
        assert_eq!(successes.len(), 1);
        assert!(!successes[0].1);
        assert!(!successes[0].2.flushed_local_dirty_state);
    }

    #[test]
    fn duplicate_close_does_not_resurrect_hint_or_second_barrier() {
        let ports = Arc::new(FakePorts::default());
        ports.set_deferred_cleanup_hint(17);
        ports.queue_release_result(Ok(ReleaseFhOutcome {
            closed_last_handle: true,
            flushed_local_dirty_state: true,
        }));
        ports.queue_release_result(Ok(ReleaseFhOutcome::default()));
        let barrier = barrier(ports.clone());

        barrier.on_close(HandleDrainRequest { ino: 17, fh: 4 });
        barrier.on_close(HandleDrainRequest { ino: 17, fh: 4 });

        assert!(!ports.has_deferred_cleanup_hint(17));
        assert_eq!(ports.release_calls.lock().len(), 2);
        assert_eq!(ports.release_successes.lock().len(), 1);
    }

    #[test]
    fn repeated_cleanup_callbacks_flush_once_then_clean_skip() {
        let ports = Arc::new(FakePorts::default());
        ports.mark_dirty(18);
        let barrier = barrier(ports.clone());

        barrier.on_cleanup_non_delete(HandleDrainRequest { ino: 18, fh: 2 });
        barrier.on_cleanup_non_delete(HandleDrainRequest { ino: 18, fh: 2 });

        assert_eq!(ports.cleanup_callbacks.load(Ordering::Relaxed), 2);
        assert_eq!(ports.flush_calls.lock().len(), 1);
        assert_eq!(
            ports.cleanup_barrier_outcomes.lock().as_slice(),
            &[true, false]
        );
        assert!(ports.flush_barrier_outcomes.lock().is_empty());
    }

    #[test]
    fn concurrent_flush_callbacks_coalesce_to_one_barrier() {
        let ports = Arc::new(FakePorts::default());
        ports.mark_dirty(19);
        let barrier = Arc::new(barrier(ports.clone()));

        let handles: Vec<_> = (0..2)
            .map(|_| {
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier
                        .on_flush(HandleDrainRequest { ino: 19, fh: 6 })
                        .expect("flush should succeed");
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("thread should finish");
        }

        assert_eq!(ports.flush_calls.lock().len(), 1);
        assert_eq!(
            ports.flush_barrier_outcomes.lock().as_slice(),
            &[true, false]
        );
    }

    #[test]
    fn cleanup_with_fh_zero_does_not_record_hint() {
        let ports = Arc::new(FakePorts::default());
        ports.mark_dirty(20);
        let barrier = barrier(ports.clone());

        barrier.on_cleanup_non_delete(HandleDrainRequest { ino: 20, fh: 0 });

        assert!(!ports.has_deferred_cleanup_hint(20));
    }

    #[test]
    fn delete_cleanup_routes_through_delete_port() {
        let ports = Arc::new(FakePorts::default());
        let barrier = barrier(ports.clone());
        let request = HandleDrainDeleteRequest {
            ino: 21,
            parent_ino: 1,
            name: b"delete-me".to_vec(),
            is_dir: false,
        };

        barrier.on_cleanup_delete(request.clone());

        assert_eq!(ports.cleanup_callbacks.load(Ordering::Relaxed), 1);
        assert_eq!(ports.delete_requests.lock().as_slice(), &[request]);
    }
}
