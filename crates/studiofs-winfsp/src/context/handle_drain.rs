use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use studiofs_vfs::vfs::{ExplicitFlushOutcome, ReleaseFhOutcome};
use studiofs_vfs::{
    BarrierObservabilityPort, DeferredCleanupHintPort, DeleteCleanupPort, FlushGatePort,
    HandleDrainBarrier, HandleDrainDeleteRequest, HandleDrainRequest, HandleReleasePort,
    LocalSyncBarrierPort,
};
use tracing::{debug, error, trace, warn};
use winfsp::U16CStr;

use super::{
    Ino, StudioFileHandle, StudioFsContext, is_benign_cleanup_delete_error, resolve_parent,
};

pub(super) struct CallbackBarrierState {
    flush_gates: FlushGateRegistry,
    deferred_cleanup_hints: DeferredCleanupHints,
}

impl CallbackBarrierState {
    pub(super) fn new() -> Self {
        Self {
            flush_gates: FlushGateRegistry::new(),
            deferred_cleanup_hints: DeferredCleanupHints::new(),
        }
    }

    fn flush_gate_for_inode(&self, ino: Ino) -> Arc<Mutex<()>> {
        self.flush_gates.gate_for_inode(ino)
    }

    fn maybe_release_flush_gate(&self, ino: Ino, gate: &Arc<Mutex<()>>) {
        self.flush_gates.maybe_release(ino, gate);
    }

    fn set_deferred_cleanup_hint(&self, ino: Ino) {
        self.deferred_cleanup_hints.set(ino);
    }

    pub(super) fn clear_deferred_cleanup_hint(&self, ino: Ino) {
        self.deferred_cleanup_hints.clear(ino);
    }

    fn take_deferred_cleanup_hint(&self, ino: Ino) -> bool {
        self.deferred_cleanup_hints.take(ino)
    }

    pub(super) fn has_deferred_cleanup_hint(&self, ino: Ino) -> bool {
        self.deferred_cleanup_hints.contains(ino)
    }

    #[cfg(test)]
    pub(super) fn flush_gate_count(&self) -> usize {
        self.flush_gates.len()
    }
}

pub(super) struct WinfspHandleDrainFacade<'a> {
    context: &'a StudioFsContext,
}

impl<'a> WinfspHandleDrainFacade<'a> {
    pub(super) fn new(context: &'a StudioFsContext) -> Self {
        Self { context }
    }

    pub(super) fn flush(&self, ino: Ino, fh: u64) -> studiofs_vfs::VfsResult<()> {
        self.barrier().on_flush(HandleDrainRequest { ino, fh })
    }

    pub(super) fn close(&self, ino: Ino, fh: u64) {
        self.barrier().on_close(HandleDrainRequest { ino, fh });
    }

    pub(super) fn cleanup_non_delete(&self, ino: Ino, fh: u64) -> studiofs_vfs::VfsResult<()> {
        self.barrier()
            .on_cleanup_non_delete(HandleDrainRequest { ino, fh });
        Ok(())
    }

    pub(super) fn cleanup(
        &self,
        context: &StudioFileHandle,
        file_name: Option<&U16CStr>,
        flags: u32,
    ) {
        let action = self.resolve_cleanup_callback_action(context, file_name, flags);
        self.execute_cleanup_callback_action(action);
    }

    #[cfg(test)]
    pub(super) fn flush_callback_path_if_dirty(
        &self,
        ino: Ino,
        fh: Option<u64>,
    ) -> studiofs_vfs::VfsResult<bool> {
        let local_sync = WinfspLocalSyncBarrierPort {
            context: self.context,
        };
        WinfspFlushGatePort {
            context: self.context,
        }
        .with_flush_gate(ino, || {
            if !local_sync.inode_has_local_dirty_state(ino) {
                return Ok(false);
            }
            let outcome = local_sync.explicit_flush(HandleDrainRequest {
                ino,
                fh: fh.unwrap_or(0),
            })?;
            Ok(outcome.flushed_local_dirty_state)
        })
    }

    fn barrier(&self) -> WinfspHandleDrainBarrier<'_> {
        HandleDrainBarrier::new(
            WinfspLocalSyncBarrierPort {
                context: self.context,
            },
            WinfspHandleReleasePort {
                context: self.context,
            },
            WinfspDeferredCleanupHintPort {
                context: self.context,
            },
            WinfspFlushGatePort {
                context: self.context,
            },
            WinfspDeleteCleanupPort {
                context: self.context,
            },
            WinfspBarrierObservabilityPort {
                context: self.context,
            },
        )
    }

    fn resolve_cleanup_callback_action(
        &self,
        context: &StudioFileHandle,
        file_name: Option<&U16CStr>,
        flags: u32,
    ) -> CleanupCallbackAction {
        if flags & 0x01 == 0 {
            return CleanupCallbackAction::NonDelete {
                ino: context.ino,
                fh: context.fh,
            };
        }

        if let Some(file_name) = file_name
            && let Ok((parent_ino, name)) = resolve_parent(&self.context.vfs, file_name)
        {
            return CleanupCallbackAction::Delete(HandleDrainDeleteRequest {
                ino: context.ino,
                parent_ino,
                name,
                is_dir: context.is_dir,
            });
        }

        CleanupCallbackAction::SkipDeletePath {
            ino: context.ino,
            is_dir: context.is_dir,
        }
    }

    fn execute_cleanup_callback_action(&self, action: CleanupCallbackAction) {
        match action {
            CleanupCallbackAction::Delete(request) => {
                self.barrier().on_cleanup_delete(request);
            }
            CleanupCallbackAction::SkipDeletePath { ino, is_dir } => {
                self.context.vfs.note_winfsp_cleanup_callback();
                debug!(ino, is_dir, "cleanup delete skipped: no path provided");
            }
            CleanupCallbackAction::NonDelete { ino, fh } => {
                let _ = self.cleanup_non_delete(ino, fh);
            }
        }
    }
}

struct FlushGateRegistry {
    /// Per-inode gates used to collapse concurrent callback-driven flush
    /// barriers into one effective in-flight flush.
    gates: Mutex<HashMap<Ino, Arc<Mutex<()>>>>,
}

struct DeferredCleanupHints {
    /// Inodes whose non-delete `cleanup` observed dirty local state and
    /// intentionally deferred the barrier to a later explicit `flush` or
    /// last-handle `close`/`release_fh`.
    inodes: Mutex<HashSet<Ino>>,
}

impl FlushGateRegistry {
    fn new() -> Self {
        Self {
            gates: Mutex::new(HashMap::new()),
        }
    }

    fn gate_for_inode(&self, ino: Ino) -> Arc<Mutex<()>> {
        self.gates
            .lock()
            .entry(ino)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn maybe_release(&self, ino: Ino, gate: &Arc<Mutex<()>>) {
        let mut gates = self.gates.lock();
        let should_remove = gates.get(&ino).is_some_and(|current| {
            // During `FlushGateRelease::drop`, the map entry, the caller's local
            // `gate`, and `self.gate` are the only remaining strong refs when no
            // other callbacks are waiting on this inode gate.
            Arc::ptr_eq(current, gate) && Arc::strong_count(current) == 3
        });
        if should_remove {
            gates.remove(&ino);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.gates.lock().len()
    }
}

impl DeferredCleanupHints {
    fn new() -> Self {
        Self {
            inodes: Mutex::new(HashSet::new()),
        }
    }

    fn set(&self, ino: Ino) {
        self.inodes.lock().insert(ino);
    }

    fn clear(&self, ino: Ino) {
        self.inodes.lock().remove(&ino);
    }

    fn take(&self, ino: Ino) -> bool {
        self.inodes.lock().remove(&ino)
    }

    fn contains(&self, ino: Ino) -> bool {
        self.inodes.lock().contains(&ino)
    }
}

struct FlushGateRelease<'a> {
    state: &'a CallbackBarrierState,
    ino: Ino,
    gate: Arc<Mutex<()>>,
}

impl<'a> FlushGateRelease<'a> {
    fn new(state: &'a CallbackBarrierState, ino: Ino, gate: Arc<Mutex<()>>) -> Self {
        Self { state, ino, gate }
    }
}

impl Drop for FlushGateRelease<'_> {
    fn drop(&mut self) {
        self.state.maybe_release_flush_gate(self.ino, &self.gate);
    }
}

enum CleanupCallbackAction {
    Delete(HandleDrainDeleteRequest),
    SkipDeletePath { ino: Ino, is_dir: bool },
    NonDelete { ino: Ino, fh: u64 },
}

type WinfspHandleDrainBarrier<'a> = HandleDrainBarrier<
    WinfspLocalSyncBarrierPort<'a>,
    WinfspHandleReleasePort<'a>,
    WinfspDeferredCleanupHintPort<'a>,
    WinfspFlushGatePort<'a>,
    WinfspDeleteCleanupPort<'a>,
    WinfspBarrierObservabilityPort<'a>,
>;

#[derive(Clone, Copy)]
struct WinfspLocalSyncBarrierPort<'a> {
    context: &'a StudioFsContext,
}

#[derive(Clone, Copy)]
struct WinfspHandleReleasePort<'a> {
    context: &'a StudioFsContext,
}

#[derive(Clone, Copy)]
struct WinfspDeferredCleanupHintPort<'a> {
    context: &'a StudioFsContext,
}

#[derive(Clone, Copy)]
struct WinfspFlushGatePort<'a> {
    context: &'a StudioFsContext,
}

#[derive(Clone, Copy)]
struct WinfspDeleteCleanupPort<'a> {
    context: &'a StudioFsContext,
}

#[derive(Clone, Copy)]
struct WinfspBarrierObservabilityPort<'a> {
    context: &'a StudioFsContext,
}

impl LocalSyncBarrierPort for WinfspLocalSyncBarrierPort<'_> {
    fn inode_has_local_dirty_state(&self, ino: Ino) -> bool {
        self.context.vfs.inode_has_local_dirty_state(ino)
    }

    fn explicit_flush(
        &self,
        request: HandleDrainRequest,
    ) -> studiofs_vfs::VfsResult<ExplicitFlushOutcome> {
        let vfs = self.context.vfs.clone();
        self.context.spawn_wait(async move {
            if request.fh != 0 {
                vfs.flush_fh_with_outcome(request.fh).await
            } else {
                vfs.flush_with_outcome(request.ino).await
            }
        })
    }
}

impl HandleReleasePort for WinfspHandleReleasePort<'_> {
    fn release_handle(
        &self,
        request: HandleDrainRequest,
    ) -> studiofs_vfs::VfsResult<ReleaseFhOutcome> {
        let vfs = self.context.vfs.clone();
        self.context
            .spawn_wait(async move { vfs.release_fh_with_outcome(request.fh).await })
    }
}

impl DeferredCleanupHintPort for WinfspDeferredCleanupHintPort<'_> {
    fn set_deferred_cleanup_hint(&self, ino: Ino) {
        self.context
            .callback_barrier_state
            .set_deferred_cleanup_hint(ino);
    }

    fn clear_deferred_cleanup_hint(&self, ino: Ino) {
        self.context
            .callback_barrier_state
            .clear_deferred_cleanup_hint(ino);
    }

    fn take_deferred_cleanup_hint(&self, ino: Ino) -> bool {
        self.context
            .callback_barrier_state
            .take_deferred_cleanup_hint(ino)
    }

    fn has_deferred_cleanup_hint(&self, ino: Ino) -> bool {
        self.context
            .callback_barrier_state
            .has_deferred_cleanup_hint(ino)
    }
}

impl FlushGatePort for WinfspFlushGatePort<'_> {
    fn with_flush_gate<T, F>(&self, ino: Ino, op: F) -> studiofs_vfs::VfsResult<T>
    where
        F: FnOnce() -> studiofs_vfs::VfsResult<T>,
    {
        if !self.context.vfs.inode_has_local_dirty_state(ino) {
            return op();
        }

        // Tokio recommends a blocking mutex for plain data when the guard is
        // not held across `.await`. This gate lives entirely on the synchronous
        // WinFSP callback thread and only serializes per-inode barrier entry
        // into `spawn_wait`; the async flush work itself runs after the gate is
        // acquired and never holds this guard across an `.await`.
        let gate = self
            .context
            .callback_barrier_state
            .flush_gate_for_inode(ino);
        let gate_for_release = gate.clone();
        let _flush_guard = gate.lock();
        let _gate_release =
            FlushGateRelease::new(&self.context.callback_barrier_state, ino, gate_for_release);
        op()
    }
}

impl DeleteCleanupPort for WinfspDeleteCleanupPort<'_> {
    fn cleanup_delete(&self, request: HandleDrainDeleteRequest) {
        let healed_orphans = if request.is_dir {
            match self.context.heal_directory_orphans_for_delete(request.ino) {
                Ok(count) => count,
                Err(error) => {
                    warn!(
                        ino = request.ino,
                        error = ?error,
                        "failed to heal orphan children before directory cleanup delete"
                    );
                    0
                }
            }
        } else {
            0
        };

        let result = if request.is_dir {
            self.context.vfs.rmdir(request.parent_ino, &request.name)
        } else {
            self.context.vfs.unlink(request.parent_ino, &request.name)
        };

        match result {
            Ok(()) => {
                debug!(ino = request.ino, healed_orphans, "cleanup delete");
            }
            Err(error) if is_benign_cleanup_delete_error(&error, request.is_dir) => {
                debug!(
                    ino = request.ino,
                    is_dir = request.is_dir,
                    healed_orphans,
                    error = ?error,
                    "cleanup delete skipped stale race"
                );
            }
            Err(error) => {
                error!("cleanup delete failed: {:?}", error);
            }
        }
    }
}

impl BarrierObservabilityPort for WinfspBarrierObservabilityPort<'_> {
    fn note_cleanup_callback(&self) {
        self.context.vfs.note_winfsp_cleanup_callback();
    }

    fn note_flush_callback(&self) {
        self.context.vfs.note_winfsp_flush_callback();
    }

    fn note_flush_barrier_outcome(&self, flushed_local_dirty_state: bool) {
        self.context
            .vfs
            .note_winfsp_flush_barrier_outcome(flushed_local_dirty_state);
    }

    fn note_close_callback(&self) {
        self.context.vfs.note_winfsp_close_callback();
    }

    fn note_release_success(
        &self,
        request: HandleDrainRequest,
        hint_was_taken: bool,
        outcome: ReleaseFhOutcome,
    ) {
        self.context
            .vfs
            .note_winfsp_close_barrier_outcome(outcome.flushed_local_dirty_state);
        if hint_was_taken && outcome.flushed_local_dirty_state {
            debug!(
                ino = request.ino,
                fh = request.fh,
                "close owned deferred cleanup barrier on the last handle"
            );
        } else if hint_was_taken {
            trace!(
                ino = request.ino,
                fh = request.fh,
                "close observed deferred cleanup hint but no last-handle barrier was needed"
            );
        }
    }

    fn note_release_error(
        &self,
        request: HandleDrainRequest,
        cleanup_deferred_hint: bool,
        error: &studiofs_vfs::VfsError,
    ) {
        error!(
            ino = request.ino,
            fh = request.fh,
            cleanup_deferred = cleanup_deferred_hint,
            "release_fh failed after WinFSP close: {:?}",
            error
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use studiofs_meta::ROOT_INO;
    use studiofs_meta::types::{SetattrArgs, TimeSpec};
    use studiofs_vfs::{OpenFlags, VfsError};

    use super::super::tests::{
        flush_barrier_calls, make_test_context, winfsp_callbacks, winfsp_foreground_flushes,
    };
    use super::WinfspHandleDrainFacade;

    #[test]
    fn cleanup_non_delete_on_write_committed_inode_skips_deferred_release_hint() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_defer_dirty");
        let attr = vfs
            .create(ROOT_INO, b"dirty-callback.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");
        let facade = WinfspHandleDrainFacade::new(&ctx);

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");
        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.flush(attr.ino).await }
        })
        .expect("commit dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);
        let flush_calls_before = flush_barrier_calls(&vfs);

        facade
            .cleanup_non_delete(attr.ino, fh)
            .expect("cleanup callback");
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "cleanup should not leave a deferred-close hint once write already committed local state"
        );
        let callbacks_after_cleanup = winfsp_callbacks(&vfs);
        let barriers_after_cleanup = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_cleanup = flush_barrier_calls(&vfs);
        assert_eq!(
            callbacks_after_cleanup.cleanup,
            callbacks_before.cleanup + 1,
            "cleanup callback should still be counted for dirty handles"
        );
        assert_eq!(
            barriers_after_cleanup.cleanup, barriers_before.cleanup,
            "non-delete cleanup should no longer own a foreground barrier"
        );
        assert_eq!(
            barriers_after_cleanup.cleanup_skipped_clean, barriers_before.cleanup_skipped_clean,
            "dirty cleanup should not be attributed as a clean-skip barrier either"
        );
        assert_eq!(
            flush_calls_after_cleanup, flush_calls_before,
            "cleanup should not flush local dirty state before close"
        );

        facade.close(attr.ino, fh);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "final close should clear the deferred cleanup hint"
        );
        let callbacks_after_close = winfsp_callbacks(&vfs);
        let barriers_after_close = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_close = flush_barrier_calls(&vfs);
        assert_eq!(
            callbacks_after_close.close,
            callbacks_before.close + 1,
            "close should still be counted after deferred cleanup"
        );
        assert_eq!(
            barriers_after_close.close, barriers_before.close,
            "last-handle close should not claim a barrier once write already committed local state"
        );
        assert_eq!(
            barriers_after_close.close_skipped_clean,
            barriers_before.close_skipped_clean + 1,
            "last-handle close should be attributed as a clean skip"
        );
        assert_eq!(
            flush_calls_after_close, flush_calls_before,
            "close should not trigger another ordered flush after write already committed local state"
        );
    }

    #[test]
    fn concurrent_cleanup_callbacks_do_not_add_foreground_flushes() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_defer_concurrent");
        let attr = vfs
            .create(ROOT_INO, b"coalesce.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move {
                for i in 0..16u64 {
                    let offset = i * (4 * 1024 * 1024);
                    vfs.write(attr.ino, offset, b"x").await?;
                }
                Ok::<(), VfsError>(())
            }
        })
        .expect("seed dirty blocks");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);
        let flush_calls_before = flush_barrier_calls(&vfs);
        let ctx = Arc::new(ctx);
        let start = Arc::new(Barrier::new(4));
        let mut joins = Vec::new();
        for _ in 0..4 {
            let ctx = Arc::clone(&ctx);
            let start = Arc::clone(&start);
            let ino = attr.ino;
            joins.push(std::thread::spawn(move || {
                start.wait();
                WinfspHandleDrainFacade::new(ctx.as_ref())
                    .cleanup_non_delete(ino, fh)
                    .expect("concurrent cleanup callback");
            }));
        }
        for join in joins {
            join.join().expect("cleanup thread join");
        }
        let callbacks_after_cleanup = winfsp_callbacks(&vfs);
        let barriers_after_cleanup = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_cleanup = flush_barrier_calls(&vfs);
        assert_eq!(
            callbacks_after_cleanup.cleanup,
            callbacks_before.cleanup + 4,
            "all concurrent cleanup callbacks should still be observable"
        );
        assert_eq!(
            barriers_after_cleanup.cleanup, barriers_before.cleanup,
            "cleanup callbacks should not add foreground barriers"
        );
        assert_eq!(
            barriers_after_cleanup.cleanup_skipped_clean, barriers_before.cleanup_skipped_clean,
            "cleanup callbacks should not add clean-skip barrier attribution"
        );
        assert_eq!(
            flush_calls_after_cleanup, flush_calls_before,
            "cleanup callbacks should not trigger foreground flush work"
        );

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.release_fh(fh).await }
        })
        .expect("release_fh");
    }

    #[test]
    fn cleanup_then_flush_then_close_stays_clean_after_write_commit() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_flush_close_clean_skip");
        let attr = vfs
            .create(ROOT_INO, b"cleanup-flush-close-clean-skip.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");
        let facade = WinfspHandleDrainFacade::new(&ctx);

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");
        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.flush(attr.ino).await }
        })
        .expect("commit dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);
        let flush_calls_before = flush_barrier_calls(&vfs);

        facade
            .cleanup_non_delete(attr.ino, fh)
            .expect("cleanup callback");
        let callbacks_after_cleanup = winfsp_callbacks(&vfs);
        let barriers_after_cleanup = winfsp_foreground_flushes(&vfs);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "cleanup should not leave a deferred-close hint once write already committed local state"
        );
        assert_eq!(
            callbacks_after_cleanup.cleanup,
            callbacks_before.cleanup + 1,
            "cleanup should be observable in the callback counters"
        );
        assert_eq!(
            barriers_after_cleanup.cleanup, barriers_before.cleanup,
            "cleanup should not claim a foreground barrier before the explicit flush"
        );

        facade.flush(attr.ino, fh).expect("explicit flush callback");
        let callbacks_after_flush = winfsp_callbacks(&vfs);
        let barriers_after_flush = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_flush = flush_barrier_calls(&vfs);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "explicit flush should clear the deferred cleanup hint before close"
        );
        assert_eq!(
            callbacks_after_flush.flush,
            callbacks_before.flush + 1,
            "explicit flush should be counted as a WinFSP flush callback"
        );
        assert_eq!(
            barriers_after_flush.flush, barriers_before.flush,
            "explicit flush should not own a barrier once write already committed local state"
        );
        assert_eq!(
            barriers_after_flush.flush_skipped_clean,
            barriers_before.flush_skipped_clean + 1,
            "explicit flush should be attributed as a clean skip"
        );
        assert_eq!(
            flush_calls_after_flush, flush_calls_before,
            "explicit flush should not drain the inode again after write already committed local state"
        );

        facade.close(attr.ino, fh);
        let callbacks_after_close = winfsp_callbacks(&vfs);
        let barriers_after_close = winfsp_foreground_flushes(&vfs);
        let flush_calls_after_close = flush_barrier_calls(&vfs);
        assert_eq!(
            callbacks_after_close.close,
            callbacks_before.close + 1,
            "close should still be observed after cleanup and explicit flush"
        );
        assert_eq!(
            barriers_after_close.close, barriers_before.close,
            "final close should not claim a fallback barrier once explicit flush drained the inode"
        );
        assert_eq!(
            barriers_after_close.close_skipped_clean,
            barriers_before.close_skipped_clean + 1,
            "final close should be attributed as a clean skip after the explicit flush"
        );
        assert_eq!(
            flush_calls_after_close, flush_calls_after_flush,
            "final close should not trigger another ordered flush after the explicit flush"
        );
        assert_eq!(
            vfs.open_count(attr.ino),
            0,
            "close should still release the last handle after the explicit flush path"
        );
    }

    #[test]
    fn dirty_flush_callback_releases_inode_flush_gate_after_completion() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("flush-gate-release");
        let attr = vfs
            .create(ROOT_INO, b"flush-gate-release.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");
        let facade = WinfspHandleDrainFacade::new(&ctx);

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        assert_eq!(
            ctx.flush_gate_count(),
            0,
            "test setup should start gate-free"
        );
        facade.flush(attr.ino, fh).expect("explicit flush callback");
        assert_eq!(
            ctx.flush_gate_count(),
            0,
            "flush callback should release the per-inode gate after the barrier completes"
        );

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.release_fh(fh).await }
        })
        .expect("release_fh");
    }

    #[test]
    fn cleanup_non_delete_with_fh_zero_does_not_record_deferred_close_hint() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_hint_skips_fh_zero");
        let attr = vfs
            .mkdir(ROOT_INO, b"cleanup-hint-dir", 0, 0, 0o755)
            .expect("mkdir");
        let facade = WinfspHandleDrainFacade::new(&ctx);

        vfs.setattr(
            attr.ino,
            SetattrArgs {
                mtime: TimeSpec::Now,
                ..Default::default()
            },
        )
        .expect("setattr");

        facade
            .cleanup_non_delete(attr.ino, 0)
            .expect("cleanup callback");
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "fh==0 cleanup should not leave a deferred-close hint"
        );

        facade.close(attr.ino, 0);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "fh==0 close should not retain deferred-cleanup state"
        );
    }

    #[test]
    fn duplicate_close_after_clean_cleanup_path_is_a_noop() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("cleanup_close_duplicate_close");
        let attr = vfs
            .create(ROOT_INO, b"cleanup-close-duplicate-close.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");
        let facade = WinfspHandleDrainFacade::new(&ctx);

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");
        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.flush(attr.ino).await }
        })
        .expect("commit dirty write");

        facade
            .cleanup_non_delete(attr.ino, fh)
            .expect("cleanup callback");
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "cleanup should not leave a deferred-close hint once write already committed local state"
        );

        let flush_calls_before = flush_barrier_calls(&vfs);
        let close_barriers_before = winfsp_foreground_flushes(&vfs);

        facade.close(attr.ino, fh);

        let flush_calls_after_first_close = flush_barrier_calls(&vfs);
        let close_barriers_after_first_close = winfsp_foreground_flushes(&vfs);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "the first close should consume the deferred-cleanup hint"
        );
        assert_eq!(
            vfs.open_count(attr.ino),
            0,
            "the first close should release the last handle"
        );
        assert_eq!(
            flush_calls_after_first_close, flush_calls_before,
            "the first close should not drain local state again after write already committed it"
        );
        assert_eq!(
            close_barriers_after_first_close.close, close_barriers_before.close,
            "the first close should not own a deferred cleanup barrier"
        );
        assert_eq!(
            close_barriers_after_first_close.close_skipped_clean,
            close_barriers_before.close_skipped_clean + 1,
            "the first close should be attributed as a clean skip"
        );

        facade.close(attr.ino, fh);

        let flush_calls_after_second_close = flush_barrier_calls(&vfs);
        let close_barriers_after_second_close = winfsp_foreground_flushes(&vfs);
        assert!(
            !ctx.has_deferred_cleanup_hint(attr.ino),
            "duplicate close must not resurrect deferred-cleanup state"
        );
        assert_eq!(
            flush_calls_after_second_close, flush_calls_after_first_close,
            "duplicate close should not trigger another ordered flush"
        );
        assert_eq!(
            close_barriers_after_second_close.close, close_barriers_after_first_close.close,
            "duplicate close should not claim another last-handle barrier"
        );
    }
}
