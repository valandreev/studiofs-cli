use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::{debug, error, trace, warn};
use winfsp::U16CStr;

use super::{
    Ino, StudioFileHandle, StudioFsContext, is_benign_cleanup_delete_error, resolve_parent,
};

const FSP_CLEANUP_DELETE: u32 = 0x01;

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
        self.context.vfs.note_winfsp_flush_callback();
        let flushed = if self.requires_flush_barrier(ino) {
            self.with_flush_gate(ino, || {
                if !self.requires_flush_barrier(ino) {
                    return Ok(false);
                }
                let flushed = self.flush_local_dirty_state(ino, fh)?;
                self.context
                    .callback_barrier_state
                    .clear_deferred_cleanup_hint(ino);
                Ok(flushed)
            })?
        } else {
            false
        };
        self.context.vfs.note_winfsp_flush_barrier_outcome(flushed);
        Ok(())
    }

    pub(super) fn close(&self, ino: Ino, fh: u64) {
        self.context.vfs.note_winfsp_close_callback();
        if fh == 0 {
            return;
        }

        let outcome = self
            .context
            .vfs
            .release_fh_with_deferred_last_handle_barrier(fh, &self.context.rt);
        if outcome.closed_last_handle {
            let hint_was_taken = self
                .context
                .callback_barrier_state
                .take_deferred_cleanup_hint(ino);
            self.context
                .vfs
                .note_winfsp_close_barrier_outcome(outcome.flushed_local_dirty_state);
            if hint_was_taken && outcome.flushed_local_dirty_state {
                debug!(
                    ino,
                    fh, "close scheduled deferred cleanup barrier on the last handle"
                );
            } else if hint_was_taken {
                trace!(
                    ino,
                    fh,
                    "close observed deferred cleanup hint but no last-handle barrier was needed"
                );
            }
        }
    }

    pub(super) fn cleanup_non_delete(&self, ino: Ino, fh: u64) {
        self.context.vfs.note_winfsp_cleanup_callback();
        if fh != 0 && self.context.vfs.inode_has_local_dirty_state(ino) {
            self.context
                .callback_barrier_state
                .set_deferred_cleanup_hint(ino);
        }
    }

    pub(super) fn cleanup(
        &self,
        context: &StudioFileHandle,
        file_name: Option<&U16CStr>,
        flags: u32,
    ) {
        if flags & FSP_CLEANUP_DELETE == 0 {
            self.cleanup_non_delete(context.ino, context.fh);
            return;
        }

        let Some(file_name) = file_name else {
            self.context.vfs.note_winfsp_cleanup_callback();
            debug!(
                ino = context.ino,
                is_dir = context.is_dir,
                "cleanup delete skipped: no path provided"
            );
            return;
        };
        let (parent_ino, name) = match resolve_parent(&self.context.vfs, file_name) {
            Ok(resolved) => resolved,
            Err(error) => {
                self.context.vfs.note_winfsp_cleanup_callback();
                warn!(
                    ino = context.ino,
                    is_dir = context.is_dir,
                    error = ?error,
                    "cleanup delete skipped: failed to resolve parent"
                );
                return;
            }
        };
        self.cleanup_delete(context.ino, parent_ino, name, context.is_dir);
    }

    fn flush_local_dirty_state(&self, ino: Ino, fh: u64) -> studiofs_vfs::VfsResult<bool> {
        let vfs = self.context.vfs.clone();
        let outcome = self.context.spawn_wait(async move {
            // WinFSP's FlushFileBuffers callback is the platform writeback
            // boundary. Keep it volatile; the explicit fsync API remains the
            // strict local durability boundary.
            vfs.flush_volatile_writeback_with_outcome(ino, fh).await
        })?;
        Ok(outcome.flushed_local_dirty_state)
    }

    fn requires_flush_barrier(&self, ino: Ino) -> bool {
        self.context.vfs.inode_has_local_dirty_state(ino)
            || self
                .context
                .callback_barrier_state
                .has_deferred_cleanup_hint(ino)
    }

    fn with_flush_gate<T, F>(&self, ino: Ino, op: F) -> studiofs_vfs::VfsResult<T>
    where
        F: FnOnce() -> studiofs_vfs::VfsResult<T>,
    {
        // Tokio recommends a blocking mutex for plain data when the guard is
        // not held across `.await`. This gate lives entirely on the synchronous
        // WinFSP callback thread and only serializes per-inode barrier entry
        // into `spawn_wait`; the async flush work itself runs after the gate is
        // acquired and never holds this guard across an `.await`.
        let gate = self
            .context
            .callback_barrier_state
            .flush_gate_for_inode(ino);
        // Keep this guard before `_flush_guard`: reverse declaration drop order
        // unlocks the mutex before registry cleanup can remove this gate.
        let _gate_release =
            FlushGateRelease::new(&self.context.callback_barrier_state, ino, gate.clone());
        let _flush_guard = gate.lock();
        op()
    }

    fn cleanup_delete(&self, ino: Ino, parent_ino: Ino, name: Vec<u8>, is_dir: bool) {
        self.context.vfs.note_winfsp_cleanup_callback();
        let healed_orphans = if is_dir {
            self.context
                .heal_directory_orphans_for_delete(ino)
                .unwrap_or_else(|error| {
                    warn!(
                        ino,
                        error = ?error,
                        "failed to heal orphan children before directory cleanup delete"
                    );
                    0
                })
        } else {
            0
        };

        let result = if is_dir {
            self.context.vfs.rmdir_for_cleanup(parent_ino, &name)
        } else {
            self.context.vfs.unlink(parent_ino, &name)
        };

        match result {
            Ok(()) => {
                debug!(ino, healed_orphans, "cleanup delete");
            }
            Err(error) if is_benign_cleanup_delete_error(&error, is_dir) => {
                debug!(
                    ino,
                    is_dir,
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

        facade.cleanup_non_delete(attr.ino, fh);
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
        .expect("seed dirty payloads");

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
                WinfspHandleDrainFacade::new(ctx.as_ref()).cleanup_non_delete(ino, fh);
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
    fn concurrent_dirty_flush_callbacks_preserve_volatile_barrier_contract() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("flush_coalesce_dirty");
        let attr = vfs
            .create(ROOT_INO, b"flush-coalesce-dirty.txt", 0, 0, 0o644)
            .expect("create");
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDWR, false)
            .expect("open_file");

        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.write(attr.ino, 0, b"dirty").await }
        })
        .expect("dirty write");

        let callbacks_before = winfsp_callbacks(&vfs);
        let barriers_before = winfsp_foreground_flushes(&vfs);
        let flush_calls_before = flush_barrier_calls(&vfs);
        let ctx = Arc::new(ctx);
        let start = Arc::new(Barrier::new(3));
        let mut joins = Vec::new();
        for _ in 0..2 {
            let ctx = Arc::clone(&ctx);
            let start = Arc::clone(&start);
            let ino = attr.ino;
            joins.push(std::thread::spawn(move || {
                start.wait();
                WinfspHandleDrainFacade::new(ctx.as_ref())
                    .flush(ino, fh)
                    .expect("concurrent flush callback");
            }));
        }
        start.wait();
        for join in joins {
            join.join().expect("flush thread join");
        }

        let callbacks_after = winfsp_callbacks(&vfs);
        let barriers_after = winfsp_foreground_flushes(&vfs);
        assert_eq!(
            callbacks_after.flush,
            callbacks_before.flush + 2,
            "both concurrent flush callbacks should still be observable"
        );
        assert_eq!(
            barriers_after.flush,
            barriers_before.flush + 2,
            "both dirty callbacks should be recorded as volatile flush outcomes"
        );
        assert_eq!(
            barriers_after.flush_skipped_clean, barriers_before.flush_skipped_clean,
            "dirty volatile callbacks should not be recorded as clean skips"
        );
        assert_eq!(
            flush_barrier_calls(&vfs),
            flush_calls_before,
            "volatile callbacks should not trigger a strict local barrier"
        );
        assert_eq!(
            ctx.flush_gate_count(),
            0,
            "concurrent callbacks should release the per-inode gate"
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

        facade.cleanup_non_delete(attr.ino, fh);
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

        facade.cleanup_non_delete(attr.ino, 0);
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
    fn fh_zero_close_does_not_clear_another_handle_hint() {
        let (_tmp, vfs, ctx, _queue) = make_test_context("fh_zero_close_preserves_hint");
        let attr = vfs
            .create(ROOT_INO, b"fh-zero-close-preserves-hint.txt", 0, 0, 0o644)
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
        facade.cleanup_non_delete(attr.ino, fh);
        assert!(ctx.has_deferred_cleanup_hint(attr.ino));

        facade.close(attr.ino, 0);
        assert!(
            ctx.has_deferred_cleanup_hint(attr.ino),
            "fh==0 close must not clear a hint owned by a real handle"
        );

        facade.flush(attr.ino, fh).expect("flush callback");
        ctx.spawn_wait({
            let vfs = vfs.clone();
            async move { vfs.release_fh(fh).await }
        })
        .expect("release_fh");
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

        facade.cleanup_non_delete(attr.ino, fh);
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
