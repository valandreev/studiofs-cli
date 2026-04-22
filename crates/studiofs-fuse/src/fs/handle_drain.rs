use super::StudioFuseFs;
use studiofs_meta::types::Ino;
use studiofs_vfs::vfs::{ExplicitFlushOutcome, ReleaseFhOutcome};
use studiofs_vfs::{
    BarrierObservabilityPort, DeferredCleanupHintPort, DeleteCleanupPort, FlushGatePort,
    HandleDrainBarrier, HandleDrainDeleteRequest, HandleDrainRequest, HandleReleasePort,
    LocalSyncBarrierPort,
};

pub(super) struct FuseHandleDrainFacade<'a> {
    fs: &'a StudioFuseFs,
}

impl<'a> FuseHandleDrainFacade<'a> {
    pub(super) fn new(fs: &'a StudioFuseFs) -> Self {
        Self { fs }
    }

    pub(super) fn flush(&self, ino: Ino, fh: u64) -> studiofs_vfs::VfsResult<()> {
        self.barrier().on_flush(HandleDrainRequest { ino, fh })
    }

    pub(super) fn release(&self, ino: Ino, fh: u64) -> studiofs_vfs::VfsResult<()> {
        self.barrier().on_release(HandleDrainRequest { ino, fh })
    }

    fn barrier(&self) -> FuseHandleDrainBarrier<'_> {
        HandleDrainBarrier::new(
            FuseLocalSyncBarrierPort { fs: self.fs },
            FuseHandleReleasePort { fs: self.fs },
            FuseDeferredCleanupHintPort,
            FuseFlushGatePort,
            FuseDeleteCleanupPort,
            FuseBarrierObservabilityPort,
        )
    }
}

type FuseHandleDrainBarrier<'a> = HandleDrainBarrier<
    FuseLocalSyncBarrierPort<'a>,
    FuseHandleReleasePort<'a>,
    FuseDeferredCleanupHintPort,
    FuseFlushGatePort,
    FuseDeleteCleanupPort,
    FuseBarrierObservabilityPort,
>;

#[derive(Clone, Copy)]
struct FuseLocalSyncBarrierPort<'a> {
    fs: &'a StudioFuseFs,
}

#[derive(Clone, Copy)]
struct FuseHandleReleasePort<'a> {
    fs: &'a StudioFuseFs,
}

#[derive(Clone, Copy)]
struct FuseDeferredCleanupHintPort;

#[derive(Clone, Copy)]
struct FuseFlushGatePort;

#[derive(Clone, Copy)]
struct FuseDeleteCleanupPort;

#[derive(Clone, Copy)]
struct FuseBarrierObservabilityPort;

impl LocalSyncBarrierPort for FuseLocalSyncBarrierPort<'_> {
    fn inode_has_local_dirty_state(&self, ino: Ino) -> bool {
        self.fs.vfs.inode_has_local_dirty_state(ino)
    }

    fn explicit_flush(
        &self,
        request: HandleDrainRequest,
    ) -> studiofs_vfs::VfsResult<ExplicitFlushOutcome> {
        if request.fh != 0 {
            self.fs
                .rt
                .block_on(self.fs.vfs.flush_fh_with_outcome(request.fh))
        } else {
            self.fs
                .rt
                .block_on(self.fs.vfs.flush_with_outcome(request.ino))
        }
    }
}

impl HandleReleasePort for FuseHandleReleasePort<'_> {
    fn release_handle(
        &self,
        request: HandleDrainRequest,
    ) -> studiofs_vfs::VfsResult<ReleaseFhOutcome> {
        self.fs
            .rt
            .block_on(self.fs.vfs.release_fh_with_outcome(request.fh))
    }
}

impl DeferredCleanupHintPort for FuseDeferredCleanupHintPort {
    // FUSE does not have the WinFSP-style non-delete cleanup callback split, so
    // there is no deferred-hint state to preserve in this bridge.
    fn set_deferred_cleanup_hint(&self, _ino: Ino) {}

    fn clear_deferred_cleanup_hint(&self, _ino: Ino) {}

    fn take_deferred_cleanup_hint(&self, _ino: Ino) -> bool {
        false
    }

    fn has_deferred_cleanup_hint(&self, _ino: Ino) -> bool {
        false
    }
}

impl FlushGatePort for FuseFlushGatePort {
    // FUSE calls into the barrier from a direct synchronous bridge path and
    // does not need callback-side coalescing, so the flush gate is a no-op.
    fn with_flush_gate<T, F>(&self, _ino: Ino, op: F) -> studiofs_vfs::VfsResult<T>
    where
        F: FnOnce() -> studiofs_vfs::VfsResult<T>,
    {
        op()
    }
}

impl DeleteCleanupPort for FuseDeleteCleanupPort {
    // FUSE delete handling is not routed through this facade.
    fn cleanup_delete(&self, _request: HandleDrainDeleteRequest) {}
}

impl BarrierObservabilityPort for FuseBarrierObservabilityPort {
    fn note_cleanup_callback(&self) {}

    fn note_flush_callback(&self) {}

    fn note_flush_barrier_outcome(&self, _flushed_local_dirty_state: bool) {}

    fn note_close_callback(&self) {}

    fn note_release_success(
        &self,
        _request: HandleDrainRequest,
        _hint_was_taken: bool,
        _outcome: ReleaseFhOutcome,
    ) {
    }

    fn note_release_error(
        &self,
        _request: HandleDrainRequest,
        _cleanup_deferred_hint: bool,
        _error: &studiofs_vfs::VfsError,
    ) {
    }
}

#[cfg(test)]
mod tests {
    use super::super::make_test_fuse_fs;
    use super::FuseHandleDrainFacade;
    use studiofs_meta::ROOT_INO;
    use studiofs_vfs::OpenFlags;

    #[test]
    fn fuse_handle_drain_flush_uses_handle_backed_path() {
        let (_tmp, rt, vfs, fs) = make_test_fuse_fs("flush-fh");
        let created = vfs
            .create(ROOT_INO, b"flush-fh.txt", 0, 0, 0o644)
            .expect("create file");
        let ino = created.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open file handle");

        rt.block_on(vfs.write_fh(fh, ino, 0, b"hello"))
            .expect("write via handle");
        FuseHandleDrainFacade::new(&fs)
            .flush(ino, fh)
            .expect("handle-backed flush should succeed");
        rt.block_on(vfs.release_fh(fh))
            .expect("release file handle");

        let data = rt.block_on(vfs.read(ino, 0, 5)).expect("read data");
        assert_eq!(data.as_ref(), b"hello");
    }

    #[test]
    fn fuse_handle_drain_flush_uses_inode_path_when_handle_is_zero() {
        let (_tmp, rt, vfs, fs) = make_test_fuse_fs("flush-ino");
        let created = vfs
            .create(ROOT_INO, b"flush-ino.txt", 0, 0, 0o644)
            .expect("create file");
        let ino = created.ino;

        rt.block_on(vfs.write(ino, 0, b"world"))
            .expect("write via inode path");
        FuseHandleDrainFacade::new(&fs)
            .flush(ino, 0)
            .expect("inode-backed flush should succeed");

        let data = rt.block_on(vfs.read(ino, 0, 5)).expect("read data");
        assert_eq!(data.as_ref(), b"world");
    }

    #[test]
    fn fuse_handle_drain_release_uses_shared_release_path() {
        let (_tmp, rt, vfs, fs) = make_test_fuse_fs("release");
        let created = vfs
            .create(ROOT_INO, b"release.txt", 0, 0, 0o644)
            .expect("create file");
        let ino = created.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open file handle");

        rt.block_on(vfs.write_fh(fh, ino, 0, b"release"))
            .expect("write via handle");
        FuseHandleDrainFacade::new(&fs)
            .release(ino, fh)
            .expect("release should succeed");

        let data = rt.block_on(vfs.read(ino, 0, 7)).expect("read data");
        assert_eq!(data.as_ref(), b"release");
    }
}
