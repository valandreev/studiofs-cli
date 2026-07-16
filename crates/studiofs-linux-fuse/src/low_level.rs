use super::*;
use std::collections::HashMap;
use std::ffi::{CStr, OsStr};
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use libc::{mode_t, off_t, size_t, stat, statvfs};
use studiofs_bridge::{
    fuse_liveness::{FuseInodeLiveness, LivenessCheckedKernelInvalidator},
    fuse_open_reply::FuseOpenReplySerializer,
    posix::{
        BridgeFileAttr, BridgeFileType, normalize_close_barrier_result, posix_flags_to_open_flags,
        to_bridge_attr, vfs_err_to_errno,
    },
};
use studiofs_meta::{FileType, InodeAttr, SetattrArgs, TimeSpec};
use studiofs_vfs::{
    DirEntryWithAttr, FuseBarrierKind, KernelInvalidator, LocalSyncPolicy, OpenFlags,
    OsCacheDisposition, OsCacheOpenDecision, OsCacheParticipation, StatFs, StudioVfs,
    VfsFuseAsyncWriteSnapshot, VfsFuseConnectionSnapshot, VfsFuseLivenessSnapshot,
};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::async_writes::{
    LinuxFuseAsyncWriteCompletion, LinuxFuseAsyncWriteStats, LinuxFuseAsyncWrites,
    LinuxFuseRequestTicket, LinuxFuseWriteReservation,
};

const TTL_SECONDS: f64 = 1.0;
const LINUX_FUSE_BLOCK_SIZE: u32 = 4096;
const LINUX_FUSE_MAX_READ_SIZE: usize = 4 * 1024 * 1024;
const LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;
const LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_WRITES: usize = 1024;
const RUNTIME_STATS_FH_START: u64 = u64::MAX / 2;
const ASYNC_WRITE_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const ASYNC_WRITE_SHUTDOWN_CANCEL_TIMEOUT: Duration = Duration::from_secs(2);
const MEMORY_PRESSURE_WATCHER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const SESSION_EXIT_WATCH_INTERVAL: Duration = Duration::from_millis(250);
const INVALIDATION_SHUTDOWN_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(test)]
static ASYNC_WRITE_REPLY_REJECTIONS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
const FUSE_SET_ATTR_MODE: c_int = 1 << 0;
const FUSE_SET_ATTR_UID: c_int = 1 << 1;
const FUSE_SET_ATTR_GID: c_int = 1 << 2;
const FUSE_SET_ATTR_SIZE: c_int = 1 << 3;
const FUSE_SET_ATTR_ATIME: c_int = 1 << 4;
const FUSE_SET_ATTR_MTIME: c_int = 1 << 5;
const FUSE_SET_ATTR_ATIME_NOW: c_int = 1 << 7;
const FUSE_SET_ATTR_MTIME_NOW: c_int = 1 << 8;

pub(crate) mod raw {
    use super::*;

    pub(crate) type FuseIno = u64;

    pub(crate) enum FuseReq {}
    pub(crate) enum FuseSession {}

    #[repr(C)]
    pub(crate) struct FuseArgs {
        pub(crate) argc: c_int,
        pub(crate) argv: *mut *mut c_char,
        pub(crate) allocated: c_int,
    }

    #[repr(C)]
    pub(crate) struct FuseLowLevelOps {
        _private: [u8; 0],
    }

    #[repr(C)]
    pub(crate) struct FuseEntryParam {
        pub(crate) ino: FuseIno,
        pub(crate) generation: u64,
        pub(crate) attr: stat,
        pub(crate) attr_timeout: f64,
        pub(crate) entry_timeout: f64,
    }

    #[repr(C)]
    pub(crate) struct FuseFileInfo {
        _private: [u8; 0],
    }

    #[link(name = "fuse3")]
    unsafe extern "C" {
        pub(crate) fn fuse_reply_err(req: *mut FuseReq, err: c_int) -> c_int;
        pub(crate) fn fuse_reply_none(req: *mut FuseReq);
        pub(crate) fn fuse_reply_entry(req: *mut FuseReq, entry: *const FuseEntryParam) -> c_int;
        pub(crate) fn fuse_reply_attr(req: *mut FuseReq, attr: *const stat, timeout: f64) -> c_int;
        pub(crate) fn fuse_reply_open(req: *mut FuseReq, fi: *const FuseFileInfo) -> c_int;
        pub(crate) fn fuse_reply_create(
            req: *mut FuseReq,
            entry: *const FuseEntryParam,
            fi: *const FuseFileInfo,
        ) -> c_int;
        pub(crate) fn fuse_reply_buf(req: *mut FuseReq, buf: *const c_char, size: size_t) -> c_int;
        pub(crate) fn fuse_reply_xattr(req: *mut FuseReq, count: size_t) -> c_int;
        pub(crate) fn fuse_reply_write(req: *mut FuseReq, count: size_t) -> c_int;
        pub(crate) fn fuse_reply_statfs(req: *mut FuseReq, stat: *const statvfs) -> c_int;
        pub(crate) fn fuse_req_userdata(req: *mut FuseReq) -> *mut c_void;
        pub(crate) fn fuse_add_direntry(
            req: *mut FuseReq,
            buf: *mut c_char,
            bufsize: size_t,
            name: *const c_char,
            stbuf: *const stat,
            off: off_t,
        ) -> size_t;
        pub(crate) fn fuse_opt_free_args(args: *mut FuseArgs);
        pub(crate) fn fuse_session_mount(
            session: *mut FuseSession,
            mountpoint: *const c_char,
        ) -> c_int;
        pub(crate) fn fuse_session_unmount(session: *mut FuseSession);
        pub(crate) fn fuse_session_destroy(session: *mut FuseSession);
        pub(crate) fn fuse_session_exited(session: *mut FuseSession) -> c_int;
        pub(crate) fn fuse_set_signal_handlers(session: *mut FuseSession) -> c_int;
        pub(crate) fn fuse_remove_signal_handlers(session: *mut FuseSession);
    }
}

unsafe extern "C" {
    fn studiofs_linux_fuse_low_level_ops() -> *const raw::FuseLowLevelOps;
    fn studiofs_linux_fuse_low_level_ops_size() -> libc::size_t;
    fn studiofs_linux_fuse_session_new(
        args: *mut raw::FuseArgs,
        ops: *const raw::FuseLowLevelOps,
        ops_size: libc::size_t,
        userdata: *mut c_void,
    ) -> *mut raw::FuseSession;
    fn studiofs_linux_fuse_session_loop_bounded(
        session: *mut raw::FuseSession,
        max_threads: c_uint,
        max_idle_threads: c_uint,
    ) -> c_int;
    fn studiofs_linux_fuse_notify_inval_inode(
        session: *mut raw::FuseSession,
        ino: u64,
        offset: off_t,
        len: off_t,
    ) -> c_int;
    fn studiofs_linux_fuse_dir_file_info_init(info: *mut raw::FuseFileInfo, fh: u64);
    fn studiofs_linux_fuse_file_info_init(
        info: *mut raw::FuseFileInfo,
        fh: u64,
        direct_io: c_int,
        keep_cache: c_int,
        parallel_direct_writes: c_int,
    );
    fn studiofs_linux_fuse_file_info_fh(info: *const raw::FuseFileInfo) -> u64;
    fn studiofs_linux_fuse_file_info_flags(info: *const raw::FuseFileInfo) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_low_level_ops_has_metadata_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_low_level_ops_has_forget_callback(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_low_level_ops_has_regular_file_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_low_level_ops_has_namespace_mutation_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_low_level_ops_has_posix_compat_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_init_sets_bounded_background(
        userdata: *mut c_void,
        max_background: u32,
        congestion_threshold: u32,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_init_for_test(
        userdata: *mut c_void,
        max_read: u32,
        max_write: u32,
        max_readahead: u32,
        capable_async_dio: c_int,
        capable_writeback_cache: c_int,
        preset_async_dio: c_int,
        preset_writeback_cache: c_int,
    ) -> u32;
    #[cfg(test)]
    fn studiofs_linux_fuse_loop_config_uses_loop_config_api() -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_uses_single_thread_loop() -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_supports_parallel_direct_writes() -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_conn_wants_async_dio_after_set(capable_ext: u64) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_conn_wants_writeback_cache_after_set(capable_ext: u64) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_file_info_direct_io_after_init() -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_file_info_flags_after_init(
        direct_io: c_int,
        keep_cache: c_int,
        parallel_direct_writes: c_int,
    ) -> c_int;
}

pub(crate) struct SessionState {
    vfs: Arc<StudioVfs>,
    inode_liveness: Arc<FuseInodeLiveness>,
    open_reply_serializer: FuseOpenReplySerializer,
    runtime_handle: Handle,
    async_writes: Arc<LinuxFuseAsyncWrites>,
    negotiated_max_write: Arc<AtomicUsize>,
    memory_pressure_async_write_watcher: Option<MemoryPressureAsyncWriteWatcher>,
    read_only: bool,
    mount_uid: u32,
    mount_gid: u32,
    cache_policy: LinuxFuseCachePolicy,
    requested_async_dio: bool,
    connection_config: LinuxFuseConnectionConfig,
    negotiated_geometry: OnceLock<LinuxFuseNegotiatedGeometry>,
    metric_config: LinuxFuseAsyncWriteMetricConfig,
    next_runtime_stats_fh: AtomicU64,
    runtime_stats_handles: Mutex<HashMap<u64, Vec<u8>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SessionWriteShutdownOutcome {
    watcher_stopped: bool,
    writes_stopped: bool,
}

struct MemoryPressureAsyncWriteWatcher {
    shutdown: MemoryPressureWatcherShutdown,
}

#[derive(Clone)]
struct MemoryPressureWatcherShutdown {
    cancellation: CancellationToken,
    task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

struct AbortWatcherTaskOnDrop(tokio::task::AbortHandle);

impl Drop for AbortWatcherTaskOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl MemoryPressureAsyncWriteWatcher {
    fn start(
        runtime_handle: &Handle,
        vfs: Arc<StudioVfs>,
        writes: Arc<LinuxFuseAsyncWrites>,
        negotiated_max_write: Arc<AtomicUsize>,
    ) -> Option<Self> {
        if vfs.memory_pressure_controller().snapshot().state
            == studiofs_vfs::MemoryPressureState::Disabled
        {
            return None;
        }

        writes.set_active_max_pending_bytes(linux_fuse_async_write_active_limit_bytes(
            vfs.memory_pressure_controller().snapshot(),
            negotiated_max_write.load(Ordering::Acquire),
        ));
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let changed = vfs.memory_pressure_controller().change_listener();
        let task = runtime_handle.spawn(async move {
            loop {
                let notification = changed.notified();
                tokio::pin!(notification);
                let _ = notification.as_mut().enable();
                writes.set_active_max_pending_bytes(linux_fuse_async_write_active_limit_bytes(
                    vfs.memory_pressure_controller().snapshot(),
                    negotiated_max_write.load(Ordering::Acquire),
                ));
                tokio::select! {
                    biased;
                    () = task_cancellation.cancelled() => break,
                    () = &mut notification => {}
                }
            }
        });
        Some(Self {
            shutdown: MemoryPressureWatcherShutdown {
                cancellation,
                task: Arc::new(Mutex::new(Some(task))),
            },
        })
    }

    #[cfg(test)]
    async fn stop_and_join(&self) -> bool {
        self.shutdown.stop_and_join().await
    }

    fn shutdown_handle(&self) -> MemoryPressureWatcherShutdown {
        self.shutdown.clone()
    }

    fn cancel_and_abort(&self) {
        self.shutdown.cancel_and_abort();
    }
}

impl MemoryPressureWatcherShutdown {
    async fn stop_and_join(&self) -> bool {
        self.cancellation.cancel();
        let Some(mut task) = self
            .task
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take()
        else {
            return true;
        };
        let _abort_on_drop = AbortWatcherTaskOnDrop(task.abort_handle());
        match tokio::time::timeout(MEMORY_PRESSURE_WATCHER_SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => error.is_cancelled(),
            Err(_) => {
                task.abort();
                task.await.is_err_and(|error| error.is_cancelled())
            }
        }
    }

    fn cancel_and_abort(&self) {
        self.cancellation.cancel();
        if let Some(task) = self
            .task
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take()
        {
            task.abort();
        }
    }
}

impl Drop for MemoryPressureAsyncWriteWatcher {
    fn drop(&mut self) {
        self.cancel_and_abort();
    }
}

const _: fn() = assert_send_sync_static::<SessionState>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LinuxFuseCachePolicy {
    Direct,
    Kernel,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct LinuxFuseNegotiatedGeometry {
    max_read: u32,
    max_write: u32,
    max_readahead: u32,
    max_background: u32,
    congestion_threshold: u32,
    async_dio: bool,
    writeback_cache: bool,
    parallel_direct_writes_supported: bool,
    single_thread_receive_loop: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LinuxFuseReleaseMode {
    Synchronous,
    Deferred,
}

fn linux_fuse_release_mode(
    local_sync_policy: LocalSyncPolicy,
    has_local_dirty_state: bool,
) -> LinuxFuseReleaseMode {
    if local_sync_policy.is_opportunistic() && !has_local_dirty_state {
        LinuxFuseReleaseMode::Deferred
    } else {
        LinuxFuseReleaseMode::Synchronous
    }
}

fn merge_async_and_local_barrier_results(
    async_result: Result<(), i32>,
    local_result: Result<(), i32>,
) -> Result<(), i32> {
    match async_result {
        Err(errno) => Err(errno),
        Ok(()) => local_result,
    }
}

struct LinuxFuseHandleLedgerGuard {
    async_writes: Arc<LinuxFuseAsyncWrites>,
    fh: u64,
}

impl LinuxFuseHandleLedgerGuard {
    fn new(async_writes: Arc<LinuxFuseAsyncWrites>, fh: u64) -> Self {
        Self { async_writes, fh }
    }
}

impl Drop for LinuxFuseHandleLedgerGuard {
    fn drop(&mut self) {
        self.async_writes.forget_handle(self.fh);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LinuxFuseOpenCachePolicy {
    direct_io: bool,
    keep_cache: bool,
    parallel_direct_writes: bool,
}

#[derive(Clone, Copy, Debug)]
struct LinuxFuseOpenedFile {
    fh: u64,
    ino: u64,
}

#[derive(Clone, Debug)]
struct LinuxFuseCreatedFile {
    attr: InodeAttr,
    opened: LinuxFuseOpenedFile,
}

impl SessionState {
    pub(crate) fn new(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        read_only: bool,
        connection_config: LinuxFuseConnectionConfig,
    ) -> Self {
        let cache_policy = linux_fuse_cache_policy_from_env();
        let async_dio_override = std::env::var("STUDIOFS_FUSE_ASYNC_DIO").ok();
        let requested_async_dio =
            linux_fuse_async_dio_enabled_for(cache_policy, async_dio_override.as_deref());
        Self::new_with_cache_settings(
            vfs,
            runtime_handle,
            read_only,
            connection_config,
            cache_policy,
            requested_async_dio,
        )
    }

    fn new_with_cache_settings(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        read_only: bool,
        connection_config: LinuxFuseConnectionConfig,
        cache_policy: LinuxFuseCachePolicy,
        requested_async_dio: bool,
    ) -> Self {
        let async_writes = Arc::new(LinuxFuseAsyncWrites::new(
            LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_BYTES,
            LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_WRITES,
        ));
        let negotiated_max_write = Arc::new(AtomicUsize::new(0));
        let memory_pressure_async_write_watcher = MemoryPressureAsyncWriteWatcher::start(
            &runtime_handle,
            Arc::clone(&vfs),
            Arc::clone(&async_writes),
            Arc::clone(&negotiated_max_write),
        );
        let state = Self {
            vfs,
            inode_liveness: Arc::new(FuseInodeLiveness::default()),
            open_reply_serializer: FuseOpenReplySerializer::default(),
            runtime_handle,
            async_writes,
            negotiated_max_write,
            memory_pressure_async_write_watcher,
            read_only,
            mount_uid: current_uid(),
            mount_gid: current_gid(),
            cache_policy,
            requested_async_dio,
            connection_config,
            negotiated_geometry: OnceLock::new(),
            metric_config: LinuxFuseAsyncWriteMetricConfig { cache_policy },
            next_runtime_stats_fh: AtomicU64::new(RUNTIME_STATS_FH_START),
            runtime_stats_handles: Mutex::new(HashMap::new()),
        };
        state.publish_fuse_connection_snapshot(None);
        state
    }

    #[cfg(test)]
    fn new_for_forced_unmount_test(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        read_only: bool,
        connection_config: LinuxFuseConnectionConfig,
    ) -> Self {
        let mut state = Self::new(vfs, runtime_handle, read_only, connection_config);
        state.async_writes = Arc::new(LinuxFuseAsyncWrites::new(
            LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_BYTES,
            1,
        ));
        state
    }

    fn shutdown_async_writes_after_run(&self) -> SessionWriteShutdownOutcome {
        let runtime_handle = self.runtime_handle.clone();
        let watcher = self
            .memory_pressure_async_write_watcher
            .as_ref()
            .map(MemoryPressureAsyncWriteWatcher::shutdown_handle);
        let watcher_stopped = if let Some(watcher) = watcher {
            if runtime_handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                watcher.cancel_and_abort();
                warn!(
                    "cannot synchronously join Linux FUSE memory-pressure watcher on a current-thread Tokio runtime"
                );
                false
            } else {
                let shutdown =
                    move || shutdown_memory_pressure_watcher_on_helper(runtime_handle, watcher);
                let caller_is_multi_thread = Handle::try_current().is_ok_and(|handle| {
                    handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
                });
                if caller_is_multi_thread {
                    tokio::task::block_in_place(shutdown)
                } else {
                    shutdown()
                }
            }
        } else {
            true
        };
        if !watcher_stopped {
            self.abort_memory_pressure_watcher();
        }
        let writes_stopped = shutdown_async_writes_with_timeouts(
            &self.async_writes,
            ASYNC_WRITE_SHUTDOWN_DRAIN_TIMEOUT,
            ASYNC_WRITE_SHUTDOWN_CANCEL_TIMEOUT,
        );
        SessionWriteShutdownOutcome {
            watcher_stopped,
            writes_stopped,
        }
    }

    #[cfg(test)]
    async fn shutdown_memory_pressure_watcher(&self) -> bool {
        if let Some(watcher) = &self.memory_pressure_async_write_watcher {
            watcher.stop_and_join().await
        } else {
            true
        }
    }

    fn abort_memory_pressure_watcher(&self) {
        if let Some(watcher) = &self.memory_pressure_async_write_watcher {
            watcher.cancel_and_abort();
        }
    }

    fn publish_liveness_stats(&self) {
        publish_fuse_liveness_stats(&self.vfs, &self.inode_liveness);
    }

    fn record_negotiated_geometry(&self, geometry: LinuxFuseNegotiatedGeometry) {
        let _ = self.negotiated_geometry.set(geometry);
        self.negotiated_max_write
            .store(geometry.max_write as usize, Ordering::Release);
        self.refresh_async_write_active_limit();
        self.publish_fuse_connection_snapshot(self.negotiated_geometry.get().copied());
    }

    fn refresh_async_write_active_limit(&self) {
        self.async_writes
            .set_active_max_pending_bytes(linux_fuse_async_write_active_limit(
                &self.vfs,
                self.negotiated_geometry.get().copied(),
            ));
    }

    fn publish_fuse_connection_snapshot(&self, negotiated: Option<LinuxFuseNegotiatedGeometry>) {
        self.vfs
            .record_fuse_connection_snapshot(VfsFuseConnectionSnapshot {
                configured_max_background: Some(self.connection_config.max_background),
                configured_congestion_threshold: Some(self.connection_config.congestion_threshold),
                requested_async_dio: Some(self.requested_async_dio),
                requested_writeback_cache: Some(self.cache_policy == LinuxFuseCachePolicy::Kernel),
                negotiated_max_read_bytes: negotiated
                    .and_then(|value| (value.max_read != 0).then_some(value.max_read)),
                negotiated_max_write_bytes: negotiated.map(|value| value.max_write),
                negotiated_max_readahead_bytes: negotiated.map(|value| value.max_readahead),
                negotiated_max_background: negotiated.map(|value| value.max_background),
                negotiated_congestion_threshold: negotiated.map(|value| value.congestion_threshold),
                negotiated_async_dio: negotiated.map(|value| value.async_dio),
                negotiated_writeback_cache: negotiated.map(|value| value.writeback_cache),
                parallel_direct_writes_supported: negotiated
                    .map(|value| value.parallel_direct_writes_supported),
                single_thread_receive_loop: negotiated
                    .map(|value| value.single_thread_receive_loop),
            });
    }
}

fn shutdown_memory_pressure_watcher_on_helper(
    runtime_handle: Handle,
    watcher: MemoryPressureWatcherShutdown,
) -> bool {
    match std::thread::Builder::new()
        .name("studiofs-pressure-watcher-shutdown".to_owned())
        .spawn(move || runtime_handle.block_on(watcher.stop_and_join()))
    {
        Ok(thread) => match thread.join() {
            Ok(stopped) => stopped,
            Err(_) => {
                warn!("Linux FUSE memory-pressure watcher shutdown thread panicked");
                false
            }
        },
        Err(error) => {
            warn!(%error, "failed to start Linux FUSE memory-pressure watcher shutdown thread");
            false
        }
    }
}

fn publish_fuse_liveness_stats(vfs: &StudioVfs, liveness: &FuseInodeLiveness) {
    let stats = liveness.stats();
    vfs.record_fuse_liveness_snapshot(VfsFuseLivenessSnapshot {
        snapshot_generation: stats.snapshot_generation,
        observed_references: stats.observed_references,
        forgotten_references: stats.forgotten_references,
        retired_inodes: stats.retired_inodes,
        open_unknown_inode: stats.open_unknown_inode,
        notifications_forwarded: stats.notifications_forwarded,
        notifications_suppressed: stats.notifications_suppressed,
    });
}

fn shutdown_async_writes_with_timeouts(
    async_writes: &LinuxFuseAsyncWrites,
    drain_timeout: Duration,
    cancel_timeout: Duration,
) -> bool {
    async_writes.begin_shutdown();
    if async_writes.wait_idle(drain_timeout) {
        return true;
    }
    async_writes.request_cancellation();
    async_writes.wait_idle(cancel_timeout)
}

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

#[cfg(test)]
type FuseReplyWriteFn = unsafe extern "C" fn(*mut raw::FuseReq, size_t) -> c_int;
#[cfg(test)]
type FuseReplyErrorFn = unsafe extern "C" fn(*mut raw::FuseReq, c_int) -> c_int;
#[cfg(test)]
type FuseReplyBufFn = unsafe extern "C" fn(*mut raw::FuseReq, *const c_char, size_t) -> c_int;

struct FuseReqPtr {
    raw: *mut raw::FuseReq,
    #[cfg(test)]
    reply_buf_fn: FuseReplyBufFn,
    #[cfg(test)]
    reply_write_fn: FuseReplyWriteFn,
    #[cfg(test)]
    reply_error_fn: FuseReplyErrorFn,
}

// SAFETY: libfuse low-level requests may be replied asynchronously exactly once.
// Async write transfers that reply obligation to the spawned task.
unsafe impl Send for FuseReqPtr {}

impl FuseReqPtr {
    fn new(raw: *mut raw::FuseReq) -> Self {
        Self {
            raw,
            #[cfg(test)]
            reply_buf_fn: raw::fuse_reply_buf,
            #[cfg(test)]
            reply_write_fn: raw::fuse_reply_write,
            #[cfg(test)]
            reply_error_fn: raw::fuse_reply_err,
        }
    }

    #[cfg(test)]
    fn for_test(
        raw: *mut raw::FuseReq,
        reply_write_fn: FuseReplyWriteFn,
        reply_error_fn: FuseReplyErrorFn,
    ) -> Self {
        Self {
            raw,
            reply_buf_fn: raw::fuse_reply_buf,
            reply_write_fn,
            reply_error_fn,
        }
    }

    #[cfg(test)]
    fn for_sync_test(
        raw: *mut raw::FuseReq,
        reply_buf_fn: FuseReplyBufFn,
        reply_error_fn: FuseReplyErrorFn,
    ) -> Self {
        Self {
            raw,
            reply_buf_fn,
            reply_write_fn: raw::fuse_reply_write,
            reply_error_fn,
        }
    }

    fn reply_buf(self, buffer: &[u8]) -> c_int {
        // SAFETY: ownership of the single FUSE reply was transferred with this pointer,
        // and `buffer` remains valid until the synchronous reply returns.
        #[cfg(test)]
        unsafe {
            (self.reply_buf_fn)(self.raw, buffer.as_ptr().cast::<c_char>(), buffer.len())
        }
        #[cfg(not(test))]
        unsafe {
            raw::fuse_reply_buf(self.raw, buffer.as_ptr().cast::<c_char>(), buffer.len())
        }
    }

    fn reply_write(self, written: size_t) -> c_int {
        // SAFETY: ownership of the single FUSE reply was transferred with this pointer.
        #[cfg(test)]
        unsafe {
            (self.reply_write_fn)(self.raw, written)
        }
        #[cfg(not(test))]
        unsafe {
            raw::fuse_reply_write(self.raw, written)
        }
    }

    fn reply_error(self, errno: c_int) -> c_int {
        // SAFETY: ownership of the single FUSE reply was transferred with this pointer.
        #[cfg(test)]
        unsafe {
            (self.reply_error_fn)(self.raw, errno)
        }
        #[cfg(not(test))]
        unsafe {
            raw::fuse_reply_err(self.raw, errno)
        }
    }
}

fn finish_fuse_read_reply(
    vfs: &StudioVfs,
    request: FuseReqPtr,
    requested_bytes: usize,
    started: Instant,
    result: Result<Vec<u8>, c_int>,
) {
    let (returned_bytes, errored) = match result {
        Ok(buffer) => {
            let status = request.reply_buf(&buffer);
            (if status == 0 { buffer.len() } else { 0 }, status != 0)
        }
        Err(errno) => {
            let _ = request.reply_error(errno);
            (0, true)
        }
    };
    vfs.record_fuse_read_callback(requested_bytes, returned_bytes, started.elapsed(), errored);
}

fn finish_fuse_barrier_reply(
    vfs: &StudioVfs,
    request: FuseReqPtr,
    kind: FuseBarrierKind,
    started: Instant,
    result: Result<(), c_int>,
) {
    let (errno, operation_errored) = match result {
        Ok(()) => (0, false),
        Err(errno) => (errno, true),
    };
    let status = request.reply_error(errno);
    vfs.record_fuse_barrier_callback(kind, started.elapsed(), operation_errored || status != 0);
}

#[derive(Clone, Copy)]
struct LinuxFuseAsyncWriteMetricConfig {
    cache_policy: LinuxFuseCachePolicy,
}

struct FuseWriteTaskGuard {
    request: Option<FuseReqPtr>,
    request_ticket: Option<LinuxFuseRequestTicket>,
    reservation: Option<LinuxFuseWriteReservation>,
    writes: Arc<LinuxFuseAsyncWrites>,
    vfs: Arc<StudioVfs>,
    started: Instant,
    requested_bytes: usize,
    metric_config: LinuxFuseAsyncWriteMetricConfig,
}

enum FuseWriteTaskResult {
    AdmissionRejected(i32),
    Completed(Result<u32, i32>),
    Cancelled,
}

impl FuseWriteTaskGuard {
    fn attach_reservation(&mut self, reservation: LinuxFuseWriteReservation) {
        self.reservation = Some(reservation);
    }

    fn finish(mut self, result: FuseWriteTaskResult) {
        let request = self.request.take().expect("request guard owns one reply");
        let (reply_status, returned_bytes, errored, completion) = match result {
            FuseWriteTaskResult::AdmissionRejected(errno) => (
                request.reply_error(errno),
                0,
                true,
                LinuxFuseAsyncWriteCompletion::Failed(errno),
            ),
            FuseWriteTaskResult::Completed(Ok(written)) => {
                let status = request.reply_write(written as size_t);
                let completion = if status == 0 {
                    LinuxFuseAsyncWriteCompletion::Success
                } else {
                    LinuxFuseAsyncWriteCompletion::Failed(libc::EIO)
                };
                (
                    status,
                    if status == 0 { written as usize } else { 0 },
                    status != 0,
                    completion,
                )
            }
            FuseWriteTaskResult::Completed(Err(errno)) => (
                request.reply_error(errno),
                0,
                true,
                LinuxFuseAsyncWriteCompletion::Failed(errno),
            ),
            FuseWriteTaskResult::Cancelled => (
                request.reply_error(libc::EIO),
                0,
                true,
                LinuxFuseAsyncWriteCompletion::Cancelled,
            ),
        };
        if reply_status != 0 {
            #[cfg(test)]
            ASYNC_WRITE_REPLY_REJECTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(reply_status, "libfuse rejected async write reply");
        }
        self.vfs.record_fuse_write_callback(
            self.requested_bytes,
            returned_bytes,
            self.started.elapsed(),
            errored,
        );
        drop(self.reservation.take());
        self.request_ticket
            .take()
            .expect("request sequence completes once")
            .complete(completion);
        record_linux_async_write_snapshot(&self.vfs, &self.writes, self.metric_config);
    }
}

impl Drop for FuseWriteTaskGuard {
    fn drop(&mut self) {
        let Some(request) = self.request.take() else {
            return;
        };
        let reply_status = request.reply_error(libc::EIO);
        if reply_status != 0 {
            #[cfg(test)]
            ASYNC_WRITE_REPLY_REJECTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(reply_status, "libfuse rejected async write reply");
        }
        self.vfs
            .record_fuse_write_callback(self.requested_bytes, 0, self.started.elapsed(), true);
        drop(self.reservation.take());
        drop(self.request_ticket.take());
        record_linux_async_write_snapshot(&self.vfs, &self.writes, self.metric_config);
    }
}

fn record_linux_async_write_snapshot(
    vfs: &StudioVfs,
    writes: &LinuxFuseAsyncWrites,
    metric_config: LinuxFuseAsyncWriteMetricConfig,
) {
    record_linux_async_write_stats(vfs, writes.stats(), metric_config);
}

fn record_linux_async_write_stats(
    vfs: &StudioVfs,
    stats: LinuxFuseAsyncWriteStats,
    metric_config: LinuxFuseAsyncWriteMetricConfig,
) {
    let profile = match metric_config.cache_policy {
        LinuxFuseCachePolicy::Direct => "direct",
        LinuxFuseCachePolicy::Kernel => "kernel",
    };
    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        snapshot_generation: stats.snapshot_generation,
        enabled: linux_fuse_async_write_enabled(),
        linux_fuse_profile: profile.into(),
        max_pending_bytes: Some(LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_BYTES),
        max_pending_writes: Some(LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_WRITES),
        active_requests: stats.active_requests,
        admission_waiters: stats.admission_waiters,
        pending_handles: stats.pending_handles,
        pending_writes: stats.pending_writes,
        pending_bytes: stats.pending_bytes,
        peak_pending_bytes: stats.peak_pending_bytes,
        max_handle_pending_writes: stats.max_handle_pending_writes,
        peak_handle_pending_writes: stats.peak_handle_pending_writes,
        completed_writes: stats.completed_writes,
        failed_writes: stats.failed_writes,
        task_failed_writes: stats.task_failed_writes,
        cancelled_writes: stats.cancelled_writes,
        admission_waits: stats.admission_waits,
        admission_wait_total_ms: stats.admission_wait_total_ms,
        barrier_waits: stats.barrier_waits,
        barrier_wait_total_ms: stats.barrier_wait_total_ms,
        ..VfsFuseAsyncWriteSnapshot::default()
    });
}

fn linux_fuse_async_write_active_limit(
    vfs: &StudioVfs,
    negotiated: Option<LinuxFuseNegotiatedGeometry>,
) -> usize {
    linux_fuse_async_write_active_limit_bytes(
        vfs.memory_pressure_controller().snapshot(),
        negotiated.map_or(0, |geometry| geometry.max_write as usize),
    )
}

fn linux_fuse_async_write_active_limit_bytes(
    snapshot: studiofs_vfs::MemoryPressureSnapshot,
    negotiated_max_write: usize,
) -> usize {
    if snapshot.state == studiofs_vfs::MemoryPressureState::Disabled {
        return LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_BYTES;
    }

    let pressure_limit = usize::try_from(snapshot.limits.async_write_bytes).unwrap_or(usize::MAX);
    pressure_limit
        .max(negotiated_max_write)
        .min(LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_BYTES)
}

struct LinuxFuseWriteRequest {
    fh: u64,
    offset: u64,
    local_sync_policy: LocalSyncPolicy,
}

fn linux_fuse_loop_thread_count(max_background: u32) -> c_uint {
    let parallelism = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    linux_fuse_loop_thread_count_for_parallelism(parallelism, max_background)
}

fn linux_fuse_loop_thread_count_for_parallelism(parallelism: usize, max_background: u32) -> c_uint {
    u32::try_from(parallelism)
        .unwrap_or(u32::MAX)
        // FUSE callbacks can block on local disk and remote object I/O, so keep
        // a small surplus over CPU count without exceeding kernel queue bounds.
        .saturating_mul(2)
        .clamp(1, max_background.max(1))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_connection_max_background(
    userdata: *mut c_void,
) -> u32 {
    callback_state_from_userdata(userdata)
        .map(|state| state.connection_config.max_background)
        .unwrap_or_else(|| LinuxFuseConnectionConfig::default().max_background)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_connection_congestion_threshold(
    userdata: *mut c_void,
) -> u32 {
    callback_state_from_userdata(userdata)
        .map(|state| state.connection_config.congestion_threshold)
        .unwrap_or_else(|| LinuxFuseConnectionConfig::default().congestion_threshold)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_connection_wants_async_dio(
    userdata: *mut c_void,
) -> c_int {
    if callback_state_from_userdata(userdata).is_some_and(|state| state.requested_async_dio) {
        1
    } else {
        0
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_connection_wants_writeback_cache(
    userdata: *mut c_void,
) -> c_int {
    if callback_state_from_userdata(userdata)
        .is_some_and(|state| state.cache_policy == LinuxFuseCachePolicy::Kernel)
    {
        1
    } else {
        0
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_record_negotiated_connection(
    userdata: *mut c_void,
    max_read: u32,
    max_write: u32,
    max_readahead: u32,
    max_background: u32,
    congestion_threshold: u32,
    async_dio: c_int,
    writeback_cache: c_int,
    parallel_direct_writes_supported: c_int,
    single_thread_receive_loop: c_int,
) {
    let Some(state) = callback_state_from_userdata(userdata) else {
        return;
    };
    state.record_negotiated_geometry(LinuxFuseNegotiatedGeometry {
        max_read,
        max_write,
        max_readahead,
        max_background,
        congestion_threshold,
        async_dio: async_dio != 0,
        writeback_cache: writeback_cache != 0,
        parallel_direct_writes_supported: parallel_direct_writes_supported != 0,
        single_thread_receive_loop: single_thread_receive_loop != 0,
    });
}

fn current_uid() -> u32 {
    // SAFETY: `geteuid` has no preconditions.
    unsafe { libc::geteuid() }
}

fn current_gid() -> u32 {
    // SAFETY: `getegid` has no preconditions.
    unsafe { libc::getegid() }
}

fn linux_fuse_cache_policy_from_env() -> LinuxFuseCachePolicy {
    // ponytail: internal benchmark knob; promote only after studiofs-bench proves it.
    std::env::var("STUDIOFS_LINUX_FUSE_CACHE_POLICY")
        .ok()
        .and_then(|value| linux_fuse_cache_policy_from_str(&value))
        .unwrap_or(LinuxFuseCachePolicy::Direct)
}

fn linux_fuse_async_dio_enabled_for(
    cache_policy: LinuxFuseCachePolicy,
    override_value: Option<&str>,
) -> bool {
    cache_policy == LinuxFuseCachePolicy::Direct
        && override_value.is_none_or(linux_fuse_env_flag_value)
}

fn linux_fuse_parallel_direct_writes_enabled(
    async_dio_enabled: bool,
    field_supported: bool,
) -> bool {
    async_dio_enabled && field_supported
}

#[cfg(test)]
fn linux_fuse_has_bounded_loop_config_for(version: (u32, u32, u32)) -> bool {
    version >= (3, 12, 0)
}

#[cfg(test)]
fn linux_fuse_supports_parallel_direct_writes_for(version: (u32, u32, u32)) -> bool {
    version >= (3, 14, 1)
}

#[cfg(test)]
fn linux_fuse_has_extended_conn_caps_for(version: (u32, u32, u32)) -> bool {
    version >= (3, 17, 1)
}

#[cfg(test)]
fn linux_fuse_bounded_loop_selection_for(
    version: (u32, u32, u32),
    max_threads: u32,
) -> (bool, u32) {
    if linux_fuse_has_bounded_loop_config_for(version) {
        (false, max_threads)
    } else {
        (true, 1)
    }
}

fn linux_fuse_env_flag_value(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn linux_fuse_async_write_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        linux_fuse_async_write_enabled_from_env(
            std::env::var("STUDIOFS_FUSE_ASYNC_WRITE").ok().as_deref(),
            std::env::var("STUDIOFS_FUSE_ASYNC_WRITE_DISABLED")
                .ok()
                .as_deref(),
        )
    })
}

fn linux_fuse_async_write_enabled_from_env(
    async_write: Option<&str>,
    disabled: Option<&str>,
) -> bool {
    if disabled.is_some_and(linux_fuse_env_flag_value) {
        return false;
    }
    async_write.is_none_or(linux_fuse_env_flag_value)
}

fn linux_fuse_metadata_admission_enabled() -> bool {
    true
}

fn linux_fuse_setattr_args_are_attr_only(args: &SetattrArgs) -> bool {
    args.size.is_none()
        && (args.mode.is_some()
            || args.uid.is_some()
            || args.gid.is_some()
            || !matches!(args.atime, TimeSpec::Omit)
            || !matches!(args.mtime, TimeSpec::Omit)
            || !matches!(args.crtime, TimeSpec::Omit)
            || args.win_attrs.is_some())
}

fn linux_fuse_cache_policy_from_str(value: &str) -> Option<LinuxFuseCachePolicy> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "direct" | "direct_io" | "dio" | "0" | "false" | "no" | "off" => {
            Some(LinuxFuseCachePolicy::Direct)
        }
        "kernel" | "cached" | "page_cache" | "1" | "true" | "yes" | "on" => {
            Some(LinuxFuseCachePolicy::Kernel)
        }
        _ => None,
    }
}

#[cfg(test)]
fn argv_strings(volume_name: &str) -> Vec<String> {
    argv_strings_for_cache_policy(volume_name, linux_fuse_cache_policy_from_env())
}

fn argv_strings_for_cache_policy(
    volume_name: &str,
    _cache_policy: LinuxFuseCachePolicy,
) -> Vec<String> {
    vec![
        "studiofs-linux-fuse".to_owned(),
        "-o".to_owned(),
        format!("fsname={volume_name}"),
        "-o".to_owned(),
        "subtype=studiofs".to_owned(),
        "-o".to_owned(),
        "auto_unmount".to_owned(),
    ]
}

fn path_to_cstring(path: &std::path::Path) -> io::Result<CString> {
    use std::os::unix::ffi::OsStrExt;

    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Linux FUSE mount point contains an interior NUL byte",
        )
    })
}

struct OwnedFuseArgs {
    _strings: Vec<CString>,
    _argv: Vec<*mut c_char>,
    raw: raw::FuseArgs,
}

impl OwnedFuseArgs {
    fn new(args: Vec<String>) -> io::Result<Self> {
        let strings = args
            .into_iter()
            .map(|arg| {
                CString::new(arg).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Linux FUSE argument contains an interior NUL byte",
                    )
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut argv = strings
            .iter()
            .map(|arg| arg.as_ptr().cast_mut())
            .collect::<Vec<_>>();
        let argc = argv.len().try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "too many Linux FUSE arguments")
        })?;
        argv.push(std::ptr::null_mut());
        let raw = raw::FuseArgs {
            argc,
            argv: argv.as_mut_ptr(),
            allocated: 0,
        };
        Ok(Self {
            _strings: strings,
            _argv: argv,
            raw,
        })
    }

    fn as_mut_ptr(&mut self) -> *mut raw::FuseArgs {
        &mut self.raw
    }
}

impl Drop for OwnedFuseArgs {
    fn drop(&mut self) {
        // SAFETY: libfuse accepts a stack-owned fuse_args with allocated == 0;
        // this frees only libfuse-owned replacements if option parsing made any.
        unsafe { raw::fuse_opt_free_args(&mut self.raw) };
    }
}

pub(crate) struct Session {
    raw: NonNull<raw::FuseSession>,
    state: Option<Box<SessionState>>,
    invalidation_state: Arc<SessionInvalidationState>,
    signal_handlers_installed: bool,
}

#[derive(Clone, Copy)]
struct BorrowedFuseSession(NonNull<raw::FuseSession>);

// SAFETY: the watcher only queries the session while `Session::run` is active,
// and it is stopped and joined before the session can be destroyed.
unsafe impl Send for BorrowedFuseSession {}

impl BorrowedFuseSession {
    fn exited(self) -> bool {
        // SAFETY: the caller keeps the session live while the watcher can call this.
        unsafe { raw::fuse_session_exited(self.0.as_ptr()) != 0 }
    }
}

struct SessionExitWatcher {
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl SessionExitWatcher {
    fn start(session: NonNull<raw::FuseSession>, async_writes: Arc<LinuxFuseAsyncWrites>) -> Self {
        let session = BorrowedFuseSession(session);
        Self::start_with_probe(async_writes, move || session.exited())
    }

    fn start_with_probe(
        async_writes: Arc<LinuxFuseAsyncWrites>,
        session_exited: impl Fn() -> bool + Send + 'static,
    ) -> Self {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let thread_stop = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            loop {
                if session_exited() {
                    async_writes.begin_shutdown();
                    break;
                }

                let (lock, changed) = &*thread_stop;
                let stopped = lock.lock().unwrap_or_else(|err| err.into_inner());
                let result = changed
                    .wait_timeout_while(stopped, SESSION_EXIT_WATCH_INTERVAL, |stopped| !*stopped)
                    .unwrap_or_else(|err| err.into_inner());
                if *result.0 {
                    break;
                }
            }
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }

    fn stop(mut self) {
        self.stop_and_join();
    }

    fn stop_and_join(&mut self) {
        let (lock, changed) = &*self.stop;
        *lock.lock().unwrap_or_else(|err| err.into_inner()) = true;
        changed.notify_all();
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            warn!("Linux FUSE session-exit watcher panicked");
        }
    }
}

impl Drop for SessionExitWatcher {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

impl Drop for SessionState {
    fn drop(&mut self) {
        self.abort_memory_pressure_watcher();
    }
}

impl Session {
    pub(crate) fn mount(
        state: SessionState,
        mount_point: impl AsRef<std::path::Path>,
        volume_name: impl AsRef<str>,
    ) -> io::Result<Self> {
        let mount_point = path_to_cstring(mount_point.as_ref())?;
        let mut args = OwnedFuseArgs::new(argv_strings_for_cache_policy(
            volume_name.as_ref(),
            state.cache_policy,
        ))?;
        let mut state = Box::new(state);
        let userdata = (&mut *state as *mut SessionState).cast::<c_void>();
        // SAFETY: the C shim expands libfuse's versioned session constructor.
        // The ops table is static, and userdata points to state owned by Session.
        let raw = NonNull::new(unsafe {
            studiofs_linux_fuse_session_new(
                args.as_mut_ptr(),
                studiofs_linux_fuse_low_level_ops(),
                studiofs_linux_fuse_low_level_ops_size(),
                userdata,
            )
        });
        let Some(raw) = raw else {
            return Err(io::Error::other(
                "libfuse3 failed to create low-level session",
            ));
        };
        // SAFETY: `raw` is a live libfuse session and `mount_point` is a valid
        // NUL-terminated path for this call.
        let mount_result = unsafe { raw::fuse_session_mount(raw.as_ptr(), mount_point.as_ptr()) };
        if mount_result == 0 {
            let invalidation_state = Arc::new(SessionInvalidationState::new());
            let session = Self {
                raw,
                state: Some(state),
                invalidation_state,
                signal_handlers_installed: false,
            };
            if let Some(state) = &session.state {
                state
                    .vfs
                    .set_kernel_invalidator(session.kernel_invalidator());
            }
            Ok(session)
        } else {
            // SAFETY: `raw` has not been returned and must be destroyed here.
            unsafe { raw::fuse_session_destroy(raw.as_ptr()) };
            Err(io::Error::last_os_error())
        }
    }

    pub(crate) fn install_signal_handlers(&mut self) -> io::Result<()> {
        // SAFETY: `self.raw` is a live libfuse session.
        let result = unsafe { raw::fuse_set_signal_handlers(self.raw.as_ptr()) };
        if result == 0 {
            self.signal_handlers_installed = true;
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(crate) fn run(&mut self) -> io::Result<()> {
        let max_background = self
            .state
            .as_ref()
            .map(|state| state.connection_config.max_background)
            .unwrap_or_else(|| LinuxFuseConnectionConfig::default().max_background);
        let loop_threads = linux_fuse_loop_thread_count(max_background);
        let watcher = self
            .state
            .as_ref()
            .map(|state| SessionExitWatcher::start(self.raw, Arc::clone(&state.async_writes)));
        // SAFETY: `self.raw` is a live libfuse session owned by this wrapper.
        // Keep idle workers equal to the cap so bursty filesystem I/O does not
        // pay thread create/destroy overhead between request spikes.
        let result = unsafe {
            studiofs_linux_fuse_session_loop_bounded(self.raw.as_ptr(), loop_threads, loop_threads)
        };
        if let Some(watcher) = watcher {
            watcher.stop();
        }
        if let Some(state) = &self.state {
            let shutdown = state.shutdown_async_writes_after_run();
            if !shutdown.watcher_stopped {
                warn!("Linux FUSE memory-pressure watcher was aborted during session shutdown");
            }
            if !shutdown.writes_stopped {
                warn!("Linux FUSE session loop exited before async write replies drained");
            }
        }
        if result == 0
            || result == libc::SIGINT
            || result == libc::SIGTERM
            || result == libc::SIGHUP
        {
            Ok(())
        } else if result < 0 {
            Err(io::Error::from_raw_os_error(-result))
        } else {
            Err(io::Error::from_raw_os_error(result))
        }
    }

    pub(crate) fn kernel_invalidator(&self) -> Arc<dyn KernelInvalidator> {
        let platform_invalidator: Arc<dyn KernelInvalidator> =
            Arc::new(LinuxFuseKernelInvalidator {
                session: self.raw,
                state: Arc::clone(&self.invalidation_state),
            });
        let state = self
            .state
            .as_ref()
            .expect("mounted session owns callback state");
        let checked: Arc<dyn KernelInvalidator> = Arc::new(LivenessCheckedKernelInvalidator::new(
            Arc::clone(&state.inode_liveness),
            platform_invalidator,
        ));
        Arc::new(PublishingLinuxKernelInvalidator::new(
            checked,
            Arc::clone(&state.inode_liveness),
            Arc::clone(&state.vfs),
        ))
    }
}

struct PublishingLinuxKernelInvalidator {
    inner: Arc<dyn KernelInvalidator>,
    inode_liveness: Arc<FuseInodeLiveness>,
    vfs: Arc<StudioVfs>,
}

impl PublishingLinuxKernelInvalidator {
    fn new(
        inner: Arc<dyn KernelInvalidator>,
        inode_liveness: Arc<FuseInodeLiveness>,
        vfs: Arc<StudioVfs>,
    ) -> Self {
        Self {
            inner,
            inode_liveness,
            vfs,
        }
    }

    fn publish_liveness_stats(&self) {
        publish_fuse_liveness_stats(&self.vfs, &self.inode_liveness);
    }
}

impl KernelInvalidator for PublishingLinuxKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.inner.inval_inode(ino);
        self.publish_liveness_stats();
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.inner.inval_inode_attrs(ino);
        self.publish_liveness_stats();
    }
}

#[derive(Debug)]
struct LinuxFuseKernelInvalidator {
    session: NonNull<raw::FuseSession>,
    state: Arc<SessionInvalidationState>,
}

#[derive(Debug)]
struct SessionInvalidationState {
    status: Mutex<SessionInvalidationStatus>,
    idle: Condvar,
}

#[derive(Debug)]
struct SessionInvalidationStatus {
    closed: bool,
    active: usize,
}

struct SessionInvalidationGuard<'a> {
    state: &'a SessionInvalidationState,
}

impl SessionInvalidationState {
    fn new() -> Self {
        Self {
            status: Mutex::new(SessionInvalidationStatus {
                closed: false,
                active: 0,
            }),
            idle: Condvar::new(),
        }
    }

    fn lock_status(&self) -> MutexGuard<'_, SessionInvalidationStatus> {
        // A panic while holding this gate should not prevent shutdown from
        // closing the gate or observing active invalidations.
        self.status.lock().unwrap_or_else(|err| err.into_inner())
    }

    fn begin_invalidation(&self) -> Option<SessionInvalidationGuard<'_>> {
        let mut status = self.lock_status();
        if status.closed {
            return None;
        }
        status.active += 1;
        Some(SessionInvalidationGuard { state: self })
    }

    fn begin_shutdown(&self) {
        self.lock_status().closed = true;
    }

    fn wait_for_idle(&self, timeout: Duration) -> bool {
        let status = self.lock_status();
        let result = self
            .idle
            .wait_timeout_while(status, timeout, |status| status.active > 0)
            .unwrap_or_else(|err| err.into_inner());
        result.0.active == 0
    }
}

impl Drop for SessionInvalidationGuard<'_> {
    fn drop(&mut self) {
        let should_notify = {
            let mut status = self.state.lock_status();
            status.active -= 1;
            status.active == 0
        };
        if should_notify {
            self.state.idle.notify_all();
        }
    }
}

// SAFETY: invalidation calls are gated against `Session::drop`, and the raw
// pointer is only used for libfuse notification calls while the session is live.
unsafe impl Send for LinuxFuseKernelInvalidator {}
// SAFETY: shared access only issues libfuse notification calls under the same gate.
unsafe impl Sync for LinuxFuseKernelInvalidator {}

impl KernelInvalidator for LinuxFuseKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.inval_inode_range(ino, 0, 0);
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.inval_inode_range(ino, -1, 0);
    }
}

impl LinuxFuseKernelInvalidator {
    fn inval_inode_range(&self, ino: u64, offset: off_t, len: off_t) {
        let Some(_active) = self.state.begin_invalidation() else {
            debug!(
                ino,
                offset, len, "Linux FUSE inode invalidation skipped after session shutdown"
            );
            return;
        };
        // SAFETY: `session` comes from a mounted libfuse3 session and the
        // invalidation guard prevents teardown from destroying it during the call.
        let result = unsafe {
            studiofs_linux_fuse_notify_inval_inode(self.session.as_ptr(), ino, offset, len)
        };
        if result != 0 && result != -libc::ENOENT {
            warn!(
                ino,
                offset, len, result, "Linux FUSE inode invalidation failed"
            );
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_destroy(userdata: *mut c_void) {
    let _ = userdata;
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_lookup(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result =
        with_name_bytes(name, |name| state.lookup(parent, name)).unwrap_or(Err(libc::EINVAL));
    reply_entry_result(req, state, result);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_forget(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    nlookup: u64,
) {
    reply_forget_with(callback_state(req), ino, nlookup, || {
        // SAFETY: libfuse owns this request and forget callbacks require exactly one none reply.
        unsafe { raw::fuse_reply_none(req) };
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_getattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    _fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.getattr(ino) {
        Ok(attr) => {
            // SAFETY: `attr` is initialized and valid for this synchronous reply.
            unsafe { raw::fuse_reply_attr(req, &attr, TTL_SECONDS) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_setattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    attr: *mut stat,
    to_set: c_int,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if state.read_only {
        reply_error(req, libc::EROFS);
        return;
    }
    let args = match setattr_args(attr.cast_const(), to_set) {
        Ok(args) => args,
        Err(errno) => {
            reply_error(req, errno);
            return;
        }
    };
    match state.setattr(state.vfs_file_handle(fi), ino, args) {
        Ok(attr) => {
            let attr = state.attr_to_stat(&attr);
            // SAFETY: `attr` is initialized and valid for this synchronous reply.
            unsafe { raw::fuse_reply_attr(req, &attr, TTL_SECONDS) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_create(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if require_file_info(fi).is_err() {
        reply_error(req, libc::EINVAL);
        return;
    }
    let result = with_name_bytes(name, |name| state.create(parent, name, mode, fi))
        .unwrap_or(Err(libc::EINVAL));
    match result {
        Ok(created) => reply_create(req, state, fi, created),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_mkdir(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result =
        with_name_bytes(name, |name| state.mkdir(parent, name, mode)).unwrap_or(Err(libc::EINVAL));
    reply_entry_result(req, state, result);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_mknod(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
    _rdev: libc::dev_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result =
        with_name_bytes(name, |name| state.mknod(parent, name, mode)).unwrap_or(Err(libc::EINVAL));
    reply_entry_result(req, state, result);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_symlink(
    req: *mut raw::FuseReq,
    link: *const c_char,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result = with_name_bytes(link, |target| {
        with_name_bytes(name, |name| state.symlink(parent, name, target))
    })
    .flatten()
    .unwrap_or(Err(libc::EINVAL));
    reply_entry_result(req, state, result);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_unlink(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match with_name_bytes(name, |name| state.unlink(parent, name)).unwrap_or(Err(libc::EINVAL)) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_rmdir(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match with_name_bytes(name, |name| state.rmdir(parent, name)).unwrap_or(Err(libc::EINVAL)) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_rename(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    newparent: raw::FuseIno,
    newname: *const c_char,
    flags: c_uint,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if flags != 0 {
        reply_error(req, libc::EINVAL);
        return;
    }
    let result = with_name_bytes(name, |name| {
        with_name_bytes(newname, |newname| {
            state.rename(parent, name, newparent, newname)
        })
    })
    .flatten()
    .unwrap_or(Err(libc::EINVAL));
    match result {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_link(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    newparent: raw::FuseIno,
    newname: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result = with_name_bytes(newname, |newname| state.link(ino, newparent, newname))
        .unwrap_or(Err(libc::EINVAL));
    reply_entry_result(req, state, result);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_open(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if require_file_info(fi).is_err() {
        reply_error(req, libc::EINVAL);
        return;
    }
    match state.open(ino, fi) {
        Ok(opened) if state.vfs.is_runtime_stats_file_ino(ino) => {
            reply_runtime_stats_open(req, state, fi, opened);
        }
        Ok(opened) => reply_file_open(req, state, fi, opened),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_read(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let started = Instant::now();
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if let Some(result) = state.read_runtime_stats(ino, size, off, file_handle(fi)) {
        match result {
            Ok(buffer) => {
                // SAFETY: `buffer` is valid for this synchronous reply.
                unsafe { raw::fuse_reply_buf(req, buffer.as_ptr().cast::<c_char>(), buffer.len()) };
            }
            Err(errno) => reply_error(req, errno),
        }
        return;
    }
    let result = state.read(ino, size, off, fi);
    finish_fuse_read_reply(&state.vfs, FuseReqPtr::new(req), size, started, result);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_readlink(req: *mut raw::FuseReq, ino: raw::FuseIno) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.readlink(ino) {
        Ok(target) => {
            // Linux readlink replies are byte buffers without a trailing NUL.
            // SAFETY: `target` is valid for this synchronous reply.
            unsafe { raw::fuse_reply_buf(req, target.as_ptr().cast::<c_char>(), target.len()) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_write(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    buf: *const c_char,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let started = Instant::now();
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.queue_async_write(req, ino, buf, size, off, fi, started) {
        Ok(true) => return,
        Ok(false) => {}
        Err(errno) => {
            let _ = FuseReqPtr::new(req).reply_error(errno);
            state
                .vfs
                .record_fuse_write_callback(size, 0, started.elapsed(), true);
            return;
        }
    }
    match state.write(ino, buf, size, off, fi) {
        Ok(written) => {
            let status = FuseReqPtr::new(req).reply_write(written as size_t);
            state.vfs.record_fuse_write_callback(
                size,
                if status == 0 { written as usize } else { 0 },
                started.elapsed(),
                status != 0,
            );
        }
        Err(errno) => {
            let _ = FuseReqPtr::new(req).reply_error(errno);
            state
                .vfs
                .record_fuse_write_callback(size, 0, started.elapsed(), true);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_flush(
    req: *mut raw::FuseReq,
    _ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let started = Instant::now();
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let fh = file_handle(fi);
    if let Some(runtime_fh) = fh.filter(|fh| state.is_runtime_stats_handle(*fh)) {
        let result = state.flush(runtime_fh);
        let errno = result.err().unwrap_or(0);
        let _ = FuseReqPtr::new(req).reply_error(errno);
        return;
    }
    let result = fh.ok_or(libc::EBADF).and_then(|fh| state.flush(fh));
    finish_fuse_barrier_reply(
        &state.vfs,
        FuseReqPtr::new(req),
        FuseBarrierKind::Flush,
        started,
        result,
    );
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_fsync(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    datasync: c_int,
    fi: *mut raw::FuseFileInfo,
) {
    let started = Instant::now();
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let fh = file_handle(fi);
    if fh.is_some_and(|fh| state.is_runtime_stats_handle(fh)) {
        let _ = FuseReqPtr::new(req).reply_error(0);
        return;
    }
    let result = state.fsync(fh, ino, datasync != 0);
    finish_fuse_barrier_reply(
        &state.vfs,
        FuseReqPtr::new(req),
        FuseBarrierKind::Fsync,
        started,
        result,
    );
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_release(
    req: *mut raw::FuseReq,
    _ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let started = Instant::now();
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let fh = file_handle(fi);
    if let Some(runtime_fh) = fh.filter(|fh| state.is_runtime_stats_handle(*fh)) {
        let result = state.release(runtime_fh);
        let errno = result.err().unwrap_or(0);
        let _ = FuseReqPtr::new(req).reply_error(errno);
        return;
    }
    let result = fh.ok_or(libc::EBADF).and_then(|fh| state.release(fh));
    finish_fuse_barrier_reply(
        &state.vfs,
        FuseReqPtr::new(req),
        FuseBarrierKind::Release,
        started,
        result,
    );
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_opendir(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.getattr_attr(ino) {
        Ok(attr) if attr.kind == FileType::Directory => match state.open_dir(ino) {
            Ok(handle) => reply_dir_open(req, fi, handle),
            Err(errno) => reply_error(req, errno),
        },
        Ok(_) => reply_error(req, libc::ENOTDIR),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_readdir(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let Some(handle) = dir_handle(fi) else {
        reply_error(req, libc::EBADF);
        return;
    };
    match state.readdir(req, ino, handle, off, size) {
        Ok(buffer) => {
            let bytes = buffer.as_slice();
            // SAFETY: `bytes` is valid for this synchronous reply.
            unsafe { raw::fuse_reply_buf(req, bytes.as_ptr().cast::<c_char>(), bytes.len()) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_releasedir(
    req: *mut raw::FuseReq,
    _ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    if let Some(handle) = dir_handle(fi) {
        // SAFETY: `opendir` created this pointer with `Box::into_raw`, and
        // libfuse calls `releasedir` once for each successful directory open.
        unsafe { drop(Box::from_raw(handle.as_ptr())) };
    }
    reply_error(req, 0);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_statfs(req: *mut raw::FuseReq, _ino: raw::FuseIno) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.statfs() {
        Ok(stat) => {
            // SAFETY: `stat` is initialized and valid for this synchronous reply.
            unsafe { raw::fuse_reply_statfs(req, &stat) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_access(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    mask: c_int,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.access(ino, mask) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_setxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
    value: *const c_char,
    size: size_t,
    flags: c_int,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let Ok(flags) = u32::try_from(flags) else {
        reply_error(req, libc::EINVAL);
        return;
    };
    let result = with_name_bytes(name, |name| {
        with_xattr_value(value, size, |value| {
            state.set_xattr(ino, name, value, flags)
        })
    })
    .flatten()
    .unwrap_or(Err(libc::EINVAL));
    match result {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_getxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
    size: size_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if size == 0 {
        let result = with_name_bytes(name, |name| state.get_xattr_size(ino, name))
            .unwrap_or(Err(libc::EINVAL));
        match result {
            Ok(Some(value_size)) => {
                // SAFETY: `req` is live and this consumes the callback reply.
                unsafe { raw::fuse_reply_xattr(req, value_size) };
            }
            Ok(None) => reply_error(req, libc::ENODATA),
            Err(errno) => reply_error(req, errno),
        }
        return;
    }

    let result =
        with_name_bytes(name, |name| state.get_xattr(ino, name)).unwrap_or(Err(libc::EINVAL));
    match result {
        Ok(Some(value)) => reply_xattr_value(req, size, &value),
        Ok(None) => reply_error(req, libc::ENODATA),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_listxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.list_xattrs(ino) {
        Ok(names) if size == 0 => {
            let value_size = names.iter().map(|name| name.len() + 1).sum();
            // SAFETY: `req` is live and this consumes the callback reply.
            unsafe { raw::fuse_reply_xattr(req, value_size) };
        }
        Ok(names) => reply_xattr_value(req, size, &format_xattr_name_list(&names)),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_removexattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result =
        with_name_bytes(name, |name| state.remove_xattr(ino, name)).unwrap_or(Err(libc::EINVAL));
    match result {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

fn callback_state(req: *mut raw::FuseReq) -> Option<&'static SessionState> {
    if req.is_null() {
        return None;
    }
    // SAFETY: libfuse returns the userdata pointer supplied at session creation.
    let userdata = unsafe { raw::fuse_req_userdata(req) };
    callback_state_from_userdata(userdata)
}

fn callback_state_from_userdata(userdata: *mut c_void) -> Option<&'static SessionState> {
    if userdata.is_null() {
        None
    } else {
        // SAFETY: userdata lives until libfuse calls the destroy callback.
        Some(unsafe { &*userdata.cast::<SessionState>() })
    }
}

fn with_name_bytes<T>(name: *const c_char, f: impl FnOnce(&[u8]) -> T) -> Option<T> {
    if name.is_null() {
        return None;
    }
    // SAFETY: libfuse passes callback names as NUL-terminated strings valid
    // for the duration of the callback.
    Some(f(unsafe { CStr::from_ptr(name) }.to_bytes()))
}

fn with_xattr_value<T>(value: *const c_char, size: usize, f: impl FnOnce(&[u8]) -> T) -> Option<T> {
    if size == 0 {
        return Some(f(&[]));
    }
    if value.is_null() {
        return None;
    }
    // SAFETY: libfuse passes `value` as a request-owned byte slice valid for
    // the duration of the callback.
    Some(f(unsafe {
        std::slice::from_raw_parts(value.cast::<u8>(), size)
    }))
}

impl SessionState {
    fn lookup(&self, parent: u64, name: &[u8]) -> Result<InodeAttr, i32> {
        self.vfs
            .lookup(parent, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn getattr_attr(&self, ino: u64) -> Result<InodeAttr, i32> {
        self.vfs
            .getattr(ino)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn getattr(&self, ino: u64) -> Result<stat, i32> {
        self.getattr_attr(ino).map(|attr| self.attr_to_stat(&attr))
    }

    fn setattr(
        &self,
        file_handle: Option<u64>,
        ino: u64,
        args: SetattrArgs,
    ) -> Result<InodeAttr, i32> {
        if linux_fuse_metadata_admission_enabled() && linux_fuse_setattr_args_are_attr_only(&args) {
            return self
                .vfs
                .setattr_attr_only_deferred_metadata(ino, args, &self.runtime_handle)
                .map_err(|error| vfs_err_to_errno(&error));
        }
        self.vfs
            .setattr_with_handle(file_handle, ino, args)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn create(
        &self,
        parent: u64,
        name: &[u8],
        mode: mode_t,
        fi: *const raw::FuseFileInfo,
    ) -> Result<LinuxFuseCreatedFile, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        require_file_info(fi)?;
        let file_type = mode & libc::S_IFMT as mode_t;
        if file_type != 0 && file_type != libc::S_IFREG as mode_t {
            return Err(libc::ENOTSUP);
        }
        let mode = (mode & 0o7777) as u16;
        let attr = if linux_fuse_metadata_admission_enabled() {
            self.vfs.create_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            )
        } else {
            self.vfs
                .create(parent, name, self.mount_uid, self.mount_gid, mode)
        }
        .map_err(|error| vfs_err_to_errno(&error))?;
        let (open_flags, trunc) = posix_flags_to_open_flags(file_info_flags(fi));
        let fh = self
            .vfs
            .open_created_file_with_known_attr(&attr, open_flags, trunc)
            .map_err(|error| vfs_err_to_errno(&error))?;
        Ok(LinuxFuseCreatedFile {
            opened: LinuxFuseOpenedFile { fh, ino: attr.ino },
            attr,
        })
    }

    fn mkdir(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let mode = (mode & 0o7777) as u16;
        let result = if linux_fuse_metadata_admission_enabled() {
            self.vfs.mkdir_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            )
        } else {
            self.vfs
                .mkdir(parent, name, self.mount_uid, self.mount_gid, mode)
        };
        result.map_err(|error| vfs_err_to_errno(&error))
    }

    fn mknod(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let file_type = mode & libc::S_IFMT as mode_t;
        if file_type != 0 && file_type != libc::S_IFREG as mode_t {
            return Err(libc::ENOTSUP);
        }
        let mode = (mode & 0o7777) as u16;
        let result = if linux_fuse_metadata_admission_enabled() {
            self.vfs.create_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            )
        } else {
            self.vfs
                .create(parent, name, self.mount_uid, self.mount_gid, mode)
        };
        result.map_err(|error| vfs_err_to_errno(&error))
    }

    fn symlink(&self, parent: u64, name: &[u8], target: &[u8]) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .symlink(parent, name, target, self.mount_uid, self.mount_gid)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn unlink(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .unlink(parent, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn rmdir(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .rmdir(parent, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn rename(&self, parent: u64, name: &[u8], newparent: u64, newname: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .rename(parent, name, newparent, newname)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn link(&self, ino: u64, newparent: u64, newname: &[u8]) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .link(ino, newparent, newname)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn open(&self, ino: u64, fi: *const raw::FuseFileInfo) -> Result<LinuxFuseOpenedFile, i32> {
        if let Some(result) = self.open_runtime_stats(ino, file_info_flags(fi)) {
            return result;
        }
        let attr = self.getattr_attr(ino)?;
        match attr.kind {
            FileType::RegularFile => {}
            FileType::Directory => return Err(libc::EISDIR),
            FileType::Symlink => return Err(libc::EINVAL),
        }
        let (open_flags, trunc) = posix_flags_to_open_flags(file_info_flags(fi));
        if self.read_only
            && (trunc
                || open_flags.contains(OpenFlags::WRONLY)
                || open_flags.contains(OpenFlags::RDWR))
        {
            return Err(libc::EROFS);
        }
        self.vfs
            .open_file_with_known_attr(&attr, open_flags, trunc)
            .map(|fh| LinuxFuseOpenedFile { fh, ino })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn open_runtime_stats(
        &self,
        ino: u64,
        flags: c_int,
    ) -> Option<Result<LinuxFuseOpenedFile, i32>> {
        if !self.vfs.is_runtime_stats_file_ino(ino) {
            return None;
        }
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        if trunc || open_flags.contains(OpenFlags::WRONLY) || open_flags.contains(OpenFlags::RDWR) {
            return Some(Err(libc::EROFS));
        }
        let fh = self.next_runtime_stats_fh.fetch_add(1, Ordering::Relaxed);
        let bytes = self.vfs.runtime_stats_file_text().into_bytes();
        self.runtime_stats_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(fh, bytes);
        Some(Ok(LinuxFuseOpenedFile { fh, ino }))
    }

    fn read_runtime_stats(
        &self,
        ino: u64,
        size: usize,
        offset: off_t,
        fh: Option<u64>,
    ) -> Option<Result<Vec<u8>, i32>> {
        if !self.vfs.is_runtime_stats_file_ino(ino) {
            return None;
        }
        Some((|| {
            let fh = fh.ok_or(libc::EBADF)?;
            let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
            let mut buffer = vec![0; clamp_read_size(size)];
            let handles = self
                .runtime_stats_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let bytes = handles.get(&fh).ok_or(libc::EBADF)?;
            let read = studiofs_vfs::read_runtime_stats_buffer(bytes, offset, &mut buffer);
            buffer.truncate(read as usize);
            Ok(buffer)
        })())
    }

    fn is_runtime_stats_handle(&self, fh: u64) -> bool {
        fh >= RUNTIME_STATS_FH_START
            && self
                .runtime_stats_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains_key(&fh)
    }

    fn read(
        &self,
        ino: u64,
        size: usize,
        offset: off_t,
        fi: *const raw::FuseFileInfo,
    ) -> Result<Vec<u8>, i32> {
        let fh = file_handle(fi).ok_or(libc::EBADF)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let size = clamp_read_size(size);
        let mut buffer = vec![0; size];
        let read = self
            .runtime_handle
            .block_on(self.vfs.read_into_fh(fh, ino, offset, &mut buffer))
            .map_err(|error| vfs_err_to_errno(&error))?;
        buffer.truncate(read as usize);
        Ok(buffer)
    }

    fn readlink(&self, ino: u64) -> Result<Vec<u8>, i32> {
        self.vfs
            .readlink(ino)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn write(
        &self,
        ino: u64,
        buf: *const c_char,
        size: usize,
        offset: off_t,
        fi: *const raw::FuseFileInfo,
    ) -> Result<u32, i32> {
        let request = self.prepare_write_metadata(buf, size, offset, fi)?;
        with_write_data_slice(buf, size, |data| {
            self.runtime_handle
                .block_on(self.vfs.write_fh_with_deferred_materialize(
                    request.fh,
                    ino,
                    request.offset,
                    data,
                    request.local_sync_policy,
                ))
        })?
        .map_err(|error| vfs_err_to_errno(&error))
    }

    // Mirrors libfuse write callback inputs plus the request start timestamp.
    #[allow(clippy::too_many_arguments)]
    fn queue_async_write(
        &self,
        req: *mut raw::FuseReq,
        ino: u64,
        buf: *const c_char,
        size: usize,
        offset: off_t,
        fi: *const raw::FuseFileInfo,
        started: Instant,
    ) -> Result<bool, i32> {
        if self.cache_policy == LinuxFuseCachePolicy::Kernel || !linux_fuse_async_write_enabled() {
            return Ok(false);
        }
        let request = self.prepare_write_metadata(buf, size, offset, fi)?;
        if !request.local_sync_policy.is_opportunistic() {
            return Ok(false);
        }

        let fh = request.fh;
        let write_offset = request.offset;
        let writes = Arc::clone(&self.async_writes);
        let vfs = self.vfs.clone();
        let metric_config = self.metric_config;
        let request_ticket = self.start_live_async_write_request(fh)?;
        self.refresh_async_write_active_limit();
        let mut guard = FuseWriteTaskGuard {
            request: Some(FuseReqPtr::new(req)),
            request_ticket: Some(request_ticket),
            reservation: None,
            writes: Arc::clone(&writes),
            vfs: Arc::clone(&vfs),
            started,
            requested_bytes: size,
            metric_config,
        };
        record_linux_async_write_snapshot(&vfs, &writes, metric_config);

        let admitted = match with_write_data_slice(buf, size, |data| {
            writes
                .admit(data.len(), |waiting_stats| {
                    record_linux_async_write_stats(&vfs, waiting_stats, metric_config);
                })
                .map(|reservation| {
                    guard.attach_reservation(reservation);
                    record_linux_async_write_snapshot(&vfs, &writes, metric_config);
                    Bytes::copy_from_slice(data)
                })
        }) {
            Ok(admitted) => admitted,
            Err(errno) => {
                guard.finish(FuseWriteTaskResult::AdmissionRejected(errno));
                return Ok(true);
            }
        };
        let data = match admitted {
            Ok(data) => data,
            Err(errno) => {
                guard.finish(FuseWriteTaskResult::AdmissionRejected(errno));
                return Ok(true);
            }
        };
        let cancellation = writes.task_cancellation_token();
        self.runtime_handle.spawn(async move {
            let result = tokio::select! {
                biased;
                _ = cancellation.cancelled() => FuseWriteTaskResult::Cancelled,
                result = vfs.write_fh_owned_with_deferred_materialize(
                    fh,
                    ino,
                    write_offset,
                    data,
                    LocalSyncPolicy::Opportunistic,
                ) => FuseWriteTaskResult::Completed(
                    result.map_err(|error| vfs_err_to_errno(&error)),
                ),
            };
            guard.finish(result);
        });
        Ok(true)
    }

    fn start_live_async_write_request(&self, fh: u64) -> Result<LinuxFuseRequestTicket, i32> {
        let request_ticket = self.async_writes.start_request(fh)?;
        if self.vfs.get_open_file(fh).is_some() {
            return Ok(request_ticket);
        }

        request_ticket.complete(LinuxFuseAsyncWriteCompletion::Failed(libc::EBADF));
        self.async_writes.forget_handle(fh);
        record_linux_async_write_snapshot(&self.vfs, &self.async_writes, self.metric_config);
        Err(libc::EBADF)
    }

    fn prepare_write_metadata(
        &self,
        buf: *const c_char,
        size: usize,
        offset: off_t,
        fi: *const raw::FuseFileInfo,
    ) -> Result<LinuxFuseWriteRequest, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let fh = file_handle(fi).ok_or(libc::EBADF)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        if buf.is_null() && size > 0 {
            return Err(libc::EINVAL);
        }
        let local_sync_policy = self.local_sync_policy_for_handle(fh);
        Ok(LinuxFuseWriteRequest {
            fh,
            offset,
            local_sync_policy,
        })
    }

    fn local_sync_policy_for_handle(&self, fh: u64) -> LocalSyncPolicy {
        self.vfs
            .get_open_file(fh)
            .filter(|open_file| open_file.flags.contains(OpenFlags::SYNC))
            .map(|_| LocalSyncPolicy::WriteThrough)
            .unwrap_or(LocalSyncPolicy::Opportunistic)
    }

    fn flush(&self, fh: u64) -> Result<(), i32> {
        if fh >= RUNTIME_STATS_FH_START
            && self
                .runtime_stats_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains_key(&fh)
        {
            return Ok(());
        }
        let watermark = self.async_writes.watermark(fh);
        let async_result = self.async_writes.wait_through(watermark);
        let local_result = normalize_close_barrier_result(
            self.runtime_handle
                .block_on(self.vfs.flush_fh(fh))
                .map_err(|error| vfs_err_to_errno(&error)),
        );
        merge_async_and_local_barrier_results(async_result, local_result)
    }

    fn fsync(&self, file_handle: Option<u64>, _ino: u64, _datasync: bool) -> Result<(), i32> {
        let fh = file_handle
            .filter(|fh| self.vfs.get_open_file(*fh).is_some())
            .ok_or(libc::EBADF)?;
        let watermark = self.async_writes.watermark(fh);
        let async_result = self.async_writes.wait_through(watermark);
        // StudioVfs does not currently distinguish fdatasync from fsync on
        // this handle-backed path, so both use the strict handle flush barrier.
        let local_result = self
            .runtime_handle
            .block_on(self.vfs.flush_fh(fh))
            .map_err(|error| vfs_err_to_errno(&error));
        merge_async_and_local_barrier_results(async_result, local_result)
    }

    fn release(&self, fh: u64) -> Result<(), i32> {
        if fh >= RUNTIME_STATS_FH_START
            && self
                .runtime_stats_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&fh)
                .is_some()
        {
            return Ok(());
        }
        let open_file = self.vfs.get_open_file(fh).ok_or(libc::EBADF)?;
        let local_sync_policy = if open_file.flags.contains(OpenFlags::SYNC) {
            LocalSyncPolicy::WriteThrough
        } else {
            LocalSyncPolicy::Opportunistic
        };
        let watermark = self.async_writes.close_handle(fh);
        let _forget_guard = LinuxFuseHandleLedgerGuard::new(Arc::clone(&self.async_writes), fh);
        let async_result = self.async_writes.wait_through(watermark);
        let mode = linux_fuse_release_mode(
            local_sync_policy,
            self.vfs.inode_has_local_dirty_state(open_file.ino),
        );
        let local_result = match mode {
            LinuxFuseReleaseMode::Synchronous => self
                .runtime_handle
                .block_on(self.vfs.release_fh_with_outcome(fh))
                .map(|_| ())
                .map_err(|error| vfs_err_to_errno(&error)),
            LinuxFuseReleaseMode::Deferred => {
                self.vfs
                    .release_fh_with_deferred_last_handle_barrier(fh, &self.runtime_handle);
                Ok(())
            }
        };
        merge_async_and_local_barrier_results(
            async_result,
            normalize_close_barrier_result(local_result),
        )
    }

    fn vfs_file_handle(&self, fi: *const raw::FuseFileInfo) -> Option<u64> {
        file_handle(fi).filter(|fh| self.vfs.get_open_file(*fh).is_some())
    }

    fn open_dir(&self, ino: u64) -> Result<NonNull<DirectoryHandle>, i32> {
        // SFS-604 snapshots each directory handle eagerly so Linux readdir offsets
        // stay stable across chunked kernel reads. Replace with VFS cursors if
        // huge-directory memory use shows up in workload evidence.
        let entries = self
            .vfs
            .readdir_with_attrs(ino)
            .map_err(|error| vfs_err_to_errno(&error))?;
        let raw = Box::into_raw(Box::new(DirectoryHandle { entries }));
        NonNull::new(raw).ok_or(libc::ENOMEM)
    }

    fn readdir(
        &self,
        req: *mut raw::FuseReq,
        ino: u64,
        handle: NonNull<DirectoryHandle>,
        offset: off_t,
        size: usize,
    ) -> Result<DirectoryEntryBuffer, i32> {
        let mut buffer = DirectoryEntryBuffer::new(size);
        let mut idx = usize::try_from(offset).map_err(|_| libc::EINVAL)?;
        let dir_attr = minimal_dir_stat(ino);
        if idx == 0 {
            if !buffer.push(req, OsStr::new("."), &dir_attr, 1)? {
                return Ok(buffer);
            }
            idx = 1;
        }
        if idx == 1 {
            if !buffer.push(req, OsStr::new(".."), &dir_attr, 2)? {
                return Ok(buffer);
            }
            idx = 2;
        }

        // SAFETY: `handle` comes from `opendir` and remains owned by the
        // matching directory handle until `releasedir`.
        let entries = unsafe { &handle.as_ref().entries };
        for (i, entry) in entries.iter().enumerate().skip(idx.saturating_sub(2)) {
            let entry_idx = i + 2;
            let attr = entry.attr.as_ref().map_or_else(
                || minimal_entry_stat(entry.entry.ino, entry.entry.kind),
                |attr| self.attr_to_stat(attr),
            );
            let next_offset = off_t::try_from(entry_idx + 1).map_err(|_| libc::EOVERFLOW)?;
            if !buffer.push(
                req,
                OsStr::from_bytes(&entry.entry.name),
                &attr,
                next_offset,
            )? {
                break;
            }
        }
        Ok(buffer)
    }

    fn statfs(&self) -> Result<statvfs, i32> {
        self.vfs
            .statfs()
            .map(statfs_to_statvfs)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn access(&self, ino: u64, mask: c_int) -> Result<(), i32> {
        let attr = self.getattr_attr(ino)?;
        if mask == libc::F_OK {
            return Ok(());
        }
        if self.read_only && mask & libc::W_OK != 0 {
            return Err(libc::EROFS);
        }
        let mode = attr.mode;
        let readable = mode & 0o444 != 0;
        let writable = mode & 0o222 != 0;
        let executable = mode & 0o111 != 0 || attr.kind == FileType::Directory;
        if (mask & libc::R_OK == 0 || readable)
            && (mask & libc::W_OK == 0 || writable)
            && (mask & libc::X_OK == 0 || executable)
        {
            Ok(())
        } else {
            Err(libc::EACCES)
        }
    }

    fn attr_to_stat(&self, attr: &InodeAttr) -> stat {
        bridge_attr_to_stat(&to_bridge_attr(
            attr,
            self.mount_uid,
            self.mount_gid,
            LINUX_FUSE_BLOCK_SIZE,
        ))
    }

    fn get_xattr(&self, ino: u64, name: &[u8]) -> Result<Option<Vec<u8>>, i32> {
        self.vfs
            .get_xattr(ino, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn get_xattr_size(&self, ino: u64, name: &[u8]) -> Result<Option<usize>, i32> {
        self.vfs
            .get_xattr_size(ino, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn set_xattr(&self, ino: u64, name: &[u8], value: &[u8], flags: u32) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .set_xattr(ino, name, value, flags)
            .map_err(|error| xattr_mutation_err_to_errno(&error))
    }

    fn remove_xattr(&self, ino: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .remove_xattr(ino, name)
            .map_err(|error| xattr_mutation_err_to_errno(&error))
    }

    fn list_xattrs(&self, ino: u64) -> Result<Vec<Vec<u8>>, i32> {
        self.vfs
            .list_xattrs(ino)
            .map_err(|error| vfs_err_to_errno(&error))
    }
}

struct DirectoryHandle {
    entries: Vec<DirEntryWithAttr>,
}

fn dir_handle(fi: *const raw::FuseFileInfo) -> Option<NonNull<DirectoryHandle>> {
    if fi.is_null() {
        return None;
    }
    // SAFETY: libfuse passes a live `fuse_file_info`; the C shim reads only
    // the `fh` field using the system header layout.
    let fh = unsafe { studiofs_linux_fuse_file_info_fh(fi) };
    NonNull::new(fh as *mut DirectoryHandle)
}

fn file_handle(fi: *const raw::FuseFileInfo) -> Option<u64> {
    if fi.is_null() {
        return None;
    }
    // SAFETY: libfuse passes a live `fuse_file_info`; the C shim reads only
    // the `fh` field using the system header layout.
    let fh = unsafe { studiofs_linux_fuse_file_info_fh(fi) };
    (fh != 0).then_some(fh)
}

fn file_info_flags(fi: *const raw::FuseFileInfo) -> c_int {
    if fi.is_null() {
        return 0;
    }
    // SAFETY: libfuse passes a live `fuse_file_info`; the C shim reads only
    // the `flags` field using the system header layout.
    unsafe { studiofs_linux_fuse_file_info_flags(fi) }
}

fn bridge_attr_to_stat(attr: &BridgeFileAttr) -> stat {
    let out = MaybeUninit::<stat>::zeroed();
    // SAFETY: zeroed `stat` is valid; fields used by libfuse are initialized.
    let mut out = unsafe { out.assume_init() };
    out.st_ino = attr.ino as _;
    out.st_size = attr.size as _;
    out.st_blocks = attr.blocks as _;
    out.st_blksize = attr.blksize as _;
    out.st_nlink = attr.nlink as _;
    out.st_uid = attr.uid as _;
    out.st_gid = attr.gid as _;
    out.st_rdev = attr.rdev as _;
    out.st_mode = mode_t::from(attr.perm) | file_type_mode(attr.kind);
    let (seconds, nanos) = system_time_parts(attr.atime);
    out.st_atime = seconds;
    out.st_atime_nsec = nanos;
    let (seconds, nanos) = system_time_parts(attr.mtime);
    out.st_mtime = seconds;
    out.st_mtime_nsec = nanos;
    let (seconds, nanos) = system_time_parts(attr.ctime);
    out.st_ctime = seconds;
    out.st_ctime_nsec = nanos;
    out
}

fn minimal_dir_stat(ino: u64) -> stat {
    minimal_entry_stat(ino, FileType::Directory)
}

fn minimal_entry_stat(ino: u64, kind: FileType) -> stat {
    let out = MaybeUninit::<stat>::zeroed();
    // SAFETY: zeroed `stat` is valid; minimal direntry fields are initialized.
    let mut out = unsafe { out.assume_init() };
    out.st_ino = ino as _;
    out.st_mode = file_type_mode(match kind {
        FileType::RegularFile => BridgeFileType::RegularFile,
        FileType::Directory => BridgeFileType::Directory,
        FileType::Symlink => BridgeFileType::Symlink,
    });
    out
}

fn file_type_mode(kind: BridgeFileType) -> mode_t {
    match kind {
        BridgeFileType::RegularFile => libc::S_IFREG,
        BridgeFileType::Directory => libc::S_IFDIR,
        BridgeFileType::Symlink => libc::S_IFLNK,
    }
}

fn system_time_parts(time: std::time::SystemTime) -> (libc::time_t, libc::c_long) {
    // StudioFS metadata normally stores Unix-epoch times; clamp pre-epoch values
    // to zero rather than reporting negative timestamps through this first Linux bridge slice.
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (duration.as_secs() as _, duration.subsec_nanos() as _)
}

fn statfs_to_statvfs(stats: StatFs) -> statvfs {
    let out = MaybeUninit::<statvfs>::zeroed();
    // SAFETY: zeroed `statvfs` is valid; fields used by libfuse are initialized.
    let mut out = unsafe { out.assume_init() };
    out.f_bsize = stats.bsize as _;
    out.f_frsize = stats.frsize as _;
    out.f_blocks = stats.blocks as _;
    out.f_bfree = stats.bfree as _;
    out.f_bavail = stats.bavail as _;
    out.f_files = stats.files as _;
    out.f_ffree = stats.ffree as _;
    out.f_namemax = stats.namelen as _;
    out
}

fn setattr_args(attr: *const stat, to_set: c_int) -> Result<SetattrArgs, i32> {
    if attr.is_null() {
        return Err(libc::EINVAL);
    }
    let mut args = SetattrArgs::default();
    // SAFETY: libfuse passes a live `stat` pointer for setattr callbacks.
    let attr = unsafe { &*attr };
    if to_set & FUSE_SET_ATTR_SIZE != 0 {
        args.size = Some(u64::try_from(attr.st_size).map_err(|_| libc::EINVAL)?);
    }
    if to_set & FUSE_SET_ATTR_MODE != 0 {
        args.mode = Some((attr.st_mode & 0o7777) as u16);
    }
    if to_set & FUSE_SET_ATTR_UID != 0 {
        args.uid = Some(attr.st_uid);
    }
    if to_set & FUSE_SET_ATTR_GID != 0 {
        args.gid = Some(attr.st_gid);
    }
    if to_set & FUSE_SET_ATTR_ATIME_NOW != 0 {
        args.atime = TimeSpec::Now;
    } else if to_set & FUSE_SET_ATTR_ATIME != 0 {
        args.atime = TimeSpec::Set(timespec_nanos(attr.st_atime, attr.st_atime_nsec)?);
    }
    if to_set & FUSE_SET_ATTR_MTIME_NOW != 0 {
        args.mtime = TimeSpec::Now;
    } else if to_set & FUSE_SET_ATTR_MTIME != 0 {
        args.mtime = TimeSpec::Set(timespec_nanos(attr.st_mtime, attr.st_mtime_nsec)?);
    }
    Ok(args)
}

fn timespec_nanos(sec: libc::time_t, nsec: libc::c_long) -> Result<i64, i32> {
    let nanos = i128::from(sec)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(nsec)))
        .ok_or(libc::EOVERFLOW)?;
    i64::try_from(nanos).map_err(|_| libc::EOVERFLOW)
}

fn require_file_info(fi: *const raw::FuseFileInfo) -> Result<(), i32> {
    if fi.is_null() {
        Err(libc::EINVAL)
    } else {
        Ok(())
    }
}

fn clamp_read_size(size: usize) -> usize {
    size.min(LINUX_FUSE_MAX_READ_SIZE)
}

fn with_write_data_slice<T>(
    buf: *const c_char,
    size: usize,
    f: impl FnOnce(&[u8]) -> T,
) -> Result<T, i32> {
    if size == 0 {
        return Ok(f(&[]));
    }
    if buf.is_null() {
        return Err(libc::EINVAL);
    }
    // SAFETY: libfuse passes `buf` as a request-owned byte slice valid for the
    // duration of this write callback; the closure cannot return a borrowed
    // reference to it because the slice has no named lifetime.
    Ok(f(unsafe {
        std::slice::from_raw_parts(buf.cast::<u8>(), size)
    }))
}

fn linux_fuse_open_cache_policy_for(
    cache_policy: LinuxFuseCachePolicy,
    async_dio_enabled: bool,
    field_supported: bool,
    disposition: OsCacheDisposition,
) -> LinuxFuseOpenCachePolicy {
    LinuxFuseOpenCachePolicy {
        direct_io: cache_policy == LinuxFuseCachePolicy::Direct,
        keep_cache: cache_policy == LinuxFuseCachePolicy::Kernel
            && disposition == OsCacheDisposition::Reuse,
        parallel_direct_writes: cache_policy == LinuxFuseCachePolicy::Direct
            && linux_fuse_parallel_direct_writes_enabled(async_dio_enabled, field_supported),
    }
}

fn fuse_flag(value: bool) -> c_int {
    if value { 1 } else { 0 }
}

fn reply_forget_with(
    state: Option<&SessionState>,
    ino: raw::FuseIno,
    nlookup: u64,
    reply_none: impl FnOnce(),
) {
    if let Some(state) = state {
        state.inode_liveness.forget(ino, nlookup);
        state.publish_liveness_stats();
    }
    reply_none();
}

fn reply_entry_result(
    req: *mut raw::FuseReq,
    state: &SessionState,
    result: Result<InodeAttr, i32>,
) {
    match result {
        Ok(attr) => reply_entry_with(state, &attr, |entry| {
            // SAFETY: `entry` is initialized and valid for this synchronous reply.
            unsafe { raw::fuse_reply_entry(req, entry) }
        }),
        Err(errno) => reply_error(req, errno),
    }
}

fn reply_entry_with(
    state: &SessionState,
    attr: &InodeAttr,
    reply: impl FnOnce(&raw::FuseEntryParam) -> c_int,
) {
    let entry = raw::FuseEntryParam {
        ino: attr.ino,
        generation: 1,
        attr: state.attr_to_stat(attr),
        attr_timeout: TTL_SECONDS,
        entry_timeout: TTL_SECONDS,
    };
    let reservation = state.inode_liveness.reserve_lookup_reply(attr.ino);
    let status = reply(&entry);
    if status == 0 {
        reservation.accept();
    } else {
        reservation.reject();
    }
    state.publish_liveness_stats();
}

fn reply_dir_open(
    req: *mut raw::FuseReq,
    fi: *mut raw::FuseFileInfo,
    handle: NonNull<DirectoryHandle>,
) {
    if fi.is_null() {
        // SAFETY: ownership has not been handed to libfuse because `fi` is invalid.
        unsafe { drop(Box::from_raw(handle.as_ptr())) };
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: libfuse owns `fi` for this callback; the C shim writes fields
    // using the exact system header layout.
    unsafe {
        studiofs_linux_fuse_dir_file_info_init(fi, handle.as_ptr() as u64);
        raw::fuse_reply_open(req, fi);
    }
}

fn reply_file_open(
    req: *mut raw::FuseReq,
    state: &SessionState,
    fi: *mut raw::FuseFileInfo,
    opened: LinuxFuseOpenedFile,
) {
    reply_file_open_with(state, fi, opened, |fi| {
        // SAFETY: `fi` is initialized and valid for this synchronous reply.
        unsafe { raw::fuse_reply_open(req, fi) }
    });
}

fn reply_file_open_with(
    state: &SessionState,
    fi: *mut raw::FuseFileInfo,
    opened: LinuxFuseOpenedFile,
    reply: impl FnOnce(*const raw::FuseFileInfo) -> c_int,
) -> OsCacheOpenDecision {
    debug_assert!(!fi.is_null());
    complete_file_open_reply_with(state, opened, false, |policy| {
        // SAFETY: libfuse owns `fi` for this callback; the C shim writes fields
        // using the exact system header layout.
        unsafe {
            studiofs_linux_fuse_file_info_init(
                fi,
                opened.fh,
                fuse_flag(policy.direct_io),
                fuse_flag(policy.keep_cache),
                fuse_flag(policy.parallel_direct_writes),
            );
        }
        reply(fi.cast_const())
    })
}

fn reply_runtime_stats_open(
    req: *mut raw::FuseReq,
    state: &SessionState,
    fi: *mut raw::FuseFileInfo,
    opened: LinuxFuseOpenedFile,
) {
    reply_runtime_stats_open_with(state, fi, opened, |fi| {
        // SAFETY: `fi` is initialized and valid for this synchronous reply.
        unsafe { raw::fuse_reply_open(req, fi) }
    });
}

fn reply_runtime_stats_open_with(
    state: &SessionState,
    fi: *mut raw::FuseFileInfo,
    opened: LinuxFuseOpenedFile,
    reply: impl FnOnce(*const raw::FuseFileInfo) -> c_int,
) -> LinuxFuseOpenCachePolicy {
    debug_assert!(!fi.is_null());
    complete_runtime_stats_open_reply_with(state, opened, |policy| {
        // SAFETY: libfuse owns `fi` for this callback; the C shim writes fields
        // using the exact system header layout.
        unsafe {
            studiofs_linux_fuse_file_info_init(
                fi,
                opened.fh,
                fuse_flag(policy.direct_io),
                fuse_flag(policy.keep_cache),
                fuse_flag(policy.parallel_direct_writes),
            );
        }
        reply(fi.cast_const())
    })
}

fn complete_runtime_stats_open_reply_with(
    state: &SessionState,
    opened: LinuxFuseOpenedFile,
    reply: impl FnOnce(LinuxFuseOpenCachePolicy) -> c_int,
) -> LinuxFuseOpenCachePolicy {
    let policy = LinuxFuseOpenCachePolicy {
        direct_io: true,
        keep_cache: false,
        parallel_direct_writes: false,
    };
    if reply(policy) != 0 {
        state
            .runtime_stats_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&opened.fh);
    }
    policy
}

fn reply_create(
    req: *mut raw::FuseReq,
    state: &SessionState,
    fi: *mut raw::FuseFileInfo,
    created: LinuxFuseCreatedFile,
) {
    reply_create_with(state, fi, created, |entry, fi| {
        // SAFETY: `entry` and `fi` are initialized and valid for this synchronous reply.
        unsafe { raw::fuse_reply_create(req, entry, fi) }
    });
}

fn reply_create_with(
    state: &SessionState,
    fi: *mut raw::FuseFileInfo,
    created: LinuxFuseCreatedFile,
    reply: impl FnOnce(&raw::FuseEntryParam, *const raw::FuseFileInfo) -> c_int,
) -> OsCacheOpenDecision {
    debug_assert!(!fi.is_null());
    let entry = raw::FuseEntryParam {
        ino: created.attr.ino,
        generation: 1,
        attr: state.attr_to_stat(&created.attr),
        attr_timeout: TTL_SECONDS,
        entry_timeout: TTL_SECONDS,
    };
    complete_file_open_reply_with(state, created.opened, true, |policy| {
        // SAFETY: the C shim writes fields using the exact system header layout.
        unsafe {
            studiofs_linux_fuse_file_info_init(
                fi,
                created.opened.fh,
                fuse_flag(policy.direct_io),
                fuse_flag(policy.keep_cache),
                fuse_flag(policy.parallel_direct_writes),
            );
        }
        reply(&entry, fi.cast_const())
    })
}

fn complete_file_open_reply_with(
    state: &SessionState,
    opened: LinuxFuseOpenedFile,
    creates_lookup_reference: bool,
    reply: impl FnOnce(LinuxFuseOpenCachePolicy) -> c_int,
) -> OsCacheOpenDecision {
    state.open_reply_serializer.with_inode(opened.ino, || {
        let participation = match state.cache_policy {
            LinuxFuseCachePolicy::Direct => OsCacheParticipation::Disabled,
            LinuxFuseCachePolicy::Kernel => OsCacheParticipation::Enabled,
        };
        let decision = state.vfs.prepare_os_cache_open(opened.fh, participation);
        let negotiated = state.negotiated_geometry.get();
        let policy = linux_fuse_open_cache_policy_for(
            state.cache_policy,
            negotiated.is_some_and(|geometry| geometry.async_dio),
            negotiated.is_some_and(|geometry| geometry.parallel_direct_writes_supported),
            decision.disposition(),
        );
        let unknown = !creates_lookup_reference && !state.inode_liveness.is_observed(opened.ino);
        let lookup_reservation =
            creates_lookup_reference.then(|| state.inode_liveness.reserve_lookup_reply(opened.ino));

        let status = reply(policy);
        if let Some(reservation) = lookup_reservation {
            if status == 0 {
                reservation.accept();
            } else {
                reservation.reject();
            }
        }
        if status != 0 {
            state.vfs.abort_os_cache_open(opened.fh);
            let cleanup_result = state
                .runtime_handle
                .block_on(state.vfs.release_fh_with_outcome(opened.fh))
                .map(|_| ())
                .map_err(|error| vfs_err_to_errno(&error));
            if let Err(errno) = normalize_close_barrier_result(cleanup_result) {
                warn!(
                    ino = opened.ino,
                    file_handle = opened.fh,
                    errno,
                    "Linux FUSE rejected open cleanup failed"
                );
            }
        }

        if unknown {
            let occurrence = state.inode_liveness.record_open_unknown_inode();
            if occurrence.is_power_of_two() {
                warn!(
                    ino = opened.ino,
                    occurrence, "Linux FUSE open reply has no observed lookup reference"
                );
            }
        }
        if creates_lookup_reference || unknown {
            state.publish_liveness_stats();
        }
        decision
    })
}

fn reply_error(req: *mut raw::FuseReq, errno: c_int) {
    // SAFETY: libfuse accepts errno 0 as a successful empty reply for callbacks
    // whose contract is `fuse_reply_err`.
    unsafe { raw::fuse_reply_err(req, errno) };
}

fn reply_xattr_value(req: *mut raw::FuseReq, size: usize, value: &[u8]) {
    debug_assert!(size > 0);
    if size < value.len() {
        reply_error(req, libc::ERANGE);
    } else {
        // SAFETY: `value` is valid for this synchronous reply.
        unsafe { raw::fuse_reply_buf(req, value.as_ptr().cast::<c_char>(), value.len()) };
    }
}

fn format_xattr_name_list(names: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(names.iter().map(|name| name.len() + 1).sum());
    for name in names {
        out.extend_from_slice(name);
        out.push(0);
    }
    out
}

fn xattr_mutation_err_to_errno(err: &studiofs_vfs::VfsError) -> i32 {
    if matches!(
        err,
        studiofs_vfs::VfsError::Meta(studiofs_meta::MetaError::EntryNotFound { .. })
    ) {
        libc::ENODATA
    } else {
        vfs_err_to_errno(err)
    }
}

struct DirectoryEntryBuffer {
    words: Vec<MaybeUninit<u64>>,
    len: usize,
    scratch_name: Vec<u8>,
    capacity: usize,
}

impl DirectoryEntryBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            words: Vec::with_capacity(capacity.div_ceil(std::mem::size_of::<u64>())),
            len: 0,
            scratch_name: Vec::with_capacity(256),
            capacity,
        }
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `self.len` bytes from `words.as_ptr()` were initialized by push.
        unsafe { std::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.len) }
    }

    fn push(
        &mut self,
        req: *mut raw::FuseReq,
        name: &OsStr,
        attr: &stat,
        next_offset: off_t,
    ) -> Result<bool, i32> {
        let name_bytes = name.as_bytes();
        if name_bytes.contains(&0) {
            return Err(libc::EINVAL);
        }
        self.scratch_name.clear();
        self.scratch_name.extend_from_slice(name_bytes);
        self.scratch_name.push(0);
        let available = self.capacity.saturating_sub(self.len);
        // SAFETY: null buffer asks libfuse for required size; pointers stay valid.
        let required = unsafe {
            raw::fuse_add_direntry(
                req,
                std::ptr::null_mut(),
                0,
                self.scratch_name.as_ptr().cast::<c_char>(),
                attr,
                next_offset,
            )
        };
        if required > available {
            return Ok(false);
        }

        let start = self.len;
        let new_len = start + required;
        let required_words = new_len.div_ceil(std::mem::size_of::<u64>());
        // SAFETY: capacity check above ensures `required` bytes of spare room.
        unsafe {
            raw::fuse_add_direntry(
                req,
                self.words
                    .as_mut_ptr()
                    .cast::<u8>()
                    .add(start)
                    .cast::<c_char>(),
                required,
                self.scratch_name.as_ptr().cast::<c_char>(),
                attr,
                next_offset,
            );
            self.words.set_len(required_words);
            self.len = new_len;
        }
        Ok(true)
    }
}

fn retain_unsafe_session<Raw, State>(raw: Raw, state: Option<Box<State>>) {
    std::mem::forget((raw, state));
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            state.vfs.clear_kernel_invalidator();
        }
        self.invalidation_state.begin_shutdown();
        let async_writes_idle = self.state.as_ref().is_none_or(|state| {
            state.abort_memory_pressure_watcher();
            shutdown_async_writes_with_timeouts(
                &state.async_writes,
                ASYNC_WRITE_SHUTDOWN_DRAIN_TIMEOUT,
                ASYNC_WRITE_SHUTDOWN_CANCEL_TIMEOUT,
            )
        });
        let invalidations_idle = self
            .invalidation_state
            .wait_for_idle(INVALIDATION_SHUTDOWN_IDLE_TIMEOUT);
        let safe_to_destroy = async_writes_idle && invalidations_idle;
        if !async_writes_idle {
            warn!(
                "Linux FUSE session shutdown timed out waiting for async write replies; \
                 unmounting and retaining session ownership to avoid use-after-free"
            );
        }
        if !invalidations_idle {
            warn!(
                "Linux FUSE session shutdown timed out waiting for active kernel invalidation; \
                 unmounting and retaining session ownership to avoid use-after-free"
            );
        }
        if self.signal_handlers_installed {
            // SAFETY: handlers were installed for this live session.
            unsafe { raw::fuse_remove_signal_handlers(self.raw.as_ptr()) };
        }
        // SAFETY: libfuse tolerates unmount after regular kernel disconnects;
        // it is needed after signal-triggered exits to avoid a stuck mount.
        unsafe { raw::fuse_session_unmount(self.raw.as_ptr()) };
        if !safe_to_destroy {
            retain_unsafe_session(self.raw, self.state.take());
            return;
        }
        // SAFETY: `self.raw` is owned by this wrapper and destroyed once.
        unsafe { raw::fuse_session_destroy(self.raw.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, OnceLock};
    use std::thread;
    use std::time::{Duration, Instant};

    use studiofs_data::store::DataStoreConfig;
    use studiofs_meta::UploadState;
    use studiofs_meta::engine::{MetaFormat, MetaFsOps, MetaInodes, MetaReplication};
    use studiofs_meta::{
        CURRENT_FORMAT_VERSION, FormatInfo, MetaEngine, MetadataBackend, MetadataConfig,
        PgMetaEngine, PostgresConfig, ROOT_INO, RedbMetaEngine, ensure_studiofs_database,
    };
    use studiofs_vfs::contract_harness::ContractVfsFixture;
    use studiofs_vfs::{
        KernelInvalidator, LocalSyncPolicy, OpenFlags, OsCacheDisposition, OsCacheReason,
        StudioVfs, VfsConfigRaw,
    };
    use uuid::Uuid;

    #[derive(Default)]
    struct RecordingLinuxInvalidator {
        inode_calls: AtomicUsize,
    }

    impl KernelInvalidator for RecordingLinuxInvalidator {
        fn inval_inode(&self, _ino: u64) {
            self.inode_calls.fetch_add(1, Ordering::Relaxed);
        }

        fn inval_inode_attrs(&self, _ino: u64) {
            self.inode_calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct UnmountGuard(Option<std::path::PathBuf>);

    struct RuntimeGateRelease(Option<mpsc::Sender<()>>);

    impl RuntimeGateRelease {
        fn release(mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    impl Drop for RuntimeGateRelease {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    impl UnmountGuard {
        fn new(path: std::path::PathBuf) -> Self {
            Self(Some(path))
        }

        fn disarm(mut self) {
            self.0 = None;
        }
    }

    impl Drop for UnmountGuard {
        fn drop(&mut self) {
            if let Some(path) = &self.0 {
                let _ = bounded_unmount(path);
            }
        }
    }

    fn mounted_target_path(label: &str) -> std::path::PathBuf {
        static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);
        let ordinal = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../../target/studiofs-linux-fuse-{label}-{}-{ordinal}",
            std::process::id()
        ))
    }

    fn run_command_bounded(
        command: &mut Command,
        timeout: Duration,
    ) -> std::io::Result<std::process::ExitStatus> {
        let mut child = command.spawn()?;
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                let kill_result = child.kill();
                let reap_deadline = Instant::now() + Duration::from_secs(1);
                while Instant::now() < reap_deadline {
                    if child.try_wait()?.is_some() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "test command exceeded its bounded timeout; kill_result={kill_result:?}"
                    ),
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn findmnt_reports_mount(path: &std::path::Path) -> std::io::Result<bool> {
        let mut command = Command::new("findmnt");
        command.arg("-M").arg(path);
        run_command_bounded(&mut command, Duration::from_secs(1)).map(|status| status.success())
    }

    fn wait_for_mount_absent(path: &std::path::Path, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if matches!(findmnt_reports_mount(path), Ok(false)) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn bounded_unmount(path: &std::path::Path) -> std::io::Result<()> {
        let mut normal = Command::new("fusermount3");
        normal.arg("-u").arg(path);
        let normal_status = run_command_bounded(&mut normal, Duration::from_secs(5));
        if normal_status.as_ref().is_ok_and(|status| status.success())
            && wait_for_mount_absent(path, Duration::from_secs(1))
        {
            return Ok(());
        }

        let mut forced = Command::new("fusermount3");
        forced.arg("-uz").arg(path);
        let forced_status = run_command_bounded(&mut forced, Duration::from_secs(5));
        if forced_status.as_ref().is_ok_and(|status| status.success())
            && wait_for_mount_absent(path, Duration::from_secs(5))
        {
            Ok(())
        } else {
            Err(std::io::Error::other(format!(
                "mounted-test teardown failed: normal={normal_status:?}, forced={forced_status:?}"
            )))
        }
    }

    fn wait_for_snapshot(
        vfs: &studiofs_vfs::StudioVfs,
        timeout: Duration,
        mut ready: impl FnMut(&studiofs_vfs::VfsWritePathObservabilitySnapshot) -> bool,
    ) -> Result<studiofs_vfs::VfsWritePathObservabilitySnapshot, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = vfs.write_path_observability_snapshot();
            if ready(&snapshot) {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "snapshot poll timed out: dirty={}, pending_uploads={}, active_uploads={}, \
                     async_reads={}, async_writes={}, active_foreground={}, active_writers={}",
                    snapshot.dirty_pressure.total_dirty_bytes,
                    snapshot.pending_uploads,
                    snapshot.upload.active_uploads,
                    snapshot.fuse_async_read.pending_reads,
                    snapshot.fuse_async_write.pending_writes,
                    snapshot.os_cache.active_foreground_handles,
                    snapshot.os_cache.active_writers,
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_local_mounted_idle(
        vfs: &studiofs_vfs::StudioVfs,
    ) -> studiofs_vfs::VfsWritePathObservabilitySnapshot {
        // ContractVfsFixture has no remote store and retains publication backlog;
        // the remote-listener test separately waits for pending and active uploads.
        wait_for_snapshot(vfs, Duration::from_secs(10), |snapshot| {
            snapshot.dirty_pressure.total_dirty_bytes == 0
                && snapshot.upload.active_uploads == 0
                && snapshot.fuse_async_read.pending_reads == 0
                && snapshot.fuse_async_read.pending_bytes == 0
                && snapshot.fuse_async_write.active_requests == 0
                && snapshot.fuse_async_write.admission_waiters == 0
                && snapshot.fuse_async_write.pending_handles == 0
                && snapshot.fuse_async_write.pending_writes == 0
                && snapshot.fuse_async_write.pending_bytes == 0
                && snapshot.os_cache.active_foreground_handles == 0
                && snapshot.os_cache.active_writers == 0
        })
        .unwrap_or_else(|error| panic!("mounted VFS did not become locally idle: {error}"))
    }

    fn wait_for_read_quiescence(
        vfs: &studiofs_vfs::StudioVfs,
    ) -> studiofs_vfs::VfsWritePathObservabilitySnapshot {
        let mut previous = None;
        let mut stable_polls = 0;
        wait_for_snapshot(vfs, Duration::from_secs(10), |snapshot| {
            let counts = (
                snapshot.fuse_callbacks.read.calls,
                snapshot.vfs_read_path.calls,
            );
            let idle = snapshot.fuse_async_read.pending_reads == 0
                && snapshot.fuse_async_read.pending_bytes == 0
                && snapshot.os_cache.active_foreground_handles == 0;
            if idle && previous == Some(counts) {
                stable_polls += 1;
            } else {
                stable_polls = 0;
            }
            previous = Some(counts);
            idle && stable_polls >= 2
        })
        .unwrap_or_else(|error| panic!("mounted reads did not quiesce: {error}"))
    }

    fn run_mounted_vfs_test(
        label: &str,
        vfs: Arc<studiofs_vfs::StudioVfs>,
        runtime_handle: tokio::runtime::Handle,
        operation: impl FnOnce(&std::path::Path, &Arc<studiofs_vfs::StudioVfs>) + Send + 'static,
    ) {
        let mount_point = mounted_target_path(label);
        if mount_point.exists() {
            assert!(
                matches!(findmnt_reports_mount(&mount_point), Ok(false)),
                "refusing to reuse a live mounted-test path: {}",
                mount_point.display()
            );
            std::fs::remove_dir_all(&mount_point).expect("remove stale mounted-test path");
        }
        std::fs::create_dir_all(&mount_point).expect("create mounted-test path");

        let state = super::SessionState::new(
            Arc::clone(&vfs),
            runtime_handle,
            false,
            default_connection_config(),
        );
        let async_writes = Arc::clone(&state.async_writes);
        let mut session = super::Session::mount(state, &mount_point, label).expect("mount session");
        let invalidations = Arc::clone(&session.invalidation_state);
        let worker_mount_point = mount_point.clone();
        let worker_vfs = Arc::clone(&vfs);
        let worker = thread::spawn(move || {
            let guard = UnmountGuard::new(worker_mount_point.clone());
            let operation_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                operation(&worker_mount_point, &worker_vfs);
            }));
            let unmount_result = bounded_unmount(&worker_mount_point);
            if unmount_result.is_ok() {
                guard.disarm();
            }
            (operation_result, unmount_result)
        });

        let run_result = session.run();
        let worker_result = worker.join();
        drop(session);

        let mount_absent = wait_for_mount_absent(&mount_point, Duration::from_secs(5));
        let invalidations_idle = invalidations.wait_for_idle(Duration::from_secs(5));
        let writes_idle = async_writes.wait_idle(Duration::from_secs(5));
        let write_stats = async_writes.stats();
        let snapshot = wait_for_local_mounted_idle(&vfs);
        let remove_result = std::fs::remove_dir_all(&mount_point);

        run_result.expect("run mounted libfuse3 session");
        let (operation_result, unmount_result) =
            worker_result.expect("mounted-test worker should not panic outside its operation");
        unmount_result.expect("unmount mounted-test session");
        assert!(mount_absent, "findmnt still reports the mounted-test path");
        assert!(
            invalidations_idle,
            "kernel invalidation remained active at teardown"
        );
        assert!(
            writes_idle,
            "async writes did not drain at teardown: {write_stats:?}"
        );
        assert_eq!(
            (
                write_stats.active_requests,
                write_stats.pending_writes,
                write_stats.pending_bytes,
                write_stats.admission_waiters,
            ),
            (0, 0, 0, 0),
            "async write tracker retained live work"
        );
        assert_eq!(
            (
                snapshot.fuse_async_read.pending_reads,
                snapshot.fuse_async_read.pending_bytes,
                snapshot.os_cache.active_foreground_handles,
                snapshot.os_cache.active_writers,
            ),
            (0, 0, 0, 0),
            "mounted lifecycle gauges were not zero at teardown"
        );
        remove_result.expect("remove mounted-test path");
        if let Err(payload) = operation_result {
            std::panic::resume_unwind(payload);
        }
    }

    struct TargetTestDirectory(std::path::PathBuf);

    impl TargetTestDirectory {
        fn new(label: &str) -> Self {
            let path = mounted_target_path(label);
            std::fs::create_dir_all(&path).expect("create target test directory");
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TargetTestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct RemoteTestEnvironment {
        pg_dsn: String,
        s3_endpoint: String,
        s3_bucket: String,
        s3_access_key: String,
        s3_secret_key: String,
    }

    fn workspace_dotenv_vars() -> &'static std::collections::HashMap<String, String> {
        static VARS: OnceLock<std::collections::HashMap<String, String>> = OnceLock::new();
        VARS.get_or_init(|| {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env");
            dotenvy::from_path_iter(path)
                .into_iter()
                .flatten()
                .flatten()
                .collect()
        })
    }

    fn remote_env_value(primary: &str, legacy: Option<&str>) -> Option<String> {
        let non_empty = |value: String| (!value.trim().is_empty()).then_some(value);
        std::env::var(primary)
            .ok()
            .and_then(non_empty)
            .or_else(|| {
                legacy
                    .and_then(|key| std::env::var(key).ok())
                    .and_then(non_empty)
            })
            .or_else(|| {
                workspace_dotenv_vars()
                    .get(primary)
                    .cloned()
                    .and_then(non_empty)
            })
            .or_else(|| {
                legacy
                    .and_then(|key| workspace_dotenv_vars().get(key).cloned())
                    .and_then(non_empty)
            })
    }

    fn remote_test_environment() -> Option<RemoteTestEnvironment> {
        let pg_dsn = remote_env_value("STUDIOFS_TEST_PG_DSN", None);
        let s3_endpoint = remote_env_value(
            "STUDIOFS_TEST_ONLINE_S3_ENDPOINT",
            Some("STUDIOFS_S3_ENDPOINT"),
        );
        let s3_bucket =
            remote_env_value("STUDIOFS_TEST_ONLINE_S3_BUCKET", Some("STUDIOFS_S3_BUCKET"));
        let s3_access_key = remote_env_value(
            "STUDIOFS_TEST_ONLINE_S3_ACCESS_KEY",
            Some("STUDIOFS_S3_ACCESS_KEY"),
        );
        let s3_secret_key = remote_env_value(
            "STUDIOFS_TEST_ONLINE_S3_SECRET_KEY",
            Some("STUDIOFS_S3_SECRET_KEY"),
        );
        let mut missing = Vec::new();
        if pg_dsn.is_none() {
            missing.push("STUDIOFS_TEST_PG_DSN");
        }
        if s3_endpoint.is_none() {
            missing.push("STUDIOFS_TEST_ONLINE_S3_ENDPOINT/STUDIOFS_S3_ENDPOINT");
        }
        if s3_bucket.is_none() {
            missing.push("STUDIOFS_TEST_ONLINE_S3_BUCKET/STUDIOFS_S3_BUCKET");
        }
        if s3_access_key.is_none() {
            missing.push("STUDIOFS_TEST_ONLINE_S3_ACCESS_KEY/STUDIOFS_S3_ACCESS_KEY");
        }
        if s3_secret_key.is_none() {
            missing.push("STUDIOFS_TEST_ONLINE_S3_SECRET_KEY/STUDIOFS_S3_SECRET_KEY");
        }
        if !missing.is_empty() {
            let reason = format!(
                "test=mounted_kernel_remote_listener_invalidation reason=missing_environment keys={}",
                missing.join(",")
            );
            let required = std::env::var("STUDIOFS_REQUIRE_REMOTE_INVALIDATION_TEST")
                .ok()
                .is_some_and(|value| super::linux_fuse_env_flag_value(&value));
            if required {
                panic!("required remote invalidation test cannot skip: {reason}");
            }
            eprintln!("SKIP {reason}");
            return None;
        }
        Some(RemoteTestEnvironment {
            pg_dsn: pg_dsn.expect("checked PG DSN"),
            s3_endpoint: s3_endpoint.expect("checked S3 endpoint"),
            s3_bucket: s3_bucket.expect("checked S3 bucket"),
            s3_access_key: s3_access_key.expect("checked S3 access key"),
            s3_secret_key: s3_secret_key.expect("checked S3 secret key"),
        })
    }

    struct RemotePgVolume {
        dsn: String,
        volume_id: i64,
        name: String,
        uuid: String,
    }

    impl RemotePgVolume {
        fn new(raw_dsn: &str, runtime: &tokio::runtime::Runtime) -> Self {
            let dsn = runtime
                .block_on(async { ensure_studiofs_database(raw_dsn).await })
                .map(|database| database.metadata_dsn)
                .unwrap_or_else(|_| panic!("configured PostgreSQL environment is unreachable"));
            let name = format!(
                "linux-fuse-remote-invalidation-{}-{}",
                std::process::id(),
                Uuid::new_v4().simple()
            );
            let uuid = Uuid::new_v4().to_string();
            let volume_id = runtime
                .block_on(async {
                    let (mut client, connection) =
                        tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                            .await
                            .map_err(|_| ())?;
                    let connection_task = tokio::spawn(async move {
                        let _ = connection.await;
                    });
                    let result = async {
                        let transaction = client.transaction().await.map_err(|_| ())?;
                        let row = transaction
                            .query_one(
                                "INSERT INTO volumes (name, uuid, block_size) \
                                 VALUES ($1, $2, $3) RETURNING vol",
                                &[&name, &uuid, &(4_i32 * 1024 * 1024)],
                            )
                            .await
                            .map_err(|_| ())?;
                        let volume_id: i64 = row.get(0);
                        transaction
                            .execute(
                                "INSERT INTO metadata_state (vol, version, snapshot) \
                                 VALUES ($1, 0, $2) ON CONFLICT (vol) DO NOTHING",
                                &[&volume_id, &Vec::<u8>::new()],
                            )
                            .await
                            .map_err(|_| ())?;
                        transaction.commit().await.map_err(|_| ())?;
                        Ok::<i64, ()>(volume_id)
                    }
                    .await;
                    connection_task.abort();
                    result
                })
                .unwrap_or_else(|_| panic!("failed to provision configured PostgreSQL volume"));
            Self {
                dsn,
                volume_id,
                name,
                uuid,
            }
        }

        fn format_info(&self, environment: &RemoteTestEnvironment) -> FormatInfo {
            FormatInfo {
                format_version: CURRENT_FORMAT_VERSION,
                name: self.name.clone(),
                uuid: self.uuid.clone(),
                endpoint: environment.s3_endpoint.clone(),
                bucket: environment.s3_bucket.clone(),
                access_key: environment.s3_access_key.clone(),
                secret_key: environment.s3_secret_key.clone(),
                region: "us-east-1".to_owned(),
                data_prefix: format!("{}/objects/", self.name),
                metadata: MetadataConfig {
                    backend: MetadataBackend::Postgres,
                    remote: None,
                    postgres: Some(PostgresConfig {
                        url: self.dsn.clone(),
                        volume_id: self.volume_id,
                    }),
                },
                encryption: studiofs_meta::EncryptionConfig::new(
                    [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
                ),
                block_size: 4 * 1024 * 1024,
            }
        }
    }

    impl Drop for RemotePgVolume {
        fn drop(&mut self) {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async {
                let Ok((client, connection)) =
                    tokio_postgres::connect(&self.dsn, tokio_postgres::NoTls).await
                else {
                    return;
                };
                let connection_task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                let _ = client
                    .execute("DELETE FROM volumes WHERE vol = $1", &[&self.volume_id])
                    .await;
                connection_task.abort();
            });
        }
    }

    fn seed_remote_local_metadata(path: &std::path::Path, format_info: &FormatInfo) {
        let seed = RedbMetaEngine::open(path).expect("open remote-test seed metadata");
        seed.set_format_info(format_info)
            .expect("seed remote-test format info");
        seed.init_root(0, 0).expect("seed remote-test root");
        let next_event_seq = seed
            .get_counter("nextEventSeq")
            .expect("read remote-test event sequence");
        seed.set_counter("pgLastFlushedLocalEventSeq", next_event_seq)
            .expect("seed remote-test flushed event sequence");
    }

    fn make_remote_vfs(
        meta: Arc<dyn MetaEngine>,
        format_info: &FormatInfo,
        cache_dir: &std::path::Path,
    ) -> Arc<StudioVfs> {
        let config = VfsConfigRaw {
            metadata_invalidation_sync_interval_ms: Some(300_000),
            max_dirty_bytes: Some(1),
            upload_delay_ms: Some(0),
            upload_worker_count: Some(1),
            upload_request_timeout_secs: Some(10),
            ..VfsConfigRaw::default()
        }
        .validate()
        .expect("validate remote-test VFS config");
        let data_config = DataStoreConfig::from(format_info);
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("remote-test cache path UTF-8"),
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
            config,
        )
        .expect("create remote-test VFS");
        vfs.start().expect("start remote-test VFS");
        vfs
    }

    fn remote_publish_timeout() -> Duration {
        Duration::from_secs(
            remote_env_value("STUDIOFS_TEST_ONLINE_PUBLISH_TIMEOUT_SECS", None)
                .and_then(|value| value.parse().ok())
                .unwrap_or(60)
                .max(1),
        )
    }

    fn regular_file_payload_ids(vfs: &StudioVfs, ino: u64) -> Vec<u64> {
        let mut ids: Vec<_> = vfs
            .meta()
            .get_regular_file_graph_object_usage(ino)
            .expect("read regular-file payload IDs")
            .into_iter()
            .map(|(object_id, _)| object_id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    // Keep the terminal full snapshot on timeout for deterministic test diagnostics.
    #[allow(clippy::result_large_err)]
    fn wait_for_async_write_stats(
        tracker: &super::LinuxFuseAsyncWrites,
        expected: impl Fn(&super::LinuxFuseAsyncWriteStats) -> bool,
    ) -> Result<super::LinuxFuseAsyncWriteStats, super::LinuxFuseAsyncWriteStats> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let stats = tracker.stats();
            if expected(&stats) {
                return Ok(stats);
            }
            if Instant::now() >= deadline {
                return Err(stats);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn mknod_regular_file(path: &std::path::Path) -> std::io::Result<()> {
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "test path contains an interior NUL byte",
            )
        })?;
        // SAFETY: `path` is NUL-terminated and valid for this syscall.
        let result = unsafe { libc::mknod(path.as_ptr(), libc::S_IFREG | 0o644, 0) };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn path_cstring(path: &std::path::Path) -> std::io::Result<std::ffi::CString> {
        std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "test path contains an interior NUL byte",
            )
        })
    }

    fn set_xattr(path: &std::path::Path, name: &[u8], value: &[u8]) -> std::io::Result<()> {
        let path = path_cstring(path)?;
        let name = std::ffi::CString::new(name).expect("test xattr name has no NUL");
        // SAFETY: path and name are NUL-terminated; value points to `value.len()` bytes.
        let result = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn get_xattr(path: &std::path::Path, name: &[u8]) -> std::io::Result<Vec<u8>> {
        let path = path_cstring(path)?;
        let name = std::ffi::CString::new(name).expect("test xattr name has no NUL");
        // SAFETY: path and name are NUL-terminated; null value asks for size.
        let size = unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
        if size < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut value = vec![0; size as usize];
        // SAFETY: value has `size` bytes of writable capacity.
        let read = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_mut_ptr().cast(),
                value.len(),
            )
        };
        if read < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            value.truncate(read as usize);
            Ok(value)
        }
    }

    fn list_xattrs(path: &std::path::Path) -> std::io::Result<Vec<Vec<u8>>> {
        let path = path_cstring(path)?;
        // SAFETY: path is NUL-terminated; null list asks for size.
        let size = unsafe { libc::listxattr(path.as_ptr(), std::ptr::null_mut(), 0) };
        if size < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut names = vec![0; size as usize];
        // SAFETY: names has `size` bytes of writable capacity.
        let read =
            unsafe { libc::listxattr(path.as_ptr(), names.as_mut_ptr().cast(), names.len()) };
        if read < 0 {
            return Err(std::io::Error::last_os_error());
        }
        names.truncate(read as usize);
        Ok(names
            .split(|byte| *byte == 0)
            .filter(|name| !name.is_empty())
            .map(|name| name.to_vec())
            .collect())
    }

    fn remove_xattr(path: &std::path::Path, name: &[u8]) -> std::io::Result<()> {
        let path = path_cstring(path)?;
        let name = std::ffi::CString::new(name).expect("test xattr name has no NUL");
        // SAFETY: path and name are NUL-terminated.
        let result = unsafe { libc::removexattr(path.as_ptr(), name.as_ptr()) };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn default_connection_config() -> super::LinuxFuseConnectionConfig {
        super::LinuxFuseConnectionConfig::default()
    }

    fn liveness_test_state(
        name: &str,
    ) -> (
        studiofs_vfs::contract_harness::ContractVfsFixture,
        super::SessionState,
    ) {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(name)
            .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        (fixture, state)
    }

    fn opened_file(ino: u64, fh: u64) -> super::LinuxFuseOpenedFile {
        super::LinuxFuseOpenedFile { fh, ino }
    }

    fn created_file(attr: &studiofs_meta::InodeAttr, fh: u64) -> super::LinuxFuseCreatedFile {
        super::LinuxFuseCreatedFile {
            attr: attr.clone(),
            opened: opened_file(attr.ino, fh),
        }
    }

    fn cache_test_state(
        name: &str,
        cache_policy: super::LinuxFuseCachePolicy,
        requested_async_dio: bool,
    ) -> (ContractVfsFixture, super::SessionState) {
        let fixture = ContractVfsFixture::new(name).expect("create VFS fixture");
        let state = super::SessionState::new_with_cache_settings(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
            cache_policy,
            requested_async_dio,
        );
        (fixture, state)
    }

    fn begin_cache_open(
        fixture: &ContractVfsFixture,
        ino: u64,
        flags: OpenFlags,
    ) -> super::LinuxFuseOpenedFile {
        let vfs = fixture.vfs();
        let attr = vfs.getattr(ino).expect("load fresh open attributes");
        let fh = vfs
            .open_file_with_known_attr(&attr, flags, false)
            .expect("open cache test handle");
        opened_file(ino, fh)
    }

    fn accept_cache_open(
        state: &super::SessionState,
        opened: super::LinuxFuseOpenedFile,
    ) -> (
        super::LinuxFuseOpenCachePolicy,
        studiofs_vfs::OsCacheOpenDecision,
    ) {
        let mut observed_policy = None;
        let decision = super::complete_file_open_reply_with(state, opened, false, |policy| {
            observed_policy = Some(policy);
            0
        });
        (
            observed_policy.expect("reply observes cache policy"),
            decision,
        )
    }

    fn release_cache_open(fixture: &ContractVfsFixture, opened: super::LinuxFuseOpenedFile) {
        fixture
            .runtime()
            .block_on(fixture.vfs().release_fh_with_outcome(opened.fh))
            .expect("release cache test handle");
    }

    fn assert_cache_lifecycle_idle(fixture: &ContractVfsFixture) {
        let cache = fixture.vfs().write_path_observability_snapshot().os_cache;
        assert_eq!(
            (cache.active_foreground_handles, cache.active_writers),
            (0, 0),
        );
    }

    fn write_via_linux_session(state: &super::SessionState, ino: u64, fh: u64, payload: &[u8]) {
        let mut file_info_storage = [0_u64; 32];
        let file_info = file_info_storage
            .as_mut_ptr()
            .cast::<super::raw::FuseFileInfo>();
        // SAFETY: the aligned scratch storage outlives this synchronous callback
        // and is large enough for the system libfuse `fuse_file_info` value.
        unsafe {
            super::studiofs_linux_fuse_file_info_init(file_info, fh, 1, 0, 0);
        }

        assert_eq!(
            state.write(ino, payload.as_ptr().cast(), payload.len(), 0, file_info),
            Ok(payload.len() as u32),
        );
    }

    #[derive(Debug, Eq, PartialEq)]
    enum FakeFuseReply {
        Write(usize),
        Error(i32),
    }

    struct FakeFuseReplyState {
        status: libc::c_int,
        replies: Vec<FakeFuseReply>,
        observations: Vec<(u64, usize, usize)>,
        writes: Arc<super::LinuxFuseAsyncWrites>,
        vfs: Arc<studiofs_vfs::StudioVfs>,
    }

    impl FakeFuseReplyState {
        fn new(
            status: libc::c_int,
            writes: Arc<super::LinuxFuseAsyncWrites>,
            vfs: Arc<studiofs_vfs::StudioVfs>,
        ) -> Self {
            Self {
                status,
                replies: Vec::new(),
                observations: Vec::new(),
                writes,
                vfs,
            }
        }

        fn record(&mut self, reply: FakeFuseReply) -> libc::c_int {
            let callbacks = self
                .vfs
                .write_path_observability_snapshot()
                .fuse_callbacks
                .write
                .calls;
            let stats = self.writes.stats();
            self.replies.push(reply);
            self.observations
                .push((callbacks, stats.pending_writes, stats.active_requests));
            self.status
        }
    }

    unsafe extern "C" fn fake_fuse_reply_write(
        req: *mut super::raw::FuseReq,
        written: libc::size_t,
    ) -> libc::c_int {
        // SAFETY: guard tests pass a live `FakeFuseReplyState` as the request pointer.
        let state = unsafe { &mut *req.cast::<FakeFuseReplyState>() };
        state.record(FakeFuseReply::Write(written))
    }

    unsafe extern "C" fn fake_fuse_reply_error(
        req: *mut super::raw::FuseReq,
        errno: libc::c_int,
    ) -> libc::c_int {
        // SAFETY: guard tests pass a live `FakeFuseReplyState` as the request pointer.
        let state = unsafe { &mut *req.cast::<FakeFuseReplyState>() };
        state.record(FakeFuseReply::Error(errno))
    }

    struct FakeSyncReplyState {
        status: libc::c_int,
        reply_size: AtomicUsize,
        reply_errno: AtomicI32,
        entered: std::sync::Mutex<Option<mpsc::Sender<()>>>,
        release: std::sync::Mutex<Option<mpsc::Receiver<()>>>,
    }

    impl FakeSyncReplyState {
        fn new(status: libc::c_int) -> Self {
            Self {
                status,
                reply_size: AtomicUsize::new(0),
                reply_errno: AtomicI32::new(i32::MIN),
                entered: std::sync::Mutex::new(None),
                release: std::sync::Mutex::new(None),
            }
        }

        fn wait_at_reply_boundary(&self) {
            if let Some(entered) = self.entered.lock().expect("reply entered lock").take() {
                entered.send(()).expect("signal reply boundary");
            }
            if let Some(release) = self.release.lock().expect("reply release lock").take() {
                release.recv().expect("release reply boundary");
            }
        }
    }

    unsafe extern "C" fn fake_sync_reply_buf(
        req: *mut super::raw::FuseReq,
        _buffer: *const libc::c_char,
        size: libc::size_t,
    ) -> libc::c_int {
        // SAFETY: sync reply tests pass a live `FakeSyncReplyState` request pointer.
        let state = unsafe { &*req.cast::<FakeSyncReplyState>() };
        state.reply_size.store(size, Ordering::Relaxed);
        state.wait_at_reply_boundary();
        state.status
    }

    unsafe extern "C" fn fake_sync_reply_error(
        req: *mut super::raw::FuseReq,
        errno: libc::c_int,
    ) -> libc::c_int {
        // SAFETY: sync reply tests pass a live `FakeSyncReplyState` request pointer.
        let state = unsafe { &*req.cast::<FakeSyncReplyState>() };
        state.reply_errno.store(errno, Ordering::Relaxed);
        state.wait_at_reply_boundary();
        state.status
    }

    #[test]
    fn linux_read_callback_metrics_cover_reply_and_use_requested_size() {
        let fixture = ContractVfsFixture::new("linux-read-callback-reply-boundary")
            .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let mut reply = Box::new(FakeSyncReplyState::new(0));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *reply.entered.lock().expect("reply entered lock") = Some(entered_tx);
        *reply.release.lock().expect("reply release lock") = Some(release_rx);
        let request = super::FuseReqPtr::for_sync_test(
            (&mut *reply as *mut FakeSyncReplyState).cast(),
            fake_sync_reply_buf,
            fake_sync_reply_error,
        );
        let callback_vfs = Arc::clone(&vfs);
        let callback = thread::spawn(move || {
            super::finish_fuse_read_reply(
                &callback_vfs,
                request,
                64 * 1024,
                Instant::now(),
                Ok(vec![1, 2, 3]),
            );
        });

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("read reply reaches boundary");
        assert_eq!(
            vfs.write_path_observability_snapshot()
                .fuse_callbacks
                .read
                .calls,
            0,
            "metrics must be recorded after the synchronous reply returns",
        );
        thread::sleep(Duration::from_millis(20));
        release_tx.send(()).expect("release read reply");
        callback.join().expect("join read reply callback");

        let read = vfs.write_path_observability_snapshot().fuse_callbacks.read;
        assert_eq!((read.calls, read.bytes, read.errors), (1, 3, 0));
        assert_eq!(read.size_histogram.bucket_counts, [0, 0, 1, 0, 0, 0, 0]);
        assert!(
            read.total_ms >= 10.0,
            "reply wait must be inside callback latency"
        );
        assert!(
            read.max_ms >= 10.0,
            "reply wait must be inside callback max"
        );
        assert_eq!(reply.reply_size.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn linux_barrier_callback_metrics_cover_reply_and_count_reply_rejection() {
        let fixture = ContractVfsFixture::new("linux-barrier-callback-reply-boundary")
            .expect("create VFS fixture");
        let vfs = fixture.vfs();

        for kind in [
            studiofs_vfs::FuseBarrierKind::Flush,
            studiofs_vfs::FuseBarrierKind::Fsync,
            studiofs_vfs::FuseBarrierKind::Release,
        ] {
            let mut reply = Box::new(FakeSyncReplyState::new(-1));
            let request = super::FuseReqPtr::for_sync_test(
                (&mut *reply as *mut FakeSyncReplyState).cast(),
                fake_sync_reply_buf,
                fake_sync_reply_error,
            );
            super::finish_fuse_barrier_reply(&vfs, request, kind, Instant::now(), Ok(()));
            assert_eq!(reply.reply_errno.load(Ordering::Relaxed), 0);
        }

        let callbacks = vfs.write_path_observability_snapshot().fuse_callbacks;
        assert_eq!((callbacks.flush.calls, callbacks.flush.errors), (1, 1));
        assert_eq!((callbacks.fsync.calls, callbacks.fsync.errors), (1, 1));
        assert_eq!((callbacks.release.calls, callbacks.release.errors), (1, 1));
    }

    fn make_async_write_guard(
        reply: &mut FakeFuseReplyState,
        writes: &Arc<super::LinuxFuseAsyncWrites>,
        vfs: &Arc<studiofs_vfs::StudioVfs>,
    ) -> super::FuseWriteTaskGuard {
        let request_ticket = writes.start_request(41).expect("start request");
        let reservation = writes.admit(8, |_| {}).expect("admit write");
        let request = super::FuseReqPtr::for_test(
            (reply as *mut FakeFuseReplyState).cast(),
            fake_fuse_reply_write,
            fake_fuse_reply_error,
        );
        super::FuseWriteTaskGuard {
            request: Some(request),
            request_ticket: Some(request_ticket),
            reservation: Some(reservation),
            writes: Arc::clone(writes),
            vfs: Arc::clone(vfs),
            started: Instant::now(),
            requested_bytes: 8,
            metric_config: super::LinuxFuseAsyncWriteMetricConfig {
                cache_policy: super::LinuxFuseCachePolicy::Direct,
            },
        }
    }

    #[test]
    fn async_write_guard_successful_write_replies_before_releasing_owned_state() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("async-write-guard-success")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));
        let mut reply = FakeFuseReplyState::new(0, Arc::clone(&writes), Arc::clone(&vfs));
        let guard = make_async_write_guard(&mut reply, &writes, &vfs);

        guard.finish(super::FuseWriteTaskResult::Completed(Ok(8)));

        assert_eq!(reply.replies, [FakeFuseReply::Write(8)]);
        assert_eq!(reply.observations, [(0, 1, 1)]);
        let callback = vfs.write_path_observability_snapshot().fuse_callbacks.write;
        assert_eq!((callback.calls, callback.errors), (1, 0));
        let stats = writes.stats();
        assert_eq!(
            (
                stats.pending_writes,
                stats.active_requests,
                stats.completed_writes,
                stats.failed_writes,
                stats.task_failed_writes,
            ),
            (0, 0, 1, 0, 0)
        );
    }

    #[test]
    fn async_write_guard_rejected_success_reply_records_eio() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "async-write-guard-rejected-success",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));
        let mut reply = FakeFuseReplyState::new(-1, Arc::clone(&writes), Arc::clone(&vfs));
        let guard = make_async_write_guard(&mut reply, &writes, &vfs);
        let watermark = writes.watermark(41);

        guard.finish(super::FuseWriteTaskResult::Completed(Ok(8)));

        assert_eq!(reply.replies, [FakeFuseReply::Write(8)]);
        assert_eq!(writes.wait_through(watermark), Err(libc::EIO));
        let callback = vfs.write_path_observability_snapshot().fuse_callbacks.write;
        assert_eq!((callback.calls, callback.errors), (1, 1));
        assert_eq!(writes.stats().failed_writes, 1);
    }

    #[test]
    fn async_write_guard_rejected_error_reply_retains_original_errno() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "async-write-guard-rejected-error",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));
        let mut reply = FakeFuseReplyState::new(-1, Arc::clone(&writes), Arc::clone(&vfs));
        let guard = make_async_write_guard(&mut reply, &writes, &vfs);
        let watermark = writes.watermark(41);

        guard.finish(super::FuseWriteTaskResult::Completed(Err(libc::ENOSPC)));

        assert_eq!(reply.replies, [FakeFuseReply::Error(libc::ENOSPC)]);
        assert_eq!(writes.wait_through(watermark), Err(libc::ENOSPC));
        assert_eq!(writes.stats().failed_writes, 1);
    }

    #[test]
    fn async_write_guard_unfinished_drop_replies_eio_once() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "async-write-guard-unfinished-drop",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));
        let mut reply = FakeFuseReplyState::new(0, Arc::clone(&writes), Arc::clone(&vfs));
        let guard = make_async_write_guard(&mut reply, &writes, &vfs);

        drop(guard);

        assert_eq!(reply.replies, [FakeFuseReply::Error(libc::EIO)]);
        assert_eq!(reply.observations, [(0, 1, 1)]);
        let callback = vfs.write_path_observability_snapshot().fuse_callbacks.write;
        assert_eq!((callback.calls, callback.errors), (1, 1));
        let stats = writes.stats();
        assert_eq!(
            (
                stats.pending_writes,
                stats.active_requests,
                stats.failed_writes,
                stats.task_failed_writes,
            ),
            (0, 0, 0, 1)
        );
    }

    #[test]
    fn async_write_guard_drop_after_finish_does_not_reply_twice() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "async-write-guard-finished-drop",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));
        let mut reply = FakeFuseReplyState::new(0, Arc::clone(&writes), Arc::clone(&vfs));
        let guard = make_async_write_guard(&mut reply, &writes, &vfs);

        guard.finish(super::FuseWriteTaskResult::Completed(Ok(8)));

        assert_eq!(reply.replies, [FakeFuseReply::Write(8)]);
    }

    #[test]
    fn async_write_shutdown_watcher_observes_exit_and_stops_promptly() {
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(8, 1));
        let exited = Arc::new(AtomicBool::new(false));
        let watcher_exited = Arc::clone(&exited);
        let watcher = super::SessionExitWatcher::start_with_probe(Arc::clone(&writes), move || {
            watcher_exited.load(Ordering::Acquire)
        });

        exited.store(true, Ordering::Release);
        let deadline = Instant::now() + Duration::from_secs(1);
        while writes.start_request(61).is_ok() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(matches!(writes.start_request(61), Err(errno) if errno == libc::EIO));

        let stop_started = Instant::now();
        watcher.stop();
        assert!(stop_started.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn async_write_shutdown_watcher_stop_interrupts_poll_wait() {
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(8, 1));
        let probes = Arc::new(AtomicUsize::new(0));
        let watcher_probes = Arc::clone(&probes);
        let watcher = super::SessionExitWatcher::start_with_probe(Arc::clone(&writes), move || {
            watcher_probes.fetch_add(1, Ordering::Relaxed);
            false
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while probes.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            thread::yield_now();
        }
        assert_ne!(probes.load(Ordering::Relaxed), 0);

        let stop_started = Instant::now();
        watcher.stop();
        assert!(stop_started.elapsed() < Duration::from_millis(100));
        assert!(writes.start_request(64).is_ok());
    }

    #[test]
    fn async_write_shutdown_watcher_wakes_blocked_callback_with_eio_reply() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "async-write-shutdown-blocked-callback",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(8, 0));
        let mut reply = Box::new(FakeFuseReplyState::new(
            0,
            Arc::clone(&writes),
            Arc::clone(&vfs),
        ));
        let request_ticket = writes.start_request(62).expect("start request");
        let request = super::FuseReqPtr::for_test(
            (&mut *reply as *mut FakeFuseReplyState).cast(),
            fake_fuse_reply_write,
            fake_fuse_reply_error,
        );
        let mut guard = super::FuseWriteTaskGuard {
            request: Some(request),
            request_ticket: Some(request_ticket),
            reservation: None,
            writes: Arc::clone(&writes),
            vfs,
            started: Instant::now(),
            requested_bytes: 1,
            metric_config: super::LinuxFuseAsyncWriteMetricConfig {
                cache_policy: super::LinuxFuseCachePolicy::Direct,
            },
        };
        let exited = Arc::new(AtomicBool::new(false));
        let watcher_exited = Arc::clone(&exited);
        let watcher = super::SessionExitWatcher::start_with_probe(Arc::clone(&writes), move || {
            watcher_exited.load(Ordering::Acquire)
        });
        let (waiting_tx, waiting_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let callback_writes = Arc::clone(&writes);
        let callback = thread::spawn(move || {
            match callback_writes.admit(1, |_| {
                waiting_tx.send(()).expect("report blocked admission")
            }) {
                Err(errno) => guard.finish(super::FuseWriteTaskResult::AdmissionRejected(errno)),
                Ok(reservation) => {
                    guard.attach_reservation(reservation);
                    guard.finish(super::FuseWriteTaskResult::Cancelled);
                }
            }
            completed_tx.send(()).expect("report callback completion");
        });

        waiting_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("callback should block in admission");
        exited.store(true, Ordering::Release);
        let callback_woke = completed_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        if !callback_woke {
            writes.begin_shutdown();
        }
        callback.join().expect("join callback");
        watcher.stop();

        assert!(callback_woke, "session-exit watcher did not wake callback");
        assert_eq!(reply.replies, [FakeFuseReply::Error(libc::EIO)]);
        let stats = writes.stats();
        assert_eq!(
            (
                stats.active_requests,
                stats.admission_waiters,
                stats.pending_writes,
                stats.pending_bytes,
            ),
            (0, 0, 0, 0),
        );
    }

    #[test]
    fn async_write_shutdown_cancels_only_after_drain_grace_period() {
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(8, 1));
        let ticket = writes.start_request(63).expect("start request");
        let reservation = writes.admit(1, |_| {}).expect("admit request");
        let cancellation = writes.task_cancellation_token();
        let shutdown_writes = Arc::clone(&writes);
        let drain_timeout = Duration::from_millis(80);
        let started = Instant::now();
        let shutdown = thread::spawn(move || {
            super::shutdown_async_writes_with_timeouts(
                &shutdown_writes,
                drain_timeout,
                Duration::from_secs(1),
            )
        });

        thread::sleep(Duration::from_millis(20));
        assert!(!cancellation.is_cancelled());
        let deadline = Instant::now() + Duration::from_secs(1);
        while !cancellation.is_cancelled() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(cancellation.is_cancelled());
        assert!(started.elapsed() >= drain_timeout);

        drop(reservation);
        ticket.complete(super::LinuxFuseAsyncWriteCompletion::Cancelled);
        assert!(shutdown.join().expect("join shutdown"));
        let stats = writes.stats();
        assert_eq!(
            (
                stats.active_requests,
                stats.admission_waiters,
                stats.pending_handles,
                stats.pending_writes,
                stats.pending_bytes,
                stats.max_handle_pending_writes,
            ),
            (0, 0, 0, 0, 0, 0),
        );
    }

    #[test]
    fn async_write_shutdown_forced_timeout_retains_raw_and_boxed_owner() {
        struct DropSentinel(Arc<AtomicUsize>);

        impl Drop for DropSentinel {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let raw_drops = Arc::new(AtomicUsize::new(0));
        let state_drops = Arc::new(AtomicUsize::new(0));
        super::retain_unsafe_session(
            DropSentinel(Arc::clone(&raw_drops)),
            Some(Box::new(DropSentinel(Arc::clone(&state_drops)))),
        );

        assert_eq!(
            (
                raw_drops.load(Ordering::Relaxed),
                state_drops.load(Ordering::Relaxed)
            ),
            (0, 0),
        );
    }

    #[test]
    fn argv_uses_system_libfuse_mount_options() {
        let args = super::argv_strings("demo");

        assert!(args.iter().any(|arg| arg == "studiofs-linux-fuse"));
        assert!(args.iter().any(|arg| arg == "fsname=demo"));
        assert!(args.iter().any(|arg| arg == "subtype=studiofs"));
        assert!(args.iter().any(|arg| arg == "auto_unmount"));
        assert!(!args.iter().any(|arg| arg == "writeback_cache"));
        assert!(!args.iter().any(|arg| arg.contains("max_background")));
        assert!(!args.iter().any(|arg| arg.contains("congestion_threshold")));
    }

    #[test]
    fn argv_kernel_cache_policy_keeps_mount_options_portable() {
        let args =
            super::argv_strings_for_cache_policy("demo", super::LinuxFuseCachePolicy::Kernel);

        assert!(!args.iter().any(|arg| arg == "writeback_cache"));
    }

    #[test]
    fn conn_feature_helper_requests_writeback_cache_when_supported() {
        let writeback_cache_cap = 1_u64 << 16;
        // SAFETY: the C shim mutates and inspects a stack-local fuse_conn_info.
        let enabled = unsafe {
            super::studiofs_linux_fuse_conn_wants_writeback_cache_after_set(writeback_cache_cap)
        };

        assert_eq!(enabled, 1);
    }

    #[test]
    fn conn_feature_helper_requests_async_dio_when_supported() {
        let async_dio_cap = 1_u64 << 15;
        // SAFETY: the C shim mutates and inspects a stack-local fuse_conn_info.
        let enabled =
            unsafe { super::studiofs_linux_fuse_conn_wants_async_dio_after_set(async_dio_cap) };

        assert_eq!(enabled, 1);
    }

    #[test]
    fn async_dio_policy_table_matches_cache_policy_and_override() {
        let cases = [
            (super::LinuxFuseCachePolicy::Direct, None, true),
            (super::LinuxFuseCachePolicy::Direct, Some("0"), false),
            (super::LinuxFuseCachePolicy::Direct, Some("1"), true),
            (super::LinuxFuseCachePolicy::Kernel, None, false),
            (super::LinuxFuseCachePolicy::Kernel, Some("1"), false),
        ];

        for (cache_policy, override_value, expected) in cases {
            assert_eq!(
                super::linux_fuse_async_dio_enabled_for(cache_policy, override_value),
                expected,
                "cache_policy={cache_policy:?}, override={override_value:?}",
            );
        }
    }

    #[test]
    fn linux_connection_init_uses_stored_cache_settings() {
        let cases = [
            (super::LinuxFuseCachePolicy::Direct, true, (1, 0)),
            (super::LinuxFuseCachePolicy::Kernel, false, (0, 1)),
        ];

        for (index, (cache_policy, requested_async_dio, expected)) in cases.into_iter().enumerate()
        {
            let (_fixture, state) = cache_test_state(
                &format!("linux-connection-stored-cache-{index}"),
                cache_policy,
                requested_async_dio,
            );
            let userdata = (&state as *const super::SessionState)
                .cast_mut()
                .cast::<std::ffi::c_void>();
            // SAFETY: userdata points to `state`, which outlives both synchronous callbacks.
            let actual = unsafe {
                (
                    super::studiofs_linux_fuse_connection_wants_async_dio(userdata),
                    super::studiofs_linux_fuse_connection_wants_writeback_cache(userdata),
                )
            };

            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn negotiated_geometry_preserves_requested_and_effective_values() {
        let (_direct_fixture, direct_state) = cache_test_state(
            "linux-negotiated-geometry-direct",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        let direct_userdata = (&direct_state as *const super::SessionState)
            .cast_mut()
            .cast::<std::ffi::c_void>();

        // SAFETY: userdata points to `direct_state`, and the C fixture owns its
        // stack-local connection for the duration of this synchronous call.
        let direct_want = unsafe {
            super::studiofs_linux_fuse_init_for_test(
                direct_userdata,
                4 * 1024 * 1024,
                1024 * 1024,
                512 * 1024,
                1,
                1,
                1,
                1,
            )
        };

        assert_eq!(direct_want, 0b01, "unrequested writeback must be cleared");
        assert_eq!(
            direct_state.negotiated_geometry.get(),
            Some(&super::LinuxFuseNegotiatedGeometry {
                max_read: 4 * 1024 * 1024,
                max_write: 1024 * 1024,
                max_readahead: 512 * 1024,
                max_background: 128,
                congestion_threshold: 96,
                async_dio: true,
                writeback_cache: false,
                parallel_direct_writes_supported: unsafe {
                    super::studiofs_linux_fuse_supports_parallel_direct_writes()
                } != 0,
                single_thread_receive_loop: unsafe {
                    super::studiofs_linux_fuse_uses_single_thread_loop()
                } != 0,
            }),
        );

        let (_kernel_fixture, kernel_state) = cache_test_state(
            "linux-negotiated-geometry-kernel",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let kernel_userdata = (&kernel_state as *const super::SessionState)
            .cast_mut()
            .cast::<std::ffi::c_void>();
        // SAFETY: same lifetime guarantee as the Direct fixture above.
        let kernel_want = unsafe {
            super::studiofs_linux_fuse_init_for_test(
                kernel_userdata,
                0,
                1024 * 1024,
                512 * 1024,
                1,
                1,
                1,
                1,
            )
        };
        assert_eq!(kernel_want, 0b10, "unrequested async DIO must be cleared");

        let (mismatch_fixture, mismatch_state) = cache_test_state(
            "linux-negotiated-geometry-mismatch",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        let mismatch_userdata = (&mismatch_state as *const super::SessionState)
            .cast_mut()
            .cast::<std::ffi::c_void>();
        // SAFETY: userdata points to `mismatch_state` for this synchronous callback.
        unsafe {
            super::studiofs_linux_fuse_record_negotiated_connection(
                mismatch_userdata,
                0,
                2 * 1024 * 1024,
                256 * 1024,
                64,
                48,
                0,
                1,
                1,
                0,
            );
        }
        let connection = mismatch_fixture
            .vfs()
            .write_path_observability_snapshot()
            .fuse_connection;

        assert_eq!(connection.configured_max_background, Some(128));
        assert_eq!(connection.configured_congestion_threshold, Some(96));
        assert_eq!(connection.requested_async_dio, Some(true));
        assert_eq!(connection.requested_writeback_cache, Some(false));
        assert_eq!(connection.negotiated_max_read_bytes, None);
        assert_eq!(connection.negotiated_max_background, Some(64));
        assert_eq!(connection.negotiated_congestion_threshold, Some(48));
        assert_eq!(connection.negotiated_async_dio, Some(false));
        assert_eq!(connection.negotiated_writeback_cache, Some(true));
    }

    #[test]
    fn async_write_active_limit_follows_memory_pressure_with_negotiated_write_floor() {
        let (_fixture, state) = cache_test_state(
            "linux-async-write-pressure-limit",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        let controller = state.vfs.memory_pressure_controller();
        let mut snapshot = controller.snapshot();
        snapshot.source = studiofs_vfs::MemoryPressureSource::Host;
        snapshot.state = studiofs_vfs::MemoryPressureState::Constrained;
        snapshot.limits.async_write_bytes = 512 * 1024;
        controller.publish(snapshot);
        let geometry = super::LinuxFuseNegotiatedGeometry {
            max_write: 1024 * 1024,
            ..Default::default()
        };

        assert_eq!(
            super::linux_fuse_async_write_active_limit(&state.vfs, Some(geometry)),
            1024 * 1024,
        );
    }

    #[test]
    fn disabled_memory_pressure_keeps_async_write_ceiling() {
        let (_fixture, state) = cache_test_state(
            "linux-async-write-pressure-disabled",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );

        assert_eq!(
            super::linux_fuse_async_write_active_limit(&state.vfs, None),
            super::LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_BYTES,
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pressure_publish_wakes_async_write_admission_without_queue_completion() {
        tokio::time::timeout(Duration::from_secs(1), async {
            let fixture = ContractVfsFixture::new("linux-async-write-pressure-watcher")
                .expect("create VFS fixture");
            let controller = fixture.vfs().memory_pressure_controller().clone();
            let mut snapshot = controller.snapshot();
            snapshot.source = studiofs_vfs::MemoryPressureSource::Host;
            snapshot.state = studiofs_vfs::MemoryPressureState::Constrained;
            snapshot.limits.async_write_bytes = 4;
            controller.publish(snapshot);
            let state = super::SessionState::new_with_cache_settings(
                fixture.vfs(),
                fixture.runtime().handle().clone(),
                false,
                default_connection_config(),
                super::LinuxFuseCachePolicy::Direct,
                true,
            );
            let admitted = state
                .async_writes
                .admit(4, |_| {})
                .expect("fill constrained budget");

            let (waiting_tx, waiting_rx) = tokio::sync::oneshot::channel();
            let writes = Arc::clone(&state.async_writes);
            let waiter = tokio::task::spawn_blocking(move || {
                writes.admit(4, |stats| {
                    let _ = waiting_tx.send(stats);
                })
            });
            waiting_rx.await.expect("admission wait observation");

            snapshot.generation += 1;
            snapshot.limits.async_write_bytes = 8;
            controller.publish(snapshot);
            let recovered = waiter
                .await
                .expect("admission waiter join")
                .expect("controller recovery admits write");
            assert_eq!(state.async_writes.stats().pending_bytes, 8);
            drop(recovered);
            drop(admitted);
            assert!(state.shutdown_memory_pressure_watcher().await);
        })
        .await
        .expect("memory-pressure watcher test timed out");
    }

    #[test]
    fn disabled_memory_pressure_does_not_start_async_write_watcher() {
        let (_fixture, state) = cache_test_state(
            "linux-async-write-pressure-watcher-disabled",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );

        assert!(state.memory_pressure_async_write_watcher.is_none());
    }

    #[test]
    fn current_thread_runtime_explicitly_joins_memory_pressure_watcher() {
        let fixture = ContractVfsFixture::new("linux-pressure-watcher-current-thread-shutdown")
            .expect("create VFS fixture");
        let controller = fixture.vfs().memory_pressure_controller().clone();
        let mut snapshot = controller.snapshot();
        snapshot.source = studiofs_vfs::MemoryPressureSource::Host;
        snapshot.state = studiofs_vfs::MemoryPressureState::Normal;
        controller.publish(snapshot);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        let state = super::SessionState::new_with_cache_settings(
            fixture.vfs(),
            runtime.handle().clone(),
            false,
            default_connection_config(),
            super::LinuxFuseCachePolicy::Direct,
            true,
        );

        assert!(runtime.block_on(state.shutdown_memory_pressure_watcher()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_worker_run_exit_joins_memory_pressure_watcher_without_panicking() {
        let fixture = ContractVfsFixture::new("linux-pressure-watcher-runtime-worker-shutdown")
            .expect("create VFS fixture");
        let controller = fixture.vfs().memory_pressure_controller().clone();
        let mut snapshot = controller.snapshot();
        snapshot.source = studiofs_vfs::MemoryPressureSource::Host;
        snapshot.state = studiofs_vfs::MemoryPressureState::Normal;
        controller.publish(snapshot);
        let state = super::SessionState::new_with_cache_settings(
            fixture.vfs(),
            tokio::runtime::Handle::current(),
            false,
            default_connection_config(),
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        let shutdown = tokio::spawn(async move {
            let outcome = state.shutdown_async_writes_after_run();
            let joined = state
                .memory_pressure_async_write_watcher
                .as_ref()
                .expect("enabled watcher")
                .shutdown
                .task
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .is_none();
            (outcome.watcher_stopped, outcome.writes_stopped, joined)
        });

        let result = tokio::time::timeout(Duration::from_secs(3), shutdown)
            .await
            .expect("run-exit watcher shutdown timed out")
            .expect("runtime worker panicked");
        assert_eq!(result, (true, true, true));
    }

    #[test]
    fn one_worker_runtime_run_exit_joins_watcher_and_tears_down_queue() {
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let fixture = ContractVfsFixture::new("linux-pressure-watcher-one-worker-shutdown")
                .expect("create VFS fixture");
            let controller = fixture.vfs().memory_pressure_controller().clone();
            let mut snapshot = controller.snapshot();
            snapshot.source = studiofs_vfs::MemoryPressureSource::Host;
            snapshot.state = studiofs_vfs::MemoryPressureState::Normal;
            controller.publish(snapshot);
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("build one-worker runtime");
            let state = super::SessionState::new_with_cache_settings(
                fixture.vfs(),
                runtime.handle().clone(),
                false,
                default_connection_config(),
                super::LinuxFuseCachePolicy::Direct,
                true,
            );
            let shutdown = runtime.spawn(async move {
                let outcome = state.shutdown_async_writes_after_run();
                let joined = state
                    .memory_pressure_async_write_watcher
                    .as_ref()
                    .expect("enabled watcher")
                    .shutdown
                    .task
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .is_none();
                (outcome.watcher_stopped, outcome.writes_stopped, joined)
            });
            let result = runtime.block_on(shutdown).expect("shutdown task join");
            done_tx.send(result).expect("send shutdown result");
        });

        let result = done_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("one-worker shutdown deadlocked");
        worker.join().expect("one-worker regression thread join");
        assert_eq!(result, (true, true, true));
    }

    #[test]
    fn current_thread_runtime_worker_run_exit_aborts_watcher_without_blocking() {
        let fixture = ContractVfsFixture::new("linux-pressure-watcher-current-worker-shutdown")
            .expect("create VFS fixture");
        let controller = fixture.vfs().memory_pressure_controller().clone();
        let mut snapshot = controller.snapshot();
        snapshot.source = studiofs_vfs::MemoryPressureSource::Host;
        snapshot.state = studiofs_vfs::MemoryPressureState::Normal;
        controller.publish(snapshot);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        let state = super::SessionState::new_with_cache_settings(
            fixture.vfs(),
            runtime.handle().clone(),
            false,
            default_connection_config(),
            super::LinuxFuseCachePolicy::Direct,
            true,
        );

        let result = runtime.block_on(async move {
            let outcome = state.shutdown_async_writes_after_run();
            let aborted = state
                .memory_pressure_async_write_watcher
                .as_ref()
                .expect("enabled watcher")
                .shutdown
                .task
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .is_none();
            (outcome.watcher_stopped, outcome.writes_stopped, aborted)
        });

        assert_eq!(result, (false, true, true));
    }

    #[test]
    fn current_thread_runtime_drop_aborts_watcher_without_waiting_for_runtime_progress() {
        let (ready_tx, ready_rx) = mpsc::channel();
        let (drop_tx, drop_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let fixture = ContractVfsFixture::new("linux-pressure-watcher-current-thread-drop")
                .expect("create VFS fixture");
            let controller = fixture.vfs().memory_pressure_controller().clone();
            let mut snapshot = controller.snapshot();
            snapshot.source = studiofs_vfs::MemoryPressureSource::Host;
            snapshot.state = studiofs_vfs::MemoryPressureState::Normal;
            controller.publish(snapshot);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread runtime");
            let state = super::SessionState::new_with_cache_settings(
                fixture.vfs(),
                runtime.handle().clone(),
                false,
                default_connection_config(),
                super::LinuxFuseCachePolicy::Direct,
                true,
            );
            ready_tx.send(()).expect("signal state ready");
            drop_rx.recv().expect("receive drop request");
            drop(state);
            dropped_tx.send(()).expect("signal state dropped");
        });
        ready_rx.recv().expect("wait for state construction");

        drop_tx.send(()).expect("request state drop");
        dropped_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("SessionState drop blocked on current-thread Tokio task");
        thread.join().expect("drop regression thread join");
    }

    #[test]
    fn negotiated_geometry_rejected_async_disables_parallel_direct_writes() {
        let (fixture, state) = cache_test_state(
            "linux-negotiated-geometry-async-rejected",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"async-rejected.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);

        let pre_init = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let (pre_init_policy, _) = accept_cache_open(&state, pre_init);
        assert!(!pre_init_policy.parallel_direct_writes);
        release_cache_open(&fixture, pre_init);

        let userdata = (&state as *const super::SessionState)
            .cast_mut()
            .cast::<std::ffi::c_void>();
        // SAFETY: userdata points to `state`, and the C fixture owns its
        // stack-local connection for this synchronous call.
        let want = unsafe {
            super::studiofs_linux_fuse_init_for_test(
                userdata,
                0,
                1024 * 1024,
                512 * 1024,
                0,
                1,
                1,
                1,
            )
        };
        assert_eq!(
            want, 0,
            "rejected async and unrequested writeback stay clear"
        );
        assert_eq!(
            state
                .negotiated_geometry
                .get()
                .map(|geometry| geometry.async_dio),
            Some(false),
        );
        let opened = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let (policy, _) = accept_cache_open(&state, opened);

        assert!(!policy.parallel_direct_writes);
        release_cache_open(&fixture, opened);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn negotiated_geometry_missing_shim_field_disables_parallel_direct_writes() {
        let (fixture, state) = cache_test_state(
            "linux-negotiated-geometry-field-missing",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        state.record_negotiated_geometry(super::LinuxFuseNegotiatedGeometry {
            async_dio: true,
            parallel_direct_writes_supported: false,
            ..Default::default()
        });
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"field-missing.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let opened = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let (policy, _) = accept_cache_open(&state, opened);

        assert!(!policy.parallel_direct_writes);
        release_cache_open(&fixture, opened);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn async_dio_policy_parallel_open_bit_requires_c_field_support() {
        let cases = [
            (true, true, true),
            (true, false, false),
            (false, true, false),
        ];

        for (async_dio_enabled, field_supported, expected) in cases {
            let policy = super::linux_fuse_open_cache_policy_for(
                super::LinuxFuseCachePolicy::Direct,
                async_dio_enabled,
                field_supported,
                studiofs_vfs::OsCacheDisposition::Reuse,
            );
            assert_eq!(policy.parallel_direct_writes, expected,);
        }
    }

    #[test]
    fn linux_runtime_stats_open_is_read_only_and_captures_once() {
        let (fixture, state) = cache_test_state(
            "linux-runtime-stats-open",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        let ino = studiofs_vfs::RUNTIME_STATS_FILE_INO;

        for flags in [libc::O_WRONLY, libc::O_RDWR, libc::O_RDONLY | libc::O_TRUNC] {
            assert!(matches!(
                state.open_runtime_stats(ino, flags),
                Some(Err(errno)) if errno == libc::EROFS
            ));
        }

        let before = fixture.vfs().write_path_observability_snapshot();
        let before_async = state.async_writes.stats();
        let opened = state
            .open_runtime_stats(ino, libc::O_RDONLY)
            .expect("runtime stats inode")
            .expect("open runtime stats");
        let after = fixture.vfs().write_path_observability_snapshot();
        let after_async = state.async_writes.stats();
        assert!(fixture.vfs().get_open_file(opened.fh).is_none());
        assert_eq!(before.fuse_liveness, after.fuse_liveness);
        assert_eq!(
            (
                before.os_cache.total,
                before.os_cache.reuse,
                before.os_cache.replace,
                before.os_cache.active_writers,
                before.os_cache.active_foreground_handles,
            ),
            (
                after.os_cache.total,
                after.os_cache.reuse,
                after.os_cache.replace,
                after.os_cache.active_writers,
                after.os_cache.active_foreground_handles,
            ),
        );
        assert_eq!(
            (
                before.vfs_write_path.open_file.calls,
                before.vfs_read_path.calls,
                before.fuse_callbacks.read.calls,
            ),
            (
                after.vfs_write_path.open_file.calls,
                after.vfs_read_path.calls,
                after.fuse_callbacks.read.calls,
            ),
        );
        assert_eq!(before_async, after_async);
        let captured = state
            .runtime_stats_handles
            .lock()
            .expect("runtime stats handles")
            .get(&opened.fh)
            .expect("captured runtime stats")
            .clone();

        fixture
            .vfs()
            .record_fuse_read_callback(17, 17, Duration::from_millis(1), false);
        let second = state
            .open_runtime_stats(ino, libc::O_RDONLY)
            .expect("runtime stats inode")
            .expect("open second runtime stats snapshot");
        let second_capture = state
            .runtime_stats_handles
            .lock()
            .expect("runtime stats handles")
            .get(&second.fh)
            .expect("second captured runtime stats")
            .clone();

        assert_ne!(
            fixture.vfs().runtime_stats_file_text().into_bytes(),
            captured
        );
        assert_ne!(second.fh, opened.fh);
        assert_ne!(second_capture, captured);
        assert_eq!(
            state
                .runtime_stats_handles
                .lock()
                .expect("runtime stats handles")
                .get(&opened.fh),
            Some(&captured),
        );
        state.release(opened.fh).expect("release first snapshot");
        state.release(second.fh).expect("release second snapshot");
    }

    #[test]
    fn linux_runtime_stats_read_slices_snapshot_without_metrics() {
        let (fixture, state) = cache_test_state(
            "linux-runtime-stats-read",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        let ino = studiofs_vfs::RUNTIME_STATS_FILE_INO;
        let opened = state
            .open_runtime_stats(ino, libc::O_RDONLY)
            .expect("runtime stats inode")
            .expect("open runtime stats");
        let captured = state
            .runtime_stats_handles
            .lock()
            .expect("runtime stats handles")
            .get(&opened.fh)
            .expect("captured runtime stats")
            .clone();
        let before = fixture.vfs().write_path_observability_snapshot();

        let actual = state
            .read_runtime_stats(ino, 7, 5, Some(opened.fh))
            .expect("runtime stats inode")
            .expect("read runtime stats");
        let short = state
            .read_runtime_stats(ino, 7, captured.len() as libc::off_t - 2, Some(opened.fh))
            .expect("runtime stats inode")
            .expect("read short runtime stats tail");
        let eof = state
            .read_runtime_stats(ino, 7, captured.len() as libc::off_t, Some(opened.fh))
            .expect("runtime stats inode")
            .expect("read runtime stats EOF");
        let missing = state
            .read_runtime_stats(ino, 7, 5, Some(opened.fh + 1))
            .expect("runtime stats inode");
        let after = fixture.vfs().write_path_observability_snapshot();

        assert_eq!(actual, captured[5..12]);
        assert_eq!(short, captured[captured.len() - 2..]);
        assert!(eof.is_empty());
        assert_eq!(missing, Err(libc::EBADF));
        assert_eq!(
            (
                before.fuse_callbacks.read.calls,
                before.fuse_callbacks.read.bytes,
                before.fuse_callbacks.read.errors,
            ),
            (
                after.fuse_callbacks.read.calls,
                after.fuse_callbacks.read.bytes,
                after.fuse_callbacks.read.errors,
            ),
        );
        assert_eq!(before.vfs_read_path.calls, after.vfs_read_path.calls);
        assert_eq!(
            before.vfs_write_path.open_file.calls,
            after.vfs_write_path.open_file.calls,
        );
        state.release(opened.fh).expect("release runtime stats");
    }

    #[test]
    fn linux_rejected_runtime_stats_open_reply_drops_only_synthetic_handle() {
        let (fixture, state) = cache_test_state(
            "linux-runtime-stats-rejected-reply",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        let ino = studiofs_vfs::RUNTIME_STATS_FILE_INO;
        let rejected = state
            .open_runtime_stats(ino, libc::O_RDONLY)
            .expect("runtime stats inode")
            .expect("open rejected runtime stats handle");
        let survivor = state
            .open_runtime_stats(ino, libc::O_RDONLY)
            .expect("runtime stats inode")
            .expect("open surviving runtime stats handle");
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"regular.bin", 0, 0, 0o644)
            .expect("create regular file");
        let regular = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let before = fixture.vfs().write_path_observability_snapshot();
        let before_async = state.async_writes.stats();
        let mut replied_fh = None;

        let mut file_info_storage = [0_u64; 32];
        let file_info = file_info_storage
            .as_mut_ptr()
            .cast::<super::raw::FuseFileInfo>();
        let policy = super::reply_runtime_stats_open_with(&state, file_info, rejected, |fi| {
            replied_fh = super::file_handle(fi);
            -1
        });
        let after = fixture.vfs().write_path_observability_snapshot();
        let after_async = state.async_writes.stats();

        assert_eq!(replied_fh, Some(rejected.fh));
        assert_eq!(
            policy,
            super::LinuxFuseOpenCachePolicy {
                direct_io: true,
                keep_cache: false,
                parallel_direct_writes: false,
            }
        );
        let handles = state
            .runtime_stats_handles
            .lock()
            .expect("runtime stats handles");
        assert!(!handles.contains_key(&rejected.fh));
        assert!(handles.contains_key(&survivor.fh));
        drop(handles);
        assert!(fixture.vfs().get_open_file(regular.fh).is_some());
        assert_eq!(before.fuse_liveness, after.fuse_liveness);
        assert_eq!(
            (
                before.os_cache.total,
                before.os_cache.active_writers,
                before.os_cache.active_foreground_handles,
                before.vfs_read_path.calls,
                before.vfs_write_path.open_file.calls,
            ),
            (
                after.os_cache.total,
                after.os_cache.active_writers,
                after.os_cache.active_foreground_handles,
                after.vfs_read_path.calls,
                after.vfs_write_path.open_file.calls,
            ),
        );
        assert_eq!(
            (
                before_async.active_requests,
                before_async.pending_handles,
                before_async.pending_writes,
                before_async.pending_bytes,
            ),
            (
                after_async.active_requests,
                after_async.pending_handles,
                after_async.pending_writes,
                after_async.pending_bytes,
            ),
        );

        state.release(survivor.fh).expect("release survivor");
        release_cache_open(&fixture, regular);
    }

    #[test]
    fn linux_runtime_stats_flush_and_release_bypass_normal_lifecycle() {
        let (fixture, state) = cache_test_state(
            "linux-runtime-stats-flush-release",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        let ino = studiofs_vfs::RUNTIME_STATS_FILE_INO;
        let opened = state
            .open_runtime_stats(ino, libc::O_RDONLY)
            .expect("runtime stats inode")
            .expect("open runtime stats");
        let before = fixture.vfs().write_path_observability_snapshot();
        let before_async = state.async_writes.stats();

        state.flush(opened.fh).expect("flush synthetic handle");
        state.release(opened.fh).expect("release synthetic handle");
        let read_after_release = state
            .read_runtime_stats(ino, 1, 0, Some(opened.fh))
            .expect("runtime stats inode");
        let second_release = state.release(opened.fh);

        let after = fixture.vfs().write_path_observability_snapshot();
        let after_async = state.async_writes.stats();
        assert!(
            !state
                .runtime_stats_handles
                .lock()
                .expect("runtime stats handles")
                .contains_key(&opened.fh)
        );
        assert!(fixture.vfs().get_open_file(opened.fh).is_none());
        assert_eq!(read_after_release, Err(libc::EBADF));
        assert_eq!(second_release, Err(libc::EBADF));
        assert_eq!(before.flush_barrier_calls, after.flush_barrier_calls);
        assert_eq!(before.fuse_liveness, after.fuse_liveness);
        assert_eq!(
            (
                before.os_cache.total,
                before.os_cache.active_writers,
                before.os_cache.active_foreground_handles,
            ),
            (
                after.os_cache.total,
                after.os_cache.active_writers,
                after.os_cache.active_foreground_handles,
            ),
        );
        assert_eq!(
            (
                before_async.active_requests,
                before_async.pending_handles,
                before_async.pending_writes,
                before_async.pending_bytes,
            ),
            (
                after_async.active_requests,
                after_async.pending_handles,
                after_async.pending_writes,
                after_async.pending_bytes,
            ),
        );
    }

    #[test]
    fn linux_cache_decision_flags_truth_table() {
        use studiofs_vfs::OsCacheDisposition::{Replace, Reuse};

        let cases = [
            (
                super::LinuxFuseCachePolicy::Direct,
                true,
                true,
                Replace,
                super::LinuxFuseOpenCachePolicy {
                    direct_io: true,
                    keep_cache: false,
                    parallel_direct_writes: true,
                },
            ),
            (
                super::LinuxFuseCachePolicy::Direct,
                true,
                true,
                Reuse,
                super::LinuxFuseOpenCachePolicy {
                    direct_io: true,
                    keep_cache: false,
                    parallel_direct_writes: true,
                },
            ),
            (
                super::LinuxFuseCachePolicy::Direct,
                true,
                false,
                Reuse,
                super::LinuxFuseOpenCachePolicy {
                    direct_io: true,
                    keep_cache: false,
                    parallel_direct_writes: false,
                },
            ),
            (
                super::LinuxFuseCachePolicy::Direct,
                false,
                true,
                Reuse,
                super::LinuxFuseOpenCachePolicy {
                    direct_io: true,
                    keep_cache: false,
                    parallel_direct_writes: false,
                },
            ),
            (
                super::LinuxFuseCachePolicy::Kernel,
                false,
                true,
                Replace,
                super::LinuxFuseOpenCachePolicy {
                    direct_io: false,
                    keep_cache: false,
                    parallel_direct_writes: false,
                },
            ),
            (
                super::LinuxFuseCachePolicy::Kernel,
                false,
                true,
                Replace,
                super::LinuxFuseOpenCachePolicy {
                    direct_io: false,
                    keep_cache: false,
                    parallel_direct_writes: false,
                },
            ),
            (
                super::LinuxFuseCachePolicy::Kernel,
                false,
                true,
                Reuse,
                super::LinuxFuseOpenCachePolicy {
                    direct_io: false,
                    keep_cache: true,
                    parallel_direct_writes: false,
                },
            ),
        ];

        for (cache_policy, async_dio, field_supported, disposition, expected) in cases {
            assert_eq!(
                super::linux_fuse_open_cache_policy_for(
                    cache_policy,
                    async_dio,
                    field_supported,
                    disposition,
                ),
                expected,
                "cache_policy={cache_policy:?}, async_dio={async_dio}, field_supported={field_supported}, disposition={disposition:?}",
            );
        }
    }

    #[test]
    fn linux_kernel_first_stable_read_replaces_cache() {
        let (fixture, state) = cache_test_state(
            "linux-kernel-first-stable-read",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"first.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let opened = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);

        let (policy, decision) = accept_cache_open(&state, opened);

        assert_eq!(decision.disposition(), OsCacheDisposition::Replace);
        assert_eq!(decision.reason(), OsCacheReason::FirstObservation);
        assert!(!policy.keep_cache);
        release_cache_open(&fixture, opened);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_kernel_second_identical_read_reuses_cache() {
        let (fixture, state) = cache_test_state(
            "linux-kernel-identical-read",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"stable.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let first = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let (_, first_decision) = accept_cache_open(&state, first);
        release_cache_open(&fixture, first);

        let second = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let (policy, second_decision) = accept_cache_open(&state, second);

        assert_eq!(first_decision.disposition(), OsCacheDisposition::Replace);
        assert_eq!(second_decision.disposition(), OsCacheDisposition::Reuse);
        assert!(policy.keep_cache);
        release_cache_open(&fixture, second);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_kernel_changed_generation_replaces_cache() {
        let (fixture, state) = cache_test_state(
            "linux-kernel-changed-generation",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let meta = fixture.meta();
        let mut attr = meta
            .create_file(studiofs_meta::ROOT_INO, b"generation.bin", 0, 0, 0o644)
            .expect("create file");
        attr.content_epoch = 7;
        attr.committed_content_epoch = 6;
        attr.upload_state = UploadState::Updating;
        meta.set_attr(&attr).expect("store initial generation");
        state.inode_liveness.observe(attr.ino);
        let first = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        accept_cache_open(&state, first);
        release_cache_open(&fixture, first);
        let mut changed = meta.get_attr(attr.ino).expect("load generation");
        changed.content_epoch = changed.content_epoch.saturating_add(1);
        meta.set_attr(&changed).expect("advance generation");

        let second = opened_file(
            attr.ino,
            fixture
                .vfs()
                .open_file_with_known_attr(&changed, OpenFlags::RDONLY, false)
                .expect("open changed generation"),
        );
        let (policy, decision) = accept_cache_open(&state, second);

        assert_eq!(decision.disposition(), OsCacheDisposition::Replace);
        assert_eq!(decision.reason(), OsCacheReason::GenerationChanged);
        assert!(!policy.keep_cache);
        release_cache_open(&fixture, second);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_kernel_dirty_inode_replaces_cache() {
        let (fixture, state) = cache_test_state(
            "linux-kernel-dirty-inode",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"dirty.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let writer = begin_cache_open(&fixture, attr.ino, OpenFlags::RDWR);
        accept_cache_open(&state, writer);
        fixture
            .runtime()
            .block_on(vfs.write_fh(writer.fh, attr.ino, 0, b"dirty"))
            .expect("write dirty payload");
        let reader = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);

        let (policy, decision) = accept_cache_open(&state, reader);

        assert_eq!(decision.disposition(), OsCacheDisposition::Replace);
        assert_eq!(decision.reason(), OsCacheReason::LocalDirtyState);
        assert!(!policy.keep_cache);
        release_cache_open(&fixture, reader);
        release_cache_open(&fixture, writer);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_kernel_active_writer_replaces_cache() {
        let (fixture, state) = cache_test_state(
            "linux-kernel-active-writer",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"writer.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let writer = begin_cache_open(&fixture, attr.ino, OpenFlags::RDWR);
        accept_cache_open(&state, writer);
        let reader = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);

        let (policy, decision) = accept_cache_open(&state, reader);

        assert_eq!(decision.disposition(), OsCacheDisposition::Replace);
        assert_eq!(decision.reason(), OsCacheReason::ActiveWriter);
        assert!(!policy.keep_cache);
        release_cache_open(&fixture, reader);
        release_cache_open(&fixture, writer);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_writer_open_never_keeps_cache() {
        let (fixture, state) = cache_test_state(
            "linux-kernel-writer-no-keep-cache",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"writer.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let writer = begin_cache_open(&fixture, attr.ino, OpenFlags::WRONLY);

        let (policy, decision) = accept_cache_open(&state, writer);

        assert_eq!(decision.reason(), OsCacheReason::WriteCapable);
        assert!(!policy.keep_cache);
        release_cache_open(&fixture, writer);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_direct_open_enrolls_foreground_lifetime_without_keep_cache() {
        let (fixture, state) = cache_test_state(
            "linux-direct-foreground-lifetime",
            super::LinuxFuseCachePolicy::Direct,
            true,
        );
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"direct.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let opened = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);

        let (policy, decision) = accept_cache_open(&state, opened);

        let active = fixture
            .vfs()
            .write_path_observability_snapshot()
            .os_cache
            .active_foreground_handles;
        assert_eq!(decision.reason(), OsCacheReason::PolicyDisabled);
        assert!(policy.direct_io);
        assert!(!policy.keep_cache);
        assert_eq!(active, 1);
        release_cache_open(&fixture, opened);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_open_prepare_is_counted_once() {
        let (fixture, state) = cache_test_state(
            "linux-open-prepare-once",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"once.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let opened = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let before = fixture
            .vfs()
            .write_path_observability_snapshot()
            .os_cache
            .total;

        accept_cache_open(&state, opened);

        let after = fixture
            .vfs()
            .write_path_observability_snapshot()
            .os_cache
            .total;
        assert_eq!(after, before + 1);
        release_cache_open(&fixture, opened);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_same_inode_prepare_reply_is_serialized() {
        let (fixture, state) = cache_test_state(
            "linux-same-inode-serialized",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"same.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let first_opened = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let second_opened = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let state = Arc::new(state);
        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let first_state = Arc::clone(&state);
        let first = thread::spawn(move || {
            super::complete_file_open_reply_with(&first_state, first_opened, false, |_| {
                first_entered_tx.send(()).expect("signal first reply");
                release_first_rx.recv().expect("release first reply");
                0
            })
        });
        first_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first reply enters");
        let (second_replied_tx, second_replied_rx) = mpsc::channel();
        let (second_started_tx, second_started_rx) = mpsc::channel();
        let second_state = Arc::clone(&state);
        let second = thread::spawn(move || {
            second_started_tx
                .send(())
                .expect("signal second completion attempt");
            super::complete_file_open_reply_with(&second_state, second_opened, false, |policy| {
                second_replied_tx.send(policy).expect("signal second reply");
                0
            })
        });
        second_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second completion starts");
        let early_policy = second_replied_rx
            .recv_timeout(Duration::from_millis(100))
            .ok();
        let progressed_early = early_policy.is_some();
        release_first_tx.send(()).expect("release first reply");
        let second_policy = early_policy.unwrap_or_else(|| {
            second_replied_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("second reply follows first")
        });
        let first_decision = first.join().expect("join first reply");
        let second_decision = second.join().expect("join second reply");

        assert!(!progressed_early, "same-inode reply must remain serialized");
        assert_eq!(first_decision.disposition(), OsCacheDisposition::Replace);
        assert_eq!(second_decision.disposition(), OsCacheDisposition::Reuse);
        assert!(second_policy.keep_cache);
        release_cache_open(&fixture, first_opened);
        release_cache_open(&fixture, second_opened);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_different_inode_reply_progresses_while_one_reply_is_blocked() {
        let (fixture, state) = cache_test_state(
            "linux-different-inode-progress",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let first_attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"first.bin", 0, 0, 0o644)
            .expect("create first file");
        let second_attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"second.bin", 0, 0, 0o644)
            .expect("create second file");
        state.inode_liveness.observe(first_attr.ino);
        state.inode_liveness.observe(second_attr.ino);
        let first_opened = begin_cache_open(&fixture, first_attr.ino, OpenFlags::RDONLY);
        let second_opened = begin_cache_open(&fixture, second_attr.ino, OpenFlags::RDONLY);
        let state = Arc::new(state);
        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let first_state = Arc::clone(&state);
        let first = thread::spawn(move || {
            super::complete_file_open_reply_with(&first_state, first_opened, false, |_| {
                first_entered_tx.send(()).expect("signal first reply");
                release_first_rx.recv().expect("release first reply");
                0
            })
        });
        first_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first reply enters");
        let (second_replied_tx, second_replied_rx) = mpsc::channel();
        let second_state = Arc::clone(&state);
        let second = thread::spawn(move || {
            super::complete_file_open_reply_with(&second_state, second_opened, false, |_| {
                second_replied_tx.send(()).expect("signal second reply");
                0
            })
        });
        let second_progress = second_replied_rx.recv_timeout(Duration::from_secs(1));
        release_first_tx.send(()).expect("release first reply");
        second_progress.expect("different inode reply progresses");
        let second_decision = second.join().expect("join second reply");
        let first_decision = first.join().expect("join first reply");

        assert_eq!(first_decision.disposition(), OsCacheDisposition::Replace);
        assert_eq!(second_decision.disposition(), OsCacheDisposition::Replace);
        release_cache_open(&fixture, first_opened);
        release_cache_open(&fixture, second_opened);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_rejected_replace_reply_makes_next_open_replace_again() {
        let (fixture, state) = cache_test_state(
            "linux-rejected-replace",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"rejected.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let rejected = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);

        let rejected_decision =
            super::complete_file_open_reply_with(&state, rejected, false, |_| -libc::EIO);

        assert_eq!(rejected_decision.disposition(), OsCacheDisposition::Replace);
        assert_eq!(fixture.vfs().open_count(attr.ino), 0);
        assert_cache_lifecycle_idle(&fixture);
        let next = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let (policy, next_decision) = accept_cache_open(&state, next);
        assert_eq!(next_decision.disposition(), OsCacheDisposition::Replace);
        assert!(!policy.keep_cache);
        release_cache_open(&fixture, next);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_failed_open_reply_releases_handle_and_lifecycle() {
        let (fixture, state) = cache_test_state(
            "linux-failed-open-reply",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"failed-open.bin", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);
        let before_liveness = state.inode_liveness.stats();
        let opened = begin_cache_open(&fixture, attr.ino, OpenFlags::RDONLY);
        let mut file_info = [0_u64; 32];

        super::reply_file_open_with(&state, file_info.as_mut_ptr().cast(), opened, |_| {
            -libc::EIO
        });

        assert_eq!(fixture.vfs().open_count(attr.ino), 0);
        assert_eq!(state.inode_liveness.stats(), before_liveness);
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn linux_failed_create_reply_releases_handle_and_lifecycle() {
        let (fixture, state) = cache_test_state(
            "linux-failed-create-reply",
            super::LinuxFuseCachePolicy::Kernel,
            false,
        );
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"failed-create.bin", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_created_file_with_known_attr(&attr, OpenFlags::WRONLY, false)
            .expect("open created file");
        let created = created_file(&attr, fh);
        let before_liveness = state.inode_liveness.stats();
        let mut file_info = [0_u64; 32];

        let decision =
            super::reply_create_with(&state, file_info.as_mut_ptr().cast(), created, |_, _| {
                assert!(state.inode_liveness.is_observed(attr.ino));
                -libc::EIO
            });

        let after_liveness = state.inode_liveness.stats();
        assert_eq!(decision.reason(), OsCacheReason::WriteCapable);
        assert_eq!(vfs.open_count(attr.ino), 0);
        assert!(!state.inode_liveness.is_observed(attr.ino));
        assert_eq!(
            after_liveness.observed_references,
            before_liveness.observed_references
        );
        assert_eq!(
            after_liveness.forgotten_references,
            before_liveness.forgotten_references
        );
        assert_cache_lifecycle_idle(&fixture);
    }

    #[test]
    fn async_dio_policy_current_c_field_support_matches_version_mapper() {
        let version = (
            env!("STUDIOFS_LIBFUSE_VERSION_MAJOR").parse().unwrap(),
            env!("STUDIOFS_LIBFUSE_VERSION_MINOR").parse().unwrap(),
            env!("STUDIOFS_LIBFUSE_VERSION_PATCH").parse().unwrap(),
        );
        // SAFETY: this C probe only returns a compile-time feature decision.
        let c_supports_field =
            unsafe { super::studiofs_linux_fuse_supports_parallel_direct_writes() } != 0;

        assert_eq!(
            c_supports_field,
            super::linux_fuse_supports_parallel_direct_writes_for(version),
        );
    }

    #[test]
    fn libfuse_compatibility_threshold_tables_are_exact() {
        assert!(!super::linux_fuse_has_bounded_loop_config_for((3, 11, 99)));
        assert!(super::linux_fuse_has_bounded_loop_config_for((3, 12, 0)));
        assert!(!super::linux_fuse_supports_parallel_direct_writes_for((
            3, 14, 0
        )));
        assert!(super::linux_fuse_supports_parallel_direct_writes_for((
            3, 14, 1
        )));
        assert!(!super::linux_fuse_has_extended_conn_caps_for((3, 17, 0)));
        assert!(super::linux_fuse_has_extended_conn_caps_for((3, 17, 1)));
    }

    #[test]
    fn bounded_loop_selection_table_matches_header_threshold() {
        assert_eq!(
            super::linux_fuse_bounded_loop_selection_for((3, 11, 99), 8),
            (true, 1),
        );
        assert_eq!(
            super::linux_fuse_bounded_loop_selection_for((3, 12, 0), 8),
            (false, 8),
        );
    }

    #[test]
    fn bounded_loop_selection_current_c_probe_matches_version_mapper() {
        let version = (
            env!("STUDIOFS_LIBFUSE_VERSION_MAJOR").parse().unwrap(),
            env!("STUDIOFS_LIBFUSE_VERSION_MINOR").parse().unwrap(),
            env!("STUDIOFS_LIBFUSE_VERSION_PATCH").parse().unwrap(),
        );
        // SAFETY: this C probe only returns a compile-time feature decision.
        let c_uses_single_thread =
            unsafe { super::studiofs_linux_fuse_uses_single_thread_loop() } != 0;

        assert_eq!(
            (
                c_uses_single_thread,
                if c_uses_single_thread { 1 } else { 8 }
            ),
            super::linux_fuse_bounded_loop_selection_for(version, 8),
        );
    }

    #[test]
    fn async_write_reservation_releases_pending_bytes_on_drop() {
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));

        let reservation = writes.admit(8, |_| {}).expect("admit write");
        let pending = writes.stats();
        assert_eq!((pending.pending_writes, pending.pending_bytes), (1, 8));

        drop(reservation);
        let released = writes.stats();
        assert_eq!((released.pending_writes, released.pending_bytes), (0, 0));
    }

    #[test]
    fn async_write_registration_after_release_returns_ebadf_without_orphan_ledger() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-async-write-release-race",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"release-race.dat", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let payload = b"race";
        let mut file_info_storage = [0_u64; 32];
        let file_info = file_info_storage
            .as_mut_ptr()
            .cast::<super::raw::FuseFileInfo>();
        // SAFETY: the aligned scratch storage outlives this synchronous test
        // and is large enough for the system libfuse `fuse_file_info` value.
        unsafe {
            super::studiofs_linux_fuse_file_info_init(file_info, fh, 1, 0, 0);
        }
        let prepared = state
            .prepare_write_metadata(payload.as_ptr().cast(), payload.len(), 0, file_info)
            .expect("prepare async write");

        state.release(fh).expect("release file before registration");
        let result = state.start_live_async_write_request(prepared.fh);
        let stats = state.async_writes.stats();
        let snapshot = vfs.write_path_observability_snapshot().fuse_async_write;

        assert!(matches!(result, Err(errno) if errno == libc::EBADF));
        assert_eq!(
            (
                stats.active_requests,
                stats.pending_handles,
                stats.pending_writes,
                stats.pending_bytes,
                stats.failed_writes,
                snapshot.active_requests,
                snapshot.pending_handles,
                snapshot.pending_writes,
                snapshot.pending_bytes,
                snapshot.failed_writes,
                vfs.get_open_file(fh).is_none(),
            ),
            (0, 0, 0, 0, 1, 0, 0, 0, 0, 1, true),
        );
    }

    #[test]
    fn async_write_defaults_to_enabled_with_explicit_disable() {
        assert!(super::linux_fuse_env_flag_value("1"));
        assert!(super::linux_fuse_env_flag_value("true"));
        assert!(super::linux_fuse_env_flag_value("yes"));
        assert!(super::linux_fuse_env_flag_value("on"));
        assert!(!super::linux_fuse_env_flag_value("0"));
        assert!(!super::linux_fuse_env_flag_value("false"));
        assert!(super::linux_fuse_async_write_enabled_from_env(None, None));
        assert!(super::linux_fuse_async_write_enabled_from_env(
            Some("1"),
            None
        ));
        assert!(!super::linux_fuse_async_write_enabled_from_env(
            Some("0"),
            None
        ));
        assert!(!super::linux_fuse_async_write_enabled_from_env(
            Some("1"),
            Some("1")
        ));
    }

    #[test]
    fn owned_fuse_args_are_null_terminated_without_counting_sentinel() {
        let args = super::OwnedFuseArgs::new(super::argv_strings("demo")).expect("build args");

        assert_eq!(args.raw.argc as usize, args._strings.len());
        assert!(args._argv.last().is_some_and(|arg| arg.is_null()));
    }

    #[test]
    fn low_level_ops_include_metadata_and_directory_callbacks() {
        // SAFETY: the C shim only inspects callback pointers in a static table.
        let has_callbacks = unsafe {
            super::studiofs_linux_fuse_low_level_ops_has_metadata_callbacks(
                super::studiofs_linux_fuse_low_level_ops(),
            )
        };

        assert_eq!(has_callbacks, 1);
    }

    #[test]
    fn linux_low_level_ops_installs_forget_callback() {
        // SAFETY: the C shim only inspects one callback pointer in a static table.
        let has_callback = unsafe {
            super::studiofs_linux_fuse_low_level_ops_has_forget_callback(
                super::studiofs_linux_fuse_low_level_ops(),
            )
        };

        assert_eq!(has_callback, 1);
    }

    #[test]
    fn linux_forget_without_callback_state_still_replies_none() {
        let replies = AtomicUsize::new(0);

        super::reply_forget_with(None, 42, 1, || {
            replies.fetch_add(1, Ordering::Relaxed);
        });

        assert_eq!(replies.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn linux_forget_decrements_exact_lookup_count() {
        let (_fixture, state) = liveness_test_state("linux-forget-exact-count");
        state.inode_liveness.observe(42);
        state.inode_liveness.observe(42);
        state.inode_liveness.observe(42);

        super::reply_forget_with(Some(&state), 42, 2, || {});

        assert!(state.inode_liveness.is_observed(42));
        assert_eq!(state.inode_liveness.stats().forgotten_references, 2);

        super::reply_forget_with(Some(&state), 42, 1, || {});
        assert!(!state.inode_liveness.is_observed(42));
        assert_eq!(state.inode_liveness.stats().forgotten_references, 3);
    }

    #[test]
    fn linux_forget_zero_count_is_a_noop() {
        let (_fixture, state) = liveness_test_state("linux-forget-zero-count");
        state.inode_liveness.observe(42);
        let before = state.inode_liveness.stats();

        super::reply_forget_with(Some(&state), 42, 0, || {});

        assert!(state.inode_liveness.is_observed(42));
        assert_eq!(state.inode_liveness.stats(), before);
    }

    #[test]
    fn linux_entry_reply_reserves_before_reply_and_rolls_back_failure() {
        let (fixture, state) = liveness_test_state("linux-entry-reply-liveness");
        let accepted = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"accepted", 0, 0, 0o644)
            .expect("create accepted entry");
        let rejected = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"rejected", 0, 0, 0o644)
            .expect("create rejected entry");

        super::reply_entry_with(&state, &accepted, |_| {
            assert!(state.inode_liveness.is_observed(accepted.ino));
            0
        });
        let before_rejected = state.inode_liveness.stats();
        super::reply_entry_with(&state, &rejected, |_| {
            assert!(state.inode_liveness.is_observed(rejected.ino));
            -libc::ENOENT
        });
        let after_rejected = state.inode_liveness.stats();

        assert!(state.inode_liveness.is_observed(accepted.ino));
        assert!(!state.inode_liveness.is_observed(rejected.ino));
        assert_eq!(
            after_rejected.observed_references,
            before_rejected.observed_references
        );
        assert_eq!(
            after_rejected.forgotten_references,
            before_rejected.forgotten_references
        );
    }

    #[test]
    fn linux_create_reply_reserves_before_reply_and_rolls_back_failure() {
        let (fixture, state) = liveness_test_state("linux-create-reply-liveness");
        let accepted = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"accepted", 0, 0, 0o644)
            .expect("create accepted entry");
        let rejected = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"rejected", 0, 0, 0o644)
            .expect("create rejected entry");
        let mut accepted_info = [0_u64; 32];
        let mut rejected_info = [0_u64; 32];

        super::reply_create_with(
            &state,
            accepted_info.as_mut_ptr().cast(),
            created_file(&accepted, 7),
            |_, _| {
                assert!(state.inode_liveness.is_observed(accepted.ino));
                0
            },
        );
        let before_rejected = state.inode_liveness.stats();
        super::reply_create_with(
            &state,
            rejected_info.as_mut_ptr().cast(),
            created_file(&rejected, 8),
            |_, _| {
                assert!(state.inode_liveness.is_observed(rejected.ino));
                -libc::ENOENT
            },
        );
        let after_rejected = state.inode_liveness.stats();

        assert!(state.inode_liveness.is_observed(accepted.ino));
        assert!(!state.inode_liveness.is_observed(rejected.ino));
        assert_eq!(
            after_rejected.observed_references,
            before_rejected.observed_references
        );
        assert_eq!(
            after_rejected.forgotten_references,
            before_rejected.forgotten_references
        );
    }

    #[test]
    fn linux_open_reply_does_not_change_lookup_reference_count() {
        let (_fixture, state) = liveness_test_state("linux-open-reply-liveness");
        state.inode_liveness.observe(42);
        state.inode_liveness.observe(42);
        let before = state.inode_liveness.stats();
        let mut accepted_info = [0_u64; 32];
        let mut rejected_info = [0_u64; 32];

        super::reply_file_open_with(
            &state,
            accepted_info.as_mut_ptr().cast(),
            opened_file(42, 7),
            |_| 0,
        );
        super::reply_file_open_with(
            &state,
            rejected_info.as_mut_ptr().cast(),
            opened_file(42, 8),
            |_| -libc::ENOENT,
        );

        assert_eq!(state.inode_liveness.stats(), before);
        state.inode_liveness.forget(42, 1);
        assert!(state.inode_liveness.is_observed(42));
    }

    #[test]
    fn linux_open_of_unobserved_inode_records_invariant_without_creating_reference() {
        let (_fixture, state) = liveness_test_state("linux-open-unknown-inode");
        let mut file_info = [0_u64; 32];

        super::reply_file_open_with(
            &state,
            file_info.as_mut_ptr().cast(),
            opened_file(42, 7),
            |_| {
                assert!(!state.inode_liveness.is_observed(42));
                0
            },
        );

        assert!(!state.inode_liveness.is_observed(42));
        assert_eq!(state.inode_liveness.stats().observed_references, 0);
        assert_eq!(state.inode_liveness.stats().open_unknown_inode, 1);
    }

    #[test]
    fn linux_unlink_does_not_force_drop_lookup_reference() {
        let (fixture, state) = liveness_test_state("linux-unlink-keeps-lookup-reference");
        let attr = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"unlinked", 0, 0, 0o644)
            .expect("create file");
        state.inode_liveness.observe(attr.ino);

        state
            .unlink(studiofs_meta::ROOT_INO, b"unlinked")
            .expect("unlink file");

        assert!(state.inode_liveness.is_observed(attr.ino));
    }

    #[test]
    fn linux_rename_overwrite_does_not_force_drop_lookup_reference() {
        let (fixture, state) = liveness_test_state("linux-rename-keeps-lookup-reference");
        fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"source", 0, 0, 0o644)
            .expect("create source");
        let replaced = fixture
            .vfs()
            .create(studiofs_meta::ROOT_INO, b"target", 0, 0, 0o644)
            .expect("create target");
        state.inode_liveness.observe(replaced.ino);

        state
            .rename(
                studiofs_meta::ROOT_INO,
                b"source",
                studiofs_meta::ROOT_INO,
                b"target",
            )
            .expect("rename over target");

        assert!(state.inode_liveness.is_observed(replaced.ino));
    }

    #[test]
    fn linux_unlinked_open_inode_remains_observed_until_forget() {
        let (fixture, state) = liveness_test_state("linux-unlinked-open-liveness");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"open-unlinked", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        state.inode_liveness.observe(attr.ino);

        state
            .unlink(studiofs_meta::ROOT_INO, b"open-unlinked")
            .expect("unlink open file");
        state.release(fh).expect("release file");

        assert!(state.inode_liveness.is_observed(attr.ino));
        super::reply_forget_with(Some(&state), attr.ino, 1, || {});
        assert!(!state.inode_liveness.is_observed(attr.ino));
    }

    #[test]
    fn linux_invalidator_suppresses_unknown_or_forgotten_inode() {
        let (fixture, state) = liveness_test_state("linux-invalidator-liveness");
        let recorder = Arc::new(RecordingLinuxInvalidator::default());
        let checked: Arc<dyn KernelInvalidator> = Arc::new(
            studiofs_bridge::fuse_liveness::LivenessCheckedKernelInvalidator::new(
                Arc::clone(&state.inode_liveness),
                recorder.clone(),
            ),
        );
        let invalidator = super::PublishingLinuxKernelInvalidator::new(
            checked,
            Arc::clone(&state.inode_liveness),
            fixture.vfs(),
        );

        invalidator.inval_inode(42);
        state.inode_liveness.observe(42);
        invalidator.inval_inode(42);
        state.inode_liveness.forget(42, 1);
        invalidator.inval_inode(42);

        assert_eq!(recorder.inode_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.inode_liveness.stats().notifications_forwarded, 1);
        assert_eq!(state.inode_liveness.stats().notifications_suppressed, 2);
    }

    #[test]
    fn linux_liveness_runtime_stats_publish_reference_and_notification_deltas() {
        let (fixture, state) = liveness_test_state("linux-liveness-runtime-deltas");
        let vfs = fixture.vfs();
        let recorder = Arc::new(RecordingLinuxInvalidator::default());
        let checked: Arc<dyn KernelInvalidator> = Arc::new(
            studiofs_bridge::fuse_liveness::LivenessCheckedKernelInvalidator::new(
                Arc::clone(&state.inode_liveness),
                recorder,
            ),
        );
        let invalidator = super::PublishingLinuxKernelInvalidator::new(
            checked,
            Arc::clone(&state.inode_liveness),
            Arc::clone(&vfs),
        );

        state.inode_liveness.observe(41);
        state.publish_liveness_stats();
        invalidator.inval_inode(41);
        vfs.reset_diagnostics_observability();

        state.inode_liveness.observe(42);
        state.publish_liveness_stats();
        super::reply_forget_with(Some(&state), 41, 1, || {});
        invalidator.inval_inode(42);
        invalidator.inval_inode(99);

        let counters = vfs.runtime_stats_snapshot().bridge.counters;
        assert_eq!(counters["fuse_liveness.snapshot_generation"].value, 6);
        assert_eq!(counters["fuse_liveness.observed_references"].value, 1);
        assert_eq!(counters["fuse_liveness.forgotten_references"].value, 1);
        assert_eq!(counters["fuse_liveness.notifications_forwarded"].value, 1);
        assert_eq!(counters["fuse_liveness.notifications_suppressed"].value, 1);
    }

    #[test]
    fn libfuse_compatibility_loop_config_probe_matches_single_thread_probe() {
        // SAFETY: the C shim returns its compile-time libfuse API selection.
        let uses_loop_config_api =
            unsafe { super::studiofs_linux_fuse_loop_config_uses_loop_config_api() };
        // SAFETY: this C probe only returns a compile-time feature decision.
        let uses_single_thread = unsafe { super::studiofs_linux_fuse_uses_single_thread_loop() };

        assert_eq!(uses_loop_config_api, 1 - uses_single_thread);
    }

    #[test]
    fn loop_thread_count_scales_with_parallelism_and_background_queue() {
        assert_eq!(
            super::linux_fuse_loop_thread_count_for_parallelism(4, 16),
            8
        );
    }

    #[test]
    fn loop_thread_count_caps_at_background_queue() {
        assert_eq!(
            super::linux_fuse_loop_thread_count_for_parallelism(64, 16),
            16
        );
    }

    #[test]
    fn init_callback_sets_bounded_kernel_background_queue() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-default-bounds")
                .expect("create VFS fixture");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let config = default_connection_config();
        // SAFETY: the C shim initializes a stack-local fuse_conn_info.
        let has_bounds = unsafe {
            super::studiofs_linux_fuse_init_sets_bounded_background(
                (&mut state as *mut super::SessionState).cast(),
                config.max_background,
                config.congestion_threshold,
            )
        };

        assert_eq!(has_bounds, 1);
    }

    #[test]
    fn init_callback_uses_request_background_queue_config() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-custom-bounds")
                .expect("create VFS fixture");
        let config = super::LinuxFuseConnectionConfig {
            max_background: 32,
            congestion_threshold: 24,
        };
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            config,
        );
        // SAFETY: the C shim initializes a stack-local fuse_conn_info.
        let has_bounds = unsafe {
            super::studiofs_linux_fuse_init_sets_bounded_background(
                (&mut state as *mut super::SessionState).cast(),
                config.max_background,
                config.congestion_threshold,
            )
        };

        assert_eq!(has_bounds, 1);
    }

    #[test]
    fn low_level_ops_include_regular_file_callbacks() {
        // SAFETY: the C shim only inspects callback pointers in a static table.
        let has_callbacks = unsafe {
            super::studiofs_linux_fuse_low_level_ops_has_regular_file_callbacks(
                super::studiofs_linux_fuse_low_level_ops(),
            )
        };

        assert_eq!(has_callbacks, 1);
    }

    #[test]
    fn low_level_ops_include_namespace_mutation_callbacks() {
        // SAFETY: the C shim only inspects callback pointers in a static table.
        let has_callbacks = unsafe {
            super::studiofs_linux_fuse_low_level_ops_has_namespace_mutation_callbacks(
                super::studiofs_linux_fuse_low_level_ops(),
            )
        };

        assert_eq!(has_callbacks, 1);
    }

    #[test]
    fn low_level_ops_include_posix_compat_callbacks() {
        // SAFETY: the C shim only inspects callback pointers in a static table.
        let has_callbacks = unsafe {
            super::studiofs_linux_fuse_low_level_ops_has_posix_compat_callbacks(
                super::studiofs_linux_fuse_low_level_ops(),
            )
        };

        assert_eq!(has_callbacks, 1);
    }

    #[test]
    fn regular_file_info_defaults_to_direct_io() {
        // SAFETY: the C shim initializes and inspects a stack-local fuse_file_info.
        let direct_io = unsafe { super::studiofs_linux_fuse_file_info_direct_io_after_init() };

        assert_eq!(direct_io, 1);
    }

    #[test]
    fn file_info_init_sets_requested_cache_flags() {
        // SAFETY: the C shim initializes and inspects a stack-local fuse_file_info.
        let flags = unsafe { super::studiofs_linux_fuse_file_info_flags_after_init(0, 1, 1) };
        // SAFETY: this C probe only returns a compile-time feature decision.
        let parallel_supported =
            unsafe { super::studiofs_linux_fuse_supports_parallel_direct_writes() } != 0;

        assert_eq!(flags, if parallel_supported { 6 } else { 2 });
    }

    #[test]
    fn kernel_cache_policy_keeps_read_only_open_cache() {
        let policy = super::linux_fuse_open_cache_policy_for(
            super::LinuxFuseCachePolicy::Kernel,
            false,
            true,
            studiofs_vfs::OsCacheDisposition::Reuse,
        );

        assert_eq!(
            policy,
            super::LinuxFuseOpenCachePolicy {
                direct_io: false,
                keep_cache: true,
                parallel_direct_writes: false,
            }
        );
    }

    #[test]
    fn kernel_cache_policy_drops_write_capable_open_cache() {
        let policy = super::linux_fuse_open_cache_policy_for(
            super::LinuxFuseCachePolicy::Kernel,
            false,
            true,
            studiofs_vfs::OsCacheDisposition::Replace,
        );

        assert_eq!(
            policy,
            super::LinuxFuseOpenCachePolicy {
                direct_io: false,
                keep_cache: false,
                parallel_direct_writes: false,
            }
        );
    }

    #[test]
    fn invalidation_state_wait_for_idle_returns_true_when_idle() {
        let state = super::SessionInvalidationState::new();

        assert!(state.wait_for_idle(Duration::from_millis(1)));
    }

    #[test]
    fn invalidation_state_wait_for_idle_wakes_when_last_guard_drops() {
        let state = Arc::new(super::SessionInvalidationState::new());
        let guard = state.begin_invalidation().expect("begin invalidation");
        let waiter_state = Arc::clone(&state);
        let (observed_tx, observed_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let waiter = thread::spawn(move || {
            let status = waiter_state.lock_status();
            let result = waiter_state
                .idle
                .wait_timeout_while(status, Duration::from_secs(1), |status| {
                    if status.active > 0 {
                        let _ = observed_tx.send(());
                    }
                    status.active > 0
                })
                .unwrap_or_else(|err| err.into_inner());
            result_tx
                .send(result.0.active == 0)
                .expect("send idle result");
        });

        observed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should observe active invalidation");
        drop(guard);
        assert!(
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("idle result")
        );
        waiter.join().expect("join idle waiter");
    }

    #[test]
    fn invalidation_state_wait_for_idle_returns_false_on_timeout() {
        let state = super::SessionInvalidationState::new();
        let _guard = state.begin_invalidation().expect("begin invalidation");

        assert!(!state.wait_for_idle(Duration::from_millis(1)));
    }

    #[test]
    fn invalidation_state_rejects_new_invalidations_after_shutdown() {
        let state = super::SessionInvalidationState::new();
        state.begin_shutdown();

        assert!(state.begin_invalidation().is_none());
    }

    #[test]
    fn require_file_info_rejects_null_pointer() {
        assert_eq!(
            super::require_file_info(std::ptr::null()),
            Err(libc::EINVAL)
        );
    }

    #[test]
    fn clamp_read_size_caps_oversized_request() {
        assert_eq!(
            super::clamp_read_size(super::LINUX_FUSE_MAX_READ_SIZE + 1),
            super::LINUX_FUSE_MAX_READ_SIZE
        );
    }

    #[test]
    fn sync_open_handle_uses_write_through_policy() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-sync-policy")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"sync.dat", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file_with_known_attr(
                &attr,
                studiofs_vfs::OpenFlags::WRONLY | studiofs_vfs::OpenFlags::SYNC,
                false,
            )
            .expect("open sync file");
        let state = super::SessionState::new(
            vfs,
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );

        assert_eq!(
            state.local_sync_policy_for_handle(fh),
            studiofs_vfs::LocalSyncPolicy::WriteThrough
        );
    }

    #[test]
    fn dirty_opportunistic_release_is_synchronous() {
        assert_eq!(
            super::linux_fuse_release_mode(LocalSyncPolicy::Opportunistic, true),
            super::LinuxFuseReleaseMode::Synchronous,
        );
    }

    #[test]
    fn clean_opportunistic_release_is_deferred() {
        assert_eq!(
            super::linux_fuse_release_mode(LocalSyncPolicy::Opportunistic, false),
            super::LinuxFuseReleaseMode::Deferred,
        );
    }

    #[test]
    fn write_through_release_is_synchronous() {
        assert_eq!(
            super::linux_fuse_release_mode(LocalSyncPolicy::WriteThrough, false),
            super::LinuxFuseReleaseMode::Synchronous,
        );
    }

    #[test]
    fn merge_async_and_local_barrier_results_uses_computed_local_error() {
        let async_result = Ok(());
        let local_result = Err(libc::EIO);

        assert_eq!(
            super::merge_async_and_local_barrier_results(async_result, local_result),
            Err(libc::EIO),
        );
    }

    #[test]
    fn merge_async_and_local_barrier_results_prefers_async_error_when_both_fail() {
        let async_result = Err(libc::ENOSPC);
        let local_result = Err(libc::EIO);

        assert_eq!(
            super::merge_async_and_local_barrier_results(async_result, local_result),
            Err(libc::ENOSPC),
        );
    }

    #[test]
    fn flush_runs_vfs_barrier_after_async_error() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-flush-after-async-error",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"flush-error.dat", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        write_via_linux_session(&state, attr.ino, fh, b"local bytes");
        let failed = state
            .async_writes
            .start_request(fh)
            .expect("start failed async write");
        failed.complete(super::LinuxFuseAsyncWriteCompletion::Failed(libc::ENOSPC));

        let result = state.flush(fh);

        assert_eq!(
            (result, vfs.inode_has_local_dirty_state(attr.ino)),
            (Err(libc::ENOSPC), false),
        );
        state.release(fh).expect("release file");
    }

    #[test]
    fn handle_backed_fsync_runs_vfs_barrier_after_async_error() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-fsync-after-async-error",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"fsync-error.dat", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        write_via_linux_session(&state, attr.ino, fh, b"fsync local bytes");
        let failed = state
            .async_writes
            .start_request(fh)
            .expect("start failed async write");
        failed.complete(super::LinuxFuseAsyncWriteCompletion::Failed(libc::ENOSPC));

        let result = state.fsync(Some(fh), attr.ino, true);

        assert_eq!(
            (result, vfs.inode_has_local_dirty_state(attr.ino)),
            (Err(libc::ENOSPC), false),
        );
        state.release(fh).expect("release file");
    }

    #[test]
    fn release_closes_vfs_handle_after_async_error() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-release-after-async-error",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"release-error.dat", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let failed = state
            .async_writes
            .start_request(fh)
            .expect("start failed async write");
        failed.complete(super::LinuxFuseAsyncWriteCompletion::Failed(libc::ENOSPC));

        let result = state.release(fh);

        assert_eq!(
            (
                result,
                vfs.get_open_file(fh).is_none(),
                vfs.file_session_stats().open_handles,
            ),
            (Err(libc::ENOSPC), true, 0),
        );
    }

    #[test]
    fn linux_flush_is_a_strict_local_durability_barrier() {
        let mut fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-strict-flush")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"strict-flush.dat", 0, 0, 0o644)
            .expect("create file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let payload: &[u8] = b"strict local durability";
        write_via_linux_session(&state, ino, fh, payload);
        assert!(vfs.inode_has_local_dirty_state(ino));

        state.flush(fh).expect("flush file");

        assert!(!vfs.inode_has_local_dirty_state(ino));
        state.release(fh).expect("release file");
        drop(state);
        drop(vfs);

        let reopened = fixture.reopen_vfs().expect("reopen VFS fixture");
        let read_fh = reopened
            .open_file(ino, studiofs_vfs::OpenFlags::RDONLY, false)
            .expect("open reopened file");
        let mut readback = vec![0; payload.len()];
        let read = fixture
            .runtime()
            .block_on(reopened.read_into_fh(read_fh, ino, 0, &mut readback))
            .expect("read reopened file");
        fixture
            .runtime()
            .block_on(reopened.release_fh(read_fh))
            .expect("release reopened file");

        assert_eq!(
            (read as usize, readback.as_slice()),
            (payload.len(), payload)
        );
    }

    #[test]
    fn linux_flush_after_async_error_still_materializes_prior_local_state() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-flush-error-materializes",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_meta::ROOT_INO,
                b"flush-error-bytes.dat",
                0,
                0,
                0o644,
            )
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let payload: &[u8] = b"bytes before async error";
        write_via_linux_session(&state, attr.ino, fh, payload);
        let failed = state
            .async_writes
            .start_request(fh)
            .expect("start failed async write");
        failed.complete(super::LinuxFuseAsyncWriteCompletion::Failed(libc::ENOSPC));

        let flush_result = state.flush(fh);
        let mut readback = vec![0; payload.len()];
        let read = fixture
            .runtime()
            .block_on(vfs.read_into_fh(fh, attr.ino, 0, &mut readback))
            .expect("read flushed file");

        assert_eq!(
            (
                flush_result,
                vfs.inode_has_local_dirty_state(attr.ino),
                read as usize,
                readback.as_slice(),
            ),
            (Err(libc::ENOSPC), false, payload.len(), payload),
        );
        state.release(fh).expect("release file");
    }

    #[test]
    fn linux_release_after_async_error_removes_handle_and_lifecycle() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-release-error-lifecycle",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_meta::ROOT_INO,
                b"release-error-lifecycle.dat",
                0,
                0,
                0o644,
            )
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        write_via_linux_session(&state, attr.ino, fh, b"dirty release bytes");
        let failed = state
            .async_writes
            .start_request(fh)
            .expect("start failed async write");
        failed.complete(super::LinuxFuseAsyncWriteCompletion::Failed(libc::ENOSPC));

        let release_result = state.release(fh);
        let sessions = vfs.file_session_stats();

        assert_eq!(
            (
                release_result,
                vfs.get_open_file(fh).is_none(),
                sessions.open_handles,
                sessions.active_sessions,
                vfs.inode_has_local_dirty_state(attr.ino),
            ),
            (Err(libc::ENOSPC), true, 0, 0, false),
        );
    }

    #[test]
    fn linux_barrier_does_not_wait_for_another_inode() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-inode-scoped-barrier",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let target = vfs
            .create(studiofs_meta::ROOT_INO, b"barrier-target.dat", 0, 0, 0o644)
            .expect("create target file");
        let unrelated = vfs
            .create(
                studiofs_meta::ROOT_INO,
                b"barrier-unrelated.dat",
                0,
                0,
                0o644,
            )
            .expect("create unrelated file");
        let target_fh = vfs
            .open_file(target.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open target file");
        let unrelated_fh = vfs
            .open_file(unrelated.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open unrelated file");
        let state = Arc::new(super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        ));
        write_via_linux_session(&state, target.ino, target_fh, b"target bytes");
        let unrelated_write = state
            .async_writes
            .start_request(unrelated_fh)
            .expect("start unrelated async write");
        let (result_tx, result_rx) = mpsc::channel();
        let barrier_state = Arc::clone(&state);
        let barrier = thread::spawn(move || {
            let _ = result_tx.send(barrier_state.flush(target_fh));
        });

        let result = result_rx.recv_timeout(Duration::from_millis(750));
        unrelated_write.complete(super::LinuxFuseAsyncWriteCompletion::Success);
        barrier.join().expect("join target barrier");
        state.release(target_fh).expect("release target file");
        state.release(unrelated_fh).expect("release unrelated file");

        assert_eq!(
            result.expect("target barrier waited for unrelated inode"),
            Ok(()),
        );
    }

    #[test]
    fn regular_file_fsync_without_handle_returns_ebadf_without_mount_global_wait() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-fsync-without-handle",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let target = vfs
            .create(studiofs_meta::ROOT_INO, b"target.dat", 0, 0, 0o644)
            .expect("create target file");
        let unrelated = vfs
            .create(studiofs_meta::ROOT_INO, b"unrelated.dat", 0, 0, 0o644)
            .expect("create unrelated file");
        let unrelated_fh = vfs
            .open_file(unrelated.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open unrelated file");
        let state = Arc::new(super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        ));
        let pending = state
            .async_writes
            .start_request(unrelated_fh)
            .expect("start unrelated async write");
        let (result_tx, result_rx) = mpsc::channel();
        let fsync_state = Arc::clone(&state);
        let fsync = thread::spawn(move || {
            let _ = result_tx.send(fsync_state.fsync(None, target.ino, false));
        });

        let result = result_rx.recv_timeout(Duration::from_millis(250));
        pending.complete(super::LinuxFuseAsyncWriteCompletion::Success);
        fsync.join().expect("join fsync callback");
        fixture
            .runtime()
            .block_on(vfs.release_fh(unrelated_fh))
            .expect("release unrelated handle");

        assert_eq!(
            result.expect("handle-less fsync waited for unrelated async write"),
            Err(libc::EBADF),
        );
    }

    #[test]
    fn read_only_write_rejects_before_using_file_info() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-read-only-write")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            true,
            default_connection_config(),
        );

        assert_eq!(
            state.write(1, std::ptr::null(), 0, 0, std::ptr::null()),
            Err(libc::EROFS)
        );
    }

    #[test]
    fn create_rejects_null_file_info_before_touching_vfs() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-create-null-fi")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );

        assert!(matches!(
            state.create(1, b"null-fi.txt", libc::S_IFREG | 0o644, std::ptr::null()),
            Err(errno) if errno == libc::EINVAL
        ));
        assert!(fixture.vfs().lookup(1, b"null-fi.txt").is_err());
    }

    #[test]
    fn read_only_namespace_mutations_reject_before_touching_vfs() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-read-only-mutate")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            true,
            default_connection_config(),
        );

        assert_eq!(state.mkdir(1, b"dir", 0o755).unwrap_err(), libc::EROFS);
        assert_eq!(
            state.mknod(1, b"file", libc::S_IFREG | 0o644).unwrap_err(),
            libc::EROFS
        );
        assert_eq!(state.unlink(1, b"file"), Err(libc::EROFS));
        assert_eq!(state.rmdir(1, b"dir"), Err(libc::EROFS));
        assert_eq!(state.rename(1, b"old", 1, b"new"), Err(libc::EROFS));
        assert_eq!(
            state.symlink(1, b"link", b"target").unwrap_err(),
            libc::EROFS
        );
        assert_eq!(state.link(2, 1, b"hard").unwrap_err(), libc::EROFS);
        assert_eq!(
            state.set_xattr(1, b"user.key", b"value", 0),
            Err(libc::EROFS)
        );
        assert_eq!(state.remove_xattr(1, b"user.key"), Err(libc::EROFS));
    }

    #[test]
    fn mknod_reports_special_files_as_unsupported() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-mknod-special")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );

        assert_eq!(
            state.mknod(1, b"fifo", libc::S_IFIFO | 0o644).unwrap_err(),
            libc::ENOTSUP
        );
    }

    #[test]
    fn setattr_args_translate_linux_mask() {
        let attr = std::mem::MaybeUninit::<libc::stat>::zeroed();
        // SAFETY: zeroed `stat` is valid for this test and fields are set below.
        let mut attr = unsafe { attr.assume_init() };
        attr.st_size = 42;
        attr.st_mode = libc::S_IFREG | 0o640;
        attr.st_uid = 1000;
        attr.st_gid = 1001;
        attr.st_mtime = 7;
        attr.st_mtime_nsec = 9;

        let args = super::setattr_args(
            &attr,
            super::FUSE_SET_ATTR_SIZE
                | super::FUSE_SET_ATTR_MODE
                | super::FUSE_SET_ATTR_UID
                | super::FUSE_SET_ATTR_GID
                | super::FUSE_SET_ATTR_MTIME,
        )
        .expect("setattr args");

        assert_eq!(args.size, Some(42));
        assert_eq!(args.mode, Some(0o640));
        assert_eq!(args.uid, Some(1000));
        assert_eq!(args.gid, Some(1001));
        assert_eq!(args.mtime.resolve(), Some(7_000_000_009));
    }

    #[test]
    fn setattr_args_attr_only_classifier_accepts_metadata_updates_without_size() {
        for args in [
            studiofs_meta::SetattrArgs {
                mode: Some(0o644),
                ..Default::default()
            },
            studiofs_meta::SetattrArgs {
                uid: Some(1000),
                ..Default::default()
            },
            studiofs_meta::SetattrArgs {
                gid: Some(1001),
                ..Default::default()
            },
            studiofs_meta::SetattrArgs {
                mtime: studiofs_meta::TimeSpec::Now,
                ..Default::default()
            },
            studiofs_meta::SetattrArgs {
                win_attrs: Some(0),
                ..Default::default()
            },
        ] {
            assert!(super::linux_fuse_setattr_args_are_attr_only(&args));
        }
    }

    #[test]
    fn setattr_args_attr_only_classifier_rejects_empty_and_size_updates() {
        for args in [
            studiofs_meta::SetattrArgs::default(),
            studiofs_meta::SetattrArgs {
                size: Some(42),
                ..Default::default()
            },
            studiofs_meta::SetattrArgs {
                size: Some(42),
                mode: Some(0o644),
                ..Default::default()
            },
        ] {
            assert!(!super::linux_fuse_setattr_args_are_attr_only(&args));
        }
    }

    #[test]
    fn xattr_name_list_is_nul_separated() {
        assert_eq!(
            super::format_xattr_name_list(&[b"user.one".to_vec(), b"user.two".to_vec()]),
            b"user.one\0user.two\0"
        );
    }

    #[test]
    fn xattr_mutation_errno_preserves_missing_inode() {
        assert_eq!(
            super::xattr_mutation_err_to_errno(&studiofs_vfs::VfsError::Meta(
                studiofs_meta::MetaError::EntryNotFound {
                    parent: 7,
                    name: "user.missing".to_owned(),
                },
            )),
            libc::ENODATA
        );
        assert_eq!(
            super::xattr_mutation_err_to_errno(&studiofs_vfs::VfsError::Meta(
                studiofs_meta::MetaError::InodeNotFound(7),
            )),
            libc::ENOENT
        );
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_smoke_mounts_and_unmounts_cleanly() {
        let fixture = ContractVfsFixture::new("linux-fuse-smoke").expect("create VFS fixture");
        fixture.start().expect("start VFS fixture");
        let vfs = fixture.vfs();
        let runtime_handle = fixture.runtime().handle().clone();

        run_mounted_vfs_test("smoke", vfs, runtime_handle, |mount_point, vfs| {
            std::fs::metadata(mount_point).expect("probe mounted root");
            wait_for_snapshot(vfs, Duration::from_secs(5), |snapshot| {
                snapshot.fuse_connection.negotiated_async_dio.is_some()
            })
            .expect("FUSE init did not publish smoke-test negotiation");
        });
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_regular_file_io_is_visible_across_reopen() {
        let fixture = ContractVfsFixture::new("linux-fuse-regular-io").expect("create VFS fixture");
        fixture.start().expect("start VFS fixture");
        let vfs = fixture.vfs();
        let runtime_handle = fixture.runtime().handle().clone();

        run_mounted_vfs_test("regular-io", vfs, runtime_handle, |mount_point, _| {
            let path = mount_point.join("regular.txt");
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)
                .expect("create mounted regular file");
            file.write_all(b"hello local-first")
                .expect("write mounted regular file");
            file.flush().expect("flush mounted regular file");
            file.seek(SeekFrom::Start(0))
                .expect("seek mounted regular file");
            let mut same_handle = String::new();
            file.read_to_string(&mut same_handle)
                .expect("read mounted regular file through same handle");
            assert_eq!(same_handle, "hello local-first");
            file.sync_all().expect("fsync mounted regular file");
            drop(file);

            assert_eq!(
                std::fs::read_to_string(&path).expect("reopen mounted regular file"),
                "hello local-first"
            );
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open mounted regular file for truncate");
            file.set_len(5).expect("truncate mounted regular file");
            file.sync_data()
                .expect("fdatasync truncated mounted regular file");
            drop(file);
            assert_eq!(
                std::fs::read_to_string(&path).expect("reopen truncated mounted regular file"),
                "hello"
            );
        });
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_same_file_parallel_direct_writes_preserve_disjoint_regions() {
        use std::os::unix::fs::FileExt;

        const WRITERS: usize = 8;
        const REGION_SIZE: usize = 1024 * 1024;

        let cache_policy = super::linux_fuse_cache_policy_from_env();
        let async_dio_override = std::env::var("STUDIOFS_FUSE_ASYNC_DIO").ok();
        assert_eq!(
            cache_policy,
            super::LinuxFuseCachePolicy::Direct,
            "parallel direct-write regression requires Direct cache policy"
        );
        assert!(
            super::linux_fuse_async_dio_enabled_for(cache_policy, async_dio_override.as_deref()),
            "parallel direct-write regression requires async DIO"
        );
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-parallel-direct-writes",
        )
        .expect("create VFS fixture");
        fixture.start().expect("start VFS fixture");
        let vfs = fixture.vfs();
        let runtime_handle = fixture.runtime().handle().clone();

        run_mounted_vfs_test(
            "parallel-direct",
            vfs,
            runtime_handle,
            |mount_point, vfs| {
                std::fs::metadata(mount_point).expect("probe mounted parallel-write root");
                let negotiation = wait_for_snapshot(vfs, Duration::from_secs(5), |snapshot| {
                    snapshot.fuse_connection.negotiated_async_dio.is_some()
                        && snapshot
                            .fuse_connection
                            .parallel_direct_writes_supported
                            .is_some()
                })
                .expect("FUSE init did not publish Direct async negotiation");
                assert_eq!(
                    negotiation.fuse_connection.negotiated_async_dio,
                    Some(true),
                    "same-file parallel writes require negotiated async DIO"
                );
                if negotiation.fuse_connection.parallel_direct_writes_supported == Some(false) {
                    eprintln!(
                        "SKIP \
                         test=mounted_same_file_parallel_direct_writes_preserve_disjoint_regions \
                         reason=unsupported_capability parallel_direct_writes_supported=false"
                    );
                    return;
                }
                assert_eq!(
                    negotiation.fuse_connection.parallel_direct_writes_supported,
                    Some(true)
                );

                let path = mount_point.join("parallel.dat");
                let base = std::fs::OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .expect("create parallel-write file");
                let start = Arc::new(std::sync::Barrier::new(WRITERS + 1));
                let mut writers = Vec::with_capacity(WRITERS);
                for index in 0..WRITERS {
                    let file = base.try_clone().expect("clone parallel-write file");
                    let start = Arc::clone(&start);
                    writers.push(std::thread::spawn(move || -> std::io::Result<()> {
                        let region =
                            vec![u8::try_from(index + 1).expect("writer byte"); REGION_SIZE];
                        start.wait();
                        file.write_all_at(&region, (index * REGION_SIZE) as u64)
                    }));
                }
                start.wait();
                for writer in writers {
                    writer
                        .join()
                        .expect("parallel writer should not panic")
                        .expect("parallel write should complete");
                }
                base.sync_all().expect("fsync parallel-write file");
                drop(base);

                let actual = std::fs::read(&path).expect("read parallel-write file");
                let mut expected = Vec::with_capacity(WRITERS * REGION_SIZE);
                for index in 0..WRITERS {
                    expected.extend(std::iter::repeat_n(
                        u8::try_from(index + 1).expect("writer byte"),
                        REGION_SIZE,
                    ));
                }
                assert_eq!(actual, expected);
            },
        );
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_forced_unmount_drains_admitted_and_waiting_raw_requests() {
        use std::os::unix::fs::FileExt;

        let reply_rejections_before = super::ASYNC_WRITE_REPLY_REJECTIONS.load(Ordering::Relaxed);
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-forced-unmount")
                .expect("create VFS fixture");
        fixture.start().expect("start VFS fixture");
        let mount_point = mounted_target_path("forced-unmount");
        std::fs::create_dir_all(&mount_point).expect("create mount point");
        let vfs = fixture.vfs();

        let state = super::SessionState::new_for_forced_unmount_test(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let tracker = Arc::clone(&state.async_writes);
        let runtime_handle = fixture.runtime().handle().clone();

        let mut session =
            super::Session::mount(state, &mount_point, "studiofs-forced-unmount").expect("mount");
        let invalidations = Arc::clone(&session.invalidation_state);
        let shutdown_probe = Arc::new(AtomicBool::new(false));
        let watcher_probe = Arc::clone(&shutdown_probe);
        let test_watcher =
            super::SessionExitWatcher::start_with_probe(Arc::clone(&tracker), move || {
                watcher_probe.load(Ordering::Acquire)
            });
        let worker_mount_point = mount_point.clone();
        let worker_tracker = Arc::clone(&tracker);
        let worker_shutdown_probe = Arc::clone(&shutdown_probe);
        let worker_vfs = Arc::clone(&vfs);
        let worker = std::thread::spawn(move || -> std::io::Result<()> {
            let guard = UnmountGuard::new(worker_mount_point.clone());
            std::fs::metadata(&worker_mount_point)?;
            let negotiation = wait_for_snapshot(&worker_vfs, Duration::from_secs(5), |snapshot| {
                snapshot.fuse_connection.negotiated_async_dio.is_some()
            })
            .map_err(std::io::Error::other)?;
            if negotiation.fuse_connection.negotiated_async_dio != Some(true) {
                return Err(std::io::Error::other(format!(
                    "forced-unmount admission test requires negotiated async DIO: {:?}",
                    negotiation.fuse_connection.negotiated_async_dio
                )));
            }
            // Synchronous kernel submissions may serialize writes to one inode.
            // The preceding test covers that case; distinct files isolate global admission.
            let first_path = worker_mount_point.join("forced-unmount-first.dat");
            let second_path = worker_mount_point.join("forced-unmount-second.dat");
            drop(
                std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&first_path)?,
            );
            drop(
                std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&second_path)?,
            );
            let first_file = std::fs::OpenOptions::new().write(true).open(&first_path)?;
            let second_file = std::fs::OpenOptions::new().write(true).open(&second_path)?;

            let (runtime_entered_tx, runtime_entered_rx) = mpsc::channel();
            let (runtime_release_tx, runtime_release_rx) = mpsc::channel();
            runtime_handle.spawn(async move {
                let _ = runtime_entered_tx.send(());
                let _ = runtime_release_rx.recv();
            });
            let runtime_gate_release = RuntimeGateRelease(Some(runtime_release_tx));
            let mut first = None;
            let mut second = None;
            let observation = (|| -> Result<(), String> {
                runtime_entered_rx
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|error| format!("runtime gate did not start: {error}"))?;

                first = Some(thread::spawn(move || first_file.write_all_at(b"first", 0)));
                wait_for_async_write_stats(&worker_tracker, |stats| {
                    stats.active_requests == 1 && stats.pending_writes == 1
                })
                .map_err(|stats| format!("first write was not admitted: {stats:?}"))?;

                second = Some(thread::spawn(move || {
                    second_file.write_all_at(b"second", 4096)
                }));
                wait_for_async_write_stats(&worker_tracker, |stats| {
                    stats.active_requests == 2
                        && stats.pending_writes == 1
                        && stats.admission_waiters == 1
                })
                .map_err(|stats| format!("second write did not wait for admission: {stats:?}"))?;
                Ok(())
            })();

            let transition = observation.and_then(|()| {
                worker_shutdown_probe.store(true, Ordering::Release);
                wait_for_async_write_stats(&worker_tracker, |stats| {
                    stats.active_requests == 1
                        && stats.pending_writes == 1
                        && stats.admission_waiters == 0
                })
                .map(|_| ())
                .map_err(|stats| {
                    format!(
                        "shutdown watcher did not drain the waiting raw request: stats={stats:?}"
                    )
                })
            });
            if let Err(error) = transition {
                runtime_gate_release.release();
                let unmount_result = bounded_unmount(&worker_mount_point);
                if unmount_result.is_ok() {
                    guard.disarm();
                }
                let first_status = first.take().map(|writer| match writer.join() {
                    Ok(result) => format!("{result:?}"),
                    Err(_) => "panicked".to_owned(),
                });
                let second_status = second.take().map(|writer| match writer.join() {
                    Ok(result) => format!("{result:?}"),
                    Err(_) => "panicked".to_owned(),
                });
                return Err(std::io::Error::other(format!(
                    "{error}; fallback_unmount={unmount_result:?}; \
                     first_writer={first_status:?}; second_writer={second_status:?}"
                )));
            }

            runtime_gate_release.release();
            let first_join = first
                .take()
                .expect("first writer exists after successful observation")
                .join();
            let second_join = second
                .take()
                .expect("second writer exists after successful observation")
                .join();
            if first_join.is_err() || second_join.is_err() {
                return Err(std::io::Error::other(format!(
                    "writer panicked during forced unmount: first={}, second={}",
                    first_join.is_err(),
                    second_join.is_err()
                )));
            }
            if !worker_tracker.wait_idle(Duration::from_secs(5)) {
                return Err(std::io::Error::other(format!(
                    "async-write tracker did not become idle: {:?}",
                    worker_tracker.stats()
                )));
            }
            let stats = worker_tracker.stats();
            let live_stats = (
                stats.active_requests,
                stats.pending_writes,
                stats.pending_bytes,
                stats.admission_waiters,
            );
            if live_stats != (0, 0, 0, 0) {
                return Err(std::io::Error::other(format!(
                    "async-write tracker retained live state: {stats:?}"
                )));
            }
            let reply_rejections = super::ASYNC_WRITE_REPLY_REJECTIONS.load(Ordering::Relaxed);
            if reply_rejections != reply_rejections_before {
                return Err(std::io::Error::other(format!(
                    "forced unmount produced late async-write replies: \
                     before={reply_rejections_before}, after={reply_rejections}, stats={stats:?}"
                )));
            }
            bounded_unmount(&worker_mount_point)?;
            guard.disarm();
            Ok(())
        });

        let run_result = session.run();
        test_watcher.stop();
        let worker_result = worker.join();
        drop(session);
        let mount_absent = wait_for_mount_absent(&mount_point, Duration::from_secs(5));
        let invalidations_idle = invalidations.wait_for_idle(Duration::from_secs(5));
        let writes_idle = tracker.wait_idle(Duration::from_secs(5));
        let write_stats = tracker.stats();
        let snapshot = wait_for_local_mounted_idle(&vfs);
        let remove_result = std::fs::remove_dir_all(&mount_point);

        run_result.expect("run libfuse3 session");
        worker_result
            .expect("forced-unmount worker should not panic")
            .expect("forced-unmount worker should complete");
        assert!(
            mount_absent,
            "findmnt still reports the forced-unmount path"
        );
        assert!(
            invalidations_idle,
            "kernel invalidation remained active at teardown"
        );
        assert!(
            writes_idle,
            "async writes did not drain at teardown: {write_stats:?}"
        );
        assert_eq!(
            (
                write_stats.active_requests,
                write_stats.pending_writes,
                write_stats.pending_bytes,
                write_stats.admission_waiters,
            ),
            (0, 0, 0, 0),
            "forced-unmount tracker retained live work"
        );
        assert_eq!(
            (
                snapshot.fuse_async_read.pending_reads,
                snapshot.fuse_async_read.pending_bytes,
                snapshot.os_cache.active_foreground_handles,
                snapshot.os_cache.active_writers,
            ),
            (0, 0, 0, 0),
            "forced-unmount lifecycle gauges were not zero at teardown"
        );
        remove_result.expect("remove forced-unmount path");
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_namespace_mutations_are_visible_through_readdir() {
        let fixture =
            ContractVfsFixture::new("linux-fuse-namespace-mutate").expect("create VFS fixture");
        fixture.start().expect("start VFS fixture");
        let vfs = fixture.vfs();
        let runtime_handle = fixture.runtime().handle().clone();

        run_mounted_vfs_test("namespace-mutate", vfs, runtime_handle, |mount_point, _| {
            let result = (|| -> std::io::Result<()> {
                let dir = mount_point.join("dir");
                let file = dir.join("file.txt");
                let hardlink = dir.join("hardlink.txt");
                let symlink = dir.join("symlink.txt");
                let mknod_file = dir.join("mknod.txt");
                let renamed = dir.join("renamed.txt");
                let xattr_name = b"user.studiofs.test";
                std::fs::create_dir(&dir)?;
                mknod_regular_file(&mknod_file)?;
                assert!(mknod_file.is_file());
                std::fs::remove_file(&mknod_file)?;
                std::fs::write(&file, b"namespace")?;
                std::fs::rename(&file, &renamed)?;
                std::fs::hard_link(&renamed, &hardlink)?;
                assert_eq!(std::fs::read_to_string(&hardlink)?, "namespace");
                assert!(std::fs::metadata(&renamed)?.nlink() >= 2);
                std::os::unix::fs::symlink("renamed.txt", &symlink)?;
                assert_eq!(
                    std::fs::read_link(&symlink)?,
                    std::path::PathBuf::from("renamed.txt")
                );
                set_xattr(&renamed, xattr_name, b"value")?;
                assert_eq!(get_xattr(&renamed, xattr_name)?, b"value");
                assert!(list_xattrs(&renamed)?.iter().any(|name| name == xattr_name));
                remove_xattr(&renamed, xattr_name)?;
                assert_eq!(
                    get_xattr(&renamed, xattr_name)
                        .expect_err("removed xattr should be missing")
                        .raw_os_error(),
                    Some(libc::ENODATA)
                );
                let mut names = std::fs::read_dir(&dir)?
                    .map(|entry| entry.map(|entry| entry.file_name()))
                    .collect::<std::io::Result<Vec<_>>>()?;
                names.sort();
                assert_eq!(
                    names,
                    vec![
                        std::ffi::OsString::from("hardlink.txt"),
                        std::ffi::OsString::from("renamed.txt"),
                        std::ffi::OsString::from("symlink.txt")
                    ]
                );
                std::fs::remove_file(&symlink)?;
                std::fs::remove_file(&hardlink)?;
                std::fs::remove_file(&renamed)?;
                std::fs::remove_dir(&dir)?;
                Ok(())
            })();
            result.expect("complete mounted namespace operations");
        });
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_kernel_cache_coherence_matrix() {
        assert_eq!(
            super::linux_fuse_cache_policy_from_env(),
            super::LinuxFuseCachePolicy::Kernel,
            "Kernel coherence matrix requires STUDIOFS_LINUX_FUSE_CACHE_POLICY=kernel"
        );
        let fixture = ContractVfsFixture::new("linux-fuse-kernel-coherence")
            .expect("create Kernel coherence fixture");
        fixture.start().expect("start Kernel coherence fixture");
        let vfs = fixture.vfs();
        let runtime_handle = fixture.runtime().handle().clone();

        run_mounted_vfs_test(
            "kernel-coherence",
            Arc::clone(&vfs),
            runtime_handle,
            move |mount_point, vfs| {
                std::fs::metadata(mount_point).expect("probe mounted root");
                let negotiation = wait_for_snapshot(vfs, Duration::from_secs(5), |snapshot| {
                    snapshot
                        .fuse_connection
                        .negotiated_writeback_cache
                        .is_some()
                        && snapshot.fuse_connection.negotiated_async_dio.is_some()
                })
                .unwrap_or_else(|error| panic!("FUSE init did not publish negotiation: {error}"));
                assert_eq!(
                    negotiation.fuse_connection.requested_writeback_cache,
                    Some(true)
                );
                assert_eq!(
                    negotiation.fuse_connection.negotiated_async_dio,
                    Some(false)
                );

                let basic = mount_point.join("write-fsync-reopen.bin");
                let basic_before = vfs.write_path_observability_snapshot();
                let mut file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(&basic)
                    .expect("create basic file");
                file.write_all(b"write-fsync-close-reopen")
                    .expect("write basic file");
                file.sync_all().expect("fsync basic file");
                drop(file);
                assert_eq!(
                    std::fs::read(&basic).expect("reopen basic file"),
                    b"write-fsync-close-reopen"
                );
                let basic_after = wait_for_local_mounted_idle(vfs);
                assert!(
                    basic_after.fuse_callbacks.write.calls
                        > basic_before.fuse_callbacks.write.calls
                );
                assert!(
                    basic_after.fuse_callbacks.fsync.calls
                        > basic_before.fuse_callbacks.fsync.calls
                );
                assert!(
                    basic_after.fuse_callbacks.release.calls
                        > basic_before.fuse_callbacks.release.calls
                );

                let append = mount_point.join("append.bin");
                std::fs::write(&append, b"prefix").expect("seed append file");
                let append_before = wait_for_local_mounted_idle(vfs);
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&append)
                    .expect("open append file");
                file.write_all(b"-suffix").expect("append bytes");
                file.sync_all().expect("fsync append file");
                drop(file);
                assert_eq!(
                    std::fs::read(&append).expect("read appended file"),
                    b"prefix-suffix"
                );
                let append_after = wait_for_local_mounted_idle(vfs);
                assert!(
                    append_after.fuse_callbacks.write.calls
                        > append_before.fuse_callbacks.write.calls
                );

                let truncate = mount_point.join("truncate.bin");
                std::fs::write(&truncate, b"abcdefgh").expect("seed truncate file");
                let truncate_before = wait_for_local_mounted_idle(vfs);
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&truncate)
                    .expect("open truncate file");
                file.set_len(3).expect("truncate smaller");
                file.sync_all().expect("fsync smaller truncate");
                drop(file);
                assert_eq!(std::fs::read(&truncate).expect("read smaller file"), b"abc");
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&truncate)
                    .expect("reopen truncate file");
                file.set_len(8).expect("truncate larger");
                file.sync_all().expect("fsync larger truncate");
                drop(file);
                assert_eq!(
                    std::fs::read(&truncate).expect("read extended file"),
                    b"abc\0\0\0\0\0"
                );
                let truncate_after = wait_for_local_mounted_idle(vfs);
                assert!(
                    truncate_after.fuse_callbacks.fsync.calls
                        >= truncate_before.fuse_callbacks.fsync.calls + 2
                );

                let overwrite = mount_point.join("same-size-overwrite.bin");
                std::fs::write(&overwrite, b"version-one").expect("seed overwrite file");
                let overwrite_before = wait_for_local_mounted_idle(vfs);
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&overwrite)
                    .expect("open overwrite file");
                file.write_all(b"version-two").expect("overwrite same size");
                file.sync_all().expect("fsync overwrite");
                drop(file);
                assert_eq!(
                    std::fs::read(&overwrite).expect("read overwrite file"),
                    b"version-two"
                );
                let overwrite_after = wait_for_local_mounted_idle(vfs);
                assert!(
                    overwrite_after.fuse_callbacks.write.calls
                        > overwrite_before.fuse_callbacks.write.calls
                );

                let sparse = mount_point.join("sparse.bin");
                let sparse_before = vfs.write_path_observability_snapshot();
                let mut file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&sparse)
                    .expect("create sparse file");
                file.seek(SeekFrom::Start(4096)).expect("seek sparse hole");
                file.write_all(b"tail").expect("write sparse tail");
                file.sync_all().expect("fsync sparse file");
                drop(file);
                let mut expected_sparse = vec![0; 4096];
                expected_sparse.extend_from_slice(b"tail");
                assert_eq!(
                    std::fs::read(&sparse).expect("read sparse file"),
                    expected_sparse
                );
                let sparse_after = wait_for_local_mounted_idle(vfs);
                assert!(
                    sparse_after.fuse_callbacks.write.calls
                        > sparse_before.fuse_callbacks.write.calls
                );

                let open_reader = mount_point.join("reader-open.bin");
                std::fs::write(&open_reader, b"old-content").expect("seed open-reader file");
                wait_for_local_mounted_idle(vfs);
                let mut reader = std::fs::File::open(&open_reader).expect("open reader");
                let reader_before = vfs.write_path_observability_snapshot();
                let mut writer = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&open_reader)
                    .expect("open concurrent writer");
                writer
                    .write_all(b"new-content")
                    .expect("write concurrent content");
                writer.sync_all().expect("fsync concurrent writer");
                drop(writer);
                reader.seek(SeekFrom::Start(0)).expect("rewind open reader");
                let mut actual = Vec::new();
                reader
                    .read_to_end(&mut actual)
                    .expect("read through open reader");
                drop(reader);
                assert_eq!(actual, b"new-content");
                let reader_after = wait_for_local_mounted_idle(vfs);
                assert!(
                    reader_after.os_cache.replace > reader_before.os_cache.replace,
                    "active writer must force a reader cache replacement"
                );

                let rename_from = mount_point.join("rename-open-from.bin");
                let rename_to = mount_point.join("rename-open-to.bin");
                std::fs::write(&rename_from, b"rename-visible").expect("seed rename file");
                wait_for_local_mounted_idle(vfs);
                let mut open = std::fs::File::open(&rename_from).expect("open rename source");
                let rename_before = vfs.write_path_observability_snapshot();
                std::fs::rename(&rename_from, &rename_to).expect("rename while open");
                let mut actual = Vec::new();
                open.read_to_end(&mut actual)
                    .expect("read renamed open file");
                drop(open);
                assert_eq!(actual, b"rename-visible");
                assert_eq!(
                    std::fs::read(&rename_to).expect("reopen rename target"),
                    b"rename-visible"
                );
                assert_eq!(
                    std::fs::metadata(&rename_from)
                        .expect_err("rename source should be absent")
                        .kind(),
                    std::io::ErrorKind::NotFound
                );
                let rename_after = wait_for_local_mounted_idle(vfs);
                assert!(
                    rename_after.fuse_callbacks.release.calls
                        > rename_before.fuse_callbacks.release.calls
                );

                let unlink = mount_point.join("unlink-open.bin");
                std::fs::write(&unlink, b"unlink-visible").expect("seed unlink file");
                wait_for_local_mounted_idle(vfs);
                let mut open = std::fs::File::open(&unlink).expect("open unlink file");
                let unlink_before = vfs.write_path_observability_snapshot();
                std::fs::remove_file(&unlink).expect("unlink while open");
                let mut actual = Vec::new();
                open.read_to_end(&mut actual)
                    .expect("read unlinked open file");
                drop(open);
                assert_eq!(actual, b"unlink-visible");
                assert_eq!(
                    std::fs::metadata(&unlink)
                        .expect_err("unlinked path should be absent")
                        .kind(),
                    std::io::ErrorKind::NotFound
                );
                let unlink_after = wait_for_local_mounted_idle(vfs);
                assert!(
                    unlink_after.fuse_callbacks.release.calls
                        > unlink_before.fuse_callbacks.release.calls
                );

                let hard_original = mount_point.join("hardlink-original.bin");
                let hard_link = mount_point.join("hardlink-peer.bin");
                std::fs::write(&hard_original, b"hardlink-old").expect("seed hardlink file");
                let hard_before = wait_for_local_mounted_idle(vfs);
                std::fs::hard_link(&hard_original, &hard_link).expect("create hardlink");
                wait_for_local_mounted_idle(vfs);
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&hard_link)
                    .expect("open hardlink writer");
                file.write_all(b"hardlink-new").expect("update hardlink");
                file.sync_all().expect("fsync hardlink update");
                drop(file);
                assert_eq!(
                    std::fs::read(&hard_original).expect("read original hardlink"),
                    b"hardlink-new"
                );
                std::fs::remove_file(&hard_original).expect("remove original hardlink");
                assert_eq!(
                    std::fs::read(&hard_link).expect("read surviving hardlink"),
                    b"hardlink-new"
                );
                std::fs::remove_file(&hard_link).expect("retire final hardlink");
                assert_eq!(
                    std::fs::metadata(&hard_link)
                        .expect_err("final hardlink should be absent")
                        .kind(),
                    std::io::ErrorKind::NotFound
                );
                let hard_after = wait_for_local_mounted_idle(vfs);
                assert!(
                    hard_after.fuse_liveness.observed_references
                        > hard_before.fuse_liveness.observed_references
                );

                let warm = mount_point.join("warm-cache.bin");
                let warm_payload: Vec<u8> = (0..256 * 1024)
                    .map(|index| u8::try_from(index % 251).expect("warm byte"))
                    .collect();
                std::fs::write(&warm, &warm_payload).expect("seed warm-cache file");
                wait_for_local_mounted_idle(vfs);
                let checksum = |bytes: &[u8]| {
                    bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
                    })
                };
                let expected_checksum = checksum(&warm_payload);
                let first_before = wait_for_read_quiescence(vfs);
                let first = std::fs::read(&warm).expect("first fresh warm read");
                assert_eq!(first, warm_payload);
                assert_eq!(checksum(&first), expected_checksum);
                let first_after = wait_for_read_quiescence(vfs);
                assert!(
                    first_after.fuse_callbacks.read.calls > first_before.fuse_callbacks.read.calls,
                    "first fresh warm read must prove a FUSE read callback"
                );
                assert!(
                    first_after.vfs_read_path.calls > first_before.vfs_read_path.calls,
                    "first fresh warm read must prove a VFS read"
                );
                assert_eq!(
                    first_after.os_cache.replace,
                    first_before.os_cache.replace + 1,
                    "first stable reopen must replace cached pages"
                );

                let second_before = wait_for_read_quiescence(vfs);
                let second = std::fs::read(&warm).expect("second fresh warm read");
                assert_eq!(second, warm_payload);
                assert_eq!(checksum(&second), expected_checksum);
                let second_after = wait_for_read_quiescence(vfs);
                assert_eq!(
                    second_after.os_cache.reuse,
                    second_before.os_cache.reuse + 1,
                    "second identical stable reopen must reuse cached pages"
                );
                assert_eq!(
                    second_after.fuse_callbacks.read.calls, second_before.fuse_callbacks.read.calls,
                    "second warm read must add zero FUSE read callbacks"
                );
                assert_eq!(
                    second_after.vfs_read_path.calls, second_before.vfs_read_path.calls,
                    "second warm read must add zero VFS reads"
                );
            },
        );

        drop(fixture);
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_kernel_writeback_flush_and_release_are_locally_durable() {
        assert_eq!(
            super::linux_fuse_cache_policy_from_env(),
            super::LinuxFuseCachePolicy::Kernel,
            "Kernel writeback test requires STUDIOFS_LINUX_FUSE_CACHE_POLICY=kernel"
        );
        let fixture = ContractVfsFixture::new("linux-fuse-kernel-writeback")
            .expect("create Kernel writeback fixture");
        fixture.start().expect("start Kernel writeback fixture");
        let callback_runtime = fixture.runtime().handle().clone();
        let operation_runtime = callback_runtime.clone();
        run_mounted_vfs_test(
            "kernel-writeback",
            fixture.vfs(),
            callback_runtime,
            move |mount_point, vfs| {
                std::fs::metadata(mount_point).expect("probe mounted root");
                let negotiation = wait_for_snapshot(vfs, Duration::from_secs(5), |snapshot| {
                    snapshot
                        .fuse_connection
                        .negotiated_writeback_cache
                        .is_some()
                })
                .expect("wait for negotiation");
                if negotiation.fuse_connection.negotiated_writeback_cache == Some(false) {
                    eprintln!(
                        "SKIP mounted_kernel_writeback_flush_and_release_are_locally_durable: \
                         kernel rejected FUSE writeback cache"
                    );
                    return;
                }
                assert_eq!(
                    negotiation.fuse_connection.negotiated_writeback_cache,
                    Some(true)
                );
                let path = mount_point.join("truncate-write.bin");
                std::fs::write(&path, b"truncate-old").expect("seed file");
                wait_for_local_mounted_idle(vfs);
                let baseline = vfs.write_path_observability_snapshot();
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&path)
                    .expect("open truncate writer");
                file.write_all(b"truncate-new").expect("write replacement");
                file.sync_all().expect("sync replacement");
                let ino = std::fs::metadata(&path).expect("stat replacement").ino();
                let local = operation_runtime
                    .block_on(vfs.read(ino, 0, 12))
                    .expect("read locally durable replacement");
                assert_eq!(local.as_ref(), b"truncate-new");
                drop(file);
                assert_eq!(
                    std::fs::read(&path).expect("reopen replacement after release"),
                    b"truncate-new"
                );
                let after = wait_for_local_mounted_idle(vfs);
                assert!(after.fuse_callbacks.write.calls > baseline.fuse_callbacks.write.calls);
                assert!(after.fuse_callbacks.fsync.calls > baseline.fuse_callbacks.fsync.calls);
                assert!(after.fuse_callbacks.flush.calls > baseline.fuse_callbacks.flush.calls);
                assert!(after.fuse_callbacks.release.calls > baseline.fuse_callbacks.release.calls);
                assert_eq!(
                    (
                        after.fuse_async_write.active_requests,
                        after.fuse_async_write.pending_writes,
                        after.fuse_async_write.pending_bytes,
                    ),
                    (0, 0, 0),
                    "Kernel writeback must not retain detached writes"
                );
            },
        );
    }

    #[test]
    #[ignore = "requires PostgreSQL, S3, /dev/fuse, and fusermount3"]
    fn mounted_kernel_remote_listener_invalidation() {
        let Some(environment) = remote_test_environment() else {
            return;
        };
        assert_eq!(
            super::linux_fuse_cache_policy_from_env(),
            super::LinuxFuseCachePolicy::Kernel,
            "remote invalidation test requires STUDIOFS_LINUX_FUSE_CACHE_POLICY=kernel"
        );

        let root = TargetTestDirectory::new("remote-listener");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .expect("build remote-test runtime");
        let volume = RemotePgVolume::new(&environment.pg_dsn, &runtime);
        let format_info = volume.format_info(&environment);
        let writer_db = root.path().join("writer.redb");
        let reader_db = root.path().join("reader.redb");
        seed_remote_local_metadata(&writer_db, &format_info);
        seed_remote_local_metadata(&reader_db, &format_info);

        let writer_meta = PgMetaEngine::open(&writer_db)
            .unwrap_or_else(|_| panic!("open configured writer PostgreSQL metadata"));
        let reader_meta = PgMetaEngine::open(&reader_db)
            .unwrap_or_else(|_| panic!("open configured reader PostgreSQL metadata"));
        let writer_meta_dyn: Arc<dyn MetaEngine> = writer_meta.clone();
        let reader_meta_dyn: Arc<dyn MetaEngine> = reader_meta.clone();
        let (writer_vfs, reader_vfs) = {
            let _runtime = runtime.enter();
            (
                make_remote_vfs(
                    writer_meta_dyn,
                    &format_info,
                    &root.path().join("writer-cache"),
                ),
                make_remote_vfs(
                    reader_meta_dyn,
                    &format_info,
                    &root.path().join("reader-cache"),
                ),
            )
        };
        let callback_runtime = runtime.handle().clone();
        let operation_runtime = callback_runtime.clone();
        let writer_for_test = Arc::clone(&writer_vfs);
        let writer_meta_for_test = writer_meta.clone();
        let reader_meta_for_test = reader_meta.clone();
        let publish_timeout = remote_publish_timeout();

        run_mounted_vfs_test(
            "remote-listener-mount",
            Arc::clone(&reader_vfs),
            callback_runtime,
            move |mount_point, reader_vfs| {
                std::fs::metadata(mount_point).expect("probe mounted remote reader root");
                wait_for_snapshot(reader_vfs, Duration::from_secs(5), |snapshot| {
                    snapshot
                        .fuse_connection
                        .negotiated_writeback_cache
                        .is_some()
                })
                .expect("wait for remote reader negotiation");
                wait_for_snapshot(reader_vfs, Duration::from_secs(15), |_| {
                    let diagnostics = reader_meta_for_test.metadata_invalidation_diagnostics();
                    diagnostics.last_successful_sync_unix_ms.is_some()
                        && diagnostics.sync_errors == 0
                })
                .unwrap_or_else(|_| {
                    panic!("configured PostgreSQL metadata listener did not become ready")
                });

                let v1: Vec<u8> = (0..512 * 1024)
                    .map(|index| u8::try_from(index % 251).expect("v1 byte"))
                    .collect();
                let v2: Vec<u8> = (0..512 * 1024)
                    .map(|index| u8::try_from((index + 97) % 251).expect("v2 byte"))
                    .collect();
                let remote_name = b"remote-listener.bin";
                let writer_attr = writer_for_test
                    .create(ROOT_INO, remote_name, 0, 0, 0o644)
                    .expect("create remote writer file");
                operation_runtime
                    .block_on(writer_for_test.write(writer_attr.ino, 0, &v1))
                    .expect("write remote v1");
                operation_runtime
                    .block_on(writer_for_test.flush(writer_attr.ino))
                    .expect("flush remote v1");
                wait_for_snapshot(&writer_for_test, publish_timeout, |snapshot| {
                    let replication = writer_meta_for_test.replication_backlog();
                    snapshot.dirty_pressure.total_dirty_bytes == 0
                        && snapshot.pending_uploads == 0
                        && snapshot.upload.active_uploads == 0
                        && replication.pending_ops == 0
                        && replication.inflight_ops == 0
                        && writer_meta_for_test
                            .get_attr(writer_attr.ino)
                            .is_ok_and(|attr| attr.upload_state == UploadState::Ready)
                })
                .unwrap_or_else(|_| panic!("configured S3 backend did not publish remote v1"));
                let writer_v1_ids = regular_file_payload_ids(&writer_for_test, writer_attr.ino);
                assert!(!writer_v1_ids.is_empty(), "remote v1 has no payload IDs");

                wait_for_snapshot(reader_vfs, Duration::from_secs(15), |_| {
                    reader_meta_for_test
                        .get_attr(writer_attr.ino)
                        .is_ok_and(|attr| {
                            attr.upload_state == UploadState::Ready && attr.size == v1.len() as u64
                        })
                        && regular_file_payload_ids(reader_vfs, writer_attr.ino) == writer_v1_ids
                })
                .unwrap_or_else(|_| {
                    panic!("production listener did not apply the remote v1 payload graph")
                });

                let mounted_path = mount_point.join("remote-listener.bin");
                wait_for_snapshot(reader_vfs, Duration::from_secs(15), |_| {
                    std::fs::metadata(&mounted_path).is_ok()
                })
                .unwrap_or_else(|_| panic!("remote v1 did not appear through the mounted reader"));
                let mounted_ino = std::fs::metadata(&mounted_path)
                    .expect("stat mounted remote v1")
                    .ino();
                assert_eq!(mounted_ino, writer_attr.ino);

                let checksum = |bytes: &[u8]| {
                    bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
                    })
                };
                let v1_checksum = checksum(&v1);
                let v1_first_before = wait_for_read_quiescence(reader_vfs);
                let v1_first = std::fs::read(&mounted_path).expect("read mounted remote v1");
                assert_eq!(v1_first, v1);
                assert_eq!(checksum(&v1_first), v1_checksum);
                let v1_first_after = wait_for_read_quiescence(reader_vfs);
                assert!(
                    v1_first_after.fuse_callbacks.read.calls
                        > v1_first_before.fuse_callbacks.read.calls
                );
                assert_eq!(
                    v1_first_after.os_cache.replace,
                    v1_first_before.os_cache.replace + 1
                );
                let reuse_deadline = Instant::now() + Duration::from_secs(10);
                let mut v1_reuse_before = v1_first_after;
                loop {
                    let reopened = std::fs::read(&mounted_path).expect("reopen mounted remote v1");
                    assert_eq!(reopened, v1);
                    assert_eq!(checksum(&reopened), v1_checksum);
                    let after = wait_for_read_quiescence(reader_vfs);
                    if after.os_cache.reuse > v1_reuse_before.os_cache.reuse {
                        assert_eq!(after.os_cache.reuse, v1_reuse_before.os_cache.reuse + 1);
                        break;
                    }
                    assert!(
                        Instant::now() < reuse_deadline,
                        "remote v1 did not reach stable cache reuse: before={:?}, after={:?}",
                        (
                            v1_reuse_before.os_cache.replace,
                            v1_reuse_before.os_cache.reuse
                        ),
                        (after.os_cache.replace, after.os_cache.reuse)
                    );
                    v1_reuse_before = after;
                }

                let reader_v1_ids = regular_file_payload_ids(reader_vfs, writer_attr.ino);
                assert_eq!(reader_v1_ids, writer_v1_ids);
                let mut stable_baseline = None;
                let mut stable_polls = 0;
                wait_for_snapshot(reader_vfs, Duration::from_secs(5), |snapshot| {
                    let diagnostics = reader_meta_for_test.metadata_invalidation_diagnostics();
                    let current = (
                        diagnostics.notifications_received,
                        diagnostics.fallback_poll_discoveries,
                        diagnostics.sync_errors,
                        snapshot.fuse_liveness.notifications_forwarded,
                        snapshot.fuse_liveness.notifications_suppressed,
                    );
                    if stable_baseline == Some(current) {
                        stable_polls += 1;
                    } else {
                        stable_baseline = Some(current);
                        stable_polls = 0;
                    }
                    stable_polls >= 200
                })
                .expect("settle remote v1 listener baseline");
                let listener_before = reader_meta_for_test.metadata_invalidation_diagnostics();
                let reader_metadata_token_before = reader_meta_for_test.metadata_version_token();
                let reader_namespace_token_before = reader_meta_for_test.namespace_version_token();
                let liveness_before = reader_vfs.write_path_observability_snapshot().fuse_liveness;

                operation_runtime
                    .block_on(writer_for_test.write(writer_attr.ino, 0, &v2))
                    .expect("write same-inode same-size remote v2");
                operation_runtime
                    .block_on(writer_for_test.flush(writer_attr.ino))
                    .expect("flush remote v2");
                wait_for_snapshot(&writer_for_test, publish_timeout, |snapshot| {
                    let replication = writer_meta_for_test.replication_backlog();
                    snapshot.dirty_pressure.total_dirty_bytes == 0
                        && snapshot.pending_uploads == 0
                        && snapshot.upload.active_uploads == 0
                        && replication.pending_ops == 0
                        && replication.inflight_ops == 0
                        && writer_meta_for_test
                            .get_attr(writer_attr.ino)
                            .is_ok_and(|attr| attr.upload_state == UploadState::Ready)
                })
                .unwrap_or_else(|_| panic!("configured S3 backend did not publish remote v2"));
                let writer_v2_ids = regular_file_payload_ids(&writer_for_test, writer_attr.ino);
                assert!(!writer_v2_ids.is_empty(), "remote v2 has no payload IDs");
                assert_ne!(writer_v2_ids, writer_v1_ids);

                wait_for_snapshot(reader_vfs, Duration::from_secs(15), |snapshot| {
                    let diagnostics = reader_meta_for_test.metadata_invalidation_diagnostics();
                    assert!(
                        diagnostics.fallback_poll_discoveries
                            <= listener_before.fallback_poll_discoveries,
                        "fallback poll, not production listener, discovered remote v2: \
                         notifications {}->{}, fallback {}->{}",
                        listener_before.notifications_received,
                        diagnostics.notifications_received,
                        listener_before.fallback_poll_discoveries,
                        diagnostics.fallback_poll_discoveries,
                    );
                    assert!(
                        diagnostics.sync_errors <= listener_before.sync_errors,
                        "remote v2 listener recorded a sync error"
                    );
                    assert!(
                        snapshot.fuse_liveness.notifications_forwarded
                            <= liveness_before.notifications_forwarded + 1,
                        "remote v2 forwarded more than one inode invalidation: forwarded {}->{}, \
                         suppressed {}->{}, notifications {}->{}, fallback {}->{}, sync_errors \
                         {}->{}, metadata_token_changed={}, namespace_token_changed={}",
                        liveness_before.notifications_forwarded,
                        snapshot.fuse_liveness.notifications_forwarded,
                        liveness_before.notifications_suppressed,
                        snapshot.fuse_liveness.notifications_suppressed,
                        listener_before.notifications_received,
                        diagnostics.notifications_received,
                        listener_before.fallback_poll_discoveries,
                        diagnostics.fallback_poll_discoveries,
                        listener_before.sync_errors,
                        diagnostics.sync_errors,
                        reader_metadata_token_before
                            != reader_meta_for_test.metadata_version_token(),
                        reader_namespace_token_before
                            != reader_meta_for_test.namespace_version_token(),
                    );
                    assert!(
                        snapshot.fuse_liveness.notifications_suppressed
                            <= liveness_before.notifications_suppressed,
                        "remote v2 unexpectedly suppressed an inode invalidation"
                    );
                    diagnostics.notifications_received > listener_before.notifications_received
                        && diagnostics.fallback_poll_discoveries
                            == listener_before.fallback_poll_discoveries
                        && diagnostics.sync_errors == listener_before.sync_errors
                        && regular_file_payload_ids(reader_vfs, writer_attr.ino) == writer_v2_ids
                        && snapshot.fuse_liveness.notifications_forwarded
                            == liveness_before.notifications_forwarded + 1
                        && snapshot.fuse_liveness.notifications_suppressed
                            == liveness_before.notifications_suppressed
                })
                .unwrap_or_else(|_| {
                    panic!("production listener did not deliver exact remote v2 invalidation")
                });
                let reader_v2_ids = regular_file_payload_ids(reader_vfs, writer_attr.ino);
                assert_eq!(reader_v2_ids, writer_v2_ids);
                assert_ne!(reader_v2_ids, reader_v1_ids);

                let v2_checksum = checksum(&v2);
                let v2_first_before = wait_for_read_quiescence(reader_vfs);
                let v2_first = std::fs::read(&mounted_path).expect("read invalidated remote v2");
                assert_eq!(v2_first, v2);
                assert_eq!(checksum(&v2_first), v2_checksum);
                let v2_first_after = wait_for_read_quiescence(reader_vfs);
                assert!(
                    v2_first_after.fuse_callbacks.read.calls
                        > v2_first_before.fuse_callbacks.read.calls,
                    "invalidated v2 read must cross the FUSE/VFS boundary"
                );
                assert_eq!(
                    v2_first_after.os_cache.replace,
                    v2_first_before.os_cache.replace + 1,
                    "stale remote generation must replace cached pages"
                );
                let v2_second_before = wait_for_read_quiescence(reader_vfs);
                let v2_second = std::fs::read(&mounted_path).expect("reopen stable remote v2");
                assert_eq!(v2_second, v2);
                assert_eq!(checksum(&v2_second), v2_checksum);
                let v2_second_after = wait_for_read_quiescence(reader_vfs);
                assert_eq!(
                    v2_second_after.os_cache.reuse,
                    v2_second_before.os_cache.reuse + 1,
                    "stable remote v2 must be reusable after replacement"
                );
            },
        );

        runtime.block_on(async {
            writer_vfs.shutdown(Duration::from_secs(15)).await;
            reader_vfs.shutdown(Duration::from_secs(15)).await;
        });
        drop(writer_vfs);
        drop(reader_vfs);
        drop(writer_meta);
        drop(reader_meta);
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_cross_file_fsync_and_close_do_not_share_async_watermark() {
        let cache_policy = super::linux_fuse_cache_policy_from_env();
        let async_dio_override = std::env::var("STUDIOFS_FUSE_ASYNC_DIO").ok();
        assert_eq!(
            cache_policy,
            super::LinuxFuseCachePolicy::Direct,
            "cross-file watermark test requires Direct cache policy"
        );
        assert!(
            super::linux_fuse_async_dio_enabled_for(cache_policy, async_dio_override.as_deref()),
            "cross-file watermark test requires async DIO"
        );
        let fixture = ContractVfsFixture::new("linux-fuse-cross-file-watermark")
            .expect("create cross-file fixture");
        fixture.start().expect("start cross-file fixture");
        run_mounted_vfs_test(
            "cross-file-watermark",
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            move |mount_point, vfs| {
                std::fs::metadata(mount_point).expect("probe mounted root");
                let negotiation = wait_for_snapshot(vfs, Duration::from_secs(5), |snapshot| {
                    snapshot.fuse_connection.negotiated_async_dio.is_some()
                })
                .expect("wait for Direct async negotiation");
                assert_eq!(
                    negotiation.fuse_connection.negotiated_async_dio,
                    Some(true),
                    "Direct async profile requires negotiated async DIO"
                );

                let first_path = mount_point.join("fsync-watermark.bin");
                let second_path = mount_point.join("close-watermark.bin");
                let first_payload = vec![0x31; 256 * 1024];
                let second_payload = vec![0x72; 256 * 1024];
                let before = vfs.write_path_observability_snapshot();
                let first_file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&first_path)
                    .expect("create fsync-watermark file");
                let second_file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&second_path)
                    .expect("create close-watermark file");
                let start = Arc::new(std::sync::Barrier::new(3));
                let (completed_tx, completed_rx) = mpsc::channel();

                let first_start = Arc::clone(&start);
                let first_completed = completed_tx.clone();
                let first_expected = first_payload.clone();
                let first = thread::spawn(move || {
                    let result = (|| -> std::io::Result<()> {
                        let mut file = first_file;
                        first_start.wait();
                        file.write_all(&first_expected)?;
                        file.sync_all()
                    })();
                    let _ = first_completed.send(("fsync", result));
                });

                let second_start = Arc::clone(&start);
                let second_completed = completed_tx;
                let second_expected = second_payload.clone();
                let second = thread::spawn(move || {
                    let result = (|| -> std::io::Result<()> {
                        let mut file = second_file;
                        second_start.wait();
                        file.write_all(&second_expected)?;
                        drop(file);
                        Ok(())
                    })();
                    let _ = second_completed.send(("close", result));
                });

                start.wait();
                for _ in 0..2 {
                    let (label, result) = completed_rx
                        .recv_timeout(Duration::from_secs(10))
                        .expect("cross-file fsync/close must make bounded progress");
                    result.unwrap_or_else(|error| panic!("cross-file {label} failed: {error}"));
                }
                first.join().expect("join fsync worker");
                second.join().expect("join close worker");

                assert_eq!(
                    std::fs::read(mount_point.join("fsync-watermark.bin"))
                        .expect("read fsync file"),
                    first_payload
                );
                assert_eq!(
                    std::fs::read(mount_point.join("close-watermark.bin"))
                        .expect("read close file"),
                    second_payload
                );
                let after = wait_for_local_mounted_idle(vfs);
                assert!(after.fuse_callbacks.fsync.calls > before.fuse_callbacks.fsync.calls);
                assert!(
                    after.fuse_callbacks.release.calls >= before.fuse_callbacks.release.calls + 2
                );
                assert!(
                    after.fuse_async_write.completed_writes
                        >= before.fuse_async_write.completed_writes + 2
                );
                assert_eq!(
                    (
                        after.fuse_async_write.active_requests,
                        after.fuse_async_write.pending_handles,
                        after.fuse_async_write.pending_writes,
                        after.fuse_async_write.pending_bytes,
                    ),
                    (0, 0, 0, 0),
                    "cross-file barriers retained async watermark state"
                );
            },
        );
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_direct_rollback_preserves_reopen_visibility() {
        let cache_policy = super::linux_fuse_cache_policy_from_env();
        let async_dio_override = std::env::var("STUDIOFS_FUSE_ASYNC_DIO").ok();
        assert_eq!(
            cache_policy,
            super::LinuxFuseCachePolicy::Direct,
            "Direct rollback test requires Direct cache policy"
        );
        assert!(
            !super::linux_fuse_async_dio_enabled_for(cache_policy, async_dio_override.as_deref()),
            "Direct rollback test requires STUDIOFS_FUSE_ASYNC_DIO=0"
        );
        let fixture = ContractVfsFixture::new("linux-fuse-direct-rollback")
            .expect("create Direct rollback fixture");
        fixture.start().expect("start Direct rollback fixture");
        run_mounted_vfs_test(
            "direct-rollback",
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            move |mount_point, vfs| {
                std::fs::metadata(mount_point).expect("probe mounted root");
                let negotiation = wait_for_snapshot(vfs, Duration::from_secs(5), |snapshot| {
                    snapshot.fuse_connection.negotiated_async_dio.is_some()
                })
                .expect("wait for Direct rollback negotiation");
                assert_eq!(
                    negotiation.fuse_connection.negotiated_async_dio,
                    Some(false),
                    "Direct rollback requires negotiated async DIO false"
                );

                let path = mount_point.join("rollback-visible.bin");
                let payload: Vec<u8> = (0..128 * 1024)
                    .map(|index| u8::try_from(index % 239).expect("rollback byte"))
                    .collect();
                let before = vfs.write_path_observability_snapshot();
                let mut file = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&path)
                    .expect("create rollback file");
                file.write_all(&payload).expect("write rollback payload");
                file.sync_all().expect("fsync rollback payload");
                drop(file);
                assert_eq!(std::fs::read(&path).expect("reopen rollback file"), payload);
                let after = wait_for_local_mounted_idle(vfs);
                assert!(after.fuse_callbacks.write.calls > before.fuse_callbacks.write.calls);
                assert!(after.fuse_callbacks.fsync.calls > before.fuse_callbacks.fsync.calls);
                assert!(after.fuse_callbacks.release.calls > before.fuse_callbacks.release.calls);
            },
        );
    }
}
