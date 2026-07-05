//! Clean Linux bridge shell for StudioFS.
//!
//! This crate owns the Linux-only `/dev/fuse` boundary and enough shell
//! operations to prove the new path is isolated from the deleted legacy backend.

use std::borrow::Cow;
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{
    Receiver as WorkReceiver, Sender as WorkSender, TrySendError as WorkTrySendError,
    bounded as work_channel, select,
};
use studiofs_bridge::posix::{BridgeFileAttr, to_bridge_attr, vfs_err_to_errno};
use studiofs_meta::ROOT_INO;
use studiofs_vfs::{LocalSyncPolicy, StudioVfs};
use tokio::runtime::Handle;

const DEFAULT_BLOCK_SIZE: u32 = 4096;
const FUSE_DEVICE: &str = "/dev/fuse";
const FUSE_OPEN_FLAGS: libc::c_int = libc::O_CLOEXEC | libc::O_NONBLOCK;
const FUSE_MOUNT_TYPE: &str = "fuse";
const FUSE_SOURCE: &str = "studiofs";
const FUSE_IN_HEADER_SIZE: usize = 40;
const FUSE_FH_SIZE: usize = 8;
const FUSE_OUT_HEADER_SIZE: usize = 16;
const FUSE_INIT_IN_BASE_SIZE: usize = 16;
const FUSE_READ_IN_SIZE: usize = 40;
const FUSE_WRITE_IN_SIZE: usize = 40;
const FUSE_WRITE_OUT_SIZE: usize = 8;
const FUSE_FLUSH_IN_SIZE: usize = 24;
const FUSE_RELEASE_IN_SIZE: usize = 24;
const FUSE_FSYNC_IN_SIZE: usize = 16;
const FUSE_COMPAT_INIT_OUT_SIZE: usize = 8;
const FUSE_COMPAT_22_INIT_OUT_SIZE: usize = 24;
const FUSE_INIT_OUT_SIZE: usize = 64;
const FUSE_FORGET: u32 = 2;
const FUSE_INIT: u32 = 26;
const FUSE_READ: u32 = 15;
const FUSE_WRITE: u32 = 16;
const FUSE_RELEASE: u32 = 18;
const FUSE_FSYNC: u32 = 20;
const FUSE_FLUSH: u32 = 25;
const FUSE_READDIR: u32 = 28;
const FUSE_DESTROY: u32 = 38;
const FUSE_BATCH_FORGET: u32 = 42;
const FUSE_READDIRPLUS: u32 = 44;
const FUSE_KERNEL_VERSION: u32 = 7;
const FUSE_KERNEL_MINOR_VERSION: u32 = 31;
const FUSE_ASYNC_READ: u32 = 1 << 0;
const FUSE_BIG_WRITES: u32 = 1 << 5;
const FUSE_AUTO_INVAL_DATA: u32 = 1 << 12;
const FUSE_PARALLEL_DIROPS: u32 = 1 << 18;
const FUSE_MAX_PAGES: u32 = 1 << 22;
const STUDIOFS_REQUESTED_CAPS: u32 = FUSE_ASYNC_READ
    | FUSE_BIG_WRITES
    | FUSE_AUTO_INVAL_DATA
    | FUSE_PARALLEL_DIROPS
    | FUSE_MAX_PAGES;
const FUSE_COMPAT_MAX_WRITE: u32 = 128 * 1024;
const FUSE_MAX_PAGES_DEFAULT: u16 = 256;
const FUSE_REQUEST_BUFFER_SIZE: usize = 1024 * 1024 + 4096;
const FUSE_POLL_TIMEOUT: Duration = Duration::from_secs(30);
const FUSE_RECEIVE_SHUTDOWN_POLL: Duration = Duration::from_millis(100);
const FUSE_WORK_QUEUE_DEPTH: usize = 128;
const FUSE_REPLY_QUEUE_DEPTH: usize = 1024;
const FUSE_METADATA_WORKERS: usize = 4;
const FUSE_STREAM_WORKERS: usize = 8;
const FUSE_WRITE_REPLY_SIZE: usize = FUSE_OUT_HEADER_SIZE + FUSE_WRITE_OUT_SIZE;

/// Linux bridge launch request owned by the CLI boundary.
pub struct LinuxMountRequest {
    /// Shared VFS instance used by the Linux bridge boundary.
    pub vfs: Arc<StudioVfs>,
    /// Runtime used to execute async VFS operations from Linux FUSE worker threads.
    pub runtime_handle: Handle,
    /// Directory where the Linux FUSE filesystem should be mounted.
    pub mount_point: PathBuf,
}

/// Minimal Linux mount shell for lifecycle and root namespace smoke checks.
pub struct LinuxMountShell {
    request: LinuxMountRequest,
}

/// Directory entry shape returned by the Linux shell boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxDirEntry {
    /// StudioFS inode number.
    pub ino: u64,
    /// Raw POSIX entry name bytes.
    pub name: Vec<u8>,
}

/// Snapshot of Linux FUSE loop queue and error counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LinuxFuseLoopStats {
    /// Requests waiting for operation work.
    pub work_queue_depth: usize,
    /// Replies waiting for the dedicated FUSE writer.
    pub reply_queue_depth: usize,
    /// Times the receive loop hit a full work queue.
    pub work_backpressure_count: usize,
    /// Times operation work hit a full reply queue.
    pub reply_backpressure_count: usize,
    /// Receive path errors.
    pub receive_error_count: usize,
    /// Operation path errors.
    pub work_error_count: usize,
    /// Reply writer errors.
    pub reply_error_count: usize,
}

/// Shared counters for the isolated Linux FUSE loops.
#[derive(Default)]
pub struct LinuxFuseLoopMetrics {
    work_queue_depth: AtomicUsize,
    reply_queue_depth: AtomicUsize,
    work_backpressure_count: AtomicUsize,
    reply_backpressure_count: AtomicUsize,
    receive_error_count: AtomicUsize,
    work_error_count: AtomicUsize,
    reply_error_count: AtomicUsize,
}

impl LinuxFuseLoopMetrics {
    /// Return a point-in-time snapshot of queue depth and error counters.
    pub fn snapshot(&self) -> LinuxFuseLoopStats {
        LinuxFuseLoopStats {
            work_queue_depth: self.work_queue_depth.load(Ordering::Relaxed),
            reply_queue_depth: self.reply_queue_depth.load(Ordering::Relaxed),
            work_backpressure_count: self.work_backpressure_count.load(Ordering::Relaxed),
            reply_backpressure_count: self.reply_backpressure_count.load(Ordering::Relaxed),
            receive_error_count: self.receive_error_count.load(Ordering::Relaxed),
            work_error_count: self.work_error_count.load(Ordering::Relaxed),
            reply_error_count: self.reply_error_count.load(Ordering::Relaxed),
        }
    }

    fn work_enqueued(&self) {
        self.work_queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    fn work_dequeued(&self) {
        decrement_counter(&self.work_queue_depth);
    }

    fn reply_enqueued(&self) {
        self.reply_queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    fn reply_dequeued(&self) {
        decrement_counter(&self.reply_queue_depth);
    }
}

fn decrement_counter(counter: &AtomicUsize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_sub(1)
    });
}

/// Mount `/dev/fuse`, negotiate the kernel session, then hold the single-fd shell.
pub fn mount_with_request(request: LinuxMountRequest) -> io::Result<()> {
    mount_with_request_and_metrics(request, Arc::new(LinuxFuseLoopMetrics::default())).map(|_| ())
}

/// Mount `/dev/fuse` with externally observable loop metrics.
pub fn mount_with_request_and_metrics(
    request: LinuxMountRequest,
    metrics: Arc<LinuxFuseLoopMetrics>,
) -> io::Result<LinuxFuseLoopStats> {
    let mut device = open_fuse_device()?;
    let bdi_mount_point = mountinfo_lookup_mount_point(&request.mount_point);
    mount_fuse_device(&request.mount_point, device.as_raw_fd())?;
    let mut buffer = vec![0_u8; FUSE_REQUEST_BUFFER_SIZE];
    let negotiated = read_and_reply_init(&mut device, &mut buffer)?;
    apply_read_ahead_kb_best_effort(&bdi_mount_point, negotiated.out.max_readahead / 1024);
    let reply_device = device.try_clone()?;
    run_fuse_shell(
        device,
        reply_device,
        buffer,
        request.vfs,
        request.runtime_handle,
        metrics,
    )
}

impl LinuxMountShell {
    /// Create an uninitialized Linux shell from a mount request.
    pub fn new(request: LinuxMountRequest) -> Self {
        Self { request }
    }

    /// Accept the Linux init lifecycle callback.
    pub fn init(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Return bridge attributes for an inode.
    pub fn getattr(&self, ino: u64) -> io::Result<BridgeFileAttr> {
        self.request
            .vfs
            .getattr(ino)
            .map(|attr| to_bridge_attr(&attr, 0, 0, DEFAULT_BLOCK_SIZE))
            .map_err(io_error_from_vfs)
    }

    /// Resolve a child name under a parent inode.
    pub fn lookup(&self, parent: u64, name: &[u8]) -> io::Result<BridgeFileAttr> {
        if parent == ROOT_INO && matches!(name, b"." | b"") {
            return self.getattr(ROOT_INO);
        }
        self.request
            .vfs
            .lookup(parent, name)
            .map(|attr| to_bridge_attr(&attr, 0, 0, DEFAULT_BLOCK_SIZE))
            .map_err(io_error_from_vfs)
    }

    /// Read directory entries for an inode.
    pub fn readdir(&self, ino: u64) -> io::Result<Vec<LinuxDirEntry>> {
        self.request
            .vfs
            .readdir(ino)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| LinuxDirEntry {
                        ino: entry.ino,
                        name: entry.name,
                    })
                    .collect()
            })
            .map_err(io_error_from_vfs)
    }

    /// Accept the Linux destroy lifecycle callback.
    pub fn destroy(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Return `ENOSYS` for shell operations not implemented yet.
    pub fn unsupported() -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }
}

fn io_error_from_vfs(error: studiofs_vfs::VfsError) -> io::Error {
    io::Error::from_raw_os_error(vfs_err_to_errno(&error))
}

fn open_fuse_device() -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(FUSE_OPEN_FLAGS)
        .open(FUSE_DEVICE)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("open {FUSE_DEVICE} failed: {error}; check FUSE device permissions"),
            )
        })
}

fn mount_fuse_device(mount_point: &Path, fd: RawFd) -> io::Result<()> {
    let mode = fuse_rootmode(mount_point)?;
    let options = format!(
        "fd={fd},rootmode={mode:o},user_id={},group_id={},default_permissions,fsname=studiofs,subtype=studiofs",
        current_uid(),
        current_gid()
    );
    let source = cstring(FUSE_SOURCE)?;
    let target = cstring_path(mount_point)?;
    let fstype = cstring(FUSE_MOUNT_TYPE)?;
    let data = cstring(&options)?;

    // SAFETY: all pointers are valid NUL-terminated C strings for the duration
    // of the call, and `data` points to the mount option string expected by the
    // Linux `mount(2)` FUSE filesystem type.
    let rc = unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            fstype.as_ptr(),
            (libc::MS_NODEV | libc::MS_NOSUID) as libc::c_ulong,
            data.as_ptr().cast(),
        )
    };
    if rc == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    let hint = match error.raw_os_error() {
        Some(libc::EACCES | libc::EPERM) => {
            "; FUSE mount requires host permission for /dev/fuse and mount(2)"
        }
        _ => "",
    };
    Err(io::Error::new(
        error.kind(),
        format!(
            "mount {FUSE_DEVICE} at {} failed: {error}{hint}",
            mount_point.display()
        ),
    ))
}

fn fuse_rootmode(mount_point: &Path) -> io::Result<u32> {
    let permissions = std::fs::metadata(mount_point)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "read mount point {} metadata failed: {error}",
                    mount_point.display()
                ),
            )
        })?
        .permissions()
        .mode()
        & 0o7777;
    Ok(libc::S_IFDIR | permissions)
}

fn current_uid() -> libc::uid_t {
    // SAFETY: getuid has no preconditions.
    unsafe { libc::getuid() }
}

fn current_gid() -> libc::gid_t {
    // SAFETY: getgid has no preconditions.
    unsafe { libc::getgid() }
}

fn cstring(value: &str) -> io::Result<CString> {
    CString::new(value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "string contains NUL byte"))
}

fn cstring_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path {} contains NUL byte", path.display()),
        )
    })
}

fn read_and_reply_init(device: &mut File, buffer: &mut [u8]) -> io::Result<NegotiatedInit> {
    let len = read_fuse_message(device, buffer)?;
    let header = parse_in_header(&buffer[..len])?;
    if header.opcode != FUSE_INIT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected FUSE_INIT as first request, got opcode {}",
                header.opcode
            ),
        ));
    }
    let init = parse_init_in(&buffer[FUSE_IN_HEADER_SIZE..len])?;
    let negotiated = negotiate_init(&init, page_size()?)?;
    write_init_reply(device, header.unique, &negotiated.out)?;
    tracing::info!(
        requested_caps = format_args!("0x{:08x}", negotiated.requested_caps),
        accepted_caps = format_args!("0x{:08x}", negotiated.accepted_caps),
        max_write = negotiated.out.max_write,
        max_pages = negotiated.out.max_pages,
        max_readahead = negotiated.out.max_readahead,
        "linux fuse init negotiated"
    );
    Ok(negotiated)
}

fn run_fuse_shell(
    device: File,
    reply_device: File,
    buffer: Vec<u8>,
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    metrics: Arc<LinuxFuseLoopMetrics>,
) -> io::Result<LinuxFuseLoopStats> {
    let (work_tx, work_rx) = work_channel(FUSE_WORK_QUEUE_DEPTH);
    let (reply_tx, reply_rx) = work_channel(FUSE_REPLY_QUEUE_DEPTH);
    let (done_tx, done_rx) = channel();
    let shutdown = Arc::new(AtomicBool::new(false));

    let receive_metrics = Arc::clone(&metrics);
    let receive_shutdown = Arc::clone(&shutdown);
    let receiver = spawn_loop("linux fuse receiver", done_tx.clone(), move || {
        receive_loop_until_shutdown(device, buffer, work_tx, &receive_metrics, &receive_shutdown)
    })?;

    let work_metrics = Arc::clone(&metrics);
    let worker = spawn_loop("linux fuse worker", done_tx.clone(), move || {
        work_loop(work_rx, reply_tx, vfs, runtime_handle, work_metrics)
    })?;

    let reply_metrics = Arc::clone(&metrics);
    let replier = spawn_loop("linux fuse replier", done_tx, move || {
        reply_loop(reply_device, reply_rx, &reply_metrics)
    })?;

    let mut first_error = None;
    let mut completed = 0;
    while completed < 3 {
        match done_rx.recv() {
            Ok(Ok(())) => completed += 1,
            Ok(Err(error)) => {
                shutdown.store(true, Ordering::Relaxed);
                first_error = Some(error);
                completed += 1;
                break;
            }
            Err(_) => {
                shutdown.store(true, Ordering::Relaxed);
                first_error = Some(io::Error::other("linux fuse loop status channel closed"));
                break;
            }
        }
    }
    while completed < 3 {
        let _ = done_rx.recv();
        completed += 1;
    }

    join_loop(receiver, "linux fuse receiver")?;
    join_loop(worker, "linux fuse worker")?;
    join_loop(replier, "linux fuse replier")?;
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(metrics.snapshot())
}

fn receive_loop_until_shutdown(
    mut device: impl Read + AsRawFd,
    mut buffer: impl AsMut<[u8]>,
    work_tx: WorkSender<FuseWorkItem>,
    metrics: &LinuxFuseLoopMetrics,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    loop {
        let Some(len) = read_fuse_message_until_shutdown(&mut device, buffer.as_mut(), shutdown)
            .inspect_err(|_| {
                metrics.receive_error_count.fetch_add(1, Ordering::Relaxed);
            })?
        else {
            return Ok(());
        };
        let message = buffer.as_mut()[..len].to_vec();
        let header = parse_in_header(&message).inspect_err(|_| {
            metrics.receive_error_count.fetch_add(1, Ordering::Relaxed);
        })?;
        // ponytail: per-request copy; swap in a buffer pool once real payload ops are profiled.
        enqueue_work(&work_tx, FuseWorkItem { header, message }, metrics).inspect_err(|_| {
            metrics.receive_error_count.fetch_add(1, Ordering::Relaxed);
        })?;
        if header.opcode == FUSE_DESTROY {
            return Ok(());
        }
    }
}

fn work_loop(
    work_rx: WorkReceiver<FuseWorkItem>,
    reply_tx: WorkSender<FuseReply>,
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    metrics: Arc<LinuxFuseLoopMetrics>,
) -> io::Result<()> {
    scheduled_work_loop(work_rx, reply_tx, metrics, move |item| {
        handle_vfs_fuse_request(&vfs, &runtime_handle, item)
    })
}

fn scheduled_work_loop<H>(
    work_rx: WorkReceiver<FuseWorkItem>,
    reply_tx: WorkSender<FuseReply>,
    metrics: Arc<LinuxFuseLoopMetrics>,
    handler: H,
) -> io::Result<()>
where
    H: Fn(&FuseWorkItem) -> Option<FuseReply> + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    let (metadata_tx, metadata_rx) = work_channel(FUSE_WORK_QUEUE_DEPTH);
    let mut stream_txs = Vec::with_capacity(FUSE_STREAM_WORKERS);
    let mut workers = Vec::with_capacity(FUSE_METADATA_WORKERS + FUSE_STREAM_WORKERS);
    let (fatal_tx, fatal_rx) = work_channel(FUSE_METADATA_WORKERS + FUSE_STREAM_WORKERS);

    for _ in 0..FUSE_METADATA_WORKERS {
        let worker_rx = metadata_rx.clone();
        let worker_reply_tx = reply_tx.clone();
        let worker_metrics = Arc::clone(&metrics);
        let worker_handler = Arc::clone(&handler);
        let worker_fatal_tx = fatal_tx.clone();
        workers.push(spawn_fuse_worker(
            "linux fuse metadata worker",
            worker_fatal_tx,
            move || {
                while let Ok(item) = worker_rx.recv() {
                    run_scheduled_item(
                        item,
                        &worker_reply_tx,
                        &worker_metrics,
                        worker_handler.as_ref(),
                    )?;
                }
                Ok(())
            },
        )?);
    }

    for _ in 0..FUSE_STREAM_WORKERS {
        let (stream_tx, stream_rx) = work_channel(FUSE_WORK_QUEUE_DEPTH);
        let worker_reply_tx = reply_tx.clone();
        let worker_metrics = Arc::clone(&metrics);
        let worker_handler = Arc::clone(&handler);
        let worker_fatal_tx = fatal_tx.clone();
        stream_txs.push(stream_tx);
        workers.push(spawn_fuse_worker(
            "linux fuse stream worker",
            worker_fatal_tx,
            move || {
                while let Ok(item) = stream_rx.recv() {
                    run_scheduled_item(
                        item,
                        &worker_reply_tx,
                        &worker_metrics,
                        worker_handler.as_ref(),
                    )?;
                }
                Ok(())
            },
        )?);
    }
    drop(fatal_tx);

    loop {
        let item = select! {
            recv(fatal_rx) -> fatal => {
                drop(metadata_tx);
                drop(stream_txs);
                let _ = join_fuse_workers(workers);
                return match fatal {
                    Ok(error) => Err(error),
                    Err(_) => Err(io::Error::other("linux fuse worker fatal channel closed")),
                };
            }
            recv(work_rx) -> item => match item {
                Ok(item) => item,
                Err(_) => break,
            },
        };
        metrics.work_dequeued();
        if item.header.opcode == FUSE_DESTROY {
            break;
        }
        let send_result = match stream_key_for(&item) {
            None => send_scheduled_work(&metadata_tx, &fatal_rx, item),
            Some(key) => send_scheduled_work(
                &stream_txs[stream_worker_index(key, stream_txs.len())],
                &fatal_rx,
                item,
            ),
        };
        if let Err(error) = send_result {
            drop(metadata_tx);
            drop(stream_txs);
            let _ = join_fuse_workers(workers);
            return Err(error);
        }
    }

    drop(metadata_tx);
    drop(stream_txs);
    join_fuse_workers(workers)
}

fn run_scheduled_item<H>(
    item: FuseWorkItem,
    reply_tx: &WorkSender<FuseReply>,
    metrics: &LinuxFuseLoopMetrics,
    handler: &H,
) -> io::Result<()>
where
    H: Fn(&FuseWorkItem) -> Option<FuseReply> + Send + Sync,
{
    if let Some(reply) = handler(&item) {
        enqueue_reply(reply_tx, reply, metrics).inspect_err(|_| {
            metrics.work_error_count.fetch_add(1, Ordering::Relaxed);
        })?;
    }
    Ok(())
}

fn reply_loop(
    mut device: File,
    reply_rx: WorkReceiver<FuseReply>,
    metrics: &LinuxFuseLoopMetrics,
) -> io::Result<()> {
    while let Ok(reply) = reply_rx.recv() {
        metrics.reply_dequeued();
        let slices = [IoSlice::new(reply.as_slice())];
        write_vectored_fuse(&mut device, &slices).inspect_err(|_| {
            metrics.reply_error_count.fetch_add(1, Ordering::Relaxed);
        })?;
    }
    Ok(())
}

enum FuseReply {
    Inline {
        bytes: [u8; FUSE_WRITE_REPLY_SIZE],
        len: usize,
    },
    Owned(Vec<u8>),
}

impl FuseReply {
    fn inline(bytes: [u8; FUSE_WRITE_REPLY_SIZE], len: usize) -> Self {
        Self::Inline { bytes, len }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { bytes, len } => &bytes[..*len],
            Self::Owned(bytes) => bytes.as_slice(),
        }
    }
}

fn spawn_loop<F>(
    name: &'static str,
    tx: Sender<io::Result<()>>,
    f: F,
) -> io::Result<thread::JoinHandle<()>>
where
    F: FnOnce() -> io::Result<()> + Send + 'static,
{
    thread::Builder::new().name(name.to_owned()).spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
            .unwrap_or_else(|panic| Err(io::Error::other(thread_panic_message(name, panic))));
        let _ = tx.send(result);
    })
}

fn join_loop(handle: thread::JoinHandle<()>, name: &str) -> io::Result<()> {
    match handle.join() {
        Ok(()) => Ok(()),
        Err(panic) => Err(io::Error::other(thread_panic_message(name, panic))),
    }
}

fn thread_panic_message(name: &str, panic: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(message) = panic.downcast_ref::<String>() {
        format!("{name} panicked: {message}")
    } else if let Some(message) = panic.downcast_ref::<&'static str>() {
        format!("{name} panicked: {message}")
    } else {
        format!("{name} panicked")
    }
}

fn enqueue_work(
    tx: &WorkSender<FuseWorkItem>,
    item: FuseWorkItem,
    metrics: &LinuxFuseLoopMetrics,
) -> io::Result<()> {
    metrics.work_enqueued();
    match tx.try_send(item) {
        Ok(()) => Ok(()),
        Err(WorkTrySendError::Full(item)) => {
            metrics
                .work_backpressure_count
                .fetch_add(1, Ordering::Relaxed);
            tx.send(item).map_err(|_| {
                metrics.work_dequeued();
                io::Error::new(io::ErrorKind::BrokenPipe, "FUSE work queue closed")
            })
        }
        Err(WorkTrySendError::Disconnected(_)) => {
            metrics.work_dequeued();
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "FUSE work queue closed",
            ))
        }
    }
}

fn enqueue_reply(
    tx: &WorkSender<FuseReply>,
    reply: FuseReply,
    metrics: &LinuxFuseLoopMetrics,
) -> io::Result<()> {
    metrics.reply_enqueued();
    match tx.try_send(reply) {
        Ok(()) => Ok(()),
        Err(WorkTrySendError::Full(reply)) => {
            metrics
                .reply_backpressure_count
                .fetch_add(1, Ordering::Relaxed);
            tx.send(reply).map_err(|_| {
                metrics.reply_dequeued();
                io::Error::new(io::ErrorKind::BrokenPipe, "FUSE reply queue closed")
            })
        }
        Err(WorkTrySendError::Disconnected(_)) => {
            metrics.reply_dequeued();
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "FUSE reply queue closed",
            ))
        }
    }
}

fn send_scheduled_work(
    tx: &WorkSender<FuseWorkItem>,
    fatal_rx: &WorkReceiver<io::Error>,
    item: FuseWorkItem,
) -> io::Result<()> {
    select! {
        send(tx, item) -> result => result.map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "FUSE scheduled work queue closed")
        }),
        recv(fatal_rx) -> fatal => match fatal {
            Ok(error) => Err(error),
            Err(_) => Err(io::Error::other("linux fuse worker fatal channel closed")),
        },
    }
}

fn spawn_fuse_worker<F>(
    name: &'static str,
    fatal_tx: WorkSender<io::Error>,
    f: F,
) -> io::Result<thread::JoinHandle<io::Result<()>>>
where
    F: FnOnce() -> io::Result<()> + Send + 'static,
{
    thread::Builder::new().name(name.to_owned()).spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
            .unwrap_or_else(|panic| Err(io::Error::other(thread_panic_message(name, panic))));
        if let Err(error) = &result {
            let _ = fatal_tx.send(io::Error::new(error.kind(), error.to_string()));
        }
        result
    })
}

fn join_fuse_workers(workers: Vec<thread::JoinHandle<io::Result<()>>>) -> io::Result<()> {
    let mut first_error = None;
    for worker in workers {
        if first_error.is_none() {
            first_error = match worker.join() {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(panic) => Some(io::Error::other(thread_panic_message(
                    "linux fuse worker",
                    panic,
                ))),
            };
        } else {
            let _ = worker.join();
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn stream_key_for(item: &FuseWorkItem) -> Option<(u64, Option<u64>)> {
    match item.header.opcode {
        FUSE_READ | FUSE_WRITE | FUSE_RELEASE | FUSE_FSYNC | FUSE_FLUSH | FUSE_READDIR
        | FUSE_READDIRPLUS => Some((item.header.nodeid, fuse_request_fh(item))),
        _ => None,
    }
}

fn stream_worker_index(key: (u64, Option<u64>), workers: usize) -> usize {
    (stream_worker_hash(key) as usize) % workers
}

fn stream_worker_hash(key: (u64, Option<u64>)) -> u64 {
    let mut hash = key.0.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    hash ^= match key.1 {
        Some(fh) => fh.rotate_left(32),
        None => 0x517c_c1b7_2722_0a95,
    };
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^ (hash >> 33)
}

fn fuse_request_fh(item: &FuseWorkItem) -> Option<u64> {
    item.message
        .get(FUSE_IN_HEADER_SIZE..FUSE_IN_HEADER_SIZE + FUSE_FH_SIZE)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_ne_bytes)
}

fn handle_vfs_fuse_request(
    vfs: &Arc<StudioVfs>,
    runtime_handle: &Handle,
    item: &FuseWorkItem,
) -> Option<FuseReply> {
    let header = item.header;
    if !fuse_opcode_expects_reply(header.opcode) {
        return None;
    }
    let started = Instant::now();
    Some(match header.opcode {
        FUSE_READ => handle_fuse_read(vfs, runtime_handle, item, started),
        FUSE_WRITE => handle_fuse_write(vfs, runtime_handle, item, started),
        FUSE_FLUSH => handle_fuse_flush(vfs, runtime_handle, item),
        FUSE_FSYNC => handle_fuse_fsync(vfs, runtime_handle, item),
        FUSE_RELEASE => handle_fuse_release(vfs, runtime_handle, item),
        _ => encode_error_reply(header.unique, libc::ENOSYS),
    })
}

fn handle_fuse_read(
    vfs: &Arc<StudioVfs>,
    runtime_handle: &Handle,
    item: &FuseWorkItem,
    started: Instant,
) -> FuseReply {
    let unique = item.header.unique;
    let input = match parse_read_in(fuse_payload(item)) {
        Ok(input) => input,
        Err(error) => return encode_error_reply(unique, io_error_errno(&error)),
    };
    let mut out = vec![0_u8; input.size as usize];
    let result = runtime_handle.block_on(vfs.read_into_fh(
        input.fh,
        item.header.nodeid,
        input.offset,
        &mut out,
    ));
    match result {
        Ok(read) => {
            out.truncate(read as usize);
            vfs.record_fuse_read_callback(out.len(), started.elapsed(), false);
            encode_data_reply(unique, &out)
        }
        Err(error) => {
            vfs.record_fuse_read_callback(input.size as usize, started.elapsed(), true);
            encode_error_reply(unique, vfs_err_to_errno(&error))
        }
    }
}

fn handle_fuse_write(
    vfs: &Arc<StudioVfs>,
    runtime_handle: &Handle,
    item: &FuseWorkItem,
    started: Instant,
) -> FuseReply {
    let unique = item.header.unique;
    let input = match parse_write_in(fuse_payload(item)) {
        Ok(input) => input,
        Err(error) => return encode_error_reply(unique, io_error_errno(&error)),
    };
    let result = runtime_handle.block_on(vfs.write_fh_with_deferred_materialize(
        input.fh,
        item.header.nodeid,
        input.offset,
        input.data,
        LocalSyncPolicy::Opportunistic,
    ));
    match result {
        Ok(written) => {
            vfs.record_fuse_write_callback(input.data.len(), started.elapsed(), false);
            encode_write_reply(unique, written)
        }
        Err(error) => {
            vfs.record_fuse_write_callback(input.data.len(), started.elapsed(), true);
            encode_error_reply(unique, vfs_err_to_errno(&error))
        }
    }
}

fn handle_fuse_flush(
    vfs: &Arc<StudioVfs>,
    runtime_handle: &Handle,
    item: &FuseWorkItem,
) -> FuseReply {
    let unique = item.header.unique;
    let input = match parse_flush_in(fuse_payload(item)) {
        Ok(input) => input,
        Err(error) => return encode_error_reply(unique, io_error_errno(&error)),
    };
    match runtime_handle.block_on(vfs.flush_fh(input)) {
        Ok(()) => encode_data_reply(unique, &[]),
        Err(error) => encode_error_reply(unique, vfs_err_to_errno(&error)),
    }
}

fn handle_fuse_fsync(
    vfs: &Arc<StudioVfs>,
    runtime_handle: &Handle,
    item: &FuseWorkItem,
) -> FuseReply {
    let unique = item.header.unique;
    let input = match parse_fsync_in(fuse_payload(item)) {
        Ok(input) => input,
        Err(error) => return encode_error_reply(unique, io_error_errno(&error)),
    };
    let datasync = input & 1 != 0;
    match runtime_handle.block_on(vfs.fsync(item.header.nodeid, datasync)) {
        Ok(()) => encode_data_reply(unique, &[]),
        Err(error) => encode_error_reply(unique, vfs_err_to_errno(&error)),
    }
}

fn handle_fuse_release(
    vfs: &Arc<StudioVfs>,
    runtime_handle: &Handle,
    item: &FuseWorkItem,
) -> FuseReply {
    let unique = item.header.unique;
    let input = match parse_release_in(fuse_payload(item)) {
        Ok(input) => input,
        Err(error) => return encode_error_reply(unique, io_error_errno(&error)),
    };
    match runtime_handle.block_on(vfs.release_fh_with_outcome(input)) {
        Ok(_) => encode_data_reply(unique, &[]),
        Err(error) => encode_error_reply(unique, vfs_err_to_errno(&error)),
    }
}

fn fuse_opcode_expects_reply(opcode: u32) -> bool {
    !matches!(opcode, FUSE_FORGET | FUSE_BATCH_FORGET)
}

fn read_fuse_message(device: &mut File, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        match device.read(buffer) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "FUSE device closed while waiting for request",
                ));
            }
            Ok(len) => return Ok(len),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                poll_fd(device.as_raw_fd(), libc::POLLIN)?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn read_fuse_message_until_shutdown(
    device: &mut (impl Read + AsRawFd),
    buffer: &mut [u8],
    shutdown: &AtomicBool,
) -> io::Result<Option<usize>> {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(None);
        }
        match device.read(buffer) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "FUSE device closed while waiting for request",
                ));
            }
            Ok(len) => return Ok(Some(len)),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                let _ = poll_fd_for(device.as_raw_fd(), libc::POLLIN, FUSE_RECEIVE_SHUTDOWN_POLL)?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn write_all_fuse(device: &mut File, bytes: &[u8]) -> io::Result<()> {
    loop {
        match device.write(bytes) {
            Ok(written) => return check_fuse_write_len(written, bytes.len()),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                poll_fd(device.as_raw_fd(), libc::POLLOUT)?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn write_vectored_fuse(device: &mut File, slices: &[IoSlice<'_>]) -> io::Result<()> {
    let expected = slices.iter().map(|slice| slice.len()).sum();
    loop {
        match device.write_vectored(slices) {
            Ok(written) => return check_fuse_write_len(written, expected),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                poll_fd(device.as_raw_fd(), libc::POLLOUT)?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn check_fuse_write_len(written: usize, expected: usize) -> io::Result<()> {
    if written == expected {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::WriteZero,
            format!("short FUSE reply write: wrote {written} of {expected} bytes"),
        ))
    }
}

fn poll_fd(fd: RawFd, events: libc::c_short) -> io::Result<()> {
    loop {
        let mut pollfd = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        let timeout_ms = poll_timeout_ms(events);
        // SAFETY: `pollfd` points to one initialized poll descriptor.
        let rc = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        match rc {
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for FUSE device readiness",
                ));
            }
            n if n > 0 => return Ok(()),
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

fn poll_fd_for(fd: RawFd, events: libc::c_short, timeout: Duration) -> io::Result<bool> {
    loop {
        let mut pollfd = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        // SAFETY: `pollfd` points to one initialized poll descriptor.
        let rc = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        match rc {
            0 => return Ok(false),
            n if n > 0 => return Ok(true),
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

fn poll_timeout_ms(events: libc::c_short) -> i32 {
    if events & libc::POLLIN != 0 {
        -1
    } else {
        i32::try_from(FUSE_POLL_TIMEOUT.as_millis()).unwrap_or(i32::MAX)
    }
}

fn page_size() -> io::Result<u32> {
    // SAFETY: sysconf has no preconditions for `_SC_PAGESIZE`.
    let value = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if value > 0 {
        Ok(value as u32)
    } else {
        Err(io::Error::last_os_error())
    }
}

fn negotiate_init(init: &FuseInitIn, page_size: u32) -> io::Result<NegotiatedInit> {
    if init.major != FUSE_KERNEL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported FUSE ABI major version {}", init.major),
        ));
    }
    let requested_caps = STUDIOFS_REQUESTED_CAPS;
    let mut accepted = init.flags & STUDIOFS_REQUESTED_CAPS;
    let max_pages = if init.flags & FUSE_MAX_PAGES == 0 {
        accepted &= !FUSE_MAX_PAGES;
        0
    } else {
        FUSE_MAX_PAGES_DEFAULT
    };
    let max_write = if max_pages == 0 {
        FUSE_COMPAT_MAX_WRITE
    } else {
        page_size.saturating_mul(u32::from(max_pages))
    };
    let out = FuseInitOut {
        major: FUSE_KERNEL_VERSION,
        minor: FUSE_KERNEL_MINOR_VERSION.min(init.minor),
        max_readahead: init.max_readahead,
        flags: accepted,
        max_background: 0,
        congestion_threshold: 0,
        max_write,
        time_gran: 1,
        max_pages,
        map_alignment: 0,
        flags2: 0,
        max_stack_depth: 0,
        request_timeout: 0,
        unused: [0; 11],
    };
    Ok(NegotiatedInit {
        out,
        requested_caps,
        accepted_caps: accepted,
    })
}

fn write_init_reply(device: &mut File, unique: u64, init: &FuseInitOut) -> io::Result<()> {
    write_all_fuse(device, &encode_init_reply(unique, init))
}

fn encode_init_reply(unique: u64, init: &FuseInitOut) -> Vec<u8> {
    let payload_size = init_out_payload_size(init.minor);
    let reply_size = FUSE_OUT_HEADER_SIZE + payload_size;
    let mut reply = vec![0_u8; FUSE_OUT_HEADER_SIZE + FUSE_INIT_OUT_SIZE];
    let mut offset = 0;
    push_out_header(&mut reply, &mut offset, unique, 0, reply_size);
    push_u32(&mut reply, &mut offset, init.major);
    push_u32(&mut reply, &mut offset, init.minor);
    push_u32(&mut reply, &mut offset, init.max_readahead);
    push_u32(&mut reply, &mut offset, init.flags);
    push_u16(&mut reply, &mut offset, init.max_background);
    push_u16(&mut reply, &mut offset, init.congestion_threshold);
    push_u32(&mut reply, &mut offset, init.max_write);
    push_u32(&mut reply, &mut offset, init.time_gran);
    push_u16(&mut reply, &mut offset, init.max_pages);
    push_u16(&mut reply, &mut offset, init.map_alignment);
    push_u32(&mut reply, &mut offset, init.flags2);
    push_u32(&mut reply, &mut offset, init.max_stack_depth);
    push_u16(&mut reply, &mut offset, init.request_timeout);
    for value in init.unused {
        push_u16(&mut reply, &mut offset, value);
    }
    reply.truncate(reply_size);
    reply
}

fn init_out_payload_size(minor: u32) -> usize {
    if minor < 5 {
        FUSE_COMPAT_INIT_OUT_SIZE
    } else if minor < 23 {
        FUSE_COMPAT_22_INIT_OUT_SIZE
    } else {
        FUSE_INIT_OUT_SIZE
    }
}

fn encode_data_reply(unique: u64, data: &[u8]) -> FuseReply {
    let reply_size = FUSE_OUT_HEADER_SIZE + data.len();
    let header = encode_reply_header(unique, 0, reply_size);
    if data.is_empty() {
        let mut reply = [0_u8; FUSE_WRITE_REPLY_SIZE];
        reply[..FUSE_OUT_HEADER_SIZE].copy_from_slice(&header);
        return FuseReply::inline(reply, FUSE_OUT_HEADER_SIZE);
    }

    let mut reply = Vec::with_capacity(reply_size);
    reply.extend_from_slice(&header);
    reply.extend_from_slice(data);
    FuseReply::Owned(reply)
}

fn encode_write_reply(unique: u64, written: u32) -> FuseReply {
    let mut reply = [0_u8; FUSE_WRITE_REPLY_SIZE];
    let mut offset = 0;
    push_out_header(&mut reply, &mut offset, unique, 0, FUSE_WRITE_REPLY_SIZE);
    push_u32(&mut reply, &mut offset, written);
    push_u32(&mut reply, &mut offset, 0);
    FuseReply::inline(reply, FUSE_WRITE_REPLY_SIZE)
}

fn encode_error_reply(unique: u64, errno: i32) -> FuseReply {
    let mut reply = [0_u8; FUSE_WRITE_REPLY_SIZE];
    let mut offset = 0;
    push_out_header(
        &mut reply,
        &mut offset,
        unique,
        -errno,
        FUSE_OUT_HEADER_SIZE,
    );
    FuseReply::inline(reply, FUSE_OUT_HEADER_SIZE)
}

fn encode_reply_header(unique: u64, error: i32, len: usize) -> [u8; FUSE_OUT_HEADER_SIZE] {
    let mut header = [0_u8; FUSE_OUT_HEADER_SIZE];
    let mut offset = 0;
    push_out_header(&mut header, &mut offset, unique, error, len);
    header
}

fn fuse_payload(item: &FuseWorkItem) -> &[u8] {
    item.message.get(FUSE_IN_HEADER_SIZE..).unwrap_or_default()
}

#[derive(Debug, Clone, Copy)]
struct FuseReadIn {
    fh: u64,
    offset: u64,
    size: u32,
}

#[derive(Debug, Clone, Copy)]
struct FuseWriteIn<'a> {
    fh: u64,
    offset: u64,
    data: &'a [u8],
}

fn parse_read_in(bytes: &[u8]) -> io::Result<FuseReadIn> {
    if bytes.len() < FUSE_READ_IN_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short FUSE_READ request",
        ));
    }
    Ok(FuseReadIn {
        fh: read_u64(bytes, 0)?,
        offset: read_u64(bytes, 8)?,
        size: read_u32(bytes, 16)?,
    })
}

fn parse_write_in(bytes: &[u8]) -> io::Result<FuseWriteIn<'_>> {
    if bytes.len() < FUSE_WRITE_IN_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short FUSE_WRITE request",
        ));
    }
    let size = read_u32(bytes, 16)? as usize;
    let data_end = FUSE_WRITE_IN_SIZE
        .checked_add(size)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "FUSE_WRITE size overflow"))?;
    let data = bytes.get(FUSE_WRITE_IN_SIZE..data_end).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "FUSE_WRITE payload shorter than declared size",
        )
    })?;
    Ok(FuseWriteIn {
        fh: read_u64(bytes, 0)?,
        offset: read_u64(bytes, 8)?,
        data,
    })
}

fn parse_flush_in(bytes: &[u8]) -> io::Result<u64> {
    if bytes.len() < FUSE_FLUSH_IN_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short FUSE_FLUSH request",
        ));
    }
    read_u64(bytes, 0)
}

fn parse_fsync_in(bytes: &[u8]) -> io::Result<u32> {
    if bytes.len() < FUSE_FSYNC_IN_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short FUSE_FSYNC request",
        ));
    }
    read_u32(bytes, 8)
}

fn parse_release_in(bytes: &[u8]) -> io::Result<u64> {
    if bytes.len() < FUSE_RELEASE_IN_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short FUSE_RELEASE request",
        ));
    }
    read_u64(bytes, 0)
}

fn io_error_errno(error: &io::Error) -> i32 {
    error.raw_os_error().unwrap_or(libc::EINVAL)
}

fn push_out_header(reply: &mut [u8], offset: &mut usize, unique: u64, error: i32, len: usize) {
    push_u32(reply, offset, len as u32);
    push_i32(reply, offset, error);
    push_u64(reply, offset, unique);
}

fn push_u16(reply: &mut [u8], offset: &mut usize, value: u16) {
    push_bytes(reply, offset, &value.to_ne_bytes());
}

fn push_u32(reply: &mut [u8], offset: &mut usize, value: u32) {
    push_bytes(reply, offset, &value.to_ne_bytes());
}

fn push_i32(reply: &mut [u8], offset: &mut usize, value: i32) {
    push_bytes(reply, offset, &value.to_ne_bytes());
}

fn push_u64(reply: &mut [u8], offset: &mut usize, value: u64) {
    push_bytes(reply, offset, &value.to_ne_bytes());
}

fn push_bytes(reply: &mut [u8], offset: &mut usize, bytes: &[u8]) {
    let end = *offset + bytes.len();
    reply[*offset..end].copy_from_slice(bytes);
    *offset = end;
}

fn parse_in_header(bytes: &[u8]) -> io::Result<FuseInHeader> {
    if bytes.len() < FUSE_IN_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short FUSE request header",
        ));
    }
    let len = read_u32(bytes, 0)?;
    if len < FUSE_IN_HEADER_SIZE as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FUSE request length shorter than header",
        ));
    }
    if usize::try_from(len).map_or(true, |len| len > bytes.len()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FUSE request length exceeds received bytes",
        ));
    }
    Ok(FuseInHeader {
        opcode: read_u32(bytes, 4)?,
        unique: read_u64(bytes, 8)?,
        nodeid: read_u64(bytes, 16)?,
    })
}

fn parse_init_in(bytes: &[u8]) -> io::Result<FuseInitIn> {
    if bytes.len() < FUSE_INIT_IN_BASE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short FUSE_INIT request",
        ));
    }
    Ok(FuseInitIn {
        major: read_u32(bytes, 0)?,
        minor: read_u32(bytes, 4)?,
        max_readahead: read_u32(bytes, 8)?,
        flags: read_u32(bytes, 12)?,
    })
}

fn read_u32(bytes: &[u8], offset: usize) -> io::Result<u32> {
    read_bytes(bytes, offset).map(u32::from_ne_bytes)
}

fn read_u64(bytes: &[u8], offset: usize) -> io::Result<u64> {
    read_bytes(bytes, offset).map(u64::from_ne_bytes)
}

fn read_bytes<const N: usize>(bytes: &[u8], offset: usize) -> io::Result<[u8; N]> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "FUSE field offset overflow"))?;
    let slice = bytes.get(offset..end).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "FUSE field extends beyond message",
        )
    })?;
    let mut value = [0_u8; N];
    value.copy_from_slice(slice);
    Ok(value)
}

fn apply_read_ahead_kb_best_effort(mount_point: &Path, kb: u32) {
    if kb == 0 {
        return;
    }
    match mountinfo_bdi_path(mount_point) {
        Ok(Some(path)) => {
            if let Err(error) = std::fs::write(&path, kb.to_string()) {
                if matches!(error.raw_os_error(), Some(libc::EACCES | libc::EPERM)) {
                    tracing::debug!(
                        path = %mount_point.display(),
                        bdi = %path.display(),
                        %error,
                        "linux fuse BDI read_ahead_kb tuning skipped"
                    );
                } else {
                    tracing::warn!(
                        path = %mount_point.display(),
                        bdi = %path.display(),
                        %error,
                        "linux fuse BDI read_ahead_kb tuning skipped"
                    );
                }
            }
        }
        Ok(None) => tracing::warn!(
            path = %mount_point.display(),
            "linux fuse BDI read_ahead_kb tuning skipped: mountinfo entry not found"
        ),
        Err(error) => tracing::warn!(
            path = %mount_point.display(),
            %error,
            "linux fuse BDI read_ahead_kb tuning skipped: mountinfo unreadable"
        ),
    }
}

fn mountinfo_bdi_path(mount_point: &Path) -> io::Result<Option<PathBuf>> {
    std::fs::read("/proc/self/mountinfo")
        .map(|mountinfo| mountinfo_bdi_path_from_bytes(&mountinfo, mount_point))
}

fn mountinfo_lookup_mount_point(mount_point: &Path) -> PathBuf {
    match std::fs::canonicalize(mount_point) {
        Ok(path) => path,
        Err(error) => {
            tracing::debug!(
                path = %mount_point.display(),
                %error,
                "linux fuse BDI mountinfo lookup using non-canonical mount path"
            );
            mount_point.to_path_buf()
        }
    }
}

fn mountinfo_bdi_path_from(mountinfo: &str, mount_point: &Path) -> Option<PathBuf> {
    let target = mount_point.to_string_lossy();
    for line in mountinfo.lines() {
        let mut fields = line.split_whitespace();
        // mountinfo starts with: id parent major:minor root mountpoint ...
        let (Some(_id), Some(_parent), Some(majmin), Some(_root), Some(mounted_at)) = (
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
        ) else {
            continue;
        };
        if unescape_mountinfo_field(mounted_at) == target {
            return Some(PathBuf::from(format!(
                "/sys/class/bdi/{majmin}/read_ahead_kb"
            )));
        }
    }
    None
}

fn mountinfo_bdi_path_from_bytes(mountinfo: &[u8], mount_point: &Path) -> Option<PathBuf> {
    mountinfo_bdi_path_from(&String::from_utf8_lossy(mountinfo), mount_point)
}

fn unescape_mountinfo_field(value: &str) -> Cow<'_, str> {
    if !value.contains('\\') {
        return Cow::Borrowed(value);
    }
    let bytes = value.as_bytes();
    let mut unescaped = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' && index + 3 < bytes.len() {
            match &bytes[index + 1..index + 4] {
                b"040" => {
                    unescaped.push(b' ');
                    index += 4;
                    continue;
                }
                b"011" => {
                    unescaped.push(b'\t');
                    index += 4;
                    continue;
                }
                b"012" => {
                    unescaped.push(b'\n');
                    index += 4;
                    continue;
                }
                b"134" => {
                    unescaped.push(b'\\');
                    index += 4;
                    continue;
                }
                _ => {}
            }
        }
        unescaped.push(bytes[index]);
        index += 1;
    }
    Cow::Owned(
        String::from_utf8(unescaped)
            .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned()),
    )
}

#[derive(Debug)]
struct NegotiatedInit {
    out: FuseInitOut,
    requested_caps: u32,
    accepted_caps: u32,
}

#[derive(Debug, Clone, Copy)]
struct FuseInHeader {
    opcode: u32,
    unique: u64,
    nodeid: u64,
}

#[derive(Debug, Clone)]
struct FuseWorkItem {
    header: FuseInHeader,
    message: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct FuseInitIn {
    major: u32,
    minor: u32,
    max_readahead: u32,
    flags: u32,
}

#[derive(Debug, Clone, Copy)]
struct FuseInitOut {
    major: u32,
    minor: u32,
    max_readahead: u32,
    flags: u32,
    max_background: u16,
    congestion_threshold: u16,
    max_write: u32,
    time_gran: u32,
    max_pages: u16,
    map_alignment: u16,
    flags2: u32,
    max_stack_depth: u32,
    request_timeout: u16,
    unused: [u16; 11],
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_data::DataStoreConfig;
    use studiofs_meta::{
        CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, RedbMetaEngine,
    };
    use studiofs_vfs::{OpenFlags, VfsConfig};
    use tokio::runtime::Runtime;

    const FUSE_INIT_OUT_MAX_WRITE_OFFSET: usize = 20;
    const FUSE_LOOKUP: u32 = 1;

    #[test]
    fn negotiate_init_uses_kernel_readahead_and_supported_pages() {
        let init = FuseInitIn {
            major: FUSE_KERNEL_VERSION,
            minor: FUSE_KERNEL_MINOR_VERSION,
            max_readahead: 768 * 1024,
            flags: FUSE_MAX_PAGES | FUSE_ASYNC_READ,
        };

        let negotiated = negotiate_init(&init, 4096).expect("negotiate init");

        assert_eq!(negotiated.out.max_readahead, 768 * 1024);
        assert_eq!(negotiated.out.max_pages, 256);
        assert_eq!(negotiated.out.max_write, 1024 * 1024);
        assert_ne!(negotiated.accepted_caps & FUSE_MAX_PAGES, 0);
    }

    #[test]
    fn negotiate_init_omits_pages_when_kernel_does_not_offer_it() {
        let init = FuseInitIn {
            major: FUSE_KERNEL_VERSION,
            minor: FUSE_KERNEL_MINOR_VERSION,
            max_readahead: 256 * 1024,
            flags: FUSE_ASYNC_READ,
        };

        let negotiated = negotiate_init(&init, 4096).expect("negotiate init");

        assert_eq!(negotiated.out.max_readahead, 256 * 1024);
        assert_eq!(negotiated.out.max_pages, 0);
        assert_eq!(negotiated.out.max_write, 128 * 1024);
        assert_eq!(negotiated.accepted_caps & FUSE_MAX_PAGES, 0);
    }

    #[test]
    fn mountinfo_bdi_path_from_returns_read_ahead_path() {
        let mountinfo = "126 32 0:114 / /mnt/studiofs rw,relatime shared:98 - fuse studiofs rw,user_id=1000,group_id=1000\n";

        assert_eq!(
            mountinfo_bdi_path_from(mountinfo, std::path::Path::new("/mnt/studiofs")),
            Some(std::path::PathBuf::from(
                "/sys/class/bdi/0:114/read_ahead_kb"
            ))
        );
    }

    #[test]
    fn fuse_rootmode_includes_directory_type_bits() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o750))
            .expect("set mode");

        let mode = fuse_rootmode(tmp.path()).expect("rootmode");

        assert_eq!(mode & libc::S_IFMT, libc::S_IFDIR);
        assert_eq!(mode & 0o7777, 0o750);
    }

    #[test]
    fn mountinfo_lookup_mount_point_canonicalizes_relative_mount_point() {
        let cwd = std::env::current_dir().expect("current dir");
        assert_eq!(mountinfo_lookup_mount_point(std::path::Path::new(".")), cwd);
    }

    #[test]
    fn mountinfo_bdi_path_from_expects_precanonical_mount_point() {
        let cwd = std::env::current_dir().expect("current dir");
        let mountinfo = format!(
            "126 32 0:114 / {} rw,relatime shared:98 - fuse studiofs rw,user_id=1000,group_id=1000\n",
            cwd.display()
        );

        assert_eq!(
            mountinfo_bdi_path_from(&mountinfo, &cwd),
            Some(std::path::PathBuf::from(
                "/sys/class/bdi/0:114/read_ahead_kb"
            ))
        );
        assert_eq!(
            mountinfo_bdi_path_from(&mountinfo, std::path::Path::new(".")),
            None
        );
    }

    #[test]
    fn mountinfo_bdi_path_from_bytes_ignores_unrelated_invalid_utf8() {
        let mountinfo = b"1 0 0:1 / /bad\xff rw - tmpfs tmpfs rw\n126 32 0:114 / /mnt/studiofs rw,relatime shared:98 - fuse studiofs rw,user_id=1000,group_id=1000\n";

        assert_eq!(
            mountinfo_bdi_path_from_bytes(mountinfo, std::path::Path::new("/mnt/studiofs")),
            Some(std::path::PathBuf::from(
                "/sys/class/bdi/0:114/read_ahead_kb"
            ))
        );
    }

    #[test]
    fn unescape_mountinfo_field_borrows_plain_paths() {
        assert!(matches!(
            unescape_mountinfo_field("/mnt/studiofs"),
            Cow::Borrowed("/mnt/studiofs")
        ));
        assert_eq!(unescape_mountinfo_field("/mnt/a\\040b"), "/mnt/a b");
    }

    #[test]
    fn unescape_mountinfo_field_decodes_each_escape_once() {
        assert_eq!(unescape_mountinfo_field("/mnt/a\\134040b"), "/mnt/a\\040b");
    }

    #[test]
    fn pollin_waits_without_idle_timeout() {
        assert_eq!(poll_timeout_ms(libc::POLLIN), -1);
    }

    #[test]
    fn pollout_keeps_bounded_timeout() {
        assert_eq!(poll_timeout_ms(libc::POLLOUT), 30_000);
    }

    #[test]
    fn forget_opcodes_do_not_expect_replies() {
        assert!(!fuse_opcode_expects_reply(FUSE_FORGET));
        assert!(!fuse_opcode_expects_reply(FUSE_BATCH_FORGET));
        assert!(fuse_opcode_expects_reply(FUSE_INIT));
    }

    #[test]
    fn parse_in_header_rejects_len_shorter_than_header() {
        let mut bytes = [0_u8; FUSE_IN_HEADER_SIZE];
        bytes[0..4].copy_from_slice(&(FUSE_IN_HEADER_SIZE as u32 - 1).to_ne_bytes());

        let error = parse_in_header(&bytes).expect_err("short header len");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn fuse_reply_write_rejects_partial_syscall() {
        let error = check_fuse_write_len(8, FUSE_OUT_HEADER_SIZE).expect_err("partial write");

        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
    }

    #[test]
    fn init_reply_encoding_uses_negotiated_wire_size() {
        let init = negotiate_init(
            &FuseInitIn {
                major: FUSE_KERNEL_VERSION,
                minor: FUSE_KERNEL_MINOR_VERSION,
                max_readahead: 512 * 1024,
                flags: FUSE_ASYNC_READ,
            },
            4096,
        )
        .expect("negotiate init");

        let reply = encode_init_reply(99, &init.out);

        assert_eq!(reply.len(), FUSE_OUT_HEADER_SIZE + FUSE_INIT_OUT_SIZE);
        assert_eq!(read_u32(&reply, 0).expect("reply len"), reply.len() as u32);
        assert_eq!(read_u64(&reply, 8).expect("unique"), 99);
        assert_eq!(
            read_u32(&reply, FUSE_OUT_HEADER_SIZE).expect("major"),
            FUSE_KERNEL_VERSION
        );
        assert_eq!(
            read_u32(
                &reply,
                FUSE_OUT_HEADER_SIZE + FUSE_INIT_OUT_MAX_WRITE_OFFSET
            )
            .expect("max write"),
            128 * 1024
        );
    }

    #[test]
    fn init_reply_encoding_truncates_legacy_wire_sizes() {
        let init = negotiate_init(
            &FuseInitIn {
                major: FUSE_KERNEL_VERSION,
                minor: 22,
                max_readahead: 512 * 1024,
                flags: FUSE_ASYNC_READ,
            },
            4096,
        )
        .expect("negotiate init");

        let reply = encode_init_reply(99, &init.out);

        assert_eq!(
            reply.len(),
            FUSE_OUT_HEADER_SIZE + FUSE_COMPAT_22_INIT_OUT_SIZE
        );
        assert_eq!(read_u32(&reply, 0).expect("reply len"), reply.len() as u32);

        let init = negotiate_init(
            &FuseInitIn {
                major: FUSE_KERNEL_VERSION,
                minor: 4,
                max_readahead: 512 * 1024,
                flags: FUSE_ASYNC_READ,
            },
            4096,
        )
        .expect("negotiate init");

        let reply = encode_init_reply(99, &init.out);

        assert_eq!(
            reply.len(),
            FUSE_OUT_HEADER_SIZE + FUSE_COMPAT_INIT_OUT_SIZE
        );
        assert_eq!(read_u32(&reply, 0).expect("reply len"), reply.len() as u32);
    }

    #[test]
    fn read_u32_rejects_out_of_bounds_offset() {
        let error = read_u32(&[0_u8; 3], 0).expect_err("short u32");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn error_reply_encoding_is_header_only() {
        let reply = encode_error_reply(7, libc::ENOSYS);
        let reply = reply.as_slice();

        assert_eq!(reply.len(), FUSE_OUT_HEADER_SIZE);
        assert_eq!(
            read_u32(reply, 0).expect("reply len"),
            FUSE_OUT_HEADER_SIZE as u32
        );
        assert_eq!(
            i32::from_ne_bytes(reply[4..8].try_into().expect("i32")),
            -libc::ENOSYS
        );
        assert_eq!(read_u64(reply, 8).expect("unique"), 7);
    }

    #[test]
    fn vfs_handler_wires_read_write_flush_fsync_and_release() {
        let tmp = studiofs_test_support::workspace_target_tempdir("linux-vfs-handler-");
        let (runtime, vfs) = make_test_vfs(&tmp, "wire-io");
        let attr = vfs
            .create(ROOT_INO, b"linux-wire.txt", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"linux-local-first";

        let write = handle_vfs_fuse_request(
            &vfs,
            runtime.handle(),
            &work_item_with_payload(FUSE_WRITE, 80, attr.ino, &write_in(fh, 0, payload)),
        )
        .expect("write reply");
        let write = write.as_slice();
        assert_ok_reply(write, 80);
        assert_eq!(
            read_u32(write, FUSE_OUT_HEADER_SIZE).unwrap(),
            payload.len() as u32
        );

        let read = handle_vfs_fuse_request(
            &vfs,
            runtime.handle(),
            &work_item_with_payload(
                FUSE_READ,
                81,
                attr.ino,
                &read_in(fh, 0, payload.len() as u32),
            ),
        )
        .expect("read reply");
        let read = read.as_slice();
        assert_ok_reply(read, 81);
        assert_eq!(&read[FUSE_OUT_HEADER_SIZE..], payload);

        for (opcode, unique, body) in [
            (FUSE_FLUSH, 82, flush_in(fh)),
            (FUSE_FSYNC, 83, fsync_in(fh, 0)),
            (FUSE_RELEASE, 84, release_in(fh)),
        ] {
            let reply = handle_vfs_fuse_request(
                &vfs,
                runtime.handle(),
                &work_item_with_payload(opcode, unique, attr.ino, &body),
            )
            .expect("barrier reply");
            let reply = reply.as_slice();
            assert_ok_reply(reply, unique);
            assert_eq!(reply.len(), FUSE_OUT_HEADER_SIZE);
        }

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(snapshot.fuse_callbacks.write.calls, 1);
        assert_eq!(snapshot.fuse_callbacks.write.bytes, payload.len() as u64);
        assert_eq!(snapshot.fuse_callbacks.read.calls, 1);
        assert_eq!(snapshot.vfs_write_path.calls, 1);
        assert_eq!(snapshot.vfs_write_path.write_bytes, payload.len() as u64);
        assert!(snapshot.flush_barrier_calls >= 1);

        runtime.block_on(vfs.shutdown(Duration::ZERO));
    }

    #[test]
    fn queue_depth_decrement_does_not_underflow() {
        let metrics = LinuxFuseLoopMetrics::default();

        metrics.work_dequeued();
        metrics.reply_dequeued();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.work_queue_depth, 0);
        assert_eq!(snapshot.reply_queue_depth, 0);
    }

    #[test]
    fn fuse_device_open_flags_include_nonblocking() {
        assert_ne!(FUSE_OPEN_FLAGS & libc::O_NONBLOCK, 0);
    }

    #[test]
    fn bounded_work_enqueue_records_backpressure() {
        let (tx, rx) = work_channel(1);
        let metrics = Arc::new(LinuxFuseLoopMetrics::default());
        enqueue_work(
            &tx,
            FuseWorkItem {
                header: FuseInHeader {
                    opcode: 999,
                    unique: 1,
                    nodeid: 1,
                },
                message: Vec::new(),
            },
            &metrics,
        )
        .expect("first enqueue");

        let metrics_for_thread = Arc::clone(&metrics);
        let sender = std::thread::spawn(move || {
            enqueue_work(
                &tx,
                FuseWorkItem {
                    header: FuseInHeader {
                        opcode: 999,
                        unique: 2,
                        nodeid: 1,
                    },
                    message: Vec::new(),
                },
                &metrics_for_thread,
            )
        });

        while metrics.snapshot().work_backpressure_count == 0 {
            std::thread::yield_now();
        }
        let _first = rx.recv().expect("first work item");
        sender
            .join()
            .expect("sender thread")
            .expect("second enqueue");

        assert_eq!(metrics.snapshot().work_backpressure_count, 1);
    }

    #[test]
    fn shell_routes_request_through_reply_loop() {
        use std::os::fd::{FromRawFd, IntoRawFd};
        use std::os::unix::net::UnixStream;

        let tmp = studiofs_test_support::workspace_target_tempdir("linux-shell-loop-");
        let (runtime, vfs) = make_test_vfs(&tmp, "shell-loop");
        let (mut kernel, device) = UnixStream::pair().expect("socket pair");
        let reply_device = device.try_clone().expect("clone device");
        let metrics = Arc::new(LinuxFuseLoopMetrics::default());
        let metrics_for_shell = Arc::clone(&metrics);
        let runtime_handle = runtime.handle().clone();
        let vfs_for_shell = Arc::clone(&vfs);
        let shell = std::thread::spawn(move || {
            let device = unsafe {
                // SAFETY: `device.into_raw_fd()` transfers ownership to `File`.
                File::from_raw_fd(device.into_raw_fd())
            };
            let reply_device = unsafe {
                // SAFETY: `reply_device.into_raw_fd()` transfers ownership to `File`.
                File::from_raw_fd(reply_device.into_raw_fd())
            };
            run_fuse_shell(
                device,
                reply_device,
                vec![0_u8; FUSE_REQUEST_BUFFER_SIZE],
                vfs_for_shell,
                runtime_handle,
                metrics_for_shell,
            )
        });

        kernel
            .write_all(&fuse_request_for_test(999, 55))
            .expect("write request");
        let mut reply = [0_u8; FUSE_OUT_HEADER_SIZE];
        kernel.read_exact(&mut reply).expect("read reply");
        kernel
            .write_all(&fuse_request_for_test(FUSE_DESTROY, 56))
            .expect("write destroy");
        shell.join().expect("shell thread").expect("shell result");

        assert_eq!(read_u64(&reply, 8).expect("unique"), 55);
        assert_eq!(metrics.snapshot().reply_error_count, 0);
        runtime.block_on(vfs.shutdown(Duration::ZERO));
    }

    #[test]
    fn receive_loop_passes_full_request_message_to_worker() {
        use std::os::fd::{FromRawFd, IntoRawFd};
        use std::os::unix::net::UnixStream;

        let (mut kernel, device) = UnixStream::pair().expect("socket pair");
        let (tx, rx) = work_channel(2);
        let metrics = Arc::new(LinuxFuseLoopMetrics::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let metrics_for_receiver = Arc::clone(&metrics);
        let shutdown_for_receiver = Arc::clone(&shutdown);
        let receiver = std::thread::spawn(move || {
            let device = unsafe {
                // SAFETY: `device.into_raw_fd()` transfers ownership to `File`.
                File::from_raw_fd(device.into_raw_fd())
            };
            receive_loop_until_shutdown(
                device,
                vec![0_u8; FUSE_REQUEST_BUFFER_SIZE],
                tx,
                &metrics_for_receiver,
                &shutdown_for_receiver,
            )
        });
        let request = fuse_request_with_payload_for_test(999, 77, b"payload");

        kernel.write_all(&request).expect("write request");
        let item = rx.recv().expect("work item");
        kernel
            .write_all(&fuse_request_for_test(FUSE_DESTROY, 78))
            .expect("write destroy");
        receiver.join().expect("receiver thread").expect("receiver");

        assert_eq!(item.message, request);
    }

    #[test]
    fn shell_returns_when_reply_path_fails_while_receiver_is_idle() {
        use std::net::Shutdown;
        use std::os::fd::{FromRawFd, IntoRawFd};
        use std::os::unix::net::UnixStream;
        use std::time::Duration;

        let tmp = studiofs_test_support::workspace_target_tempdir("linux-shell-reply-fails-");
        let (runtime, vfs) = make_test_vfs(&tmp, "reply-fails");
        let (mut kernel, device) = UnixStream::pair().expect("socket pair");
        device.set_nonblocking(true).expect("nonblocking device");
        let reply_device = device.try_clone().expect("clone device");
        let metrics = Arc::new(LinuxFuseLoopMetrics::default());
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let runtime_handle = runtime.handle().clone();
        let vfs_for_shell = Arc::clone(&vfs);
        std::thread::spawn(move || {
            let device = unsafe {
                // SAFETY: `device.into_raw_fd()` transfers ownership to `File`.
                File::from_raw_fd(device.into_raw_fd())
            };
            let reply_device = unsafe {
                // SAFETY: `reply_device.into_raw_fd()` transfers ownership to `File`.
                File::from_raw_fd(reply_device.into_raw_fd())
            };
            let _ = done_tx.send(run_fuse_shell(
                device,
                reply_device,
                vec![0_u8; FUSE_REQUEST_BUFFER_SIZE],
                vfs_for_shell,
                runtime_handle,
                metrics,
            ));
        });

        kernel
            .write_all(&fuse_request_for_test(999, 79))
            .expect("write request");
        kernel.shutdown(Shutdown::Read).expect("shutdown read half");

        let result = done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("shell should return after reply failure");
        assert!(result.is_err());
        runtime.block_on(vfs.shutdown(Duration::ZERO));
    }

    #[test]
    fn scheduler_orders_writes_on_same_handle() {
        let items = vec![
            work_item_with_fh(FUSE_WRITE, 1, 7, 10),
            work_item_with_fh(FUSE_WRITE, 2, 7, 10),
            work_item_with_fh(FUSE_WRITE, 3, 7, 10),
        ];

        let seen = run_scheduled_work_for_test(items, |_item| {}).expect("scheduled work");

        assert_eq!(seen, vec![1, 2, 3]);
    }

    #[test]
    fn scheduler_runs_unrelated_handles_concurrently() {
        let first_started = Arc::new(std::sync::Barrier::new(2));
        let release_first = Arc::new(std::sync::Barrier::new(2));
        let first_started_for_handler = Arc::clone(&first_started);
        let release_first_for_handler = Arc::clone(&release_first);
        let items = vec![
            work_item_with_fh(FUSE_WRITE, 1, 7, 10),
            work_item_with_fh(FUSE_WRITE, 2, 8, 10),
        ];

        let (seen_rx, worker) = spawn_scheduled_work_for_test(items, move |item| {
            if item.header.unique == 1 {
                first_started_for_handler.wait();
                release_first_for_handler.wait();
            }
        });

        first_started.wait();
        let observed = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second handle should not wait for first");
        release_first.wait();
        worker
            .join()
            .expect("scheduler thread")
            .expect("scheduled work");

        assert_eq!(observed, 2);
    }

    #[test]
    fn scheduler_barriers_wait_for_same_handle_writes() {
        let items = vec![
            work_item_with_fh(FUSE_WRITE, 1, 7, 10),
            work_item_with_fh(FUSE_FLUSH, 2, 7, 10),
        ];

        let seen = run_scheduled_work_for_test(items, |_item| {}).expect("scheduled work");

        assert_eq!(seen, vec![1, 2]);
    }

    #[test]
    fn scheduler_routes_directory_reads_as_stream_work() {
        assert_eq!(
            stream_key_for(&work_item_with_fh(FUSE_READDIR, 1, 7, 10)),
            Some((10, Some(7)))
        );
        assert_eq!(
            stream_key_for(&work_item_with_fh(FUSE_READDIRPLUS, 2, 8, 11)),
            Some((11, Some(8)))
        );
    }

    #[test]
    fn scheduler_reuses_fixed_stream_worker_after_release() {
        let (work_tx, work_rx) = work_channel(4);
        let (reply_tx, _reply_rx) = work_channel(4);
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        let metrics = Arc::new(LinuxFuseLoopMetrics::default());
        let scheduler = std::thread::spawn(move || {
            scheduled_work_loop(work_rx, reply_tx, metrics, move |item| {
                seen_tx
                    .send((item.header.unique, std::thread::current().id()))
                    .expect("record worker thread");
                None
            })
        });

        enqueue_work(
            &work_tx,
            work_item_with_fh(FUSE_RELEASE, 1, 7, 10),
            &LinuxFuseLoopMetrics::default(),
        )
        .expect("release enqueue");
        let (release_unique, release_thread) = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("release should run");
        std::thread::sleep(Duration::from_millis(10));
        enqueue_work(
            &work_tx,
            work_item_with_fh(FUSE_WRITE, 2, 7, 10),
            &LinuxFuseLoopMetrics::default(),
        )
        .expect("write enqueue");
        let (write_unique, write_thread) = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("write should run");
        drop(work_tx);
        scheduler
            .join()
            .expect("scheduler thread")
            .expect("scheduled work");

        assert_eq!((release_unique, write_unique), (1, 2));
        assert_eq!(release_thread, write_thread);
    }

    #[test]
    fn scheduler_applies_backpressure_when_one_stream_queue_fills() {
        let (work_tx, work_rx) = work_channel(FUSE_WORK_QUEUE_DEPTH + 4);
        let (reply_tx, _reply_rx) = work_channel(1);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let block_slow_stream = Arc::new(std::sync::Barrier::new(2));
        let release_slow_stream = Arc::new(std::sync::Barrier::new(2));
        let block_slow_stream_for_handler = Arc::clone(&block_slow_stream);
        let release_slow_stream_for_handler = Arc::clone(&release_slow_stream);
        let metrics = Arc::new(LinuxFuseLoopMetrics::default());
        std::thread::spawn(move || {
            let result = scheduled_work_loop(work_rx, reply_tx, metrics, move |item| {
                if item.header.unique == 1 {
                    block_slow_stream_for_handler.wait();
                    release_slow_stream_for_handler.wait();
                }
                None
            });
            done_tx.send(result).expect("send scheduler result");
        });

        for unique in 1..=(FUSE_WORK_QUEUE_DEPTH as u64 + 2) {
            if work_tx
                .send(work_item_with_fh(FUSE_WRITE, unique, 7, 10))
                .is_err()
            {
                break;
            }
        }
        block_slow_stream.wait();
        std::thread::sleep(Duration::from_millis(50));
        release_slow_stream.wait();
        drop(work_tx);
        let result = done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("scheduler should finish after backpressure releases");

        result.expect("scheduler result");
    }

    #[test]
    fn stream_worker_hash_distributes_equal_nodeid_and_fh() {
        let workers = (1..=16)
            .map(|value| stream_worker_index((value, Some(value)), FUSE_STREAM_WORKERS))
            .collect::<std::collections::HashSet<_>>();

        assert!(workers.len() > 1);
    }

    #[test]
    fn stream_worker_hash_distinguishes_missing_and_zero_fh() {
        assert_ne!(
            stream_worker_hash((7, None)),
            stream_worker_hash((7, Some(0)))
        );
    }

    #[test]
    fn scheduler_returns_when_worker_panics() {
        let (work_tx, work_rx) = work_channel(1);
        let (reply_tx, _reply_rx) = work_channel(1);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let metrics = Arc::new(LinuxFuseLoopMetrics::default());
        std::thread::spawn(move || {
            let result =
                scheduled_work_loop(work_rx, reply_tx, metrics, |_item| -> Option<FuseReply> {
                    panic!("worker panic for test");
                });
            done_tx.send(result).expect("send scheduler result");
        });

        work_tx
            .send(work_item_with_fh(FUSE_WRITE, 1, 7, 10))
            .expect("enqueue work");
        let result = done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("scheduler should return after worker panic");

        assert!(result.is_err());
    }

    #[test]
    fn join_fuse_workers_joins_remaining_workers_after_error() {
        let slow_worker_finished = Arc::new(AtomicBool::new(false));
        let slow_worker_finished_for_thread = Arc::clone(&slow_worker_finished);
        let failing_worker =
            std::thread::spawn(|| -> io::Result<()> { Err(io::Error::other("worker failed")) });
        let slow_worker = std::thread::spawn(move || -> io::Result<()> {
            std::thread::sleep(Duration::from_millis(50));
            slow_worker_finished_for_thread.store(true, Ordering::SeqCst);
            Ok(())
        });

        let result = join_fuse_workers(vec![failing_worker, slow_worker]);

        assert!(result.is_err());
        assert!(slow_worker_finished.load(Ordering::SeqCst));
    }

    #[test]
    fn scheduler_uses_fixed_stream_worker_count() {
        let items = (0..16)
            .map(|index| work_item_with_fh(FUSE_WRITE, index + 1, index + 1, 10))
            .collect::<Vec<_>>();
        let seen_threads = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        let seen_threads_for_handler = Arc::clone(&seen_threads);

        run_scheduled_work_for_test(items, move |_item| {
            seen_threads_for_handler
                .lock()
                .expect("seen threads")
                .insert(std::thread::current().id());
        })
        .expect("scheduled work");

        assert!(seen_threads.lock().expect("seen threads").len() <= 8);
    }

    #[test]
    fn scheduler_keeps_metadata_from_blocking_stream_work() {
        let metadata_started = Arc::new(std::sync::Barrier::new(2));
        let release_metadata = Arc::new(std::sync::Barrier::new(2));
        let metadata_started_for_handler = Arc::clone(&metadata_started);
        let release_metadata_for_handler = Arc::clone(&release_metadata);
        let items = vec![
            work_item_for_test(FUSE_LOOKUP, 1),
            work_item_with_fh(FUSE_WRITE, 2, 7, 10),
        ];

        let (seen_rx, worker) = spawn_scheduled_work_for_test(items, move |item| {
            if item.header.unique == 1 {
                metadata_started_for_handler.wait();
                release_metadata_for_handler.wait();
            }
        });

        metadata_started.wait();
        let observed = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("stream work should not wait for metadata");
        release_metadata.wait();
        worker
            .join()
            .expect("scheduler thread")
            .expect("scheduled work");

        assert_eq!(observed, 2);
    }

    #[test]
    fn spawn_loop_assigns_thread_name() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let (name_tx, name_rx) = std::sync::mpsc::channel();
        let handle = spawn_loop("studiofs-test-loop", done_tx, move || {
            let name = std::thread::current().name().unwrap_or("").to_owned();
            name_tx.send(name).expect("send thread name");
            Ok(())
        })
        .expect("spawn loop");

        join_loop(handle, "studiofs-test-loop").expect("join");
        done_rx.recv().expect("loop result").expect("loop ok");

        assert_eq!(name_rx.recv().expect("thread name"), "studiofs-test-loop");
    }

    fn run_scheduled_work_for_test(
        items: Vec<FuseWorkItem>,
        handler: impl Fn(&FuseWorkItem) + Send + Sync + 'static,
    ) -> io::Result<Vec<u64>> {
        let (seen_rx, worker) = spawn_scheduled_work_for_test(items, handler);
        worker.join().expect("scheduler thread")?;
        Ok(seen_rx.try_iter().collect())
    }

    fn spawn_scheduled_work_for_test(
        items: Vec<FuseWorkItem>,
        handler: impl Fn(&FuseWorkItem) + Send + Sync + 'static,
    ) -> (
        std::sync::mpsc::Receiver<u64>,
        std::thread::JoinHandle<io::Result<()>>,
    ) {
        let (work_tx, work_rx) = work_channel(items.len().max(1));
        let (reply_tx, _reply_rx) = work_channel(items.len().max(1));
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        let metrics = Arc::new(LinuxFuseLoopMetrics::default());
        for item in items {
            enqueue_work(&work_tx, item, &metrics).expect("enqueue work");
        }
        drop(work_tx);
        let worker = std::thread::spawn(move || {
            scheduled_work_loop(work_rx, reply_tx, metrics, move |item| {
                handler(item);
                seen_tx.send(item.header.unique).expect("record seen");
                None
            })
        });
        (seen_rx, worker)
    }

    fn fuse_request_for_test(opcode: u32, unique: u64) -> [u8; FUSE_IN_HEADER_SIZE] {
        let mut request = [0_u8; FUSE_IN_HEADER_SIZE];
        request[0..4].copy_from_slice(&(FUSE_IN_HEADER_SIZE as u32).to_ne_bytes());
        request[4..8].copy_from_slice(&opcode.to_ne_bytes());
        request[8..16].copy_from_slice(&unique.to_ne_bytes());
        request
    }

    fn work_item_for_test(opcode: u32, unique: u64) -> FuseWorkItem {
        FuseWorkItem {
            header: FuseInHeader {
                opcode,
                unique,
                nodeid: 1,
            },
            message: Vec::from(fuse_request_for_test(opcode, unique)),
        }
    }

    fn work_item_with_fh(opcode: u32, unique: u64, fh: u64, nodeid: u64) -> FuseWorkItem {
        let mut item = work_item_for_test(opcode, unique);
        item.header.nodeid = nodeid;
        item.message.extend_from_slice(&fh.to_ne_bytes());
        item
    }

    fn fuse_request_with_payload_for_test(opcode: u32, unique: u64, payload: &[u8]) -> Vec<u8> {
        let mut request = Vec::from(fuse_request_for_test(opcode, unique));
        let len = request.len() + payload.len();
        request[0..4].copy_from_slice(&(len as u32).to_ne_bytes());
        request.extend_from_slice(payload);
        request
    }

    fn make_test_vfs(tmp: &tempfile::TempDir, volume_suffix: &str) -> (Runtime, Arc<StudioVfs>) {
        let runtime = Runtime::new().expect("runtime");
        let _guard = runtime.enter();
        let meta = Arc::new(RedbMetaEngine::open(tmp.path().join("meta.redb")).expect("open redb"));
        meta.init_root(0, 0).expect("init root");
        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);
        let cache_dir = tmp.path().join("cache");
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir utf8"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        vfs.start().expect("start vfs");
        (runtime, vfs)
    }

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("linux-vfs-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("linux-vfs-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            encryption: studiofs_meta::EncryptionConfig::new(
                [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
            ),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn work_item_with_payload(
        opcode: u32,
        unique: u64,
        nodeid: u64,
        payload: &[u8],
    ) -> FuseWorkItem {
        let message = fuse_request_with_payload_for_test(opcode, unique, payload);
        FuseWorkItem {
            header: FuseInHeader {
                opcode,
                unique,
                nodeid,
            },
            message,
        }
    }

    fn write_in(fh: u64, offset: u64, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0_u8; FUSE_WRITE_IN_SIZE];
        bytes[0..8].copy_from_slice(&fh.to_ne_bytes());
        bytes[8..16].copy_from_slice(&offset.to_ne_bytes());
        bytes[16..20].copy_from_slice(&(payload.len() as u32).to_ne_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn read_in(fh: u64, offset: u64, size: u32) -> Vec<u8> {
        let mut bytes = vec![0_u8; FUSE_READ_IN_SIZE];
        bytes[0..8].copy_from_slice(&fh.to_ne_bytes());
        bytes[8..16].copy_from_slice(&offset.to_ne_bytes());
        bytes[16..20].copy_from_slice(&size.to_ne_bytes());
        bytes
    }

    fn flush_in(fh: u64) -> Vec<u8> {
        let mut bytes = vec![0_u8; FUSE_FLUSH_IN_SIZE];
        bytes[0..8].copy_from_slice(&fh.to_ne_bytes());
        bytes
    }

    fn release_in(fh: u64) -> Vec<u8> {
        let mut bytes = vec![0_u8; FUSE_RELEASE_IN_SIZE];
        bytes[0..8].copy_from_slice(&fh.to_ne_bytes());
        bytes
    }

    fn fsync_in(fh: u64, flags: u32) -> Vec<u8> {
        let mut bytes = vec![0_u8; FUSE_FSYNC_IN_SIZE];
        bytes[0..8].copy_from_slice(&fh.to_ne_bytes());
        bytes[8..12].copy_from_slice(&flags.to_ne_bytes());
        bytes
    }

    fn assert_ok_reply(reply: &[u8], unique: u64) {
        assert_eq!(read_u32(reply, 0).unwrap() as usize, reply.len());
        assert_eq!(read_u64(reply, 8).unwrap(), unique);
        assert_eq!(
            i32::from_ne_bytes(reply[4..8].try_into().expect("reply error")),
            0
        );
    }
}
