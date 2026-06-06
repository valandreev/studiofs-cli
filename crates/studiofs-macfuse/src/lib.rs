#![doc = "macOS native bridge boundary for StudioFS."]
#![cfg(target_os = "macos")]
#![cfg_attr(target_os = "macos", deny(missing_docs))]

//! macOS native bridge boundary for StudioFS.
//!
//! This crate intentionally exposes only the mount request and serve API while
//! the low-level macFUSE session layer is still being built.

use std::io;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use studiofs_vfs::StudioVfs;
use tokio::runtime::Handle;

/// Low-level macFUSE repeated-read probe support.
pub mod probe;

/// Isolated low-level macFUSE session and reply boundary.
pub mod low_level;

mod adapter;

/// Request used by the CLI backend router to launch the macOS bridge.
pub struct MacFuseMountRequest {
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    mount_point: PathBuf,
    volume_name: String,
    read_only: bool,
    run_in_foreground: bool,
    signal_handling: SignalHandling,
    unmount_signal: Option<MacFuseUnmountSignal>,
}

impl MacFuseMountRequest {
    /// Create a macFUSE mount request with embeddable defaults.
    pub fn new(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        mount_point: impl Into<PathBuf>,
        volume_name: impl Into<String>,
    ) -> Self {
        Self {
            vfs,
            runtime_handle,
            mount_point: mount_point.into(),
            volume_name: volume_name.into(),
            read_only: false,
            run_in_foreground: false,
            signal_handling: SignalHandling::default(),
            unmount_signal: None,
        }
    }

    /// Set whether the bridge should reject write operations.
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set whether the mount session should stay attached to the CLI process.
    pub fn with_run_in_foreground(mut self, run_in_foreground: bool) -> Self {
        self.run_in_foreground = run_in_foreground;
        self
    }

    /// Set process signal ownership policy.
    pub fn with_signal_handling(mut self, signal_handling: SignalHandling) -> Self {
        self.signal_handling = signal_handling;
        self
    }
}

/// Controls whether StudioFS owns process-global macFUSE shutdown signals.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SignalHandling {
    /// Do not install SIGINT/SIGTERM handlers.
    #[default]
    Disabled,
    /// Install temporary SIGINT/SIGTERM handlers for the StudioFS CLI.
    InstallForCli,
}

/// Caller-owned handle for requesting graceful macFUSE unmount.
#[derive(Clone, Debug, Default)]
pub struct MacFuseUnmountSignal {
    requested: Arc<AtomicBool>,
}

impl MacFuseUnmountSignal {
    /// Create a new unmount signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request the mount session to unmount.
    pub fn request_unmount(&self) {
        self.requested.store(true, Ordering::Release);
    }

    /// Return whether unmount has been requested.
    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
static CLI_SIGNAL_HANDLERS_INSTALLED: AtomicBool = AtomicBool::new(false);

extern "C" fn shutdown_signal_handler(_sig: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Release);
}

struct CliSignalHandlers {
    previous_sigint: libc::sighandler_t,
    previous_sigterm: libc::sighandler_t,
}

impl CliSignalHandlers {
    fn install() -> io::Result<Self> {
        CLI_SIGNAL_HANDLERS_INSTALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "macFUSE CLI signal handlers are already installed",
                )
            })?;
        // SAFETY: this explicitly mutates process-global CLI signal disposition
        // and restores the previous handlers in Drop.
        unsafe {
            let previous_sigint = libc::signal(
                libc::SIGINT,
                shutdown_signal_handler as *const () as libc::sighandler_t,
            );
            if previous_sigint == libc::SIG_ERR {
                CLI_SIGNAL_HANDLERS_INSTALLED.store(false, Ordering::Release);
                return Err(io::Error::last_os_error());
            }
            let previous_sigterm = libc::signal(
                libc::SIGTERM,
                shutdown_signal_handler as *const () as libc::sighandler_t,
            );
            if previous_sigterm == libc::SIG_ERR {
                let _ = libc::signal(libc::SIGINT, previous_sigint);
                CLI_SIGNAL_HANDLERS_INSTALLED.store(false, Ordering::Release);
                return Err(io::Error::last_os_error());
            }
            Ok(Self {
                previous_sigint,
                previous_sigterm,
            })
        }
    }
}

impl Drop for CliSignalHandlers {
    fn drop(&mut self) {
        // SAFETY: these values came from successful `libc::signal` calls.
        unsafe {
            libc::signal(libc::SIGINT, self.previous_sigint);
            libc::signal(libc::SIGTERM, self.previous_sigterm);
        }
        CLI_SIGNAL_HANDLERS_INSTALLED.store(false, Ordering::Release);
    }
}

fn signal_handlers_for_policy(
    signal_handling: SignalHandling,
) -> io::Result<Option<CliSignalHandlers>> {
    match signal_handling {
        SignalHandling::Disabled => Ok(None),
        SignalHandling::InstallForCli => CliSignalHandlers::install().map(Some),
    }
}

struct SignalWatcher {
    stop: Arc<AtomicBool>,
    handlers: Arc<Mutex<Option<CliSignalHandlers>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for SignalWatcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Ok(mut handlers) = self.handlers.lock() {
            let _ = handlers.take();
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn spawn_signal_watcher(
    exit_handle: low_level::SessionExitHandle,
    signal_handling: SignalHandling,
    unmount_signal: Option<MacFuseUnmountSignal>,
) -> io::Result<SignalWatcher> {
    let stop = Arc::new(AtomicBool::new(false));
    let handlers = Arc::new(Mutex::new(signal_handlers_for_policy(signal_handling)?));
    let watcher_stop = Arc::clone(&stop);
    let watcher_handlers = Arc::clone(&handlers);
    let thread = std::thread::Builder::new()
        .name("macfuse-signal-watcher".into())
        .spawn(move || {
            while !watcher_stop.load(Ordering::Acquire)
                && !SHUTDOWN_REQUESTED.load(Ordering::Acquire)
                && !unmount_signal
                    .as_ref()
                    .is_some_and(MacFuseUnmountSignal::is_requested)
            {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            if watcher_stop.load(Ordering::Acquire) {
                return;
            }
            if SHUTDOWN_REQUESTED.load(Ordering::Acquire)
                && let Ok(mut handlers) = watcher_handlers.lock()
            {
                let _ = handlers.take();
            }
            exit_handle.request_exit();
        })?;
    Ok(SignalWatcher {
        stop,
        handlers,
        thread: Some(thread),
    })
}

/// Start serving the macOS native bridge.
///
/// Real macFUSE callbacks are tracked by the follow-up macOS bridge issues.
pub fn mount_with_request(request: MacFuseMountRequest) -> io::Result<()> {
    let MacFuseMountRequest {
        vfs,
        runtime_handle,
        mount_point,
        volume_name,
        read_only,
        run_in_foreground,
        signal_handling,
        unmount_signal,
    } = request;
    let config = low_level::SessionConfig::new(mount_point, volume_name)
        .with_read_only(read_only)
        .with_run_in_foreground(run_in_foreground);
    let mut callback_state = Box::new(adapter::CallbackState::new(
        Arc::clone(&vfs),
        runtime_handle,
        read_only,
    ));
    let userdata =
        (&mut *callback_state) as *mut adapter::CallbackState as *mut std::os::raw::c_void;
    // SAFETY: `callback_state` remains boxed and live until after the mounted
    // session is dropped. Both callback tables handle this exact userdata type.
    let mut session = unsafe {
        low_level::Session::mount_with_ops(
            &config,
            studio_vfs_ops_for_read_only(read_only),
            userdata,
        )
    }?;
    SHUTDOWN_REQUESTED.store(false, Ordering::Release);
    let _signal_watcher =
        spawn_signal_watcher(session.exit_handle(), signal_handling, unmount_signal)?;
    vfs.set_kernel_invalidator(session.kernel_invalidator());
    let result = session.run();
    vfs.clear_kernel_invalidator();
    drop(session);
    drop(callback_state);
    result
}

fn studio_vfs_ops_for_read_only(read_only: bool) -> low_level::LowLevelOperations {
    if read_only {
        low_level::LowLevelOperations::read_only_studio_vfs()
    } else {
        low_level::LowLevelOperations::studio_vfs()
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    use studiofs_meta::MetaFsOps;

    #[test]
    fn callback_file_info_snapshot_is_independent_from_callback_pointer() {
        let mut raw = super::low_level::raw::FuseFileInfo::for_test(0x20, 7, true, false);

        let snapshot = super::low_level::FileInfoSnapshot::copy_from_callback(&raw);
        raw.fh = 99;
        raw.flags = 0x40;
        raw.set_cache_flags_for_test(false, true);

        assert_eq!(snapshot.file_handle(), 7);
        assert_eq!(snapshot.flags(), 0x20);
        assert!(snapshot.direct_io());
        assert!(!snapshot.keep_cache());
    }

    #[test]
    fn directory_entries_keep_names_and_offsets_in_owned_buffer() {
        let mut buffer = super::low_level::DirectoryEntryBuffer::new(512);

        buffer
            .push_for_test(2, OsStr::new("alpha"), 1)
            .expect("first entry fits");
        buffer
            .push_for_test(3, OsStr::from_bytes(b"beta.bin"), 2)
            .expect("second entry fits");

        assert_eq!(buffer.len(), buffer.as_slice().len());
        assert!(
            buffer
                .as_slice()
                .windows(b"alpha".len())
                .any(|w| w == b"alpha")
        );
        assert!(
            buffer
                .as_slice()
                .windows(b"beta.bin".len())
                .any(|w| w == b"beta.bin")
        );
    }

    #[test]
    fn session_config_builds_low_level_mount_arguments_without_kernel_read_only_option() {
        let config = super::low_level::SessionConfig::new("/Volumes/Studio FS", "StudioFS Native")
            .with_read_only(true)
            .with_run_in_foreground(true);

        let args = config.argv_strings();
        let mount_args = config.mount_argv_strings();

        assert_eq!(
            config.mount_point().as_os_str(),
            OsStr::new("/Volumes/Studio FS")
        );
        assert_eq!(config.volume_name(), "StudioFS Native");
        assert!(config.run_in_foreground());
        assert!(args.iter().any(|arg| arg == "studiofs-macfuse"));
        assert!(
            mount_args
                .iter()
                .any(|arg| arg == "-ovolname=StudioFS Native")
        );
        assert!(mount_args.iter().any(|arg| arg == "-olocal"));
        assert!(mount_args.iter().any(|arg| arg == "-onovncache"));
        assert!(mount_args.iter().any(|arg| arg == "-odaemon_timeout=60"));
        let expected_geometry = super::low_level::MACOS_FUSE_IOSIZE_BYTES;
        assert!(
            mount_args
                .iter()
                .any(|arg| arg == &format!("-oiosize={expected_geometry}"))
        );
        assert!(
            mount_args
                .iter()
                .any(|arg| arg == &format!("-omax_read={expected_geometry}"))
        );
        assert!(mount_args.iter().any(|arg| arg == "-onoappledouble"));
        assert!(mount_args.iter().any(|arg| arg == "-odefer_permissions"));
        assert!(mount_args.iter().any(|arg| arg == "-osparse"));
        assert!(!mount_args.iter().any(|arg| arg == "-odirect_io"));
        assert!(!mount_args.iter().any(|arg| arg == "-onolocalcaches"));
        assert!(!args.iter().any(|arg| arg == "-f"));
        assert!(!args.iter().any(|arg| arg == "-oro"));
        assert!(!args.iter().any(|arg| arg == "ro"));
        assert!(!args.iter().any(|arg| arg.contains("StudioFS Native")));
    }

    #[test]
    fn session_config_builds_direct_cache_policy_mount_arguments_for_rollback_runs() {
        let config = super::low_level::SessionConfig::new("/Volumes/Studio FS", "StudioFS Native");

        let mount_args = config
            .mount_argv_strings_with_cache_policy(super::low_level::MacosFuseCachePolicy::Direct);

        assert!(mount_args.iter().any(|arg| arg == "-odirect_io"));
        assert!(mount_args.iter().any(|arg| arg == "-onolocalcaches"));
        assert!(mount_args.iter().any(|arg| arg == "-oauto_cache"));
        assert!(mount_args.iter().any(|arg| arg == "-osparse"));
        assert!(!mount_args.iter().any(|arg| arg == "-onovncache"));
        assert!(!mount_args.iter().any(|arg| arg == "-oiosize=1048576"));
        assert!(!mount_args.iter().any(|arg| arg == "-omax_read=1048576"));
        assert!(!mount_args.iter().any(|arg| arg == "-onoappledouble"));
        assert!(!mount_args.iter().any(|arg| arg == "-odefer_permissions"));
    }

    #[test]
    fn empty_low_level_ops_come_from_c_header_layout() {
        let ops = super::low_level::LowLevelOperations::empty();

        assert!(ops.size() > 0);
        assert!(!ops.as_ptr().is_null());
    }

    #[test]
    fn studio_vfs_low_level_ops_come_from_c_header_layout() {
        let ops = super::low_level::LowLevelOperations::studio_vfs();

        assert!(ops.size() > 0);
        assert!(!ops.as_ptr().is_null());
    }

    #[test]
    fn read_only_low_level_ops_come_from_c_header_layout() {
        let ops = super::low_level::LowLevelOperations::read_only_studio_vfs();

        assert!(ops.size() > 0);
        assert!(!ops.as_ptr().is_null());
    }

    #[test]
    fn studio_vfs_low_level_ops_include_native_xattr_callbacks() {
        assert!(super::low_level::LowLevelOperations::studio_vfs().has_xattr_callbacks());
        assert!(super::low_level::LowLevelOperations::read_only_studio_vfs().has_xattr_callbacks());
    }

    #[test]
    fn studio_vfs_low_level_ops_install_connection_init_callback() {
        assert!(super::low_level::LowLevelOperations::studio_vfs().has_init_callback());
        assert!(super::low_level::LowLevelOperations::read_only_studio_vfs().has_init_callback());
    }

    #[test]
    fn writable_studio_vfs_low_level_ops_install_rmdir_callback() {
        assert!(super::low_level::LowLevelOperations::studio_vfs().has_rmdir_callback());
        assert!(!super::low_level::LowLevelOperations::read_only_studio_vfs().has_rmdir_callback());
    }

    #[test]
    fn read_only_mount_uses_restricted_low_level_ops() {
        let read_only_ops = super::studio_vfs_ops_for_read_only(true);
        let writable_ops = super::studio_vfs_ops_for_read_only(false);

        assert_eq!(
            read_only_ops.as_ptr(),
            super::low_level::LowLevelOperations::read_only_studio_vfs().as_ptr()
        );
        assert_eq!(
            writable_ops.as_ptr(),
            super::low_level::LowLevelOperations::studio_vfs().as_ptr()
        );
        assert_ne!(read_only_ops.as_ptr(), writable_ops.as_ptr());
    }

    #[test]
    fn low_level_session_can_be_signaled_across_threads() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<super::low_level::Session>();
    }

    #[test]
    fn low_level_request_can_move_to_worker_thread() {
        fn assert_send<T: Send>() {}

        assert_send::<super::low_level::Request>();
    }

    #[test]
    fn directory_entry_buffer_storage_is_eight_byte_aligned() {
        let buffer = super::low_level::DirectoryEntryBuffer::new(512);

        assert_eq!(buffer.as_slice().as_ptr().align_offset(8), 0);
    }

    #[test]
    fn low_level_session_mount_unmount_smoke_is_gated() {
        if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
            return;
        }

        let mount_point = std::path::Path::new("target/studiofs-macfuse-mount-smoke");
        let _ = std::fs::remove_dir(mount_point);
        std::fs::create_dir_all(mount_point).expect("create macFUSE smoke mount point");
        let config = super::low_level::SessionConfig::new(mount_point, "StudioFS Smoke")
            .with_run_in_foreground(true);

        // SAFETY: The empty operation table is a static header-derived table
        // used only for a mount/unmount lifecycle smoke; no callbacks are run.
        let session = unsafe {
            super::low_level::Session::mount_with_ops(
                &config,
                super::low_level::LowLevelOperations::empty(),
                std::ptr::null_mut(),
            )
        }
        .expect("mount empty low-level macFUSE session");
        session.request_exit();
        drop(session);
        std::fs::remove_dir(mount_point).expect("remove macFUSE smoke mount point");
    }

    #[test]
    fn read_only_adapter_reads_existing_vfs_payload_and_records_diagnostics() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-read-only-adapter-read",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"hello.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"hello from macFUSE"))
            .expect("seed payload");
        vfs.reset_diagnostics_observability();

        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let opened = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only");
        let bytes = adapter
            .read(attr.ino, opened.file_handle(), 6, 4)
            .expect("read payload through adapter");
        adapter
            .release(attr.ino, opened.file_handle())
            .expect("release read handle");

        assert_eq!(&bytes[..], b"from");
        let metrics = vfs.write_path_observability_snapshot();
        assert_eq!(metrics.fuse_callbacks.read.calls, 1);
        assert_eq!(metrics.fuse_callbacks.read.bytes, 4);
        assert_eq!(metrics.fuse_callbacks.read.errors, 0);
        assert_eq!(metrics.vfs_read_path.calls, 1);
    }

    #[test]
    fn read_only_adapter_read_lets_vfs_handle_eof_without_pre_clamp_getattr() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-read-eof-no-preclamp")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"short.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"short"))
            .expect("seed payload");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let opened = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only");

        let bytes = adapter
            .read(attr.ino, opened.file_handle(), 1024, 4096)
            .expect("read past EOF through adapter");

        assert!(bytes.is_empty());
    }

    #[test]
    fn read_only_adapter_queues_read_without_waiting_for_vfs_completion() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-queues")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"queued.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"queued macFUSE read"))
            .expect("seed payload");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open writer to hold read gate");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");

        let queued = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 6)
            .expect("queue read without waiting for VFS completion");

        assert!(
            !queued.is_finished(),
            "queued read should still be pending while the writer gate is held"
        );

        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writer gate");
        let bytes = queued.wait().expect("queued read should complete");

        assert_eq!(&bytes[..], b"queued");
    }

    #[test]
    fn read_only_adapter_delays_read_when_async_queue_is_full() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-async-read-full")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"full.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        fixture
            .runtime()
            .block_on(vfs.write(attr.ino, 0, b"bounded macFUSE read"))
            .expect("seed payload");
        let adapter = super::adapter::StudioVfsAdapter::new_with_read_queue_limit_for_test(
            vfs.clone(),
            fixture.runtime().handle().clone(),
            1,
        );
        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open writer to hold read gate");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");
        let pending = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 0, 7)
            .expect("first read should enter the bounded queue");

        let delayed = adapter
            .queue_read_for_test(attr.ino, reader.file_handle(), 7, 7)
            .expect("second read should wait for bounded queue capacity");

        assert!(
            !delayed.is_finished(),
            "second read should wait instead of returning EAGAIN while queue is full"
        );

        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writer gate");
        let bytes = pending.wait().expect("pending read should complete");
        let delayed_bytes = delayed.wait().expect("delayed read should complete");

        assert_eq!(&bytes[..], b"bounded");
        assert_eq!(&delayed_bytes[..], b" macFUS");
    }

    #[test]
    fn read_only_adapter_rejects_negative_read_offset_before_queueing() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-async-read-negative-offset",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let rejected = adapter
            .queue_read_for_test(1, 1, -1, 1)
            .expect_err("negative offsets should fail before queueing");

        assert_eq!(rejected, libc::EINVAL);
    }

    #[test]
    fn adapter_write_keeps_accepted_bytes_visible_to_same_mount_reader() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-write-read-after-write",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"hello.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open writer through macFUSE adapter");
        let accepted = adapter
            .write(attr.ino, writer.file_handle(), 0, b"hello from macFUSE")
            .expect("write payload through macFUSE adapter");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader through macFUSE adapter");
        let bytes = adapter
            .read(attr.ino, reader.file_handle(), 0, 18)
            .expect("read accepted local bytes through same adapter");

        assert_eq!(accepted, 18);
        assert_eq!(&bytes[..], b"hello from macFUSE");
    }

    #[test]
    fn adapter_flush_fsync_release_and_truncate_preserve_local_first_bytes() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-write-flush-fsync-release-truncate",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"hello.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let writer = adapter
            .open(attr.ino, libc::O_RDWR)
            .expect("open writable handle through macFUSE adapter");

        adapter
            .write(attr.ino, writer.file_handle(), 0, b"hello from macFUSE")
            .expect("write payload through macFUSE adapter");
        adapter
            .flush(attr.ino, writer.file_handle())
            .expect("flush writable handle through macFUSE adapter");
        adapter
            .fsync(Some(writer.file_handle()), attr.ino, false)
            .expect("fsync inode through macFUSE adapter");
        let truncated = adapter
            .set_len(Some(writer.file_handle()), attr.ino, 5)
            .expect("truncate through macFUSE adapter");
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writable handle through macFUSE adapter");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("reopen reader through macFUSE adapter");
        let bytes = adapter
            .read(attr.ino, reader.file_handle(), 0, 32)
            .expect("read truncated accepted local bytes");

        assert_eq!(truncated.size, 5);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn adapter_handle_backed_fsync_survives_unlink_of_open_file() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-open-unlink-fsync",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"open-unlink.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let writer = adapter
            .open(attr.ino, libc::O_RDWR)
            .expect("open writable handle through macFUSE adapter");

        let initial = b"open handle payload";
        adapter
            .write(attr.ino, writer.file_handle(), 0, initial)
            .expect("write initial payload through macFUSE adapter");
        adapter
            .fsync(Some(writer.file_handle()), attr.ino, false)
            .expect("seed fsync should persist initial payload");
        fixture
            .meta()
            .unlink(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"open-unlink.txt",
            )
            .expect("unlink file while handle is open");

        adapter
            .write(
                attr.ino,
                writer.file_handle(),
                initial.len() as i64,
                b" after unlink",
            )
            .expect("write through open handle after foreign unlink");

        adapter
            .fsync(Some(writer.file_handle()), attr.ino, false)
            .expect("handle-backed fsync should not depend on removed namespace entry");
    }

    #[test]
    fn adapter_rmdir_removes_empty_directory_and_respects_read_only() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-adapter-rmdir")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let read_only_adapter = super::adapter::StudioVfsAdapter::new_with_read_only(
            vfs.clone(),
            fixture.runtime().handle().clone(),
            true,
        );

        adapter
            .mkdir(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"empty-dir",
                0o755,
            )
            .expect("create directory through macFUSE adapter");
        assert_eq!(
            read_only_adapter
                .rmdir(
                    studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                    b"empty-dir",
                )
                .unwrap_err(),
            libc::EROFS
        );
        adapter
            .rmdir(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"empty-dir",
            )
            .expect("remove empty directory through macFUSE adapter");

        assert!(
            vfs.lookup(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"empty-dir",
            )
            .is_err()
        );
    }

    #[test]
    fn read_only_adapter_still_rejects_write_or_truncate_open_flags() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-read-only-adapter-open-flags",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"hello.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter = super::adapter::StudioVfsAdapter::new_with_read_only(
            vfs,
            fixture.runtime().handle().clone(),
            true,
        );

        assert_eq!(
            adapter.open(attr.ino, libc::O_WRONLY).unwrap_err(),
            libc::EROFS
        );
        assert_eq!(
            adapter
                .open(attr.ino, libc::O_RDONLY | libc::O_TRUNC)
                .unwrap_err(),
            libc::EROFS
        );
    }

    #[test]
    fn cache_policy_keeps_kernel_cache_only_for_read_only_handles() {
        let read_only = super::adapter::macfuse_open_cache_policy(libc::O_RDONLY, false);
        assert!(!read_only.direct_io);
        assert!(read_only.keep_cache);

        let write_only = super::adapter::macfuse_open_cache_policy(libc::O_WRONLY, false);
        assert!(!write_only.direct_io);
        assert!(!write_only.keep_cache);

        let read_write = super::adapter::macfuse_open_cache_policy(libc::O_RDWR, false);
        assert!(!read_write.direct_io);
        assert!(!read_write.keep_cache);

        let append =
            super::adapter::macfuse_open_cache_policy(libc::O_RDONLY | libc::O_APPEND, false);
        assert!(!append.direct_io);
        assert!(!append.keep_cache);

        let truncate = super::adapter::macfuse_open_cache_policy(libc::O_RDONLY, true);
        assert!(!truncate.direct_io);
        assert!(!truncate.keep_cache);
    }

    #[test]
    fn adapter_roundtrips_native_resource_fork_xattr_bytes() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-adapter-resource-fork-xattr",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"forked.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let fork_name = b"com.apple.ResourceFork";
        let fork_bytes = b"native resource fork bytes";

        adapter
            .set_xattr(attr.ino, fork_name, fork_bytes, 0)
            .expect("set resource fork through macFUSE adapter");

        assert_eq!(
            adapter
                .get_xattr(attr.ino, fork_name)
                .expect("get resource fork through macFUSE adapter"),
            Some(fork_bytes.to_vec())
        );
        assert_eq!(
            adapter
                .list_xattrs(attr.ino)
                .expect("list xattrs through macFUSE adapter"),
            vec![fork_name.to_vec()]
        );
        adapter
            .remove_xattr(attr.ino, fork_name)
            .expect("remove resource fork through macFUSE adapter");
        assert_eq!(
            adapter
                .get_xattr(attr.ino, fork_name)
                .expect("get removed resource fork through macFUSE adapter"),
            None
        );
    }

    #[test]
    fn adapter_reports_native_resource_fork_xattr_decoded_size() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-adapter-xattr-size")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"fork-size.txt",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let fork_name = b"com.apple.ResourceFork";
        let fork_bytes = vec![0x66; 64 * 1024];

        adapter
            .set_xattr(attr.ino, fork_name, &fork_bytes, 0)
            .expect("set resource fork through macFUSE adapter");

        assert_eq!(
            adapter
                .get_xattr_size(attr.ino, fork_name)
                .expect("get resource fork size through macFUSE adapter"),
            Some(fork_bytes.len())
        );
    }

    #[test]
    fn read_only_adapter_readdir_stops_successfully_when_reply_buffer_is_full() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-readdir-buffer-full")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        vfs.create(root, b"alpha.txt", 501, 20, 0o644)
            .expect("create first file");
        vfs.create(root, b"beta.txt", 501, 20, 0o644)
            .expect("create second file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        let buffer = adapter
            .readdir_for_test(root, 2, 32)
            .expect("full readdir buffer should return a partial success");

        assert!(!buffer.is_empty());
    }

    #[test]
    fn read_only_adapter_readdir_reuses_cached_entries_for_chunked_offsets() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-readdir-entry-cache")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        vfs.create(root, b"alpha.txt", 501, 20, 0o644)
            .expect("create first file");
        vfs.create(root, b"beta.txt", 501, 20, 0o644)
            .expect("create second file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());

        adapter
            .readdir_for_test(root, 2, 32)
            .expect("first readdir chunk");
        adapter
            .readdir_for_test(root, 3, 32)
            .expect("second readdir chunk");

        assert_eq!(adapter.readdir_cache_entry_count_for_test(), 1);
    }

    #[test]
    fn read_only_adapter_readdir_prunes_expired_cache_entries_on_refresh() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("macfuse-readdir-cache-prune")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        vfs.create(root, b"alpha.txt", 501, 20, 0o644)
            .expect("create first file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        adapter.fill_expired_readdir_cache_entries_for_test();

        adapter
            .readdir_for_test(root, 2, 32)
            .expect("refresh readdir cache");

        assert_eq!(adapter.readdir_cache_entry_count_for_test(), 1);
    }

    #[test]
    fn read_only_adapter_readdir_rate_limits_fresh_cache_pruning() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-readdir-cache-prune-rate-limit",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        let first_dir = vfs
            .mkdir(root, b"first", 501, 20, 0o755)
            .expect("create first directory");
        let second_dir = vfs
            .mkdir(root, b"second", 501, 20, 0o755)
            .expect("create second directory");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        adapter.fill_fresh_readdir_cache_entries_for_test();

        adapter
            .readdir_for_test(first_dir.ino, 2, 32)
            .expect("populate first readdir cache");
        let first_miss_count = adapter.readdir_cache_entry_count_for_test();
        adapter
            .readdir_for_test(second_dir.ino, 2, 32)
            .expect("populate second missing readdir cache");

        assert_eq!(first_miss_count, 1025);
        assert_eq!(adapter.readdir_cache_entry_count_for_test(), 1026);
    }

    #[test]
    fn read_only_adapter_readdir_refresh_records_fresh_cache_timestamp() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-readdir-cache-fresh-timestamp",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let root = studiofs_vfs::contract_harness::CONTRACT_ROOT_INO;
        vfs.create(root, b"alpha.txt", 501, 20, 0o644)
            .expect("create first file");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        adapter
            .readdir_for_test(root, 2, 32)
            .expect("populate readdir cache");
        let expired = std::time::Instant::now() - std::time::Duration::from_secs(30);
        adapter.set_ready_readdir_cache_loaded_at_for_test(root, expired);

        adapter
            .readdir_for_test(root, 2, 32)
            .expect("refresh expired readdir cache");

        let refreshed = adapter
            .readdir_cache_loaded_at_for_test(root)
            .expect("ready readdir cache entry should exist");
        assert!(refreshed > expired);
    }

    #[test]
    fn read_only_adapter_reads_symlink_targets() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "macfuse-read-only-adapter-readlink",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .symlink(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                b"link.txt",
                b"target.txt",
                501,
                20,
            )
            .expect("create symlink");
        let adapter =
            super::adapter::StudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());

        let target = adapter.readlink(attr.ino).expect("read symlink target");

        assert_eq!(target, b"target.txt");
    }
}
